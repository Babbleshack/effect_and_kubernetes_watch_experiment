import { Effect, Stream, Fiber, Exit, pipe, StreamEmit, Chunk, Sink, Console, Match, Schedule, Option, Context, Queue, Cause } from "effect";
import { NodeRuntime } from "@effect/platform-node"
import { KubeConfig, Watch, V1Pod } from '@kubernetes/client-node';
import {some} from "effect/Option";

type KubernetesEventType = 'ADDED' | 'MODIFIED' | 'DELETED' | 'BOOKMARK';
const isKuberneteEventType = (type: string) => {
  return type === 'ADDED' || type === 'MODIFIED' || type === 'DELETED' || type === 'BOOKMARK'
}

class InvalidKubernetesEventTypeError {
  readonly _tag = "InvalidKubernetesEventTypeError"
}

interface KubernetesPodEvent {
  type: KubernetesEventType
  apiObj: V1Pod;
}

interface Backend {
  name: string,
  ip: string
}

const kc = new KubeConfig();
kc.loadFromDefault();

const watch = new Watch(kc)

/**
 * Service managing Backend Pod Cache
 */
class BackendPodCacheService extends Context.Tag("BackendPodCacheService") <
  BackendPodCacheService, 
  { 
    /**
     * Insert a new backend
     */
    insert: (backend: Backend) =>  Effect.Effect<void>,
    /**
     * Remove a backend
     */
    delete: (backend: Backend) => Effect.Effect<Option.Option<Backend>, Error>,
    /**
     * Remove a backend
     */
    listBackends: () => Effect.Effect<Array<Backend>>
  }
>() {}

/**
 * BackendPodCacheService service implementation for testing
 */
const createTestBackendPodCache = () => {
  //const pods = new Map()
  console.log("CREATING BackendPodCacheService")
  return {
    insert: (backend: Backend) => pipe(
      Effect.log(`INSERTING INTO BackendPodCacheService: ${JSON.stringify(backend)}`),
      Effect.flatMap(() =>
        Effect.sync(() => {
          console.log(`inserting new pod ${backend.name} with IP ${backend.ip}`);
        })
      )
    ),
    delete: (backend: Backend) => pipe(
      Effect.log(`DELETE FROM BackendPodCacheService`),
      Effect.flatMap(() =>
        Effect.sync(() => {
          return Option.none()
        })
                    )
    ).pipe(Effect.annotateLogs("Backend", JSON.stringify(backend))),
    listBackends: () => pipe(
      Effect.log(`Call listBackends`),
      Effect.flatMap(() => Effect.succeed([]))
    )
  }
}


const createKubernetesWatchEventStream = (namespace: string = 'default')  => 
  Stream.async(
    (emit: StreamEmit.Emit<never, never, KubernetesPodEvent, void>) => {
      const doneHandler = (err: Error) => {
        if (err) {
          Stream.fail(Error(`Stream was unexpectidly closed: ${err}`))
        } else {
          emit(Effect.fail(Option.none()))
        }
      }
      const eventHandler = (type: string, apiObj: any, _?: any) => {
        Effect.logInfo('received Kubernetes Event')
        if( isKuberneteEventType(type) === false) {
          return Effect.fail(new InvalidKubernetesEventTypeError())
        }
        const e: KubernetesPodEvent = {
          type: type as KubernetesEventType,
          apiObj
        }
        emit(Effect.succeed(Chunk.of(e)))
      }
      watch.watch(`/api/v1/namespaces/${namespace}/pods`, {}, eventHandler, doneHandler)
    }
  )

const podToBackend = (pod: Pick<V1Pod,  'metadata' | 'status'> ): Effect.Effect<Backend, Error>  => {
  const name = pod.metadata?.name
  const ip = pod.status?.podIP
  if (!name || !ip) {
    return Effect.fail(new Error("error invalid pod object"))
  }
  return Effect.succeed({name, ip})
}

interface BackendPodEvent {
  backend: Backend,
  type: KubernetesEventType
}


const pipelineWithDo = pipe(
  Effect.Do,
  Effect.bind('becs', () => BackendPodCacheService),
  Effect.let("podEventMatcher", ({ becs }) =>
    Match.type<BackendPodEvent>().pipe(
      Match.when({ type: 'ADDED' }, (_) => (backend: Backend) =>
                 pipe(
                   Effect.log("MATCHING INSERT"), // Log the event as an effect
                   Effect.flatMap(() => 
                                  becs.insert(backend)
                                 )
                 )
      ),
      Match.when({ type: 'MODIFIED' }, (_) => (backend: Backend) =>
                  Effect.log(`MATCHING MODIFIED ${backend}`),
      ),
      Match.when({ type: 'DELETED' }, (_) => (backend: Backend) =>
                 pipe(
                  Effect.log(`MATCHING DELETE ${backend}`),
                  Effect.flatMap(() => becs.delete(backend))
                 )
      ),
      Match.when({ type: 'BOOKMARK' }, (_) => (backend: Backend) =>
                 Effect.log(`BOOKMARK RECEIVED FOR ${backend}`)
      ),
      Match.exhaustive
    )
  ),
  Effect.bind("receiverPipeline", ({ podEventMatcher }) =>
    pipe(
      createKubernetesWatchEventStream(),
      Stream.tap((kpe) =>
                 Effect.log(`received KubernetesPodEvent: ${JSON.stringify(kpe)}`)
      ),
      Stream.mapEffect((kpe) =>
        pipe(
          podToBackend(kpe.apiObj),
          Effect.map((backend) => ({
            backend,
            type: kpe.type,
          }))
        )
      ),
      Stream.tap((bpe) =>
                 Effect.log(`received BackendPodEvent: ${JSON.stringify(bpe)}`)
      ),
      Stream.mapEffect((bpe) => {
        const operation = podEventMatcher(bpe); // Match event type
        return operation(bpe.backend);          // Apply the operation on backend
      }),
      Stream.runDrain,
      Effect.fork
    ),
  ),
)

const runnableWithDo = Effect.provideService(pipelineWithDo, BackendPodCacheService, createTestBackendPodCache())

pipe(
  runnableWithDo,
  Effect.flatMap(({ receiverPipeline }) =>
    pipe(
      receiverPipeline.await, // Await the fiber
      Effect.zipRight(Effect.never) // Keep the program alive indefinitely
    )
  ),
  Effect.runPromise
).then(() => {
  console.log("Pipeline started successfully.");
}).catch((err: unknown) => { // Explicitly type the error parameter
  console.error("Pipeline failed with error: ", err);
});

const podWatcherProgram = Effect.gen(function* () {
  const becs = yield* BackendPodCacheService
  const q = yield* Queue.bounded<BackendPodEvent>(100);
  const watcherEffect = createKubernetesWatchEventStream().pipe(
      Stream.runForEach((kpe) => {
        return Effect.gen(function* (_) {
          console.log(`received: ${kpe.type}`)
          Effect.log(`Received Event`)
          const type: KubernetesEventType = kpe.type
          const backend = yield* podToBackend(kpe.apiObj)
          yield* q.offer({backend, type})
        })
      })
    )
  const podEventMatcher = Match.type<BackendPodEvent>().pipe(
    Match.when({type: 'ADDED'}, (_) => (backend: Backend) => 
               Effect.sync(() =>  { 
                 console.log(`matching insert: ${backend}`)
                 becs.insert(backend)
               })),
    Match.when({type: 'MODIFIED'}, (_) => (backend: Backend) => 
               Effect.sync(() => becs.insert(backend))),
    Match.when({type: 'DELETED'}, (_) => (backend: Backend) => 
               Effect.sync( () => becs.delete(backend))
              ),
    Match.when({type: 'BOOKMARK'}, (_) => (backend: Backend) => 
               Effect.sync( () => {
                 Console.log(`BOOKMARK RECEIVED FOR ${backend}`)
               })),
    Match.exhaustive
  )
  const consumerEffect = Stream.fromQueue(q).pipe(
      Stream.runForEach((bpe) => {
        return Effect.gen(function* () {
          console.log(`processing: ${bpe.type}`)
          Effect.log(`Received Event`)
          console.log(`Attempting match on ${JSON.stringify(bpe)}`)
          const f = podEventMatcher(bpe);
          console.log(JSON.stringify((f)))
          f(bpe.backend)
        })
      })
    )

  const receiverPipeline = createKubernetesWatchEventStream().pipe(
    Stream.tap((kpe) => Effect.succeed(
      () => { console.log(`received KubernetesPodEvent: ${JSON.stringify(kpe)}`) }
    )),
    Stream.mapEffect((kpe) => 
      Effect.map(podToBackend(kpe.apiObj), (backend) => ({
        backend,
        type: kpe.type
      }))
    ),
    Stream.tap((bpe) => Effect.succeed(
      () => { console.log(`received BackendPodEvent: ${JSON.stringify(bpe)}`) }
    )),
    Stream.mapEffect((bpe) => {
        const operation = podEventMatcher(bpe);
        return operation(bpe.backend)
    })
  )
  //Composite Fiber
  const execThreadFiber = Effect.forkAll([watcherEffect, consumerEffect])
  const s = yield* execThreadFiber
  const exit = yield* Fiber.await(s)
  return exit
})

const podWatcherProgramPipeline = Effect.gen(function* () {
  const becs = yield* BackendPodCacheService
  const q = yield* Queue.bounded<BackendPodEvent>(100);
  const podEventMatcher = Match.type<BackendPodEvent>().pipe(
    Match.when({type: 'ADDED'}, (_) => (backend: Backend) => 
               Effect.sync(() =>  { 
                 console.log(`matching insert: ${backend}`)
                 becs.insert(backend)
               })),
    Match.when({type: 'MODIFIED'}, (_) => (backend: Backend) => 
               Effect.sync(() => becs.insert(backend))),
    Match.when({type: 'DELETED'}, (_) => (backend: Backend) => 
               Effect.sync( () => becs.delete(backend))
              ),
    Match.when({type: 'BOOKMARK'}, (_) => (backend: Backend) => 
               Effect.sync( () => {
                 Console.log(`BOOKMARK RECEIVED FOR ${backend}`)
               })),
    Match.exhaustive
  )
  const receiverPipeline = createKubernetesWatchEventStream().pipe(
    Stream.tap((kpe) => Effect.succeed(
      () => { console.log(`received KubernetesPodEvent: ${JSON.stringify(kpe)}`) }
    )),
    Stream.mapEffect((kpe) => 
      Effect.map(podToBackend(kpe.apiObj), (backend) => ({
        backend,
        type: kpe.type
      }))
    ),
    Stream.tap((bpe) => Effect.succeed(
      () => { console.log(`received BackendPodEvent: ${JSON.stringify(bpe)}`) }
    )),
    Stream.mapEffect((bpe) => {
        const operation = podEventMatcher(bpe);
        return operation(bpe.backend)
    })
  )
})

//const runnable = Effect.provideService(podWatcherProgram, BackendPodCacheService, createTestBackendPodCache())

//Effect.runPromise(runnable)
