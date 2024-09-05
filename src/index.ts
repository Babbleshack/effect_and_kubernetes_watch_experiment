import { Effect, Stream, pipe, StreamEmit, Chunk, Match, Option, Context, Logger } from "effect";
import { NodeRuntime } from "@effect/platform-node"
import { KubeConfig, Watch, V1Pod } from '@kubernetes/client-node';
import {some} from "effect/Option";

type KubernetesEventType = 'ADDED' | 'MODIFIED' | 'DELETED' | 'BOOKMARK';
// type is <type> -> type assertions
const isKuberneteEventType = (type: string): type is KubernetesEventType => {
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
    listBackends: Effect.Effect<Array<Backend>>
  }
>() {}

/**
 * BackendPodCacheService service implementation for testing
 */
const createTestBackendPodCache = () => {
  //const pods = new Map()
  return {
    insert: (backend: Backend) => pipe(
      Effect.log(`INSERTING INTO BackendPodCacheService: ${JSON.stringify(backend)}`),
    ).pipe(Effect.annotateLogs("Backend", backend)),
    delete: (backend: Backend) => pipe(
      Effect.log(`DELETE FROM BackendPodCacheService`),
      Effect.flatMap(() =>
        Effect.sync(() => {
          return Option.none()
        }))
    ).pipe(Effect.annotateLogs("Backend", backend)),
    listBackends: pipe(
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
      // TODO: Investigate Effect.Schema
      const eventHandler = (type: string, apiObj: V1Pod) => {
        if( isKuberneteEventType(type) === false) {
          return Effect.fail(new InvalidKubernetesEventTypeError())
        }
        const e: KubernetesPodEvent = {
          type: type, 
          apiObj
        }
        emit(Effect.succeed(Chunk.of(e)))
      }
      watch.watch(`/api/v1/namespaces/${namespace}/pods`, {}, eventHandler, doneHandler)
    }
  )

// TODO: Rather than testing if these fields are null use Effect.Schema
// SEE: Parse dont validate blog post
  // Ideally this fucntion would only accept a valid V1Pod
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
      Match.when({ type: 'ADDED' }, (e) => pipe(
                   Effect.log("MATCHING INSERT"), // Log the event as an effect
                   Effect.flatMap(() => 
                                  becs.insert(e.backend)
                                 )
                 )
      ),
      Match.when({ type: 'MODIFIED' }, (e) => Effect.log(`MATCHING MODIFIED ${e.backend}`)),
      Match.when({ type: 'DELETED' }, (e) => pipe(
                  Effect.log(`MATCHING DELETE ${e.backend}`),
                  Effect.flatMap(() => becs.delete(e.backend))
                 )
      ),
      Match.when({ type: 'BOOKMARK' }, (e) => Effect.log(`BOOKMARK RECEIVED FOR ${e.backend}`)),
      Match.exhaustive
    )
  ),
  Effect.bind("receiverPipeline", ({ podEventMatcher }) =>
    pipe(
      createKubernetesWatchEventStream(),
      Stream.tap((kpe) =>
                 Effect.log(`received KubernetesPodEvent for: ${JSON.stringify(kpe.apiObj.metadata?.name)}`)
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
      Stream.mapEffect(podEventMatcher),
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
      receiverPipeline.await, // Await pipeline
      // Run both receiverPipeline.await effect and Effect.never, returning only Effect.never, e.g. run forever?
      Effect.zipRight(Effect.never)
    )
  ),
  Effect.provide(Logger.structured),
  NodeRuntime.runMain
);

console.log("I am something else")

