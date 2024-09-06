import { Effect, Stream, pipe, StreamEmit, Chunk, Match, Option, Context, Logger, Schedule, Fiber } from "effect";
import { NodeRuntime } from "@effect/platform-node"
import { KubeConfig, Watch, V1Pod, RequestResult } from '@kubernetes/client-node';
import {some} from "effect/Option";
import {promise} from "effect/Effect";
import {acquire} from "effect/TSemaphore";

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
      Effect.flatMap( () => Effect.sync ( () => console.log(`==== INSERTING INTO BackendPodCacheService: ${JSON.stringify(backend)} ========`)))
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

const createWatch =  () => Effect.gen(function* () {
  const kc = new KubeConfig();
  kc.loadFromDefault();
  const watch = new Watch(kc)
  return watch
})

const kc = new KubeConfig();
kc.loadFromDefault();
const watch = new Watch(kc)

//const scoped = Stream.asyncScoped((emit) =>  
//  Effect.acquireRelease(
//    Effect.tryPromise({
//          try: () => watch.watch(
//            "", 
//            {}, 
//            (e) => emit(Effect.succeed(Chunk.of(e))),
//            (_) => emit(Effect.fail(Option.none()))
//          ) as unknown,
//          catch: (e) => new Error(JSON.stringify(e))
//        }),
//    (w) => Effect.promise(() => w.abort())
//  )
//);

const createKubernetesWatchEventStream = (watch: Watch, namespace: string = 'default')  => 
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
      const watchInitPromise = watch.watch(`/api/v1/namespaces/${namespace}/pods`, {}, eventHandler, doneHandler)
      return Effect.promise(() => watchInitPromise.then((req) => {
        req.abort()
      }))
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

const pipeline = Effect.gen(function* () {
  const becs = yield* BackendPodCacheService
  const podEventMatcher = Match.type<BackendPodEvent>().pipe(
      Match.when({ type: 'ADDED' }, (e) => pipe(
                   Effect.log("MATCHING INSERT"), // Log the event as an effect
                   Effect.flatMap(() => 
                                  becs.insert(e.backend)
                                 ),
                   Effect.flatMap( () => Effect.sync ( () => console.log(`======= MATCHING INSERT: ${e.backend} ========`)))
                 ),
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
    createKubernetesWatchEventStream(watch).pipe(
      Stream.tap((kpe) => Effect.log(`received KubernetesPodEvent for: ${JSON.stringify(kpe.apiObj.metadata?.name)} type: ${kpe.type}`)
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
    )
    Stream.run
})

const runnable = Effect.provideService(pipeline, BackendPodCacheService, createTestBackendPodCache())
const fiber = Effect.runFork(runnable)

setTimeout(() => {
  console.log("ABOUT TO INTERUPT")
  Effect.runPromiseExit(
    pipe (
      Fiber.interrupt(fiber),
      Effect.map((exit) => {
        console.log(`GOT EXIT ${exit}`)
      })
    )
  )
  console.log(`RETURNING: ${JSON.stringify(fiber.status)}`)
}, 60000)

setInterval(() => console.log("working...."), 1000)

