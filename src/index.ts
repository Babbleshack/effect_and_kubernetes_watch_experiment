import { Effect, Exit, pipe } from "effect";
import { NodeRuntime } from "@effect/platform-node"

import { KubeConfig, Watch } from '@kubernetes/client-node';

class ValidationFailure {
  readonly _tag = "ValidationFailure"
}

const main = () => {
  //const runHelloWorld = (): Effect.Effect<string> => {
  //  return Effect.succeed("Excellent work, Snake!")
  //}

  const runHelloWorld = () => Effect.sync(() => {
      console.log("Excellent work, Snake!")
    })

  const output = runHelloWorld()
  console.log(output)
  console.log(typeof output)
  Effect.runSync(runHelloWorld())
};

const errorHandling = () => {
  const doError = () => Effect.try({
    try: () => { throw new ValidationFailure() },
    catch: (unknown) => new Error(`something went wrong ${unknown}`)
  })
  const exitState = Effect.runSyncExit(doError())
  Exit.match(exitState, {
    onSuccess: () => {"Effect was success"},
    onFailure: (cause) => console.log(`Exited with failure: ${cause}`)
  })
}

const effectGen = () => {
  const program = Effect.gen(function* () {
    const a = yield* Effect.sync(() => {
      console.log("I DO SOME WORK")
      return 1;
    });
    console.log(a)
  })
  Effect.runSync(program)
}

const functionWithCallback = (value: number, callback: (data: number, err: any | null) => void):  void  => {
  console.log("doing callback");
  callback(value, null)
  console.log("callback done")
}

//functionWithCallback(99, (data: number, err: any | null ) => { 
//  if (data < 100) {
//    throw new Error(`value is less than 100: ${data}`)
//  } else {
//    console.log(`GOT NUMBER: ${data}`)
//    return data + 100;
//  }
//})
//

const doCallbackFunction = (n: number) => 
  Effect.async<number, Error>((resume) => {
    functionWithCallback(n, (data: number, err: any | null ) => { 
      if (err) {
        resume(Effect.fail(err))
      } else if (data < 100) {
        resume(Effect.fail(new Error(`value is less than 100: ${data}`)))
      } else {
        console.log(`GOT NUMBER: ${n}`)
        resume(Effect.succeed(data + 100));
      }
    })
  })

const programShouldFail = doCallbackFunction(99)
const programShouldSucceed = doCallbackFunction(100)
pipe(
  programShouldSucceed,
  NodeRuntime.runMain
)

