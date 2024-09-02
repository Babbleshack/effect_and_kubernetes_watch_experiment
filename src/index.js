"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const effect_1 = require("effect");
class ValidationFailure {
    constructor() {
        this._tag = "ValidationFailure";
    }
}
const main = () => {
    const failure = effect_1.Effect.fail(new ValidationFailure());
    console.log(failure);
    effect_1.Effect.runSync(failure);
};
main();
