import { FakeUrsula } from "../src/stream.ts";
import { registerModes } from "./conformance-modes.ts";

let n = 0;
registerModes("fake Ursula", () => new FakeUrsula().stream(`/b/s${n++}`));
