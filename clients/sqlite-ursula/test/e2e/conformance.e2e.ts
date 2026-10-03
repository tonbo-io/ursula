import { registerModes } from "../conformance-modes.ts";
import { freshStream, httpStream } from "./env.ts";

registerModes("real Ursula", () => httpStream(freshStream()));
