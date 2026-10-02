// In-memory fake Ursula for tests: both transports, real fold semantics, record_match, faults.
export { type FakeOp, type FakeRequest, type Fault, type FaultHook, FakeUrsula, type FakeUrsulaOptions } from "./server.ts";
export { faults } from "./faults.ts";
