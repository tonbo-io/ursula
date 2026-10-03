import { inject } from "vitest";
import { HttpWalStream } from "../../src/stream.ts";

let counter = 0;
/** A fresh stream on the spawned node. */
export const freshStream = (): string => `sqlite-e2e/s-${process.pid}-${Date.now().toString(36)}-${counter++}`;
export const httpStream = (stream: string): HttpWalStream => new HttpWalStream({ baseUrl: inject("ursulaUrl"), stream });
