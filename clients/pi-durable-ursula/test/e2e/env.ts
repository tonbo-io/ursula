// Shared e2e settings and helpers: HTTP transports against the spawned node.
import { inject } from "vitest";
import { httpTransports } from "../../src/http.ts";
import { type UrsulaStorage, UrsulaStorage as Storage, type UrsulaStorageOptions } from "../../src/storage.ts";
import { FAST } from "../helpers.ts";

export const E2E_BUCKET = "pi-e2e";

/**
 * Whether open requires `keyed-batch-v1`. The node advertises it since server-side P2 merged, so
 * the suite requires it by default. Set E2E_REQUIRE_KEYED=0 to run against an older node that
 * serves only the generic JSON surface.
 */
export const REQUIRE_KEYED = process.env.E2E_REQUIRE_KEYED !== "0";

let counter = 0;
/** A fresh `{bucket}/{stream}` path. */
export const freshStream = (): string => `${E2E_BUCKET}/h-${process.pid}-${Date.now().toString(36)}-${counter++}`;

export const baseUrl = (): string => inject("ursulaUrl");

/** Open UrsulaStorage on `stream` through the HTTP transports. */
export function openHttp(stream: string, options: Partial<UrsulaStorageOptions> = {}): Promise<UrsulaStorage> {
	return Storage.open({
		...httpTransports({ baseUrl: baseUrl(), stream }),
		requireKeyedBatch: REQUIRE_KEYED,
		host: "e2e",
		pid: 1,
		...options,
		timing: { ...FAST, ...options.timing },
	});
}
