// Shared e2e settings and helpers: HTTP transports against the spawned node.
import { inject } from "vitest";
import { httpTransports } from "../../src/http.ts";
import { type StateStoreKind, type UrsulaStorage, UrsulaStorage as Storage, type UrsulaStorageOptions } from "../../src/storage.ts";
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

/** Layout of the spawned stack (global-setup.ts). */
export interface StackInfo {
	readonly nodes: readonly { readonly id: number; readonly url: string; readonly adminUrl: string }[];
	/** Present when the stack runs on S3 (E2E_S3=1). */
	readonly s3?: {
		readonly endpoint: string;
		readonly bucket: string;
		/** The node cold root, which holds `.keyed/`. */
		readonly root: string;
		readonly accessKey: string;
		readonly secretKey: string;
		readonly region: string;
	};
}

export const stackInfo = (): StackInfo => JSON.parse(inject("stackInfo")) as StackInfo;

/**
 * The owner's state store. The e2e stack serves keyed-state (node proxy + keyed indexer), so the
 * suite runs the bounded owner (LocalStore over keyed-state) by default; E2E_STATE_STORE=auto or
 * full-resident selects another.
 */
export const STATE_STORE = (process.env.E2E_STATE_STORE ?? "bounded") as StateStoreKind;

/** Restarts the keyed indexer (same port and object store, fresh process). */
export async function restartIndexer(): Promise<void> {
	const r = await fetch(`${inject("controlUrl")}/indexer/restart`, { method: "POST" });
	if (!r.ok) throw new Error(`restart indexer: ${r.status} ${await r.text()}`);
}

/** Open UrsulaStorage on `stream` through the HTTP transports (the stack entry URL unless `url` is given). */
export function openHttp(stream: string, options: Partial<UrsulaStorageOptions> = {}, url: string = baseUrl()): Promise<UrsulaStorage> {
	return Storage.open({
		...httpTransports({ baseUrl: url, stream }),
		requireKeyedBatch: REQUIRE_KEYED,
		stateStore: STATE_STORE,
		host: "e2e",
		pid: 1,
		...options,
		timing: { ...FAST, ...options.timing },
	});
}
