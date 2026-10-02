// Spawns the real keyed stack for the e2e suite (test/stack/cluster.ts): `ursula server` serving
// `{stream}/keyed-state` through a keyed-mode `ursula indexer`, with the feature level raised to 1.
//
// Knobs:
// - E2E_NODES=3: a 3-node cluster behind `ursula gateway` (default one node).
// - E2E_S3=1: S3 (MinIO; URSULA_S3_ENDPOINT or a local `minio`/MINIO_BIN) is both the node cold
//   store and the indexer's object store, the indexer writing `.keyed/` under the node cold root.
//   Default: no cold store, indexer on the filesystem.
// - E2E_KEYED_PUBLISH_INTERVAL_MS: the indexer's publication spacing (default 100).
//
// Provides the entry URL (node or gateway), a control URL the tests use to restart the indexer, and
// the stack layout (node URLs, S3 location) for the S3 and cluster suites.
import { createServer as createHttpServer } from "node:http";
import type { TestProject } from "vitest/node";
import { Stack } from "../stack/cluster.ts";
import { E2E_BUCKET, type StackInfo } from "./env.ts";

declare module "vitest" {
	export interface ProvidedContext {
		ursulaUrl: string;
		controlUrl: string;
		stackInfo: string;
	}
}

export default async function setup(project: TestProject): Promise<() => Promise<void>> {
	const nodes = process.env.E2E_NODES === "3" ? 3 : 1;
	const s3 = process.env.E2E_S3 === "1";
	const stack = await Stack.start({
		nodes,
		s3,
		publishIntervalMs: Number(process.env.E2E_KEYED_PUBLISH_INTERVAL_MS ?? "100"),
	});
	// Test control: POST /indexer/restart stops the indexer and starts a fresh process on the same
	// port and object store (the in-memory state and write cache are lost; `.keyed/` persists).
	const control = createHttpServer((req, res) => {
		if (req.method === "POST" && req.url === "/indexer/restart") {
			stack.restartIndexer().then(
				() => res.writeHead(200).end("restarted"),
				(error: unknown) => res.writeHead(500).end(String(error)),
			);
			return;
		}
		res.writeHead(404).end();
	});
	const controlPort = await new Promise<number>((res) =>
		control.listen(0, "127.0.0.1", () => {
			const address = control.address();
			res(typeof address === "object" && address !== null ? address.port : 0);
		}),
	);
	try {
		await stack.createBucket(E2E_BUCKET);
	} catch (error) {
		control.close();
		await stack.stop();
		throw error;
	}
	const info: StackInfo = {
		nodes: stack.nodes.map((node) => ({ id: node.id, url: node.url, adminUrl: node.adminUrl })),
		...(stack.s3 === undefined
			? {}
			: {
					s3: {
						endpoint: stack.s3.endpoint,
						bucket: stack.s3Bucket,
						root: stack.s3Root,
						accessKey: stack.s3.accessKey,
						secretKey: stack.s3.secretKey,
						region: stack.s3.region,
					},
				}),
	};
	project.provide("ursulaUrl", stack.url);
	project.provide("controlUrl", `http://127.0.0.1:${controlPort}`);
	project.provide("stackInfo", JSON.stringify(info));
	return async () => {
		control.close();
		await stack.stop();
	};
}
