// Keyed lifecycle on S3 (design §3.8, U22, U23), run when the stack uses S3 (E2E_S3=1): the indexer
// writes its namespaces under the node cold root's `.keyed/`, so the node's stream-delete GC and
// bucket purge really remove them.
import { describe, expect, it } from "vitest";
import { K, META } from "../../src/families.ts";
import { b64 } from "../../src/tuple.ts";
import { ctx } from "../helpers.ts";
import { S3Client } from "../stack/s3.ts";
import { until } from "../stack/proc.ts";
import { baseUrl, E2E_BUCKET, freshStream, openHttp, stackInfo } from "./env.ts";

const info = stackInfo();
const s3 = info.s3;
const conv = (id: number) => [{ type: "conversation" as const, value: { id } as never }];

function client(): S3Client {
	if (s3 === undefined) throw new Error("no S3");
	return new S3Client(s3.endpoint, s3);
}

/** Object keys under `{root}/{prefix}`. */
const list = (prefix: string): Promise<string[]> => client().list(s3?.bucket ?? "", `${s3?.root ?? ""}/${prefix}`);

/** `.keyed/{bucket}/{key}/`: every namespace of one stream (the key is one percent-encoded component). */
const keyedPrefix = (stream: string): string => {
	const [bucket, ...rest] = stream.split("/");
	return `.keyed/${bucket}/${rest.join("/").replace(/%/g, "%25").replace(/\//g, "%2F")}/`;
};

/** Writes a few commits and waits until the indexer has published them. */
async function writeAndPublish(stream: string, url = baseUrl()): Promise<number> {
	const s = await openHttp(stream);
	for (let i = 0; i < 4; i++) await s.commit(conv(10 + i), ctx);
	const tail = s.tail;
	await s.close(ctx);
	// 503 is retryable (keyed-state API): other e2e files restart the shared indexer concurrently.
	const r = await until(
		`keyed state of ${stream} through ${tail}`,
		async () => {
			const response = await fetch(`${url}/${stream}/keyed-state?key=${b64(K.m(META.owner))}&min_through_record=${tail}&timeout_ms=20000`);
			if (response.status !== 503) return response;
			await response.text();
			return undefined;
		},
		60_000,
		250,
	);
	expect(r.status, r.status === 200 ? "" : await r.text()).toBe(200);
	return tail;
}

/** Bucket purge on the node until it reports completion (the node retries nothing itself). */
async function purge(bucket: string): Promise<Record<string, unknown>> {
	return until(
		`purge of ${bucket}`,
		async () => {
			// Each node purges the groups it leads; ask every node in turn until one reports completion.
			for (const node of info.nodes) {
				const r = await fetch(`${node.url}/__ursula/purge/${bucket}`, { method: "DELETE", redirect: "manual" });
				if (!r.ok) {
					await r.text();
					continue;
				}
				const report = (await r.json()) as Record<string, unknown>;
				if (report.cold_gc_complete === true && report.keyed_drain_complete === true) return report;
			}
			return undefined;
		},
		60_000,
		500,
	);
}

describe.runIf(s3 !== undefined)("keyed lifecycle on S3", () => {
	it("the indexer writes under the node cold root, and stream delete GC removes the namespace", async () => {
		const stream = freshStream();
		const neighbour = freshStream();
		await writeAndPublish(stream);
		await writeAndPublish(neighbour);
		const objects = await list(keyedPrefix(stream));
		expect(objects.some((key) => key.endsWith("/CURRENT")), objects.join("\n")).toBe(true);
		expect(objects.some((key) => /\/v1\/parts\//.test(key)), objects.join("\n")).toBe(true);

		const r = await fetch(`${baseUrl()}/${stream}`, { method: "DELETE" });
		expect(r.status).toBe(204);
		await until(`GC of ${keyedPrefix(stream)}`, async () => ((await list(keyedPrefix(stream))).length === 0 ? true : undefined), 60_000, 250);
		// Only the deleted incarnation's namespace goes; the neighbour stream keeps its own.
		expect((await list(keyedPrefix(neighbour))).length).toBeGreaterThan(0);
		const head = await fetch(`${baseUrl()}/${stream}/keyed-state?key=${b64(K.m(META.owner))}`);
		expect(head.status).toBe(404);
	});

	it("bucket purge drains the indexer and erases both prefixes", async () => {
		const bucket = `pi-purge-${Date.now().toString(36)}`;
		const created = await fetch(`${baseUrl()}/${bucket}`, { method: "PUT" });
		expect(created.ok).toBe(true);
		const streams = [`${bucket}/a`, `${bucket}/b`];
		for (const stream of streams) {
			await writeAndPublish(stream);
			// Push the log to the cold store too, so `{bucket}/` holds objects (the stream's leader
			// flushes; other nodes refuse).
			for (const node of info.nodes) await (await fetch(`${node.adminUrl}/__ursula/flush-cold/${stream}`, { method: "POST" })).text();
		}
		await until(`cold objects under ${bucket}/`, async () => ((await list(`${bucket}/`)).length > 0 ? true : undefined), 30_000, 250);
		expect((await list(`.keyed/${bucket}/`)).length).toBeGreaterThan(0);
		// Another bucket's namespaces must survive.
		const keep = freshStream();
		await writeAndPublish(keep);

		const report = await purge(bucket);
		expect(report.bucket_prefix_absent).toBe(true);
		expect(report.keyed_prefix_absent).toBe(true);
		expect(await list(`${bucket}/`)).toEqual([]);
		expect(await list(`.keyed/${bucket}/`)).toEqual([]);
		expect((await list(keyedPrefix(keep))).length).toBeGreaterThan(0);
		expect((await list(`${E2E_BUCKET}/`)).length + (await list(`.keyed/${E2E_BUCKET}/`)).length).toBeGreaterThan(0);
		// The purged bucket's streams are gone for keyed-state too.
		const gone = await fetch(`${baseUrl()}/${streams[0]}/keyed-state?key=${b64(K.m(META.owner))}`);
		expect(gone.status).toBe(404);
	});
});
