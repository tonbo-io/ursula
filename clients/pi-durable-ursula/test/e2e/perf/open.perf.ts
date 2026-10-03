// MANUAL TOOL, NOT RUN IN CI. Run with `URSULA_BIN=../../target/release/ursula npm run perf:manual`
// (vitest.perf.config.ts); it takes several minutes. Results are recorded in README.md.
// M3 performance gates (design §9.3, §10 M3 exit, §11.10) against the real keyed stack: a
// single-node memory-engine `ursula` with the keyed indexer (filesystem object store), spawned by
// the e2e global setup.
//
// - Harness-level open = UrsulaStorage.open (bounded) + Harness.open + root + resume + first
//   taskGraph + first viewState: p50 ≤ 250 ms, p99 ≤ 1 s.
// - First submit after open, submit → provider request: p50 ≤ 300 ms.
// - Scaling: p50 open at 100k records ≤ 1.2 × p50 at 1k; 1,000 conversations ≤ 1.2 × 10.
//
// Histories are written by a real Pi Harness on the in-memory fake (fast), then bulk-loaded into the
// real node as top-level JSON-array appends (one record per element), so the node and the indexer
// hold byte-identical Pi logs. Scenarios of one gate are measured interleaved, so drift in machine
// load affects both sides alike. Results go to stdout and to PERF_OUT (JSON) when set.
//
// Knobs: PERF_SAMPLES (30), PERF_SMALL_RECORDS (1000), PERF_LARGE_RECORDS (100000),
// PERF_FEW_CONVERSATIONS (10), PERF_MANY_CONVERSATIONS (1000), PERF_GATES=0 to report only,
// PERF_CACHE_DIR to keep generated histories between runs, PERF_SETTLE_MS (15000) after loading.
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { Harness } from "@earendil-works/pi-durable";
import { describe, expect, it } from "vitest";
import { FakeUrsula } from "../../../src/fake/index.ts";
import { HttpKeyedStateTransport, httpTransports, streamUrl } from "../../../src/http.ts";
import { KEYED_CONTENT_TYPE } from "../../../src/protocol.ts";
import { UrsulaStorage } from "../../../src/storage.ts";
import { K, META } from "../../../src/families.ts";
import { b64 } from "../../../src/tuple.ts";
import { agent, faux, models, openHarness, registry, textTurn, toolTurn } from "../../harness-kit.ts";
import { ctx } from "../../helpers.ts";
import { baseUrl, freshStream } from "../env.ts";

const env = (name: string, fallback: number): number => Number(process.env[name] ?? fallback);
const SAMPLES = env("PERF_SAMPLES", 30);
const SMALL = env("PERF_SMALL_RECORDS", 1000);
const LARGE = env("PERF_LARGE_RECORDS", 100_000);
const FEW = env("PERF_FEW_CONVERSATIONS", 10);
const MANY = env("PERF_MANY_CONVERSATIONS", 1000);
const GATES = process.env.PERF_GATES !== "0";
/** Pause after loading, so the indexer's post-load compaction does not overlap the samples. */
const SETTLE_MS = env("PERF_SETTLE_MS", 15_000);

interface History {
	readonly name: string;
	readonly records: string[];
	readonly conversations: number;
}

/** Root turns per context: a `pi.reset` every so many turns keeps the live context bounded (§9.3). */
const TURNS_PER_CONTEXT = 30;

/**
 * Write a history with a real Harness on the fake: `conversations − 1` extra conversations with one
 * text turn each, then root turns (two text, one tool) until the log holds at least `records` records.
 * Long sessions do not grow one context forever (Pi compacts or resets), so the root starts a new
 * context every TURNS_PER_CONTEXT turns; what grows is the stored history, which is what the gate is about.
 */
async function generate(name: string, records: number, conversations: number): Promise<History> {
	const cacheDir = process.env.PERF_CACHE_DIR;
	const cached = cacheDir === undefined ? undefined : join(cacheDir, `history-${records}-${conversations}-${TURNS_PER_CONTEXT}.ndjson`);
	if (cached !== undefined && existsSync(cached)) {
		return { name, records: readFileSync(cached, "utf8").split("\n").filter((l) => l.length > 0), conversations };
	}
	const fake = new FakeUrsula({ indexer: "aggressive" });
	const path = "/gen/history";
	const storage = await UrsulaStorage.open({
		log: fake.logTransport(path),
		keyedState: fake.keyedStateTransport(path),
		stateStore: "full-resident",
		host: "gen",
		pid: 1,
	});
	const { harness, root } = await openHarness(storage);
	for (let c = 1; c < conversations; c++) {
		const conversation = await harness.createConversation({ ownership: { kind: "ownerless" }, agent }, ctx);
		await textTurn(conversation, c);
		if (c % 100 === 0) fake.requests.length = 0;
	}
	for (let n = 0; fake.records(path).length < records; n++) {
		if (n % 3 === 2) await toolTurn(root, n);
		else await textTurn(root, n);
		if (n % TURNS_PER_CONTEXT === TURNS_PER_CONTEXT - 1) await root.reset(undefined, ctx);
		if (n % 100 === 0) fake.requests.length = 0;
	}
	await harness.close(ctx);
	await storage.close(ctx);
	const out = [...fake.records(path)];
	if (cached !== undefined) writeFileSync(cached, `${out.join("\n")}\n`);
	return { name, records: out, conversations };
}

/** Bulk-load `history` into a fresh real stream, then wait until keyed-state reflects all of it. */
async function load(history: History): Promise<string> {
	const stream = freshStream();
	const url = streamUrl(baseUrl(), stream);
	const created = await fetch(url, { method: "PUT", headers: { "content-type": KEYED_CONTENT_TYPE } });
	if (!created.ok) throw new Error(`create ${stream}: ${created.status} ${await created.text()}`);
	const MAX_BODY = 8 * 1024 * 1024;
	let batch: string[] = [];
	let bytes = 0;
	let next = 0;
	const flush = async (): Promise<void> => {
		if (batch.length === 0) return;
		const r = await fetch(url, {
			method: "POST",
			headers: { "content-type": KEYED_CONTENT_TYPE, "stream-record-match": String(next) },
			body: `[${batch.join(",")}]`,
		});
		if (!r.ok) throw new Error(`load ${stream} at ${next}: ${r.status} ${await r.text()}`);
		next += batch.length;
		batch = [];
		bytes = 0;
	};
	for (const record of history.records) {
		if (bytes + record.length > MAX_BODY) await flush();
		batch.push(record);
		bytes += record.length + 1;
	}
	await flush();
	// Wait for the indexer: keyed-state at min_through_record = tail.
	const key = b64(K.m(META.owner));
	const deadline = Date.now() + 600_000;
	for (;;) {
		const r = await fetch(`${url}/keyed-state?key=${key}&min_through_record=${next}&timeout_ms=10000`);
		await r.arrayBuffer();
		if (r.status === 200) return stream;
		if (Date.now() > deadline) throw new Error(`keyed-state of ${stream} did not reach ${next}`);
	}
}

interface Sample {
	readonly openMs: number;
	readonly storageOpenMs: number;
	readonly firstSubmitMs: number;
	readonly openRemoteReads: number;
	readonly firstTurnRemoteReads: number;
	/** Mean latency of this open's keyed-state reads. */
	readonly remoteReadMeanMs: number;
	readonly steps: { readonly harnessOpen: number; readonly root: number; readonly taskGraph: number; readonly viewState: number };
}

/** One Harness-level open, then the first submit; closes everything. */
async function sample(stream: string): Promise<Sample> {
	const t0 = performance.now();
	const storage = await UrsulaStorage.open({ ...httpTransports({ baseUrl: baseUrl(), stream }), stateStore: "bounded", host: "perf", pid: 1 });
	const storageOpenMs = performance.now() - t0;
	const harness = await Harness.open(storage, { models, registry }, ctx);
	const t1 = performance.now();
	const root = await harness.root(ctx, { agent });
	harness.resume();
	const t2 = performance.now();
	const graph = await harness.taskGraph(ctx);
	const t3 = performance.now();
	const view = await root.viewState(ctx);
	const openMs = performance.now() - t0;
	const steps = { harnessOpen: t1 - t0 - storageOpenMs, root: t2 - t1, taskGraph: t3 - t2, viewState: t0 + openMs - t3 };
	graph.dispose();
	view.dispose();
	const openRemoteReads = storage.metrics().openRemoteReads + storage.metrics().sessionLineRemoteReads;
	const remoteReadMeanMs = storage.metrics().remoteReadLatency.meanMs;

	let providerAt = Number.NaN;
	faux.setResponses([
		() => {
			providerAt = performance.now();
			return fauxAssistantMessage("ok");
		},
	]);
	const s0 = performance.now();
	const submission = await root.submit({ type: "input", content: "first question after open" }, ctx);
	const settled = await submission.wait(ctx);
	if (settled.status !== "done") throw new Error(`first submit settled ${settled.status}`);
	const firstSubmitMs = providerAt - s0;
	const firstTurnRemoteReads = storage.metrics().openRemoteReads + storage.metrics().sessionLineRemoteReads - openRemoteReads;
	await harness.close(ctx);
	await storage.close(ctx);
	if (storage.poison !== undefined) throw storage.poison;
	return { openMs, storageOpenMs, firstSubmitMs, openRemoteReads, firstTurnRemoteReads, remoteReadMeanMs, steps };
}

const quantile = (xs: readonly number[], q: number): number => {
	const s = [...xs].sort((a, b) => a - b);
	return s[Math.min(s.length - 1, Math.max(0, Math.ceil(q * s.length) - 1))] as number;
};

interface Summary {
	readonly name: string;
	readonly records: number;
	readonly conversations: number;
	readonly samples: number;
	readonly openP50: number;
	readonly openP99: number;
	readonly storageOpenP50: number;
	/** p50 of each step after UrsulaStorage.open: Harness.open, root + resume, taskGraph, viewState. */
	readonly stepsP50: Record<string, number>;
	readonly firstSubmitP50: number;
	readonly firstSubmitP99: number;
	readonly openRemoteReadsP50: number;
	readonly firstTurnRemoteReadsP50: number;
	readonly remoteReadMeanMsP50: number;
	/** §11.10 "keyed-state point read, warm": sequential `m/owner` point reads through the node. */
	readonly pointReadP50: number;
	readonly pointReadP99: number;
}

/** Sequential warm point reads of `m/owner` (no concurrency), in ms. */
async function pointReads(stream: string, n = 200): Promise<number[]> {
	const ks = new HttpKeyedStateTransport({ baseUrl: baseUrl(), stream });
	const key = b64(K.m(META.owner));
	const out: number[] = [];
	for (let i = 0; i < n + 10; i++) {
		const t0 = performance.now();
		const r = await ks.scan({ key, minThroughRecord: 0 });
		if (r.status !== 200) throw new Error(`point read answered ${r.status}`);
		if (i >= 10) out.push(performance.now() - t0);
	}
	return out;
}

function summarize(history: History, samples: readonly Sample[], points: readonly number[]): Summary {
	const pick = (f: (s: Sample) => number): number[] => samples.map(f);
	const r = (x: number): number => Math.round(x * 10) / 10;
	return {
		name: history.name,
		records: history.records.length,
		conversations: history.conversations,
		samples: samples.length,
		openP50: r(quantile(pick((s) => s.openMs), 0.5)),
		openP99: r(quantile(pick((s) => s.openMs), 0.99)),
		storageOpenP50: r(quantile(pick((s) => s.storageOpenMs), 0.5)),
		stepsP50: Object.fromEntries((["harnessOpen", "root", "taskGraph", "viewState"] as const).map((k) => [k, r(quantile(pick((s) => s.steps[k]), 0.5))])),
		firstSubmitP50: r(quantile(pick((s) => s.firstSubmitMs), 0.5)),
		firstSubmitP99: r(quantile(pick((s) => s.firstSubmitMs), 0.99)),
		openRemoteReadsP50: quantile(pick((s) => s.openRemoteReads), 0.5),
		firstTurnRemoteReadsP50: quantile(pick((s) => s.firstTurnRemoteReads), 0.5),
		remoteReadMeanMsP50: r(quantile(pick((s) => s.remoteReadMeanMs), 0.5)),
		pointReadP50: Math.round(quantile(points, 0.5) * 100) / 100,
		pointReadP99: Math.round(quantile(points, 0.99) * 100) / 100,
	};
}

/** Measure two loaded histories interleaved: one warm-up open each, then SAMPLES alternating pairs. */
async function measurePair(a: History, b: History): Promise<[Summary, Summary]> {
	const streams = [await load(a), await load(b)] as const;
	await new Promise((r) => setTimeout(r, SETTLE_MS));
	for (const s of streams) await sample(s);
	const out: [Sample[], Sample[]] = [[], []];
	for (let i = 0; i < SAMPLES; i++) {
		const order = i % 2 === 0 ? [0, 1] : [1, 0];
		for (const j of order) out[j as 0 | 1].push(await sample(streams[j as 0 | 1]));
	}
	const points = [await pointReads(streams[0]), await pointReads(streams[1])] as const;
	return [summarize(a, out[0], points[0]), summarize(b, out[1], points[1])];
}

const results: Record<string, unknown> = {};
const report = (key: string, value: unknown): void => {
	results[key] = value;
	console.log(`${key}: ${JSON.stringify(value)}`);
	if (process.env.PERF_OUT !== undefined) writeFileSync(process.env.PERF_OUT, `${JSON.stringify(results, null, 2)}\n`);
};

describe("M3 performance gates on the real keyed stack", () => {
	it(`Harness-level open and first submit; ${SMALL} vs ${LARGE} records`, async () => {
		const g0 = performance.now();
		const small = await generate(`${SMALL} records`, SMALL, 1);
		const large = await generate(`${LARGE} records`, LARGE, 1);
		report("historyGenerationSeconds", Math.round((performance.now() - g0) / 100) / 10);
		const [s, l] = await measurePair(small, large);
		report("records.small", s);
		report("records.large", l);
		const ratio = l.openP50 / s.openP50;
		report("records.openP50Ratio", Math.round(ratio * 100) / 100);
		if (!GATES) return;
		for (const x of [s, l]) {
			expect(x.openP50, `${x.name}: open p50`).toBeLessThanOrEqual(250);
			expect(x.openP99, `${x.name}: open p99`).toBeLessThanOrEqual(1000);
			expect(x.firstSubmitP50, `${x.name}: first submit p50`).toBeLessThanOrEqual(300);
		}
		expect(ratio, "open p50 at 100k records / at 1k").toBeLessThanOrEqual(1.2);
	});

	it(`Harness-level open; ${FEW} vs ${MANY} conversations`, async () => {
		const many = await generate(`${MANY} conversations`, 0, MANY);
		// The same number of records, in fewer conversations (the rest are root turns).
		const few = await generate(`${FEW} conversations`, many.records.length, FEW);
		const [f, m] = await measurePair(few, many);
		report("conversations.few", f);
		report("conversations.many", m);
		const ratio = m.openP50 / f.openP50;
		report("conversations.openP50Ratio", Math.round(ratio * 100) / 100);
		if (!GATES) return;
		for (const x of [f, m]) {
			expect(x.openP50, `${x.name}: open p50`).toBeLessThanOrEqual(250);
			expect(x.openP99, `${x.name}: open p99`).toBeLessThanOrEqual(1000);
			expect(x.firstSubmitP50, `${x.name}: first submit p50`).toBeLessThanOrEqual(300);
		}
		expect(ratio, "open p50 at 1,000 conversations / at 10").toBeLessThanOrEqual(1.2);
	});
});
