// Keyed streams soak (keyed-streams-pi-durable.md §10 M4 soak; bounded-stream-state.md §7.5), a
// shortened local form of the design's 7-day / 72-hour soak: a 3-node cluster with disk WAL behind
// the gateway, the keyed indexer, MinIO as the cold store and the indexer's store, at the highest
// supported feature level, under a mixed population of real Pi Harnesses on the bounded owner (see
// population.ts): fast streaming harnesses, slow trickle harnesses and long-history harnesses, with
// periodic owner takeovers and one indexer restart.
//
// Every SOAK_SAMPLE_S it forces a snapshot of every group replica (so snapshot sizes are current)
// and records per-group state gauges, Raft log and snapshot bytes, node RSS, the indexer's N−D lag
// and S3 requests, MinIO's S3 requests by API, and owner counters, to samples.jsonl. At the end it
// spot-checks keyed-state against the fold with `ursula indexer keyed verify`, counts faulted tasks,
// evaluates the pass criteria (summary.json), and asserts them.
//
// Knobs (defaults are the 1-hour soak): SOAK_DURATION_S (3600), SOAK_SAMPLE_S (300), SOAK_FAST (20),
// SOAK_SLOW (100), SOAK_LONG (3), SOAK_LONG_RECORDS (50000), SOAK_TAKEOVER_S (300),
// SOAK_INDEXER_RESTART_S (half the duration; 0 = none), SOAK_WARMUP_S (600), SOAK_LEVEL (3),
// SOAK_VERIFY_EACH (5 per kind), SOAK_RESULTS_DIR (soak-results/<timestamp>), SOAK_ASSERT (1).
// The stack's data lives in a temp dir removed at the end (KEEP_STACK=1 keeps it).
import { execFile } from "node:child_process";
import { appendFileSync, mkdirSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { promisify } from "node:util";
import { Harness } from "@earendil-works/pi-durable";
import { afterAll, expect, it } from "vitest";
import { FakeUrsula } from "../../src/fake/index.ts";
import { K, META } from "../../src/families.ts";
import { streamUrl } from "../../src/http.ts";
import { KEYED_CONTENT_TYPE } from "../../src/protocol.ts";
import { UrsulaStorage } from "../../src/storage.ts";
import { b64 } from "../../src/tuple.ts";
import { agent, models, registry, textTurn, toolTurn } from "../harness-kit.ts";
import { ctx } from "../helpers.ts";
import { Stack } from "../stack/cluster.ts";
import { sleep, ursulaBinary } from "../stack/proc.ts";
import { s3Available } from "../stack/s3.ts";
import { type HarnessKind, type PopulationOptions, SoakHarness } from "./population.ts";
import { classifyS3, forceSnapshots, type IndexerSample, type NodeSample, sampleIndexer, rssOf, sampleMinio, sampleNodes } from "./probe.ts";

const run = promisify(execFile);
const knob = (name: string, fallback: number): number => {
	const raw = process.env[name];
	if (raw === undefined || raw === "") return fallback;
	const v = Number(raw);
	if (!Number.isFinite(v) || v < 0) throw new Error(`${name} must be a non-negative number, got ${raw}`);
	return v;
};

const DURATION_S = knob("SOAK_DURATION_S", 3600);
const SAMPLE_S = knob("SOAK_SAMPLE_S", 300);
const FAST = knob("SOAK_FAST", 20);
const SLOW = knob("SOAK_SLOW", 100);
const LONG = knob("SOAK_LONG", 3);
const LONG_RECORDS = knob("SOAK_LONG_RECORDS", 50_000);
const TAKEOVER_S = knob("SOAK_TAKEOVER_S", 300);
const INDEXER_RESTART_S = knob("SOAK_INDEXER_RESTART_S", Math.floor(DURATION_S / 2));
const WARMUP_S = knob("SOAK_WARMUP_S", Math.min(600, Math.floor(DURATION_S / 6)));
const LEVEL = knob("SOAK_LEVEL", 3);
const VERIFY_EACH = knob("SOAK_VERIFY_EACH", 5);
const ASSERT = process.env.SOAK_ASSERT !== "0";
const BUCKET = "soak";
const MIB = 1024 * 1024;
/** Replicated bytes of one shared pack ref (bounded-state F2). */
const REF_BYTES = 250;

/** Pass thresholds. */
const LIMITS = {
	/** Pi §10 M4: per streaming harness, projection ≤ 50 PUT-class/h and ≤ 50 GET/h. */
	projectionPutPerHour: 50,
	projectionGetPerHour: 50,
	/** Pi §10 M4: log ≤ 50 PUT-class/h and ≤ 250 GET/h per active harness. */
	logPutPerHour: 50,
	logGetPerHour: 250,
	/** Bounded-state I1: marks ≤ ⌈K⌉ + 2 per stream; shared refs ≤ 64 and staged refs ≤ 16 per stream. */
	sharedRefsPerStream: 64,
	stagedRefsPerStream: 16,
	/**
	 * Residual growth per stream after warm-up (state minus H, 8 B·U, 16 B·cold MiB and the capped pack refs) must stay
	 * within this many bytes per hour: the measurement noise of a 1-hour run. The design's 72-hour
	 * gate is 1% per day, which one hour cannot resolve.
	 */
	residualSlopeBytesPerHarnessHour: 256,
	/** Pi §10 M4 / I29: replicated state per stream ≤ 32 KiB + 8 B·U + 16 B per cold MiB, at every scrape after warm-up. */
	residualBytesPerStream: 32 * 1024,
};

const POPULATION: PopulationOptions = {
	baseUrl: "",
	tokensPerSecond: knob("SOAK_TOKENS_PER_S", 60),
	answerChars: knob("SOAK_ANSWER_CHARS", 1500),
	fastPauseMs: knob("SOAK_FAST_PAUSE_MS", 1000),
	longPauseMs: knob("SOAK_LONG_PAUSE_MS", 6000),
	slowIntervalMs: [knob("SOAK_SLOW_MIN_MS", 20_000), knob("SOAK_SLOW_MAX_MS", 40_000)],
};

let stack: Stack | undefined;
const harnesses: SoakHarness[] = [];

function resultsDir(): string {
	const dir = resolve(process.env.SOAK_RESULTS_DIR ?? join(import.meta.dirname, "../../soak-results", new Date().toISOString().replace(/[:.]/g, "-")));
	mkdirSync(dir, { recursive: true });
	return dir;
}

/** Writes a history with a real Harness on the in-memory fake (open.perf.ts's generator). */
async function generateHistory(records: number): Promise<string[]> {
	const fake = new FakeUrsula({ indexer: "aggressive" });
	const path = "/gen/history";
	const storage = await UrsulaStorage.open({ log: fake.logTransport(path), keyedState: fake.keyedStateTransport(path), stateStore: "full-resident", host: "gen", pid: 1 });
	const harness = await Harness.open(storage, { models, registry }, ctx);
	const root = await harness.root(ctx, { agent });
	harness.resume();
	for (let n = 0; fake.records(path).length < records; n++) {
		if (n % 3 === 2) await toolTurn(root, n);
		else await textTurn(root, n);
		if (n % 30 === 29) await root.reset(undefined, ctx);
		if (n % 100 === 0) fake.requests.length = 0;
	}
	await harness.close(ctx);
	await storage.close(ctx);
	return [...fake.records(path)];
}

/** Bulk-loads `records` into a fresh keyed stream as JSON-array appends; returns the bytes loaded. */
async function loadHistory(baseUrl: string, stream: string, records: readonly string[]): Promise<number> {
	const url = streamUrl(baseUrl, stream);
	const created = await fetch(url, { method: "PUT", headers: { "content-type": KEYED_CONTENT_TYPE } });
	if (!created.ok) throw new Error(`create ${stream}: ${created.status} ${await created.text()}`);
	let batch: string[] = [];
	let bytes = 0;
	let next = 0;
	let total = 0;
	const flush = async (): Promise<void> => {
		if (batch.length === 0) return;
		const r = await fetch(url, { method: "POST", headers: { "content-type": KEYED_CONTENT_TYPE, "stream-record-match": String(next) }, body: `[${batch.join(",")}]` });
		if (!r.ok) throw new Error(`load ${stream} at ${next}: ${r.status} ${await r.text()}`);
		next += batch.length;
		batch = [];
		bytes = 0;
	};
	for (const record of records) {
		if (bytes + record.length > 4 * MIB) await flush();
		batch.push(record);
		bytes += record.length + 1;
		total += record.length;
	}
	await flush();
	// Wait until keyed-state reflects the whole history.
	const key = b64(K.m(META.owner));
	const deadline = Date.now() + 600_000;
	for (;;) {
		const r = await fetch(`${url}/keyed-state?key=${key}&min_through_record=${next}&timeout_ms=10000`);
		await r.arrayBuffer();
		if (r.status === 200) return total;
		if (Date.now() > deadline) throw new Error(`keyed-state of ${stream} did not reach ${next}`);
	}
}

interface Sample {
	readonly at: string;
	readonly elapsedS: number;
	readonly nodes: NodeSample[];
	readonly indexer: IndexerSample;
	readonly indexerRssBytes: number;
	readonly snapshotTriggers: { node: number; group: number; status: number }[];
	readonly minio: Record<string, number> | undefined;
	readonly owners: Record<HarnessKind, { harnesses: number; open: number; commits: number; appendBytes: number; logBytes: number; turns: number; turnFailures: number; takeovers: number; poisons: number; fences: number; unexpectedPoison: number; sessionLineRemoteReads: number; maxPinnedBytes: number; maxLagRecords: number }>;
	readonly derived: Derived;
}

interface Derived {
	streams: number;
	harnesses: number;
	coldMiB: number;
	recordMarks: number;
	markBound: number;
	denseRecordEntries: number;
	hotRecords: number;
	maxSharedRefsPerStream: number;
	stagedExternalRefs: number;
	receiptItems: number;
	producers: number;
	ttlHeapEntries: number;
	ttlStreams: number;
	hotPayloadBytes: number;
	hotRealBytes: number;
	hotOverheadBytes: number;
	livePacks: number;
	sharedRefs: number;
	pendingColdGc: number;
	snapshotBytes: number;
	logBytesSinceSnapshot: number;
	maxRssBytes: number;
	rssBytes: number[];
	corruptions: number;
	/** Formula-sized index bytes per harness: 16 B·marks + 8 B·dense + 250 B·shared refs + staged + producers + hot block headers. */
	indexBytesPerHarness: number;
	/** Snapshot bytes per stream minus hot payload (snapshots taken just before the scrape): replicated state that is not payload. */
	snapshotStatePerStream: number;
	/** snapshotStatePerStream − (8 B·U + 16 B·cold MiB) per stream: I1 without H. */
	residualPerStream: number;
	/**
	 * residualPerStream without the pack refs (≈250 B each): refs grow by one per cold flush until F2's
	 * driver compacts a stream at T = 64 refs (or after an hour of quiet tail), so inside the cap they
	 * are part of I1's constant, checked by `sharedRefsWithinCap`, not by the slope.
	 */
	residualExRefsPerStream: number;
	indexerLagMax: number;
	indexerLagTotal: number;
}

function leaderGroups(nodes: NodeSample[]): Map<number, NodeSample["groups"][number]> {
	const out = new Map<number, NodeSample["groups"][number]>();
	for (const node of nodes) for (const g of node.groups) if (g.leader || !out.has(g.group)) out.set(g.group, g);
	return out;
}

function derive(nodes: NodeSample[], indexer: IndexerSample, logBytes: number): Derived {
	const groups = [...leaderGroups(nodes).values()];
	const sum = (k: string): number => groups.reduce((n, g) => n + (g.gauges[k] ?? 0), 0);
	const max = (k: string): number => groups.reduce((n, g) => Math.max(n, g.gauges[k] ?? 0), 0);
	const streams = sum("streams");
	const hotPayload = sum("hot_payload_bytes");
	const coldMiB = Math.max(0, logBytes - hotPayload) / MIB;
	const snapshotBytes = groups.reduce((n, g) => n + g.lastSnapshotBytes, 0);
	const hotReal = sum("hot_real_bytes");
	const harnessCount = harnesses.length;
	const indexBytes = 16 * sum("record_marks") + 8 * sum("dense_record_entries") + REF_BYTES * sum("shared_refs") + 128 * sum("staged_external_refs") + sum("producer_bytes") + sum("hot_overhead_bytes");
	const snapshotState = streams === 0 ? 0 : (snapshotBytes - hotPayload) / streams;
	const residual = streams === 0 ? 0 : snapshotState - (8 * sum("hot_records") + 16 * coldMiB) / streams;
	return {
		streams,
		harnesses: harnessCount,
		coldMiB,
		recordMarks: sum("record_marks"),
		markBound: Math.ceil(coldMiB) + 3 * streams,
		denseRecordEntries: sum("dense_record_entries"),
		hotRecords: sum("hot_records"),
		maxSharedRefsPerStream: max("max_shared_refs_per_stream"),
		stagedExternalRefs: sum("staged_external_refs"),
		receiptItems: sum("receipt_items"),
		producers: sum("producers"),
		ttlHeapEntries: sum("ttl_heap_entries"),
		ttlStreams: sum("ttl_streams"),
		hotPayloadBytes: hotPayload,
		hotRealBytes: hotReal,
		hotOverheadBytes: sum("hot_overhead_bytes"),
		livePacks: sum("live_packs"),
		sharedRefs: sum("shared_refs"),
		pendingColdGc: sum("pending_cold_gc"),
		snapshotBytes,
		logBytesSinceSnapshot: groups.reduce((n, g) => n + g.logBytesSinceSnapshot, 0),
		maxRssBytes: Math.max(0, ...nodes.map((n) => n.rssBytes)),
		rssBytes: nodes.map((n) => n.rssBytes),
		corruptions: nodes.reduce((n, node) => n + node.corruptions, 0),
		indexBytesPerHarness: harnessCount === 0 ? 0 : indexBytes / harnessCount,
		snapshotStatePerStream: snapshotState,
		residualPerStream: residual,
		residualExRefsPerStream: streams === 0 ? 0 : residual - (REF_BYTES * sum("shared_refs")) / streams,
		indexerLagMax: indexer.lagMax,
		indexerLagTotal: indexer.lagTotal,
	};
}

/** Least-squares slope of y over x. */
function slope(points: readonly (readonly [number, number])[]): number {
	const n = points.length;
	if (n < 2) return 0;
	const mx = points.reduce((s, [x]) => s + x, 0) / n;
	const my = points.reduce((s, [, y]) => s + y, 0) / n;
	let num = 0;
	let den = 0;
	for (const [x, y] of points) {
		num += (x - mx) * (y - my);
		den += (x - mx) ** 2;
	}
	return den === 0 ? 0 : num / den;
}

/** Accumulates counters across indexer restarts (a counter that drops means a fresh process). */
class Accumulator {
	private base = new Map<string, number>();
	private last = new Map<string, number>();
	add(key: string, value: number): number {
		const prev = this.last.get(key) ?? 0;
		if (value < prev) this.base.set(key, (this.base.get(key) ?? 0) + prev);
		this.last.set(key, value);
		return (this.base.get(key) ?? 0) + value;
	}
	total(key: string): number {
		return (this.base.get(key) ?? 0) + (this.last.get(key) ?? 0);
	}
}

function ownerSummary(): Sample["owners"] {
	const out = {} as Sample["owners"];
	for (const kind of ["fast", "slow", "long"] as const) {
		const group = harnesses.filter((h) => h.kind === kind);
		out[kind] = {
			harnesses: group.length,
			open: group.filter((h) => h.gauges().open).length,
			commits: group.reduce((n, h) => n + h.counters.commits, 0),
			appendBytes: group.reduce((n, h) => n + h.counters.appendBytes, 0),
			logBytes: group.reduce((n, h) => n + h.counters.appendBytes + h.preloadBytes, 0),
			turns: group.reduce((n, h) => n + h.counters.turns, 0),
			turnFailures: group.reduce((n, h) => n + h.counters.turnFailures, 0),
			takeovers: group.reduce((n, h) => n + h.counters.takeovers, 0),
			poisons: group.reduce((n, h) => n + h.metrics.poisons, 0),
			fences: group.reduce((n, h) => n + h.metrics.fences, 0),
			unexpectedPoison: group.reduce((n, h) => n + h.counters.unexpectedPoison, 0),
			sessionLineRemoteReads: group.reduce((n, h) => n + h.metrics.sessionLineRemoteReads, 0),
			maxPinnedBytes: Math.max(0, ...group.map((h) => h.gauges().pinnedBytes)),
			maxLagRecords: Math.max(0, ...group.map((h) => h.gauges().tail - h.gauges().overlayFloor)),
		};
	}
	return out;
}

it.runIf(s3Available())("soak: mixed harness population stays bounded, unpoisoned and within the S3 budget", async () => {
	const dir = resultsDir();
	const samplesFile = join(dir, "samples.jsonl");
	const log = (msg: string): void => {
		const line = `[soak ${new Date().toISOString()}] ${msg}`;
		console.log(line);
		appendFileSync(join(dir, "progress.log"), `${line}\n`);
	};
	stack = await Stack.start({ nodes: 3, s3: true, wal: "disk", featureLevel: LEVEL, publishIntervalMs: 5000, coldCadence: "default" });
	const s = stack;
	await s.createBucket(BUCKET);
	const population = { ...POPULATION, baseUrl: s.url };
	log(`stack up at ${s.url}, level ${await s.minFeatureLevel()}, results in ${dir}`);

	const runId = Date.now().toString(36);
	if (LONG > 0) {
		const history = await generateHistory(LONG_RECORDS);
		log(`generated a ${history.length}-record history`);
		for (let i = 0; i < LONG; i++) {
			const h = new SoakHarness("long", `${BUCKET}/long-${runId}-${i}`, 1000 + i, population);
			h.preloadBytes = await loadHistory(s.url, h.stream, history);
			harnesses.push(h);
		}
		log(`loaded ${LONG} long histories`);
	}
	for (let i = 0; i < FAST; i++) harnesses.push(new SoakHarness("fast", `${BUCKET}/fast-${runId}-${i}`, 2000 + i, population));
	for (let i = 0; i < SLOW; i++) harnesses.push(new SoakHarness("slow", `${BUCKET}/slow-${runId}-${i}`, 3000 + i, population));
	for (const h of harnesses) h.start(h.kind === "slow" ? Math.random() * POPULATION.slowIntervalMs[1] : Math.random() * 2000);

	const indexerTotals = new Accumulator();
	const nsTotals = new Accumulator();
	const nsIncarnation = new Map<string, number>();
	const samples: Sample[] = [];
	let minioBase: Record<string, number> | undefined;
	const takeSample = async (elapsedS: number): Promise<Sample> => {
		const snapshotTriggers = await forceSnapshots(s);
		const nodes = await sampleNodes(s);
		const indexerRssBytes = await rssOf(s.indexer?.proc.child.pid);
		const indexer = await sampleIndexer(s.indexer?.url ?? "");
		for (const k of ["put", "get", "head", "list", "delete"] as const) indexerTotals.add(k, indexer.s3[k]);
		indexerTotals.add("publishes", indexer.publishes);
		indexerTotals.add("compactionPublishes", indexer.compactionPublishes);
		for (const ns of indexer.perNamespace) {
			nsIncarnation.set(ns.key, ns.incarnation);
			for (const k of ["put", "get", "head", "list", "delete"] as const) nsTotals.add(`${ns.key}|${k}`, ns.s3[k]);
		}
		const minio = s.s3 === undefined ? undefined : await sampleMinio(s.s3.endpoint);
		const owners = ownerSummary();
		const logBytes = owners.fast.logBytes + owners.slow.logBytes + owners.long.logBytes;
		const sample: Sample = { at: new Date().toISOString(), elapsedS, nodes, indexer, indexerRssBytes, minio, owners, snapshotTriggers, derived: derive(nodes, indexer, logBytes) };
		appendFileSync(samplesFile, `${JSON.stringify(sample)}\n`);
		const d = sample.derived;
		log(
			`t=${elapsedS}s streams=${d.streams} coldMiB=${d.coldMiB.toFixed(1)} marks=${d.recordMarks}/${d.markBound} dense=${d.denseRecordEntries} hotRecords=${d.hotRecords} sharedRefsMax=${d.maxSharedRefsPerStream} staged=${d.stagedExternalRefs} snapshot=${d.snapshotBytes} state/stream=${d.snapshotStatePerStream.toFixed(0)} rss=${d.rssBytes.map((r) => (r / MIB).toFixed(0)).join("/")}MiB snapshots=${snapshotTriggers.filter((t) => t.status === 200).length}/${snapshotTriggers.length} lag=${d.indexerLagMax} commits=${owners.fast.commits + owners.slow.commits + owners.long.commits} poison=${owners.fast.unexpectedPoison + owners.slow.unexpectedPoison + owners.long.unexpectedPoison}`,
		);
		return sample;
	};

	const start = Date.now();
	minioBase = s.s3 === undefined ? undefined : await sampleMinio(s.s3.endpoint);
	samples.push(await takeSample(0));
	let nextSample = SAMPLE_S;
	let nextTakeover = TAKEOVER_S;
	let takeoverIndex = 0;
	let indexerRestarted = false;
	const takeovers: Promise<void>[] = [];
	for (;;) {
		const elapsed = (Date.now() - start) / 1000;
		if (elapsed >= DURATION_S) break;
		if (TAKEOVER_S > 0 && elapsed >= nextTakeover) {
			nextTakeover += TAKEOVER_S;
			// Rotate through fast, slow and long harnesses.
			const kinds: HarnessKind[] = ["fast", "slow", "long"];
			const kind = kinds[takeoverIndex % kinds.length] as HarnessKind;
			const candidates = harnesses.filter((h) => h.kind === kind);
			const victim = candidates[Math.floor(takeoverIndex / kinds.length) % Math.max(1, candidates.length)];
			takeoverIndex++;
			if (victim !== undefined) {
				log(`takeover of ${victim.stream}`);
				takeovers.push(victim.requestTakeover());
			}
		}
		if (INDEXER_RESTART_S > 0 && !indexerRestarted && elapsed >= INDEXER_RESTART_S) {
			indexerRestarted = true;
			log("restarting the indexer");
			await s.restartIndexer();
		}
		if (elapsed >= nextSample) {
			samples.push(await takeSample(Math.round(elapsed)));
			nextSample += SAMPLE_S;
		}
		await sleep(1000);
	}
	const durationH = (Date.now() - start) / 3_600_000;
	log("stopping the population");
	await Promise.race([Promise.all(takeovers), sleep(120_000)]);
	await Promise.all(harnesses.map((h) => h.stop()));
	const last = await takeSample(Math.round((Date.now() - start) / 1000));
	samples.push(last);

	// Spot-check keyed-state against the fold.
	const verifyTargets = (["long", "fast", "slow"] as const).flatMap((kind) => {
		const group = harnesses.filter((h) => h.kind === kind);
		const taken = group.filter((h) => h.counters.takeovers > 0);
		return [...new Set([...taken, ...group])].slice(0, Math.max(VERIFY_EACH, taken.length));
	});
	const verifications: { stream: string; ok: boolean; incarnation: number | undefined; report: string }[] = [];
	for (const h of verifyTargets) {
		const incarnation = nsIncarnation.get(h.stream);
		if (incarnation === undefined) {
			verifications.push({ stream: h.stream, ok: false, incarnation, report: "no indexer namespace seen" });
			continue;
		}
		const args = ["indexer", "keyed", "verify", "--s3-bucket", s.s3Bucket, "--keyed-s3-root", s.s3Root, "--s3-endpoint", s.s3?.endpoint ?? "", "--s3-region", s.s3?.region ?? "us-east-1"];
		args.push("--bucket", BUCKET, "--stream", h.stream.slice(BUCKET.length + 1), "--incarnation", String(incarnation), "--source-url", s.url);
		try {
			const out = await run(ursulaBinary(), args, {
				env: { ...process.env, AWS_ACCESS_KEY_ID: s.s3?.accessKey, AWS_SECRET_ACCESS_KEY: s.s3?.secretKey, AWS_REGION: s.s3?.region },
				maxBuffer: 64 * MIB,
				timeout: 600_000,
			});
			verifications.push({ stream: h.stream, ok: true, incarnation, report: out.stdout.trim().slice(0, 2000) });
		} catch (error) {
			const e = error as { stdout?: string; stderr?: string; message?: string };
			verifications.push({ stream: h.stream, ok: false, incarnation, report: `${e.stdout ?? ""}\n${e.stderr ?? e.message ?? ""}`.trim().slice(0, 4000) });
		}
	}

	// S3 budget. Projection: per-namespace indexer counters; log: MinIO totals minus the indexer's.
	const perKind = (kind: HarnessKind): { putPerHour: number; getPerHour: number; maxPutPerHour: number; maxGetPerHour: number } => {
		const group = harnesses.filter((h) => h.kind === kind);
		const puts = group.map((h) => nsTotals.total(`${h.stream}|put`) + nsTotals.total(`${h.stream}|list`));
		const gets = group.map((h) => nsTotals.total(`${h.stream}|get`) + nsTotals.total(`${h.stream}|head`));
		const avg = (xs: number[]): number => (xs.length === 0 ? 0 : xs.reduce((a, b) => a + b, 0) / xs.length);
		return { putPerHour: avg(puts) / durationH, getPerHour: avg(gets) / durationH, maxPutPerHour: Math.max(0, ...puts) / durationH, maxGetPerHour: Math.max(0, ...gets) / durationH };
	};
	const minioDelta: Record<string, number> = {};
	for (const [api, n] of Object.entries(last.minio ?? {})) minioDelta[api] = n - (minioBase?.[api] ?? 0);
	const total = classifyS3(minioDelta);
	const idx = { put: indexerTotals.total("put") + indexerTotals.total("list"), get: indexerTotals.total("get") + indexerTotals.total("head"), delete: indexerTotals.total("delete") };
	const active = harnesses.length;
	const logS3 = { putPerHarnessHour: (total.put - idx.put) / active / durationH, getPerHarnessHour: (total.get - idx.get) / active / durationH };

	// State: residual per stream after warm-up, regressed over hours.
	const steady = samples.filter((x) => x.elapsedS >= WARMUP_S);
	const residualSlope = slope(steady.map((x) => [x.elapsedS / 3600, x.derived.residualExRefsPerStream] as const));
	const rawResidualSlope = slope(steady.map((x) => [x.elapsedS / 3600, x.derived.residualPerStream] as const));
	const indexSlope = slope(steady.map((x) => [x.elapsedS / 3600, x.derived.indexBytesPerHarness - (REF_BYTES * x.derived.sharedRefs) / Math.max(1, x.derived.harnesses)] as const));
	const coldSlope = slope(steady.map((x) => [x.elapsedS / 3600, x.derived.coldMiB / Math.max(1, x.derived.harnesses)] as const));
	const rssSlope = slope(steady.map((x) => [x.elapsedS / 3600, x.derived.maxRssBytes] as const));
	const structural = samples.map((x) => ({
		elapsedS: x.elapsedS,
		marksWithinBound: x.derived.recordMarks <= x.derived.markBound,
		denseWithinHot: x.derived.denseRecordEntries <= x.derived.hotRecords + x.derived.streams,
		sharedRefsWithinCap: x.derived.maxSharedRefsPerStream <= LIMITS.sharedRefsPerStream,
		stagedWithinCap: x.derived.stagedExternalRefs <= LIMITS.stagedRefsPerStream * Math.max(1, x.derived.streams),
		ttlHeapWithinCap: x.derived.ttlHeapEntries <= 2 * x.derived.ttlStreams,
		// Checked after warm-up only: the first cold flushes (5 min hot age) land between a snapshot build
		// and the gauge scrape and skew (snapshot − hot payload) by the bytes they move.
		stateWithinFormula: x.elapsedS < WARMUP_S || x.derived.residualPerStream <= LIMITS.residualBytesPerStream,
	}));
	const owners = ownerSummary();
	const sumOwners = (k: keyof Sample["owners"]["fast"]): number => owners.fast[k] + owners.slow[k] + owners.long[k];
	const faulted = harnesses.reduce((n, h) => n + h.counters.faulted, 0);
	const tasks = harnesses.reduce((n, h) => n + h.counters.tasks, 0);
	const streaming = { fast: perKind("fast"), long: perKind("long"), slow: perKind("slow") };

	const criteria = {
		stateFlat: residualSlope <= LIMITS.residualSlopeBytesPerHarnessHour && indexSlope <= 16 * Math.max(0, coldSlope) + LIMITS.residualSlopeBytesPerHarnessHour,
		structuralBounds: structural.every((x) => x.marksWithinBound && x.denseWithinHot && x.sharedRefsWithinCap && x.stagedWithinCap && x.ttlHeapWithinCap && x.stateWithinFormula),
		noPoison: sumOwners("unexpectedPoison") === 0,
		noFaultedTask: faulted === 0,
		noCorruption: samples.every((x) => x.derived.corruptions === 0),
		keyedStateEqualsFold: verifications.length > 0 && verifications.every((v) => v.ok),
		projectionS3WithinBudget: [streaming.fast, streaming.long].every((k) => k.maxPutPerHour <= LIMITS.projectionPutPerHour && k.maxGetPerHour <= LIMITS.projectionGetPerHour),
		logS3WithinBudget: logS3.putPerHarnessHour <= LIMITS.logPutPerHour && logS3.getPerHarnessHour <= LIMITS.logGetPerHour,
	};
	const summary = {
		knobs: { DURATION_S, SAMPLE_S, FAST, SLOW, LONG, LONG_RECORDS, TAKEOVER_S, INDEXER_RESTART_S, WARMUP_S, LEVEL, population: POPULATION },
		durationH,
		limits: LIMITS,
		criteria,
		pass: Object.values(criteria).every(Boolean),
		owners,
		tasks,
		faulted,
		state: { residualSlopeBytesPerStreamHour: residualSlope, rawResidualSlopeBytesPerStreamHour: rawResidualSlope, indexBytesPerHarnessSlopePerHour: indexSlope, coldMiBPerHarnessSlopePerHour: coldSlope, maxRssSlopeBytesPerHour: rssSlope, first: samples[0]?.derived, last: last.derived, structural },
		s3: { minioDelta, total, indexer: idx, indexerPublishes: indexerTotals.total("publishes"), indexerCompactionPublishes: indexerTotals.total("compactionPublishes"), projectionPerHarnessHour: streaming, log: logS3 },
		verifications,
		errors: harnesses.filter((h) => h.errors.length > 0).map((h) => ({ stream: h.stream, errors: h.errors.slice(-5) })),
	};
	writeFileSync(join(dir, "summary.json"), `${JSON.stringify(summary, null, 2)}\n`);
	log(`summary: ${JSON.stringify({ pass: summary.pass, criteria })}`);
	if (ASSERT) expect(criteria).toEqual(Object.fromEntries(Object.keys(criteria).map((k) => [k, true])));
});

afterAll(async () => {
	await Promise.all(harnesses.map((h) => h.stop().catch(() => undefined)));
	await stack?.stop();
});
