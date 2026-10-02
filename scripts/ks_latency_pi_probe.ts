// Keyed-streams M0c: drive a real Pi Durable Harness whose Storage.commit is a
// Stream-Record-Match append to an Ursula cluster (one stream per harness, one
// commit in flight). Not part of any build; it needs a Pi checkout and a module
// loader that maps the @earendil-works/* imports onto that checkout (see
// docs/architecture/keyed-streams-latency-m0c.md, "Pi probe"). Adjust the
// Harness import below to your checkout.
//
//   TARGET=http://127.0.0.1:15440 N=16 TURNS=10 \
//     node --experimental-strip-types --import ./register.mjs ks_latency_pi_probe.ts
//
// M0c: Pi Harness whose commits are real Stream-Record-Match appends to a local Ursula cluster.
// M0c: Pi Harness whose commits are real Stream-Record-Match appends to a local Ursula cluster.
// N-1 background conversations stream long replies (partial sources); conversation 0 then
// submits TURNS short turns one after another and we record submit -> provider request.
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import { createModels } from "@earendil-works/pi-ai/models";
import { fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai/providers/faux";
import { type Conversation, createRegistry, Harness, MemoryStorage, type Storage } from "../pi/packages/durable/src/index.ts";

const N = Number(process.env.N ?? 4);
const TURNS = Number(process.env.TURNS ?? 10);
const TPS = Number(process.env.TPS ?? 100);
const BG_CHARS = Number(process.env.BG_CHARS ?? 16000);
const TARGET = process.env.TARGET ?? "http://127.0.0.1:15400";
const BUCKET = process.env.BUCKET ?? "pi-probe";
const STREAM = process.env.STREAM ?? `h-${Date.now()}`;
const MODE = process.env.MODE ?? "remote"; // remote | none

const url = `${TARGET}/${BUCKET}/${STREAM}`;
if (MODE === "remote") {
	await fetch(`${TARGET}/${BUCKET}`, { method: "PUT" });
	for (let attempt = 0; ; attempt++) {
		const r = await fetch(url, { method: "PUT", headers: { "content-type": "application/json" } });
		await r.arrayBuffer();
		if (r.ok || r.status === 409) break;
		if ((r.status === 503 || r.status === 429) && attempt < 100) { await new Promise((res) => setTimeout(res, 20)); continue; }
		throw new Error(`create ${url}: ${r.status}`);
	}
}

let nextRecord = 0;
let seq = 0;
let measuring = false;
let backpressureRetries = 0;
const commitLat: number[] = [];
const commitSizes: number[] = [];
const commitAt: { t: number; partial: boolean }[] = [];

function replacer(_k: string, v: unknown) {
	if (typeof v === "bigint") return v.toString();
	if (v instanceof Uint8Array) return Buffer.from(v).toString("base64");
	return v;
}

function wrap(storage: Storage): Storage {
	return new Proxy(storage, {
		get(target, prop) {
			const value = Reflect.get(target, prop, target);
			if (typeof value !== "function") return value;
			if (prop === "commit") {
				return async (writes: any[], ctx: unknown) => {
					const body = JSON.stringify({ o: seq++, ops: writes }, replacer);
					const t0 = performance.now();
					if (MODE === "remote") {
						let r: Response;
						for (;;) {
							r = await fetch(url, {
								method: "POST",
								headers: { "content-type": "application/json", "stream-record-match": String(nextRecord) },
								body,
							});
							await r.arrayBuffer();
							if (r.status !== 503 && r.status !== 429) break;
							backpressureRetries++;
							await new Promise((res) => setTimeout(res, 20));
						}
						if (!r.ok) throw new Error(`append ${r.status} match=${nextRecord} next=${r.headers.get("stream-record-next")}`);
						nextRecord = Number(r.headers.get("stream-record-next"));
					}
					const t1 = performance.now();
					const out = await value.call(target, writes, ctx);
					if (measuring) {
						commitLat.push(t1 - t0);
						commitSizes.push(Buffer.byteLength(body));
						const partial = writes.length === 1 && writes[0].type === "document.change" && writes[0].content.kind === "delta";
						commitAt.push({ t: performance.now(), partial });
					}
					return out;
				};
			}
			if (prop === "mintId" || prop === "close") return value.bind(target);
			return (...args: unknown[]) => value.apply(target, args);
		},
	}) as Storage;
}

const faux = fauxProvider({ tokensPerSecond: TPS });
const models = createModels();
const requestAt: number[] = [];
const origStream = models.streamSimple.bind(models);
(models as any).streamSimple = (...a: any[]) => {
	requestAt.push(performance.now());
	return (origStream as any)(...a);
};
models.setProvider(faux.provider);
const harness = await Harness.open(wrap(new MemoryStorage()), { models, registry: createRegistry() }, context);
const agent = { model: { provider: "faux", modelId: "faux-1" } } as const;
const convs: Conversation[] = [await harness.root(context, { agent })];
for (let i = 1; i < N; i++) convs.push(await harness.createConversation({ ownership: { kind: "ownerless" }, agent }, context));

const bgText = "lorem ipsum dolor sit amet ".repeat(Math.ceil(BG_CHARS / 27)).slice(0, BG_CHARS);
const shortText = "ok ".repeat(40);
measuring = true;
const bgStarted = performance.now();
faux.setResponses(Array.from({ length: N - 1 }, () => fauxAssistantMessage(bgText)));
const bg = convs.slice(1).map(async (c) => {
	const s = await c.submit({ type: "input", content: "go" }, context);
	await s.wait(context);
});
// Let the background conversations reach steady streaming.
while (faux.getPendingResponseCount() > 0) await new Promise((r) => setTimeout(r, 10));
await new Promise((r) => setTimeout(r, 500));

const submitToProvider: number[] = [];
const blockingBeforeProvider: number[] = [];
for (let t = 0; t < TURNS; t++) {
	faux.appendResponses([fauxAssistantMessage(shortText)]);
	const before = requestAt.length;
	const t0 = performance.now();
	const nonPartialBefore = commitAt.filter((c) => !c.partial).length;
	const s = await convs[0]!.submit({ type: "input", content: `turn ${t}` }, context);
	const waitReq = (async () => {
		while (requestAt.length === before) await new Promise((r) => setTimeout(r, 0));
		return requestAt[before]! - t0;
	})();
	submitToProvider.push(await waitReq);
	blockingBeforeProvider.push(commitAt.filter((c) => !c.partial).length - nonPartialBefore);
	await s.wait(context);
}
const fgEnd = performance.now();
const bgCommitsDuringFg = commitAt.filter((c) => c.partial).length;
await Promise.all(bg);
measuring = false;
await harness.close(context);

const pct = (xs: number[], p: number) => {
	const s = [...xs].sort((a, b) => a - b);
	return Math.round((s[Math.min(s.length - 1, Math.floor(s.length * p))] ?? 0) * 10) / 10;
};
const mean = (xs: number[]) => Math.round((xs.reduce((a, b) => a + b, 0) / Math.max(1, xs.length)) * 10) / 10;
const window = (fgEnd - bgStarted) / 1000;
console.log(
	JSON.stringify({
		N,
		MODE,
		turns: TURNS,
		submitToProviderMs: { mean: mean(submitToProvider), p50: pct(submitToProvider, 0.5), max: pct(submitToProvider, 1), all: submitToProvider.map((x) => Math.round(x)) },
		commitL: { count: commitLat.length, mean: mean(commitLat), p50: pct(commitLat, 0.5), p99: pct(commitLat, 0.99), max: pct(commitLat, 1) },
		commitBytes: { mean: mean(commitSizes), p50: pct(commitSizes, 0.5), p99: pct(commitSizes, 0.99), max: pct(commitSizes, 1) },
		commitsPerSecDuringFg: Math.round(commitAt.filter((c) => c.t <= fgEnd).length / window),
		partialCommits: bgCommitsDuringFg,
		backpressureRetries,
		nonPartialCommitsBeforeProvider: blockingBeforeProvider,
		stream: STREAM,
	}),
);
