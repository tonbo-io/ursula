// The soak's harness population (keyed streams design §9.1, §10 M4 soak): real Pi Harnesses on the
// bounded owner over HTTP, each with its own faux model so concurrent turns never share a response
// queue.
//
// - fast: profile (a), streaming text and tool turns back to back (partials re-armed by Pi's
//   throttle), a short pause between turns, a context reset every 30 turns.
// - slow: profile (c), one non-streaming text turn every 20–40 s.
// - long: profile (d), a stream preloaded with a long history, then fast-shaped turns.
//
// Takeover (§3.6, §11.7): between two turns a second owner opens the same stream in `fence` mode;
// the old Harness is closed (its writes now fail as fenced) and the new one carries on. Poison that
// is not a fence of a replaced owner reopens in `fence` mode, as a host does, and is counted.
import { Type } from "@earendil-works/pi-ai";
import { createModels } from "@earendil-works/pi-ai/models";
import { fauxAssistantMessage, fauxProvider, fauxText, fauxToolCall } from "@earendil-works/pi-ai/providers/faux";
import { type Conversation, createRegistry, defineExtension, defineTool, Harness } from "@earendil-works/pi-durable";
import { httpTransports } from "../../src/http.ts";
import { OwnerMetrics } from "../../src/metrics.ts";
import { type OpenMode, UrsulaStorage } from "../../src/storage.ts";
import type { LogTransport } from "../../src/transport.ts";
import { faultedTasks } from "../harness-kit.ts";
import { ctx } from "../helpers.ts";
import { sleep } from "../stack/proc.ts";

export type HarnessKind = "fast" | "slow" | "long";

const echo = defineTool({
	name: "echo",
	description: "Echo the text",
	parameters: Type.Object({ text: Type.String() }),
	execute: async (args) => ({ content: [{ type: "text", text: args.text }] }),
});
const registry = createRegistry();
registry.install(defineExtension({ name: "echo", tools: [echo] }));

const TURNS_PER_CONTEXT = 30;
const WORDS = "the quick brown fox jumps over a lazy dog while durable streams keep every record in order".split(" ");

/** Deterministic pseudo-random numbers per harness (mulberry32). */
function rng(seed: number): () => number {
	let a = seed >>> 0;
	return () => {
		a = (a + 0x6d2b79f5) >>> 0;
		let t = a;
		t = Math.imul(t ^ (t >>> 15), t | 1);
		t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
}

export interface PopulationOptions {
	readonly baseUrl: string;
	/** Streaming speed of fast and long harnesses (faux tokens per second). */
	readonly tokensPerSecond: number;
	/** Characters of one streamed answer. */
	readonly answerChars: number;
	/** Pause between turns of fast and long harnesses. */
	readonly fastPauseMs: number;
	readonly longPauseMs: number;
	/** Slow harnesses: one turn every [min, max) ms. */
	readonly slowIntervalMs: readonly [number, number];
}

export interface HarnessCounters {
	turns: number;
	turnFailures: number;
	commits: number;
	appendBytes: number;
	opens: number;
	takeovers: number;
	/** Poison that was not the fence of a replaced owner. */
	unexpectedPoison: number;
	faulted: number;
	tasks: number;
}

/** One live harness. */
export class SoakHarness {
	readonly kind: HarnessKind;
	readonly stream: string;
	/** Shared by every open of this harness: poisons, fences, remote reads. */
	readonly metrics = new OwnerMetrics();
	readonly counters: HarnessCounters = { turns: 0, turnFailures: 0, commits: 0, appendBytes: 0, opens: 0, takeovers: 0, unexpectedPoison: 0, faulted: 0, tasks: 0 };
	readonly errors: string[] = [];
	/** Bytes of the log before the soak (long-history preload). */
	preloadBytes = 0;
	private storage: UrsulaStorage | undefined;
	private harness: Harness | undefined;
	private root: Conversation | undefined;
	private readonly faux;
	private readonly models;
	private readonly agent;
	private readonly options: PopulationOptions;
	private readonly random: () => number;
	private stopped = false;
	private takeoverWanted: (() => void) | undefined;
	private loop: Promise<void> | undefined;
	/** Fences expected from owners this harness replaced. */
	private expectedFences = 0;

	constructor(kind: HarnessKind, stream: string, seed: number, options: PopulationOptions) {
		this.kind = kind;
		this.stream = stream;
		this.options = options;
		this.random = rng(seed);
		this.faux = kind === "slow" ? fauxProvider() : fauxProvider({ tokensPerSecond: options.tokensPerSecond, tokenSize: { min: 3, max: 8 } });
		this.models = createModels();
		this.models.setProvider(this.faux.provider);
		const model = this.faux.getModel();
		this.agent = { model: { provider: model.provider, modelId: model.id } };
	}

	private transports(): { log: LogTransport; keyedState: ReturnType<typeof httpTransports>["keyedState"] } {
		const t = httpTransports({ baseUrl: this.options.baseUrl, stream: this.stream });
		const log: LogTransport = {
			head: () => t.log.head(),
			create: () => t.log.create(),
			append: async (body, match) => {
				const outcome = await t.log.append(body, match);
				if (outcome.status >= 200 && outcome.status < 300) {
					this.counters.commits++;
					this.counters.appendBytes += body.byteLength;
				}
				return outcome;
			},
			readRecords: (from, o) => t.log.readRecords(from, o),
		};
		return { log, keyedState: t.keyedState };
	}

	private async open(mode: OpenMode, host: string): Promise<void> {
		const storage = await UrsulaStorage.open({
			...this.transports(),
			stateStore: "bounded",
			requireKeyedBatch: true,
			mode,
			host,
			pid: process.pid,
			metrics: this.metrics,
		});
		this.counters.opens++;
		const harness = await Harness.open(storage, { models: this.models, registry }, ctx);
		const root = await harness.root(ctx, { agent: this.agent });
		harness.resume();
		this.storage = storage;
		this.harness = harness;
		this.root = root;
	}

	private note(error: unknown): void {
		this.errors.push(`${new Date().toISOString()} ${error instanceof Error ? `${error.name}: ${error.message}` : String(error)}`);
		if (this.errors.length > 20) this.errors.shift();
	}

	private async closeCurrent(): Promise<void> {
		const { harness, storage } = this;
		this.harness = undefined;
		this.root = undefined;
		this.storage = undefined;
		await harness?.close(ctx).catch(() => undefined);
		await storage?.close(ctx).catch(() => undefined);
	}

	private answer(): string {
		const words: string[] = [];
		let n = 0;
		while (n < this.options.answerChars) {
			const w = WORDS[Math.floor(this.random() * WORDS.length)] ?? "x";
			words.push(w);
			n += w.length + 1;
		}
		return words.join(" ");
	}

	private async turn(n: number): Promise<void> {
		const root = this.root;
		if (root === undefined) throw new Error("not open");
		const tool = this.kind !== "slow" && n % 3 === 2;
		if (tool) {
			this.faux.appendResponses([fauxAssistantMessage([fauxText("calling"), fauxToolCall("echo", { text: `t${n}` })]), fauxAssistantMessage(this.answer())]);
		} else {
			this.faux.appendResponses([fauxAssistantMessage(this.kind === "slow" ? `answer ${n}` : this.answer())]);
		}
		const settled = await (await root.submit({ type: "input", content: tool ? `use the tool ${n}` : `question ${n}` }, ctx)).wait(ctx);
		if (settled.status !== "done") throw new Error(`turn ${n} settled ${settled.status}`);
		if (n % TURNS_PER_CONTEXT === TURNS_PER_CONTEXT - 1) await root.reset(undefined, ctx);
	}

	private pauseMs(): number {
		if (this.kind === "fast") return this.options.fastPauseMs;
		if (this.kind === "long") return this.options.longPauseMs;
		const [min, max] = this.options.slowIntervalMs;
		return min + this.random() * (max - min);
	}

	/** Starts the turn loop (opens first, `fail-if-active`). Slow harnesses start at a random phase. */
	start(initialDelayMs: number): void {
		this.loop = this.run(initialDelayMs);
	}

	private async run(initialDelayMs: number): Promise<void> {
		await this.sleepUnlessStopped(initialDelayMs);
		let n = 0;
		while (!this.stopped) {
			if (this.storage === undefined) {
				try {
					await this.open(this.counters.opens === 0 ? "fail-if-active" : "fence", "soak");
				} catch (error) {
					this.note(error);
					await this.sleepUnlessStopped(1000);
				}
				continue;
			}
			const wanted = this.takeoverWanted;
			if (wanted !== undefined) {
				this.takeoverWanted = undefined;
				await this.takeover().catch((error: unknown) => this.note(error));
				wanted();
				continue;
			}
			try {
				await this.turn(n++);
				this.counters.turns++;
			} catch (error) {
				this.counters.turnFailures++;
				this.note(error);
				if (this.storage?.poison !== undefined) {
					this.countPoison();
					await this.closeCurrent();
				}
			}
			await this.sleepUnlessStopped(this.pauseMs());
		}
	}

	private countPoison(): void {
		// Poison counted by the shared metrics; fences of replaced owners are expected.
		const unexpected = this.metrics.poisons - Math.min(this.metrics.fences, this.expectedFences);
		this.counters.unexpectedPoison = Math.max(this.counters.unexpectedPoison, unexpected);
	}

	private async sleepUnlessStopped(ms: number): Promise<void> {
		const end = Date.now() + ms;
		while (!this.stopped && Date.now() < end) await sleep(Math.min(250, end - Date.now()));
	}

	/** A second owner fences this one between two turns; the old Harness is closed. */
	private async takeover(): Promise<void> {
		const old = { harness: this.harness, storage: this.storage };
		const fresh = await UrsulaStorage.open({
			...this.transports(),
			stateStore: "bounded",
			requireKeyedBatch: true,
			mode: "fence",
			host: `soak-takeover-${this.counters.takeovers + 1}`,
			pid: process.pid,
			metrics: this.metrics,
		});
		this.expectedFences++;
		this.counters.takeovers++;
		// The replaced owner's close marker is the write that observes the fence.
		await old.harness?.close(ctx).catch(() => undefined);
		await old.storage?.close(ctx).catch(() => undefined);
		this.counters.opens++;
		const harness = await Harness.open(fresh, { models: this.models, registry }, ctx);
		const root = await harness.root(ctx, { agent: this.agent });
		harness.resume();
		this.storage = fresh;
		this.harness = harness;
		this.root = root;
	}

	/** Asks for a takeover at the next turn boundary and resolves when it is done. */
	requestTakeover(): Promise<void> {
		return new Promise((resolve) => {
			this.takeoverWanted = resolve;
		});
	}

	/** Owner gauges of the current storage. */
	gauges(): { open: boolean; tail: number; overlayFloor: number; pinnedBytes: number; cacheBytes: number } {
		const m = this.storage?.metrics();
		return { open: m !== undefined, tail: m?.tail ?? 0, overlayFloor: m?.overlayFloor ?? 0, pinnedBytes: m?.pinnedBytes ?? 0, cacheBytes: m?.cacheBytes ?? 0 };
	}

	/** Stops the loop, waits for the harness to go idle, counts faulted tasks, closes. */
	async stop(): Promise<void> {
		this.stopped = true;
		await this.loop;
		this.countPoison();
		const { storage, harness } = this;
		if (storage !== undefined && harness !== undefined && storage.poison === undefined) {
			try {
				await harness.waitForIdle(ctx);
				const tasks = await faultedTasks(storage);
				this.counters.tasks = tasks.total;
				this.counters.faulted = tasks.faulted.length;
			} catch (error) {
				this.note(error);
			}
		}
		await this.closeCurrent();
		this.countPoison();
	}
}
