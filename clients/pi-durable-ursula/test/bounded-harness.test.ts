// A real Pi Harness on the bounded owner (design §10 M3 exit): after reopening a harness with
// history and one warm-up turn, steady-state text and tool turns issue zero remote reads on the
// Session line, and no task is ever faulted, under every indexer mode. The keyed-state transport is
// instrumented: flush-waits and finalize requests (point reads of `m/owner` with a wait) are the
// background loop's; every other keyed-state request is a Session-line (or open) read.
import { Type } from "@earendil-works/pi-ai";
import { createModels } from "@earendil-works/pi-ai/models";
import { fauxAssistantMessage, fauxProvider, fauxText, fauxToolCall } from "@earendil-works/pi-ai/providers/faux";
import { createRegistry, defineExtension, defineTool, Harness, type Conversation } from "@earendil-works/pi-durable";
import { describe, expect, it } from "vitest";
import { K, META } from "../src/families.ts";
import type { KeyedScanRequest, KeyedStateTransport } from "../src/transport.ts";
import { b64 } from "../src/tuple.ts";
import { BOUNDED_TIMING, fakeFor, INDEXER_MODES } from "./bounded-helpers.ts";
import { ctx, freshPath, openOn } from "./helpers.ts";
import type { UrsulaStorage } from "../src/storage.ts";
import type { FakeUrsula } from "../src/fake/index.ts";

const OWNER = b64(K.m(META.owner));
const isBackground = (r: KeyedScanRequest): boolean => r.key === OWNER && r.timeoutMs !== undefined;

/** Counts keyed-state requests by class. */
class CountingKeyedState implements KeyedStateTransport {
	sessionLine = 0;
	background = 0;
	private readonly inner: KeyedStateTransport;
	constructor(inner: KeyedStateTransport) {
		this.inner = inner;
	}
	scan(request: KeyedScanRequest): ReturnType<KeyedStateTransport["scan"]> {
		if (isBackground(request)) this.background++;
		else this.sessionLine++;
		return this.inner.scan(request);
	}
}

const faux = fauxProvider();
const models = createModels();
models.setProvider(faux.provider);
const model = faux.getModel();

const echo = defineTool({
	name: "echo",
	description: "Echo the text",
	parameters: Type.Object({ text: Type.String() }),
	execute: async (args) => ({ content: [{ type: "text", text: args.text }] }),
});
const registry = createRegistry();
registry.install(defineExtension({ name: "echo", tools: [echo] }));

async function textTurn(root: Conversation, n: number): Promise<void> {
	faux.appendResponses([fauxAssistantMessage(`answer ${n}`)]);
	const settled = await (await root.submit({ type: "input", content: `question ${n}` }, ctx)).wait(ctx);
	expect(settled.status).toBe("done");
}

async function toolTurn(root: Conversation, n: number): Promise<void> {
	faux.appendResponses([fauxAssistantMessage([fauxText("calling"), fauxToolCall("echo", { text: `t${n}` })]), fauxAssistantMessage(`tool answer ${n}`)]);
	const settled = await (await root.submit({ type: "input", content: `use the tool ${n}` }, ctx)).wait(ctx);
	expect(settled.status).toBe("done");
}

async function open(fake: FakeUrsula, path: string): Promise<{ storage: UrsulaStorage; counter: CountingKeyedState; harness: Harness; root: Conversation }> {
	const counter = new CountingKeyedState(fake.keyedStateTransport(path));
	const storage = await openOn(fake, path, { stateStore: "bounded", keyedState: counter, timing: BOUNDED_TIMING });
	const harness = await Harness.open(storage, { models, registry }, ctx);
	const root = await harness.root(ctx, { agent: { model: { provider: model.provider, modelId: model.id } } });
	harness.resume();
	return { storage, counter, harness, root };
}

/** Every task in the store must have settled without a fault. */
async function expectNoFaultedTask(storage: UrsulaStorage): Promise<void> {
	let cursor: Parameters<UrsulaStorage["scanTasks"]>[2];
	let total = 0;
	do {
		const page = await storage.scanTasks({}, 100, cursor, ctx);
		for (const t of page.items) {
			total++;
			if (t.state.status === "terminal") expect(t.state.outcome.status, `task ${t.id} (${t.kind})`).not.toBe("faulted");
		}
		cursor = page.next;
	} while (cursor !== undefined);
	expect(total).toBeGreaterThan(0);
}

describe("steady-state turns on the bounded owner", () => {
	for (const mode of INDEXER_MODES) {
		it(`issue zero Session-line remote reads after warm-up, with no faulted task (indexer ${mode})`, async () => {
			const fake = fakeFor(mode);
			const path = freshPath();
			// History written by a previous owner.
			{
				const s = await open(fake, path);
				await textTurn(s.root, 1);
				await toolTurn(s.root, 2);
				await textTurn(s.root, 3);
				await s.harness.close(ctx);
				await s.storage.close(ctx);
			}
			const s = await open(fake, path);
			// Warm-up: the first turns after open may read remotely.
			await textTurn(s.root, 4);
			await toolTurn(s.root, 5);
			const before = s.counter.sessionLine;
			const remoteBefore = s.storage.localStore?.metrics.remoteReads ?? 0;
			for (let n = 6; n < 12; n++) {
				if (n % 2 === 0) await textTurn(s.root, n);
				else await toolTurn(s.root, n);
			}
			// Open and warm-up did read keyed-state; the flush loop raised E unless the indexer is paused.
			expect(before).toBeGreaterThan(0);
			if (mode !== "paused") expect(s.storage.localStore?.overlayFloor ?? 0).toBeGreaterThan(0);
			expect(s.counter.sessionLine - before).toBe(0);
			expect((s.storage.localStore?.metrics.remoteReads ?? 0) - remoteBefore).toBe(0);
			await s.harness.waitForIdle(ctx);
			await expectNoFaultedTask(s.storage);
			expect(s.storage.poison).toBeUndefined();
			await s.harness.close(ctx);
			await s.storage.close(ctx);
		});
	}
});
