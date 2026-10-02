// A real Pi Harness over UrsulaStorage, shared by the fake-backed and e2e suites and the benchmarks:
// a faux model, an `echo` tool, text and tool turns, and a keyed-state transport that counts
// requests by class. Flush-waits and finalize requests (point reads of `m/owner` with a wait) are the
// background loop's; every other keyed-state request is an open or Session-line read.
import { Type } from "@earendil-works/pi-ai";
import { createModels } from "@earendil-works/pi-ai/models";
import { fauxAssistantMessage, fauxProvider, fauxText, fauxToolCall } from "@earendil-works/pi-ai/providers/faux";
import { type Conversation, createRegistry, defineExtension, defineTool, Harness, type Storage } from "@earendil-works/pi-durable";
import { K, META } from "../src/families.ts";
import type { UrsulaStorage } from "../src/storage.ts";
import type { KeyedScanRequest, KeyedStateTransport } from "../src/transport.ts";
import { b64 } from "../src/tuple.ts";
import { ctx } from "./helpers.ts";

const OWNER = b64(K.m(META.owner));
export const isBackgroundScan = (r: KeyedScanRequest): boolean => r.key === OWNER && r.timeoutMs !== undefined;

/** Counts keyed-state requests by class. */
export class CountingKeyedState implements KeyedStateTransport {
	sessionLine = 0;
	background = 0;
	private readonly inner: KeyedStateTransport;
	constructor(inner: KeyedStateTransport) {
		this.inner = inner;
	}
	scan(request: KeyedScanRequest): ReturnType<KeyedStateTransport["scan"]> {
		if (isBackgroundScan(request)) this.background++;
		else this.sessionLine++;
		return this.inner.scan(request);
	}
}

export const faux = fauxProvider();
export const models = createModels();
models.setProvider(faux.provider);
export const model = faux.getModel();
export const agent = { model: { provider: model.provider, modelId: model.id } };

const echo = defineTool({
	name: "echo",
	description: "Echo the text",
	parameters: Type.Object({ text: Type.String() }),
	execute: async (args) => ({ content: [{ type: "text", text: args.text }] }),
});
export const registry = createRegistry();
registry.install(defineExtension({ name: "echo", tools: [echo] }));

/** One text turn: submit, the model answers, the submission settles. */
export async function textTurn(conversation: Conversation, n: number): Promise<void> {
	faux.appendResponses([fauxAssistantMessage(`answer ${n}`)]);
	const settled = await (await conversation.submit({ type: "input", content: `question ${n}` }, ctx)).wait(ctx);
	if (settled.status !== "done") throw new Error(`text turn ${n} settled ${settled.status}`);
}

/** One tool turn: the model calls `echo`, then answers. */
export async function toolTurn(conversation: Conversation, n: number): Promise<void> {
	faux.appendResponses([fauxAssistantMessage([fauxText("calling"), fauxToolCall("echo", { text: `t${n}` })]), fauxAssistantMessage(`tool answer ${n}`)]);
	const settled = await (await conversation.submit({ type: "input", content: `use the tool ${n}` }, ctx)).wait(ctx);
	if (settled.status !== "done") throw new Error(`tool turn ${n} settled ${settled.status}`);
}

/** Open a Harness and its root conversation over `storage`, and resume scheduling. */
export async function openHarness(storage: Storage): Promise<{ harness: Harness; root: Conversation }> {
	const harness = await Harness.open(storage, { models, registry }, ctx);
	const root = await harness.root(ctx, { agent });
	harness.resume();
	return { harness, root };
}

/** Every task in the store must have settled without a fault. Returns the task count. */
export async function faultedTasks(storage: UrsulaStorage): Promise<{ total: number; faulted: number[] }> {
	let cursor: Parameters<UrsulaStorage["scanTasks"]>[2];
	let total = 0;
	const faulted: number[] = [];
	do {
		const page = await storage.scanTasks({}, 100, cursor, ctx);
		for (const t of page.items) {
			total++;
			if (t.state.status === "terminal" && t.state.outcome.status === "faulted") faulted.push(t.id);
		}
		cursor = page.next;
	} while (cursor !== undefined);
	return { total, faulted };
}
