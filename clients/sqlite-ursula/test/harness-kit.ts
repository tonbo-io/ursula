// A real Pi Harness with a faux model and an `echo` tool (copied from clients/pi-durable-ursula).
import { Type } from "@earendil-works/pi-ai";
import { createModels } from "@earendil-works/pi-ai/models";
import { fauxAssistantMessage, fauxProvider, fauxText, fauxToolCall } from "@earendil-works/pi-ai/providers/faux";
import { type Conversation, createRegistry, defineExtension, defineTool, Harness, type Storage } from "@earendil-works/pi-durable";
import { ctx } from "./helpers.ts";

export const faux = fauxProvider();
export const models = createModels();
models.setProvider(faux.provider);
const model = faux.getModel();
export const agent = { model: { provider: model.provider, modelId: model.id } };

const echo = defineTool({
	name: "echo",
	description: "Echo the text",
	parameters: Type.Object({ text: Type.String() }),
	execute: async (args) => ({ content: [{ type: "text", text: args.text }] }),
});
export const registry = createRegistry();
registry.install(defineExtension({ name: "echo", tools: [echo] }));

export async function textTurn(conversation: Conversation, n: number): Promise<void> {
	faux.appendResponses([fauxAssistantMessage(`answer ${n}`)]);
	const settled = await (await conversation.submit({ type: "input", content: `question ${n}` }, ctx)).wait(ctx);
	if (settled.status !== "done") throw new Error(`text turn ${n} settled ${settled.status}`);
}

export async function toolTurn(conversation: Conversation, n: number): Promise<void> {
	faux.appendResponses([fauxAssistantMessage([fauxText("calling"), fauxToolCall("echo", { text: `t${n}` })]), fauxAssistantMessage(`tool answer ${n}`)]);
	const settled = await (await conversation.submit({ type: "input", content: `use the tool ${n}` }, ctx)).wait(ctx);
	if (settled.status !== "done") throw new Error(`tool turn ${n} settled ${settled.status}`);
}

export async function openHarness(storage: Storage): Promise<{ harness: Harness; root: Conversation }> {
	const harness = await Harness.open(storage, { models, registry }, ctx);
	const root = await harness.root(ctx, { agent });
	harness.resume();
	return { harness, root };
}
