import type { TestProject } from "vitest/node";
import { startNode } from "./stack.ts";

declare module "vitest" {
	export interface ProvidedContext {
		ursulaUrl: string;
	}
}

export default async function setup(project: TestProject): Promise<() => Promise<void>> {
	const node = await startNode();
	project.provide("ursulaUrl", node.url);
	return () => node.stop();
}
