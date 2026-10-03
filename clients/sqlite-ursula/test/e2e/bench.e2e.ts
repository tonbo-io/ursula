import { appendFileSync } from "node:fs";
import { expect, it } from "vitest";
import { runBench } from "../bench.ts";
import { freshStream, httpStream } from "./env.ts";

it("benchmark: Pi commits on real Ursula", async () => {
	const stream = freshStream();
	const report = await runBench(() => httpStream(stream), [1000, 10_000]);
	console.log(`\n=== sqlite-ursula benchmark (single node, memory WAL) ===\n${report}\n`);
	const summary = process.env.GITHUB_STEP_SUMMARY;
	if (summary !== undefined) appendFileSync(summary, `### sqlite-ursula benchmark\n\n\`\`\`\n${report}\n\`\`\`\n`);
	expect(report).toContain("cold rebuild");
}, 900_000);
