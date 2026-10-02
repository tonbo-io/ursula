import { defineConfig } from "vitest/config";

// The M4 drills (test/drills/*.drill.ts): each file starts its own stack (S3, nodes, gateway,
// indexer) and disrupts it, so files run one at a time. Durations come from DRILL_* knobs.
export default defineConfig({
	test: {
		include: ["test/drills/**/*.drill.ts"],
		fileParallelism: false,
		testTimeout: 4 * 60 * 60_000,
		hookTimeout: 10 * 60_000,
	},
});
