import { defineConfig } from "vitest/config";

// MANUAL TOOL, NOT RUN IN CI: `URSULA_BIN=... npm run perf:manual`.
// M3 performance gates against the real keyed stack (design §9.3, §10 M3, §11.10), spawned by the e2e
// global setup. Not part of `test:e2e`: building the 100k-record history takes minutes.
// The 100k-record logs exceed the node's default 64 MiB per-group hot cap (no cold tier in the e2e stack).
process.env.E2E_MAX_HOT_PER_GROUP ??= "2GiB";

export default defineConfig({
	test: {
		include: ["test/e2e/perf/**/*.perf.ts"],
		globalSetup: ["test/e2e/global-setup.ts"],
		fileParallelism: false,
		testTimeout: 1_800_000,
		hookTimeout: 1_800_000,
	},
});
