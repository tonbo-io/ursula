import { defineConfig } from "vitest/config";

// End-to-end suite against a real single-node `ursula` (URSULA_BIN), spawned once by the global setup.
export default defineConfig({
	test: {
		include: ["test/e2e/**/*.e2e.ts"],
		globalSetup: ["test/e2e/global-setup.ts"],
		testTimeout: 120_000,
		hookTimeout: 120_000,
		// The watchdog ends a run whose teardown hangs after the results are known (CI only needs them).
		reporters: ["default", "./test/e2e/watchdog-reporter.ts"],
	},
});
