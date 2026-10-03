import { defineConfig } from "vitest/config";

// Every test runs plain node:sqlite (and Pi's official node driver) over the sqlite-ursula-vfs
// extension (SQLITE_URSULA_VFS) against a real single-node `ursula` (URSULA_BIN).
export default defineConfig({
	test: {
		include: ["test/**/*.e2e.ts"],
		globalSetup: ["test/stack.ts"],
		testTimeout: 120_000,
		hookTimeout: 120_000,
		fileParallelism: false,
	},
});
