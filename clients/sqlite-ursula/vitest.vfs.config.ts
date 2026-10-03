import { defineConfig } from "vitest/config";

// The VFS spike: plain node:sqlite (and Pi's official node driver) over the sqlite-ursula-vfs
// extension (SQLITE_URSULA_VFS), against a real single-node `ursula` (URSULA_BIN).
export default defineConfig({
	test: {
		include: ["test/vfs/**/*.e2e.ts"],
		globalSetup: ["test/e2e/global-setup.ts"],
		testTimeout: 120_000,
		hookTimeout: 120_000,
		fileParallelism: false,
	},
});
