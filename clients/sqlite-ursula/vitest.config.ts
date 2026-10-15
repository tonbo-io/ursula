import { defineConfig } from "vitest/config";

// Every test runs node:sqlite (and Pi's official node driver) over the sqlite-ursula-vfs extension
// (SQLITE_URSULA_VFS) against the server test/stack.ts starts: the model of the 0.8 server with
// E2E_SERVER=model.
export default defineConfig({
	test: {
		include: ["test/**/*.e2e.ts"],
		globalSetup: ["test/stack.ts"],
		testTimeout: 120_000,
		hookTimeout: 120_000,
		fileParallelism: false,
	},
});
