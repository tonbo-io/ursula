import { defineConfig } from "vitest/config";

// The keyed-streams soak (test/soak/*.soak.ts): one long run on its own 3-node S3 stack. Duration and
// population come from SOAK_* knobs; scripts/ks_soak.sh runs it for a nightly.
export default defineConfig({
	test: {
		include: ["test/soak/**/*.soak.ts"],
		fileParallelism: false,
		testTimeout: 80 * 60 * 60_000,
		hookTimeout: 10 * 60_000,
	},
});
