import { defineConfig } from "vitest/config";

// MANUAL TOOL, NOT RUN IN CI: `npm run soak:manual` (or scripts/ks_soak.sh, which builds ursula first).
// The keyed-streams soak (test/soak/*.soak.ts): one long run on its own 3-node S3 stack. Duration and
// population come from SOAK_* knobs. The default `vitest.config.ts` only picks up `*.test.ts`.
export default defineConfig({
	test: {
		include: ["test/soak/**/*.soak.ts"],
		fileParallelism: false,
		testTimeout: 80 * 60 * 60_000,
		hookTimeout: 10 * 60_000,
	},
});
