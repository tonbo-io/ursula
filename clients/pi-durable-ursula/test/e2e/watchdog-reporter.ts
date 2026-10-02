// CI watchdog for the e2e runs: once the test run has ended, vitest closes the pool and runs the
// global teardown (stopping the stack). If the process is still alive WATCHDOG_MS later, something
// (a child process, socket or timer) keeps it from exiting: print what is still open, then exit
// with the run's outcome, so a stuck teardown can never hang CI after the results are known.
import type { Reporter, TestRunEndReason } from "vitest/node";

const WATCHDOG_MS = Number(process.env.E2E_EXIT_WATCHDOG_MS ?? "90000");

export default class WatchdogReporter implements Reporter {
	onTestRunEnd(_modules: unknown, unhandledErrors: readonly unknown[], reason: TestRunEndReason): void {
		const failed = reason !== "passed" || unhandledErrors.length > 0;
		const started = Date.now();
		const timer = setTimeout(() => {
			const open = process.getActiveResourcesInfo().reduce<Record<string, number>>((counts, kind) => {
				counts[kind] = (counts[kind] ?? 0) + 1;
				return counts;
			}, {});
			console.error(
				`[e2e watchdog] still running ${Date.now() - started} ms after the test run ended (${reason}); open resources: ${JSON.stringify(open)}; exiting with ${failed ? 1 : 0}`,
			);
			process.exit(failed ? 1 : (process.exitCode ?? 0));
		}, WATCHDOG_MS);
		timer.unref();
	}
}
