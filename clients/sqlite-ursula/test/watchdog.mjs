// Runs a server process for test/stack.ts so that it never outlives the test process: argv[2] with
// the rest of argv as its arguments, its output on this process's stdout and stderr. It writes the
// server's process id to fd 3, then:
// - stdin closes (the test process exited, however it died): kills the server with SIGKILL;
// - SIGTERM: passes it on, for a clean stop;
// - the server exits: exits too, with its code (128 + the signal number when a signal ended it).
import { spawn } from "node:child_process";
import { closeSync, writeSync } from "node:fs";
import { constants } from "node:os";

const child = spawn(process.argv[2], process.argv.slice(3), { stdio: ["ignore", "inherit", "inherit"] });
child.once("error", (error) => {
	process.stderr.write(`watchdog: ${error}\n`);
	process.exit(127);
});
child.once("spawn", () => {
	writeSync(3, `${child.pid}\n`);
	closeSync(3);
});
child.once("exit", (code, signal) => process.exit(code ?? 128 + (constants.signals[signal] ?? 0)));
process.on("SIGTERM", () => child.kill("SIGTERM"));
process.stdin.on("end", () => child.kill("SIGKILL"));
process.stdin.resume();
