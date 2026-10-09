// The long-run soak (#477): Pi databases at mixed paces for hours, each driven by bench/owner.ts in
// segments, with faults between and inside them:
//
// - a crash: the owner is SIGKILLed partway through a segment (same-boot resume on the next attach);
// - a reboot: the sidecar's boot id is changed between segments (the next attach discards the
//   local files and rebuilds them from the latest snapshot and the tail);
// - a verification: between segments, a separate process attaches the owner's file and a fresh copy
//   rebuilt from the stream, and compares them table by table (integrity check, row hashes).
//
// The first `LONG` owners instead run at agent pace for the whole soak in one process, with no
// fault of their own, so their RSS shows whether the extension's memory stays flat.
//
//   node bench/soak.ts <base url> <label> <owners> <hours> <out dir>
//
// Appends one JSON line per event to `<out dir>/<label>/events.jsonl` and prints a summary line
// every 10 minutes. Raft leader kills happen outside (kubectl), on their own schedule.
import { spawn } from "node:child_process";
import { appendFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

if (process.argv.length < 7) throw new Error("usage: soak.ts <base url> <label> <owners> <hours> <out dir>");
const [base, label, ownersArg, hoursArg, outDir] = process.argv.slice(2) as [string, string, string, string, string];
const here = dirname(fileURLToPath(import.meta.url));
const dir = join(outDir, label);
mkdirSync(dir, { recursive: true });
const files = join(tmpdir(), `vfs-soak-${label}`);
mkdirSync(files, { recursive: true });
const events = join(dir, "events.jsonl");
const end = Date.now() + Number(hoursArg) * 3_600_000;
const run = Date.now().toString(36);
const log = (event: Record<string, unknown>): void => appendFileSync(events, `${JSON.stringify({ at: new Date().toISOString(), ...event })}\n`);

/** Paces cycle through agent pace (2 s per turn), a busy agent (500 ms) and flat out. */
const PACES = [2000, 2000, 500, 0];
/** Owners that run in one process for the whole soak. */
const LONG = 2;

const counts = { segments: 0, crashes: 0, reboots: 0, verifications: 0, divergences: 0, poisoned: 0, failed: 0 };

function exec(args: string[], killAfterMs?: number): Promise<{ code: number | null; killed: boolean }> {
	return new Promise((res) => {
		const child = spawn(process.execPath, args, { stdio: ["ignore", "ignore", "pipe"] });
		let stderr = "";
		child.stderr.on("data", (d: Buffer) => {
			stderr = (stderr + d.toString()).slice(-8000);
		});
		let killed = false;
		const timer =
			killAfterMs === undefined
				? undefined
				: setTimeout(() => {
						killed = true;
						child.kill("SIGKILL");
					}, killAfterMs);
		child.on("exit", (code) => {
			if (timer !== undefined) clearTimeout(timer);
			if (code !== 0 && !killed) log({ kind: "stderr", args: args.slice(1, 3), stderr });
			res({ code, killed });
		});
	});
}

async function owner(i: number): Promise<void> {
	const url = `${base.replace(/\/+$/, "")}/vfs-bench/soak-${label}-${run}-${i}`;
	const file = join(files, `db-${i}.sqlite`);
	const pace = PACES[i % PACES.length] as number;
	const long = i < LONG;
	for (let segment = 0; Date.now() < end; segment++) {
		const minutes = long ? (end - Date.now()) / 60_000 : Math.min(5 + Math.random() * 10, (end - Date.now()) / 60_000);
		const crash = !long && Math.random() < 0.3;
		const out = join(dir, `owner-${i}-seg-${segment}.json`);
		const result = await exec([join(here, "owner.ts"), url, file, `${pace}`, "0", `${minutes * 60}`, out], crash ? Math.random() * minutes * 60_000 : undefined);
		counts.segments++;
		if (result.killed) {
			counts.crashes++;
			log({ kind: "crash", owner: i, segment });
		} else if (existsSync(out)) {
			const r = JSON.parse(readFileSync(out, "utf8")) as {
				failed: string | null;
				rss: [number, number][];
				resumed: number;
				status: { offset: string; retained: string; poisoned: boolean; reason: string | null; log_bytes: number; snapshot_due_bytes: number; snapshot_failures: number };
			};
			// The stream's retained log, from the server: a new process's status does not know the
			// retention its predecessor set. Offsets are opaque to the VFS; this tool reads
			// Ursula's as byte positions.
			const head = await fetch(url, { method: "HEAD" }).catch(() => undefined);
			const tail = Number(head?.headers.get("stream-next-offset"));
			const retained = Number(head?.headers.get("stream-retained-offset") ?? 0);
			const retainedLog = Number.isFinite(tail) && Number.isFinite(retained) ? tail - retained : null;
			if (r.status.poisoned) counts.poisoned++;
			if (r.failed !== null) counts.failed++;
			log({ kind: "segment", owner: i, segment, pace, failed: r.failed, poisoned: r.status.poisoned, reason: r.status.reason, retained_log: retainedLog, resumed: r.resumed, log_bytes: r.status.log_bytes, snapshot_due_bytes: r.status.snapshot_due_bytes, snapshot_failures: r.status.snapshot_failures, rss_mb: r.rss.at(-1)?.[1] ?? null });
			// Only the summary is kept: a day of samples would fill the disk.
			writeFileSync(out, JSON.stringify({ failed: r.failed, status: r.status, rss: r.rss }));
		} else {
			counts.failed++;
			log({ kind: "owner_died", owner: i, segment, code: result.code });
		}
		if (long) continue;
		const roll = Math.random();
		if (roll < 0.15) {
			// A reboot: the next attach must not trust the local files.
			const sidecar = `${file}-ursula`;
			if (existsSync(sidecar)) {
				writeFileSync(sidecar, readFileSync(sidecar, "utf8").replace(/ boot=\S+/, " boot=before-a-reboot"));
				counts.reboots++;
				log({ kind: "reboot", owner: i, segment });
			}
		} else if (roll < 0.35 && !result.killed) {
			const verdict = join(dir, `verify-${i}-${segment}.json`);
			await exec([join(here, "verify.ts"), url, file, join(files, `copy-${i}-${segment}.sqlite`), verdict]);
			counts.verifications++;
			const v = existsSync(verdict) ? (JSON.parse(readFileSync(verdict, "utf8")) as { same: boolean }) : { same: false };
			if (!v.same) counts.divergences++;
			log({ kind: "verify", owner: i, segment, ...v });
		}
	}
}

const ticker = setInterval(() => {
	const line = { kind: "summary", ...counts, rss_mb: Math.round(process.memoryUsage().rss / 1048576) };
	log(line);
	console.log(JSON.stringify(line));
}, 600_000);
await Promise.all(Array.from({ length: Number(ownersArg) }, (_, i) => owner(i)));
clearInterval(ticker);
const final = { kind: "final", ...counts };
log(final);
writeFileSync(join(dir, "summary.json"), JSON.stringify(final, null, 2));
console.log(JSON.stringify(final));
