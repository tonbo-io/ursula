// One `ursula server` node (memory WAL, no cold store, no indexer), spawned from URSULA_BIN.
// Process helpers trimmed from clients/pi-durable-ursula/test/stack/proc.ts.
import { type ChildProcess, spawn } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

function freePort(): Promise<number> {
	return new Promise((res, rej) => {
		const server = createServer();
		server.once("error", rej);
		server.listen(0, "127.0.0.1", () => {
			const address = server.address();
			const port = typeof address === "object" && address !== null ? address.port : 0;
			server.close(() => res(port));
		});
	});
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

export interface Node {
	readonly url: string;
	stop(): Promise<void>;
}

export async function startNode(): Promise<Node> {
	const bin = process.env.URSULA_BIN;
	if (bin === undefined || bin.length === 0) throw new Error("set URSULA_BIN to an ursula binary");
	const dir = mkdtempSync(join(tmpdir(), "sqlite-ursula-node-"));
	const port = await freePort();
	const adminPort = await freePort();
	const config = join(dir, "ursula.toml");
	writeFileSync(
		config,
		[
			"[server]",
			`listen = "127.0.0.1:${port}"`,
			`admin_listen = "127.0.0.1:${adminPort}"`,
			"",
			"[runtime]",
			"core_count = 2",
			"",
			"[raft]",
			"node_id = 1",
			"group_count = 4",
			"",
			"[raft.wal]",
			'backend = "memory"',
			"",
			// No cold store: everything stays hot. The VFS spike's page-image records reach the default
			// 64 MiB per-group admission limit within a few seconds of the benchmark.
			"[storage.cold]",
			'max_hot_size_per_group = "8GiB"',
			"",
		].join("\n"),
	);
	let tail = "";
	const child: ChildProcess = spawn(resolve(bin), ["server", "--config", config], { cwd: dir, stdio: ["ignore", "pipe", "pipe"] });
	const keep = (chunk: Buffer): void => {
		tail = (tail + chunk.toString()).slice(-20_000);
	};
	child.stdout?.on("data", keep);
	child.stderr?.on("data", keep);
	const url = `http://127.0.0.1:${port}`;
	const stop = async (): Promise<void> => {
		if (child.exitCode === null && child.signalCode === null) {
			const exited = new Promise((r) => child.once("exit", r));
			child.kill("SIGTERM");
			const timer = setTimeout(() => child.kill("SIGKILL"), 5000);
			await exited;
			clearTimeout(timer);
		}
		child.stdout?.destroy();
		child.stderr?.destroy();
		rmSync(dir, { recursive: true, force: true });
	};
	// Ready once a bucket create succeeds (every group has a leader for a stream create below).
	const deadline = Date.now() + 120_000;
	let last = "";
	while (Date.now() < deadline) {
		if (child.exitCode !== null) throw new Error(`ursula exited ${child.exitCode}:\n${tail}`);
		try {
			const r = await fetch(`${url}/sqlite-e2e`, { method: "PUT", signal: AbortSignal.timeout(2000) });
			last = `${r.status}`;
			if (r.ok || r.status === 409) {
				let ok = true;
				for (let i = 0; i < 16 && ok; i++) {
					const s = await fetch(`${url}/sqlite-e2e/probe-${i}`, { method: "PUT", headers: { "content-type": "application/json" }, signal: AbortSignal.timeout(2000) });
					ok = s.ok || s.status === 409;
					last = `stream ${s.status}`;
				}
				if (ok) return { url, stop };
			}
		} catch (error) {
			last = String(error);
		}
		await sleep(200);
	}
	await stop();
	throw new Error(`ursula not ready: ${last}\n${tail}`);
}
