// Spawns one single-node, memory-engine `ursula` on a free port for the e2e suite and provides its
// base URL to the tests. The binary comes from URSULA_BIN, falling back to the repo's release build.
import { type ChildProcess, spawn } from "node:child_process";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { TestProject } from "vitest/node";
import { E2E_BUCKET } from "./env.ts";

declare module "vitest" {
	export interface ProvidedContext {
		ursulaUrl: string;
	}
}

function ursulaBinary(): string {
	const fromEnv = process.env.URSULA_BIN;
	if (fromEnv !== undefined && fromEnv.length > 0) return resolve(fromEnv);
	const local = resolve(import.meta.dirname, "../../../../target/release/ursula");
	if (existsSync(local)) return local;
	throw new Error("set URSULA_BIN to an ursula binary (cargo build --release -p ursula --bin ursula)");
}

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

async function waitReady(url: string, child: ChildProcess, logs: () => string): Promise<void> {
	const deadline = Date.now() + 60_000;
	while (Date.now() < deadline) {
		if (child.exitCode !== null) throw new Error(`ursula exited with ${child.exitCode}:\n${logs()}`);
		try {
			const r = await fetch(`${url}/__ursula/ready`, { signal: AbortSignal.timeout(1000) });
			if (r.ok) return;
		} catch {
			// not listening yet
		}
		await new Promise((r) => setTimeout(r, 100));
	}
	throw new Error(`ursula did not become ready within 60 s:\n${logs()}`);
}

export default async function setup(project: TestProject): Promise<() => Promise<void>> {
	const bin = ursulaBinary();
	const port = await freePort();
	const dir = mkdtempSync(join(tmpdir(), "pi-durable-ursula-e2e-"));
	const config = join(dir, "ursula.toml");
	writeFileSync(config, `[server]\nlisten = "127.0.0.1:${port}"\n\n[raft]\nnode_id = 1\n\n[raft.wal]\nbackend = "memory"\n`);
	let output = "";
	const child = spawn(bin, ["server", "--config", config, "--preset", "default"], { cwd: dir, stdio: ["ignore", "pipe", "pipe"] });
	const keep = (chunk: Buffer): void => {
		output = (output + chunk.toString()).slice(-20_000);
	};
	child.stdout?.on("data", keep);
	child.stderr?.on("data", keep);
	const url = `http://127.0.0.1:${port}`;
	try {
		await waitReady(url, child, () => output);
		const bucket = await fetch(`${url}/${E2E_BUCKET}`, { method: "PUT" });
		if (!bucket.ok && bucket.status !== 409) throw new Error(`create bucket: ${bucket.status} ${await bucket.text()}`);
	} catch (error) {
		child.kill("SIGKILL");
		rmSync(dir, { recursive: true, force: true });
		throw error;
	}
	project.provide("ursulaUrl", url);
	return async () => {
		const exited = new Promise((r) => child.once("exit", r));
		child.kill("SIGTERM");
		const timer = setTimeout(() => child.kill("SIGKILL"), 5000);
		await exited;
		clearTimeout(timer);
		rmSync(dir, { recursive: true, force: true });
	};
}
