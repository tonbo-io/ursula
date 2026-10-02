// Spawns the real keyed stack for the e2e suite: one single-node, memory-engine `ursula` serving
// `{stream}/keyed-state` through a keyed-mode `ursula indexer` (filesystem object store, source = the
// node). Provides the node's base URL, plus a control URL the tests use to restart the indexer. The
// binary comes from URSULA_BIN, falling back to the repo's release build.
import { type ChildProcess, spawn } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer as createHttpServer } from "node:http";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { TestProject } from "vitest/node";
import { E2E_BUCKET } from "./env.ts";

declare module "vitest" {
	export interface ProvidedContext {
		ursulaUrl: string;
		controlUrl: string;
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

async function waitReady(url: string, child: ChildProcess, logs: () => string, path = "/__ursula/ready"): Promise<void> {
	const deadline = Date.now() + 60_000;
	while (Date.now() < deadline) {
		if (child.exitCode !== null) throw new Error(`${path} server exited with ${child.exitCode}:\n${logs()}`);
		try {
			const r = await fetch(`${url}${path}`, { signal: AbortSignal.timeout(1000) });
			if (r.ok) return;
		} catch {
			// not listening yet
		}
		await new Promise((r) => setTimeout(r, 100));
	}
	throw new Error(`${url} did not become ready within 60 s:\n${logs()}`);
}

/**
 * Raises every group to feature level 1 through the admin listener, so keyed
 * (`profile=keyed-batch-v1`) creates are accepted (keyed streams design §6.3). Retries until every
 * group reports `set`, because a fresh node may still be electing some group leaders.
 */
async function enableKeyedStreams(adminUrl: string): Promise<void> {
	const deadline = Date.now() + 30_000;
	let last = "";
	while (Date.now() < deadline) {
		try {
			const r = await fetch(`${adminUrl}/__ursula/feature-level`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ level: 1 }),
				signal: AbortSignal.timeout(5000),
			});
			last = `${r.status} ${await r.text()}`;
			if (r.ok) {
				const report = JSON.parse(last.slice(last.indexOf(" ") + 1)) as { groups: { status: string }[] };
				if (report.groups.every((group) => group.status === "set")) return;
			}
		} catch (error) {
			last = String(error);
		}
		await new Promise((r) => setTimeout(r, 200));
	}
	throw new Error(`could not raise the feature level to 1: ${last}`);
}

/** A spawned child process with a bounded log tail. */
interface Proc {
	child: ChildProcess;
	logs: () => string;
}

function launch(bin: string, args: string[], cwd: string): Proc {
	let output = "";
	const child = spawn(bin, args, { cwd, stdio: ["ignore", "pipe", "pipe"] });
	const keep = (chunk: Buffer): void => {
		output = (output + chunk.toString()).slice(-20_000);
	};
	child.stdout?.on("data", keep);
	child.stderr?.on("data", keep);
	return { child, logs: () => output };
}

async function stop(proc: Proc | undefined): Promise<void> {
	if (proc === undefined || proc.child.exitCode !== null) return;
	const exited = new Promise((r) => proc.child.once("exit", r));
	proc.child.kill("SIGTERM");
	const timer = setTimeout(() => proc.child.kill("SIGKILL"), 5000);
	await exited;
	clearTimeout(timer);
}

/** Publication spacing of the e2e indexer (the default is 5000 ms; the suite runs many owners). */
const PUBLISH_INTERVAL_MS = process.env.E2E_KEYED_PUBLISH_INTERVAL_MS ?? "100";

export default async function setup(project: TestProject): Promise<() => Promise<void>> {
	const bin = ursulaBinary();
	const port = await freePort();
	const adminPort = await freePort();
	const indexerPort = await freePort();
	const dir = mkdtempSync(join(tmpdir(), "pi-durable-ursula-e2e-"));
	const config = join(dir, "ursula.toml");
	const indexerUrl = `http://127.0.0.1:${indexerPort}`;
	writeFileSync(
		config,
		`[server]\nlisten = "127.0.0.1:${port}"\nadmin_listen = "127.0.0.1:${adminPort}"\nkeyed_state_upstream = "${indexerUrl}"\n\n[raft]\nnode_id = 1\n\n[raft.wal]\nbackend = "memory"\n`,
	);
	const url = `http://127.0.0.1:${port}`;
	const objects = join(dir, "objects");
	mkdirSync(objects, { recursive: true });
	const indexerArgs = [
		"indexer",
		"--keyed-source-url",
		url,
		"--object-dir",
		objects,
		"--cache-dir",
		join(dir, "indexer-cache"),
		"--listen",
		`127.0.0.1:${indexerPort}`,
		"--keyed-min-publish-interval-ms",
		PUBLISH_INTERVAL_MS,
	];
	let indexer: Proc | undefined;
	const startIndexer = async (): Promise<void> => {
		const proc = launch(bin, indexerArgs, dir);
		indexer = proc;
		await waitReady(indexerUrl, proc.child, proc.logs, "/readyz");
	};
	const node = launch(bin, ["server", "--config", config, "--preset", "default"], dir);
	// Test control: POST /indexer/restart stops the indexer and starts a fresh process on the same
	// port and object store (the in-memory state and write cache are lost; `.keyed/` persists).
	const control = createHttpServer((req, res) => {
		if (req.method === "POST" && req.url === "/indexer/restart") {
			stop(indexer)
				.then(startIndexer)
				.then(
					() => res.writeHead(200).end("restarted"),
					(error: unknown) => res.writeHead(500).end(String(error)),
				);
			return;
		}
		res.writeHead(404).end();
	});
	const controlPort = await new Promise<number>((res) =>
		control.listen(0, "127.0.0.1", () => {
			const address = control.address();
			res(typeof address === "object" && address !== null ? address.port : 0);
		}),
	);
	try {
		await waitReady(url, node.child, node.logs);
		await startIndexer();
		const bucket = await fetch(`${url}/${E2E_BUCKET}`, { method: "PUT" });
		if (!bucket.ok && bucket.status !== 409) throw new Error(`create bucket: ${bucket.status} ${await bucket.text()}`);
		await enableKeyedStreams(`http://127.0.0.1:${adminPort}`);
	} catch (error) {
		await stop(indexer);
		node.child.kill("SIGKILL");
		control.close();
		rmSync(dir, { recursive: true, force: true });
		throw new Error(`${String(error)}\nindexer log:\n${indexer?.logs() ?? ""}`);
	}
	project.provide("ursulaUrl", url);
	project.provide("controlUrl", `http://127.0.0.1:${controlPort}`);
	return async () => {
		control.close();
		await stop(indexer);
		await stop(node);
		rmSync(dir, { recursive: true, force: true });
	};
}
