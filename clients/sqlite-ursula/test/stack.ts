// Vitest global setup: the Ursula the suite runs against, from URSULA_BIN, in the default config
// except where noted. The per-group hot admission limit always stays at its default.
// - Default: one `ursula server` node with a memory Raft WAL and an in-memory cold store
//   (URSULA_COLD=none for none).
// - E2E_NODES=3: three nodes (memory Raft WAL) behind an `ursula gateway`, with S3 (MinIO at
//   URSULA_S3_ENDPOINT) as the cold store, raised to feature level 5 (snapshot bodies in the cold
//   tier).
import { type ChildProcess, spawn } from "node:child_process";
import { appendFileSync } from "node:fs";
import { createHash, createHmac } from "node:crypto";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { TestProject } from "vitest/node";

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

function ursulaBin(): string {
	const bin = process.env.URSULA_BIN;
	if (bin === undefined || bin.length === 0) throw new Error("set URSULA_BIN to an ursula binary");
	return resolve(bin);
}

/** A spawned process with a bounded log tail. */
class Proc {
	private readonly child: ChildProcess;
	private tail = "";
	/** With E2E_LOG_DIR set, the full output also goes to `${E2E_LOG_DIR}/${name}.log`. */
	constructor(bin: string, args: readonly string[], cwd: string, env: NodeJS.ProcessEnv = {}, name?: string) {
		this.child = spawn(bin, [...args], { cwd, stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, ...env } });
		const logDir = process.env.E2E_LOG_DIR;
		const logFile = logDir !== undefined && logDir.length > 0 && name !== undefined ? join(logDir, `${name}.log`) : undefined;
		if (logFile !== undefined) mkdirSync(logDir as string, { recursive: true });
		const keep = (chunk: Buffer): void => {
			this.tail = (this.tail + chunk.toString()).slice(-20_000);
			if (logFile !== undefined) appendFileSync(logFile, chunk);
		};
		this.child.stdout?.on("data", keep);
		this.child.stderr?.on("data", keep);
	}
	get pid(): number {
		return this.child.pid ?? 0;
	}
	get exited(): boolean {
		return this.child.exitCode !== null || this.child.signalCode !== null;
	}
	logs(): string {
		return this.tail;
	}
	async stop(): Promise<void> {
		if (!this.exited) {
			const exited = new Promise((r) => this.child.once("exit", r));
			this.child.kill("SIGTERM");
			const timer = setTimeout(() => this.child.kill("SIGKILL"), 5000);
			await exited;
			clearTimeout(timer);
		}
		this.child.stdout?.destroy();
		this.child.stderr?.destroy();
	}
}

async function waitReady(url: string, proc: Proc, timeoutMs = 120_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	let last = "";
	while (Date.now() < deadline) {
		if (proc.exited) throw new Error(`process exited before ${url} was ready:\n${proc.logs()}`);
		try {
			const r = await fetch(url, { signal: AbortSignal.timeout(1000) });
			if (r.ok) return;
			last = `${r.status}`;
		} catch (error) {
			last = String(error);
		}
		await sleep(100);
	}
	throw new Error(`${url} not ready (${last}):\n${proc.logs()}`);
}

/** Ready once the bucket and a stream in every group can be created through `url`. */
async function waitWritable(url: string, logs: () => string): Promise<void> {
	const deadline = Date.now() + 120_000;
	let last = "";
	while (Date.now() < deadline) {
		try {
			const r = await fetch(`${url}/sqlite-e2e`, { method: "PUT", signal: AbortSignal.timeout(5000) });
			last = `${r.status}`;
			if (r.ok || r.status === 409) {
				let ok = true;
				for (let i = 0; i < 16 && ok; i++) {
					const s = await fetch(`${url}/sqlite-e2e/probe-${i}`, { method: "PUT", headers: { "content-type": "application/octet-stream" }, signal: AbortSignal.timeout(5000) });
					ok = s.ok || s.status === 409;
					last = `stream ${s.status}`;
				}
				if (ok) return;
			}
		} catch (error) {
			last = String(error);
		}
		await sleep(200);
	}
	throw new Error(`ursula not writable: ${last}\n${logs()}`);
}

/** A server node: its HTTP URL and process id (for RSS sampling). */
export interface StackNode {
	readonly url: string;
	readonly pid: number;
}

interface Stack {
	readonly url: string;
	readonly nodes: readonly StackNode[];
	stop(): Promise<void>;
}

async function startSingle(): Promise<Stack> {
	const cold = process.env.URSULA_COLD ?? "memory";
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
			...(cold === "none" ? [] : ["[storage.cold]", `backend = "${cold}"`, ""]),
		].join("\n"),
	);
	const node = new Proc(ursulaBin(), ["server", "--config", config], dir);
	const url = `http://127.0.0.1:${port}`;
	const stop = async (): Promise<void> => {
		await node.stop();
		rmSync(dir, { recursive: true, force: true });
	};
	try {
		await waitWritable(url, () => node.logs());
	} catch (error) {
		await stop();
		throw error;
	}
	return { url, nodes: [{ url, pid: node.pid }], stop };
}

// ---- S3 (MinIO): bucket creation with a minimal path-style SigV4 request

const sha256 = (data: string): string => createHash("sha256").update(data).digest("hex");
const hmac = (key: string | Buffer, data: string): Buffer => createHmac("sha256", key).update(data).digest();

async function createS3Bucket(endpoint: string, bucket: string, accessKey: string, secretKey: string, region: string): Promise<void> {
	const url = new URL(`${endpoint}/${bucket}`);
	const amzDate = new Date().toISOString().replace(/[:-]|\.\d{3}/g, "");
	const date = amzDate.slice(0, 8);
	const payloadHash = sha256("");
	const headers: Record<string, string> = { host: url.host, "x-amz-content-sha256": payloadHash, "x-amz-date": amzDate };
	const names = Object.keys(headers).sort();
	const canonical = ["PUT", url.pathname, "", names.map((n) => `${n}:${headers[n]}\n`).join(""), names.join(";"), payloadHash].join("\n");
	const scope = `${date}/${region}/s3/aws4_request`;
	const toSign = ["AWS4-HMAC-SHA256", amzDate, scope, sha256(canonical)].join("\n");
	const key = hmac(hmac(hmac(hmac(`AWS4${secretKey}`, date), region), "s3"), "aws4_request");
	const signature = createHmac("sha256", key).update(toSign).digest("hex");
	const { host: _host, ...sent } = headers;
	const r = await fetch(url, {
		method: "PUT",
		headers: { ...sent, authorization: `AWS4-HMAC-SHA256 Credential=${accessKey}/${scope}, SignedHeaders=${names.join(";")}, Signature=${signature}` },
		signal: AbortSignal.timeout(10_000),
	});
	const body = await r.text();
	if (!r.ok && !body.includes("BucketAlreadyOwnedByYou")) throw new Error(`create S3 bucket ${bucket}: ${r.status} ${body}`);
}

async function startCluster(): Promise<Stack> {
	const endpoint = (process.env.URSULA_S3_ENDPOINT ?? "").replace(/\/+$/, "");
	if (endpoint === "") throw new Error("E2E_NODES=3 needs S3 at URSULA_S3_ENDPOINT");
	const accessKey = process.env.URSULA_S3_ACCESS_KEY ?? "minioadmin";
	const secretKey = process.env.URSULA_S3_SECRET_KEY ?? "minioadmin";
	const region = process.env.URSULA_S3_REGION ?? "us-east-1";
	const bucket = `sqlite-${process.pid}-${Date.now().toString(36)}`;
	await createS3Bucket(endpoint, bucket, accessKey, secretKey, region);
	const dir = mkdtempSync(join(tmpdir(), "sqlite-ursula-cluster-"));
	// Defaults are the e2e shape; the soak (test/soak.e2e.ts) raises them with E2E_GROUPS,
	// E2E_CORES and E2E_WAL=disk.
	const groups = Number(process.env.E2E_GROUPS ?? 4);
	const cores = Number(process.env.E2E_CORES ?? 2);
	const wal = process.env.E2E_WAL ?? "memory";
	const nodes: { id: number; port: number; admin: string }[] = [];
	for (const id of [1, 2, 3]) nodes.push({ id, port: await freePort(), admin: `http://127.0.0.1:${await freePort()}` });
	const procs: Proc[] = [];
	const logs = (): string => procs.map((p, i) => `--- ${i < 3 ? `node${i + 1}` : "gateway"}\n${p.logs()}`).join("\n");
	const stop = async (): Promise<void> => {
		await Promise.all(procs.map((p) => p.stop()));
		rmSync(dir, { recursive: true, force: true });
	};
	try {
		for (const node of nodes) {
			const nodeDir = join(dir, `node${node.id}`);
			mkdirSync(nodeDir, { recursive: true });
			const config = join(nodeDir, "ursula.toml");
			const lines = [
				"[server]",
				`listen = "127.0.0.1:${node.port}"`,
				`admin_listen = "${node.admin.replace("http://", "")}"`,
				"",
				"[runtime]",
				`core_count = ${cores}`,
				"",
				"[raft]",
				`node_id = ${node.id}`,
				`group_count = ${groups}`,
				`init_membership = ${node.id === 1}`,
				"init_membership_per_group = false",
				"",
				"[raft.wal]",
				...(wal === "disk" ? ['backend = "disk"', `path = "${join(nodeDir, "wal")}"`] : ['backend = "memory"', "allow_volatile_multi_peer = true"]),
			];
			for (const peer of nodes) lines.push("", "[[raft.peers]]", `node_id = ${peer.id}`, `url = "http://127.0.0.1:${peer.port}"`);
			lines.push(
				"",
				"[storage.cold]",
				'backend = "s3"',
				'root = "ursula"',
				"",
				"[storage.cold.s3]",
				`bucket = "${bucket}"`,
				`region = "${region}"`,
				`endpoint = "${endpoint}"`,
				`access_key_id = "${accessKey}"`,
				`secret_access_key = "${secretKey}"`,
				'server_side_encryption = "none"',
			);
			writeFileSync(config, `${lines.join("\n")}\n`);
			procs.push(new Proc(ursulaBin(), ["server", "--config", config], nodeDir, { AWS_ACCESS_KEY_ID: accessKey, AWS_SECRET_ACCESS_KEY: secretKey, AWS_REGION: region }, `node${node.id}`));
		}
		await Promise.all(nodes.map((node, i) => waitReady(`http://127.0.0.1:${node.port}/__ursula/ready`, procs[i] as Proc)));
		const gatewayPort = await freePort();
		const args = ["gateway", "--listen", `127.0.0.1:${gatewayPort}`, "--raft-group-count", `${groups}`];
		for (const node of nodes) args.push("--upstream", `http://127.0.0.1:${node.port}`);
		procs.push(new Proc(ursulaBin(), args, dir, {}, "gateway"));
		const url = `http://127.0.0.1:${gatewayPort}`;
		await waitWritable(url, logs);
		// Feature level 5 on every group: POST to every node until every hosted replica reports it.
		const deadline = Date.now() + 60_000;
		for (;;) {
			let min = Number.POSITIVE_INFINITY;
			for (const node of nodes) {
				await fetch(`${node.admin}/__ursula/feature-level`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ level: 5 }), signal: AbortSignal.timeout(10_000) });
				const report = (await (await fetch(`${node.admin}/__ursula/feature-level`, { signal: AbortSignal.timeout(10_000) })).json()) as { groups: { hosted: boolean; level?: number }[] };
				for (const g of report.groups) if (g.hosted) min = Math.min(min, g.level ?? 0);
			}
			if (min >= 5 && Number.isFinite(min)) break;
			if (Date.now() > deadline) throw new Error(`feature level 5 not reached (min ${min})\n${logs()}`);
			await sleep(250);
		}
		return { url, nodes: nodes.map((node, i) => ({ url: `http://127.0.0.1:${node.port}`, pid: (procs[i] as Proc).pid })), stop };
	} catch (error) {
		await stop();
		throw error;
	}
}

declare module "vitest" {
	export interface ProvidedContext {
		ursulaUrl: string;
		ursulaNodes: StackNode[];
	}
}

export default async function setup(project: TestProject): Promise<() => Promise<void>> {
	const stack = process.env.E2E_NODES === "3" ? await startCluster() : await startSingle();
	project.provide("ursulaUrl", stack.url);
	project.provide("ursulaNodes", [...stack.nodes]);
	return () => stack.stop();
}
