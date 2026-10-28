// Vitest global setup: the server the suite runs against.
// - E2E_SERVER=model: the model of the 0.8 server (`ursula-vfs-model`, clients/sqlite-vfs with the
//   `model` feature), from URSULA_VFS_MODEL or the crate's release build.
// - E2E_SERVER=ursula (the default): the Ursula from URSULA_BIN, in the default config except where
//   noted, behind test/gate.mjs, a transparent proxy that scripts the answer delays and statuses the
//   suites ask for (`fault` in test/kit.ts), as the model does, and records each request's token. The per-group hot admission limit always stays
//   at its default.
//   - Default: one `ursula server` node (`server.cluster_id` set) with its Raft WAL in its
//     temporary directory and an in-memory cold store (URSULA_COLD=none for none).
//   - E2E_NODES=3: three nodes (each with its own Raft WAL directory, `server.cluster_id` set)
//     behind an `ursula gateway`, with S3 as the cold store (see `resolveS3`: an endpoint (MinIO)
//     with static keys, or AWS S3 with the ambient AWS_* credentials), or with no cold tier when
//     URSULA_COLD=none (the nodes cannot share an in-memory one). A control server
//     (`ursulaControl`) kills and restarts nodes for test/failover.e2e.ts. With
//     E2E_GATEWAY_MAX_REQUEST_BYTES set, the gateway runs with that `--max-request-body-bytes`, and
//     the setup checks that HEAD through it advertises that `Ursula-Max-Request-Bytes` and, with
//     S3, the node's `Ursula-Max-Snapshot-Bytes`: snapshots above it go up in parts through it.
// Every server runs under test/watchdog.mjs, and the gate and the model exit with their stdin, so
// none outlives the vitest process.
import { type ChildProcess, spawn } from "node:child_process";
import { appendFileSync } from "node:fs";
import { createHash, createHmac, randomBytes } from "node:crypto";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer as createHttpServer } from "node:http";
import { createServer } from "node:net";
import { createInterface } from "node:readline";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { TestProject } from "vitest/node";

export function freePort(): Promise<number> {
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

export function ursulaBin(): string {
	const bin = process.env.URSULA_BIN;
	if (bin === undefined || bin.length === 0) throw new Error("set URSULA_BIN to an ursula binary");
	return resolve(bin);
}

/**
 * A server process with a bounded log tail. It runs under test/watchdog.mjs, which kills it once
 * this process's end of the watchdog's stdin closes: a vitest process that dies before its
 * teardown leaves no server behind.
 */
export class Proc {
	private readonly child: ChildProcess;
	private tail = "";
	private serverPid = 0;
	/** With E2E_LOG_DIR set, the full output also goes to `${E2E_LOG_DIR}/${name}.log`. */
	constructor(bin: string, args: readonly string[], cwd: string, env: NodeJS.ProcessEnv = {}, name?: string) {
		this.child = spawn(process.execPath, ["--no-warnings", join(import.meta.dirname, "watchdog.mjs"), bin, ...args], {
			cwd,
			stdio: ["pipe", "pipe", "pipe", "pipe"],
			env: { ...process.env, ...env },
		});
		(this.child.stdio[3] as NodeJS.ReadableStream | null)?.on("data", (chunk: Buffer) => {
			this.serverPid = Number.parseInt(chunk.toString(), 10);
		});
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
	/** The server's process id (for RSS sampling), once it started. */
	get pid(): number {
		return this.serverPid;
	}
	get exited(): boolean {
		return this.child.exitCode !== null || this.child.signalCode !== null;
	}
	logs(): string {
		return this.tail;
	}
	private signal(signal: NodeJS.Signals): void {
		try {
			if (this.serverPid > 0) process.kill(this.serverPid, signal);
		} catch {
			// It exited meanwhile.
		}
	}
	/** Kills the server with SIGKILL, as a crash would. */
	async kill(): Promise<void> {
		if (!this.exited) {
			const exited = new Promise((r) => this.child.once("exit", r));
			this.signal("SIGKILL");
			await exited;
		}
		this.release();
	}
	async stop(): Promise<void> {
		if (!this.exited) {
			const exited = new Promise((r) => this.child.once("exit", r));
			this.child.kill("SIGTERM");
			const timer = setTimeout(() => this.signal("SIGKILL"), 5000);
			await exited;
			clearTimeout(timer);
		}
		this.release();
	}
	private release(): void {
		this.child.stdin?.destroy();
		this.child.stdout?.destroy();
		this.child.stderr?.destroy();
		this.child.stdio[3]?.destroy();
	}
}

export async function waitReady(url: string, proc: Proc, timeoutMs = 120_000): Promise<void> {
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
export async function waitWritable(url: string, logs: () => string): Promise<void> {
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

/** A server node: its Raft node id, its HTTP URL, its admin URL and its process id (for RSS sampling). */
export interface StackNode {
	readonly id: number;
	readonly url: string;
	readonly admin: string;
	readonly pid: number;
}

interface Stack {
	readonly url: string;
	readonly nodes: readonly StackNode[];
	/** The control server's URL (E2E_NODES=3). */
	readonly control?: string;
	stop(): Promise<void>;
}

/** test/gate.mjs in front of `target`: its proxy URL, its control URL, and a stop. */
async function startGate(target: string): Promise<{ url: string; control: string; stop(): Promise<void> }> {
	const proc = spawn(process.execPath, ["--no-warnings", join(import.meta.dirname, "gate.mjs"), target], { stdio: ["pipe", "pipe", "inherit"] });
	const exited = new Promise((r) => proc.once("exit", r));
	const lines = createInterface({ input: proc.stdout as NodeJS.ReadableStream })[Symbol.asyncIterator]();
	const first = await lines.next();
	if (first.done === true) throw new Error("test/gate.mjs exited before it printed its ports");
	const { port, control } = JSON.parse(first.value as string) as { port: number; control: number };
	return {
		url: `http://127.0.0.1:${port}`,
		control: `http://127.0.0.1:${control}`,
		stop: async () => {
			// It exits when its stdin closes.
			proc.stdin?.end();
			await exited;
		},
	};
}

/**
 * The model server: it prints `{"port": n}` once it accepts connections. It exits when its stdin
 * closes, so it never outlives the vitest process, even one killed before its teardown ran.
 */
async function startModel(): Promise<Stack> {
	const bin = resolve(process.env.URSULA_VFS_MODEL ?? join(import.meta.dirname, "../../sqlite-vfs/target/release/ursula-vfs-model"));
	const child = spawn(bin, ["--exit-with-stdin"], { stdio: ["pipe", "pipe", "inherit"] });
	const port = await new Promise<number>((res, rej) => {
		let out = "";
		child.once("error", rej);
		child.once("exit", (code) => rej(new Error(`${bin} exited (${code}) before it printed its port`)));
		child.stdout?.on("data", (chunk: Buffer) => {
			out += chunk.toString();
			const line = out.split("\n").find((l) => l.startsWith("{"));
			if (line !== undefined) res((JSON.parse(line) as { port: number }).port);
		});
	});
	const stop = async (): Promise<void> => {
		if (child.exitCode === null && child.signalCode === null) {
			const exited = new Promise((r) => child.once("exit", r));
			child.kill("SIGTERM");
			await exited;
		}
	};
	return { url: `http://127.0.0.1:${port}`, nodes: [], stop };
}

/** The deployment id every server of the suites runs under (`server.cluster_id`). */
export const CLUSTER_ID = "sqlite-ursula-test";

/**
 * The TOML of one `ursula server` node bootstrapping alone: its Raft WAL in `dir`, `server.cluster_id`
 * set, so a node whose WAL was wiped and restored (test/history-real.e2e.ts) stays the same
 * deployment, itself as its only `[[raft.peers]]` entry, as every node with a WAL path lists
 * itself, and the cold store `cold` (`none` for none).
 */
export function singleNodeToml(o: { port: number; adminPort: number; dir: string; cores: number; groups: number; cold: string }): string {
	return [
		"[server]",
		`listen = "127.0.0.1:${o.port}"`,
		`admin_listen = "127.0.0.1:${o.adminPort}"`,
		`cluster_id = "${CLUSTER_ID}"`,
		"",
		"[runtime]",
		`core_count = ${o.cores}`,
		"",
		"[raft]",
		"node_id = 1",
		`group_count = ${o.groups}`,
		"",
		"[raft.wal]",
		`path = "${join(o.dir, "wal")}"`,
		"",
		"[[raft.peers]]",
		"node_id = 1",
		`url = "http://127.0.0.1:${o.port}"`,
		"",
		...(o.cold === "none" ? [] : ["[storage.cold]", `backend = "${o.cold}"`, ""]),
	].join("\n");
}

async function startSingle(): Promise<Stack> {
	const cold = process.env.URSULA_COLD ?? "memory";
	const dir = mkdtempSync(join(tmpdir(), "sqlite-ursula-node-"));
	const port = await freePort();
	const adminPort = await freePort();
	const config = join(dir, "ursula.toml");
	writeFileSync(config, singleNodeToml({ port, adminPort, dir, cores: 2, groups: 4, cold }));
	const node = new Proc(ursulaBin(), ["server", "--config", config], dir, {}, "node1");
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
	return { url, nodes: [{ id: 1, url, admin: `http://127.0.0.1:${adminPort}`, pid: node.pid }], stop };
}

// ---- S3: backend resolution, and MinIO bucket creation with a minimal path-style SigV4 request

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
	if (!r.ok && !body.includes("BucketAlreadyOwnedByYou")) throw new Error(`create S3 bucket ${bucket}: ${r.status} ${body.slice(0, 500)}`);
}

/** Where the cluster's cold tier lives: the bucket, the root inside it, and the node TOML/env. */
interface S3Target {
	readonly bucket: string;
	readonly root: string;
	readonly toml: readonly string[];
	readonly env: NodeJS.ProcessEnv;
}

/**
 * 1. URSULA_S3_ENDPOINT set (MinIO): static keys (default minioadmin). With URSULA_S3_BUCKET too,
 *    that existing bucket under URSULA_S3_PREFIX; otherwise a new bucket per stack, root `ursula`.
 * 2. Otherwise URSULA_S3_BUCKET set (AWS): the existing bucket under URSULA_S3_PREFIX, no endpoint
 *    and no keys in the TOML (the nodes inherit AWS_*, and SSE-S3 applies, as in production).
 * 3. Otherwise there is no S3 (`undefined`).
 * A shared bucket gets a per-stack root `<prefix>/sqlite-<pid>-<ts36>-<8 hex>`.
 */
async function resolveS3(): Promise<S3Target | undefined> {
	const endpoint = (process.env.URSULA_S3_ENDPOINT ?? "").replace(/\/+$/, "");
	const shared = process.env.URSULA_S3_BUCKET ?? "";
	const region = process.env.URSULA_S3_REGION ?? process.env.AWS_REGION ?? "us-east-1";
	const prefix = (process.env.URSULA_S3_PREFIX ?? "").replace(/^\/+|\/+$/g, "");
	const nonce = `sqlite-${process.pid}-${Date.now().toString(36)}-${randomBytes(4).toString("hex")}`;
	const sharedRoot = prefix === "" ? nonce : `${prefix}/${nonce}`;
	if (endpoint !== "") {
		const accessKey = process.env.URSULA_S3_ACCESS_KEY ?? "minioadmin";
		const secretKey = process.env.URSULA_S3_SECRET_KEY ?? "minioadmin";
		const bucket = shared !== "" ? shared : `sqlite-${process.pid}-${Date.now().toString(36)}`;
		if (shared === "") await createS3Bucket(endpoint, bucket, accessKey, secretKey, region);
		return {
			bucket,
			root: shared !== "" ? sharedRoot : "ursula",
			toml: [
				`bucket = "${bucket}"`,
				`region = "${region}"`,
				`endpoint = "${endpoint}"`,
				`access_key_id = "${accessKey}"`,
				`secret_access_key = "${secretKey}"`,
				'server_side_encryption = "none"',
			],
			// A developer shell's session token would otherwise ride along with the static keys.
			env: { AWS_ACCESS_KEY_ID: accessKey, AWS_SECRET_ACCESS_KEY: secretKey, AWS_REGION: region, AWS_SESSION_TOKEN: undefined },
		};
	}
	if (shared !== "") {
		if (process.env.AWS_ENDPOINT_URL) throw new Error("AWS S3 mode (URSULA_S3_BUCKET) needs AWS_ENDPOINT_URL unset");
		if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
			throw new Error("AWS S3 mode (URSULA_S3_BUCKET) needs AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY");
		}
		return { bucket: shared, root: sharedRoot, toml: [`bucket = "${shared}"`, `region = "${region}"`], env: {} };
	}
	return undefined;
}

/** E2E_GATEWAY_MAX_REQUEST_BYTES, the gateway's `--max-request-body-bytes`, or `undefined` when unset. */
function gatewayMaxRequestBytes(): number | undefined {
	const raw = process.env.E2E_GATEWAY_MAX_REQUEST_BYTES ?? "";
	if (raw === "") return undefined;
	const limit = Number(raw);
	if (!Number.isSafeInteger(limit) || limit <= 0) throw new Error(`E2E_GATEWAY_MAX_REQUEST_BYTES is a positive byte count, not ${raw}`);
	return limit;
}

/**
 * Fails the setup unless HEAD of a stream through the gateway at `url` advertises `limit` as
 * `Ursula-Max-Request-Bytes` (the gateway lowered the node's) and, with a cold store (`cold`),
 * `Ursula-Max-Snapshot-Bytes` (the node's, passed through), so a run meant to send snapshots in
 * parts through the gateway cannot pass without them.
 */
async function checkGatewayLimits(url: string, limit: number, cold: boolean): Promise<void> {
	const r = await fetch(`${url}/sqlite-e2e/probe-0`, { method: "HEAD", signal: AbortSignal.timeout(5000) });
	const request = r.headers.get("ursula-max-request-bytes");
	const snapshot = r.headers.get("ursula-max-snapshot-bytes");
	if (r.status !== 200 || request !== `${limit}` || (cold && snapshot === null)) {
		throw new Error(`HEAD through the gateway: ${r.status}, Ursula-Max-Request-Bytes ${request} (want ${limit}), Ursula-Max-Snapshot-Bytes ${snapshot}${cold ? " (want one)" : ""}`);
	}
}

/** One node of the cluster: what restarting it needs, and its process while it runs. */
interface ClusterNode {
	readonly id: number;
	readonly port: number;
	readonly admin: string;
	readonly dir: string;
	readonly config: string;
	proc: Proc;
}

async function startCluster(): Promise<Stack> {
	// S3, unless the run opts out of a cold tier with URSULA_COLD=none: the soak never runs without
	// one by accident.
	const noCold = process.env.URSULA_COLD === "none";
	const s3 = noCold ? undefined : await resolveS3();
	if (!noCold && s3 === undefined) {
		throw new Error("E2E_NODES=3 needs S3 (URSULA_S3_ENDPOINT for MinIO, URSULA_S3_BUCKET for AWS), or URSULA_COLD=none for no cold tier");
	}
	const dir = mkdtempSync(join(tmpdir(), "sqlite-ursula-cluster-"));
	// Defaults are the e2e shape; the soak (test/soak.e2e.ts) raises them with E2E_GROUPS and
	// E2E_CORES.
	const groups = Number(process.env.E2E_GROUPS ?? 4);
	const cores = Number(process.env.E2E_CORES ?? 2);
	const shape: { id: number; port: number; admin: string }[] = [];
	for (const id of [1, 2, 3]) shape.push({ id, port: await freePort(), admin: `http://127.0.0.1:${await freePort()}` });
	const nodes: ClusterNode[] = [];
	let gateway: Proc | undefined;
	let control: ReturnType<typeof createHttpServer> | undefined;
	const logs = (): string => [...nodes.map((n) => `--- node${n.id}\n${n.proc.logs()}`), `--- gateway\n${gateway?.logs() ?? ""}`].join("\n");
	const stop = async (): Promise<void> => {
		await new Promise<void>((r) => (control?.listening === true ? control.close(() => r()) : r()));
		await Promise.all([...nodes.map((n) => n.proc.stop()), gateway?.stop()]);
		rmSync(dir, { recursive: true, force: true });
	};
	const spawnNode = (node: { id: number; dir: string; config: string }): Proc => new Proc(ursulaBin(), ["server", "--config", node.config], node.dir, s3?.env ?? {}, `node${node.id}`);
	try {
		for (const node of shape) {
			const nodeDir = join(dir, `node${node.id}`);
			mkdirSync(nodeDir, { recursive: true });
			const config = join(nodeDir, "ursula.toml");
			const lines = [
				"[server]",
				`listen = "127.0.0.1:${node.port}"`,
				`admin_listen = "${node.admin.replace("http://", "")}"`,
				`cluster_id = "${CLUSTER_ID}"`,
				"",
				"[runtime]",
				`core_count = ${cores}`,
				"",
				"[raft]",
				`node_id = ${node.id}`,
				`group_count = ${groups}`,
				"",
				"[raft.wal]",
				`path = "${join(nodeDir, "wal")}"`,
			];
			for (const peer of shape) lines.push("", "[[raft.peers]]", `node_id = ${peer.id}`, `url = "http://127.0.0.1:${peer.port}"`);
			if (s3 !== undefined) lines.push("", "[storage.cold]", 'backend = "s3"', `root = "${s3.root}"`, "", "[storage.cold.s3]", ...s3.toml);
			writeFileSync(config, `${lines.join("\n")}\n`);
			const spec = { ...node, dir: nodeDir, config };
			nodes.push({ ...spec, proc: spawnNode(spec) });
		}
		await Promise.all(nodes.map((node) => waitReady(`http://127.0.0.1:${node.port}/__ursula/ready`, node.proc)));
		const gatewayPort = await freePort();
		const args = ["gateway", "--listen", `127.0.0.1:${gatewayPort}`, "--raft-group-count", `${groups}`];
		for (const node of nodes) args.push("--upstream", `http://127.0.0.1:${node.port}`);
		const gatewayLimit = gatewayMaxRequestBytes();
		if (gatewayLimit !== undefined) args.push("--max-request-body-bytes", `${gatewayLimit}`);
		gateway = new Proc(ursulaBin(), args, dir, {}, "gateway");
		const url = `http://127.0.0.1:${gatewayPort}`;
		await waitWritable(url, logs);
		if (gatewayLimit !== undefined) await checkGatewayLimits(url, gatewayLimit, s3 !== undefined);
		// `GET /nodes` lists the nodes (id, url, admin, pid, running), `POST /nodes/<id>/kill` kills
		// one with SIGKILL, `POST /nodes/<id>/start` starts it again on its config and WAL and answers
		// once it is ready. Both answer the listing.
		const listing = (): unknown => nodes.map((n) => ({ id: n.id, url: `http://127.0.0.1:${n.port}`, admin: n.admin, pid: n.proc.pid, running: !n.proc.exited }));
		control = createHttpServer((req, res) => {
			const answer = (status: number, body: unknown): void => {
				res.writeHead(status, { "content-type": "application/json" }).end(JSON.stringify(body));
			};
			if (req.method === "GET" && req.url === "/nodes") return answer(200, listing());
			const route = /^\/nodes\/(\d+)\/(kill|start)$/.exec(req.url ?? "");
			const node = route === null ? undefined : nodes.find((n) => n.id === Number(route[1]));
			if (req.method !== "POST" || route === null || node === undefined) return answer(404, { error: `no route ${req.method} ${req.url}` });
			const act = async (): Promise<void> => {
				if (route[2] === "kill") return node.proc.kill();
				if (!node.proc.exited) throw new Error(`node${node.id} is running`);
				node.proc = spawnNode(node);
				await waitReady(`http://127.0.0.1:${node.port}/__ursula/ready`, node.proc);
			};
			act().then(
				() => answer(200, listing()),
				(error: unknown) => answer(500, { error: String(error) }),
			);
		});
		const controlPort = await freePort();
		await new Promise<void>((r) => control?.listen(controlPort, "127.0.0.1", () => r()));
		return {
			url,
			nodes: nodes.map((node) => ({ id: node.id, url: `http://127.0.0.1:${node.port}`, admin: node.admin, pid: node.proc.pid })),
			control: `http://127.0.0.1:${controlPort}`,
			stop,
		};
	} catch (error) {
		await stop();
		throw error;
	}
}

declare module "vitest" {
	export interface ProvidedContext {
		/** The URL the suites open streams at: the model, or the gate in front of Ursula. */
		ursulaUrl: string;
		/** The server itself, with no gate in front (the gateway with E2E_NODES=3). */
		ursulaDirect: string;
		/** Where `fault` (test/kit.ts) scripts a fault: the model's `/__model/faults` or the gate's `/faults`. */
		ursulaFaults: string;
		/** The gate's control URL (test/gate.mjs), or "" on the model. */
		ursulaGate: string;
		ursulaNodes: StackNode[];
		/** The cluster's control server (E2E_NODES=3), or "". */
		ursulaControl: string;
		/** `model` for the model server, `ursula` otherwise. */
		ursulaServer: "model" | "ursula";
	}
}

export default async function setup(project: TestProject): Promise<() => Promise<void>> {
	const server = process.env.E2E_SERVER ?? "ursula";
	if (server !== "model" && server !== "ursula") throw new Error(`E2E_SERVER is model or ursula, not ${server}`);
	if (server === "model") {
		const stack = await startModel();
		project.provide("ursulaServer", "model");
		project.provide("ursulaUrl", stack.url);
		project.provide("ursulaDirect", stack.url);
		project.provide("ursulaFaults", `${stack.url}/__model/faults`);
		project.provide("ursulaGate", "");
		project.provide("ursulaNodes", []);
		project.provide("ursulaControl", "");
		return () => stack.stop();
	}
	const stack = process.env.E2E_NODES === "3" ? await startCluster() : await startSingle();
	let gate: Awaited<ReturnType<typeof startGate>>;
	try {
		gate = await startGate(stack.url);
	} catch (error) {
		await stack.stop();
		throw error;
	}
	project.provide("ursulaServer", "ursula");
	project.provide("ursulaUrl", gate.url);
	project.provide("ursulaDirect", stack.url);
	project.provide("ursulaFaults", `${gate.control}/faults`);
	project.provide("ursulaGate", gate.control);
	project.provide("ursulaNodes", [...stack.nodes]);
	project.provide("ursulaControl", stack.control ?? "");
	return async () => {
		await gate.stop();
		await stack.stop();
	};
}
