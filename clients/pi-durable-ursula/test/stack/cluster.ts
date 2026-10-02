// The real keyed stack for the e2e suite and the drills (keyed streams design §3.1): one or three
// `ursula server` nodes (three behind an `ursula gateway`), a keyed-mode `ursula indexer`, and
// optionally S3 (MinIO) as both the node cold store and the indexer's object store, so the indexer's
// namespaces live under the node cold root's `.keyed/` and node stream-delete GC and bucket purge
// reach them (§3.8).
//
// The nodes reach the indexer, and every component reaches S3, through FaultProxy instances, so a
// drill can take either one down, or cut keyed-state over to another indexer, without touching the
// processes. Every process logs to a file under the stack directory.
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { freePort, Proc, sleep, ursulaBinary, waitReady } from "./proc.ts";
import { FaultProxy } from "./proxy.ts";
import { S3Client, type S3Server, startS3 } from "./s3.ts";

export interface StackOptions {
	/** Directory for configs, data and logs (a fresh temp dir by default; removed by `stop()` unless KEEP_STACK=1). */
	readonly dir?: string;
	/** 1 (default) or 3 nodes; three run behind a gateway. */
	readonly nodes?: 1 | 3;
	/** Node cold store and indexer on S3 (default false: no cold store, indexer on the filesystem). */
	readonly s3?: boolean;
	/** Raft WAL backend (default memory). Node restarts that must keep data need `disk`. */
	readonly wal?: "memory" | "disk";
	/** Initial binary per node (default URSULA_BIN). */
	readonly nodeBins?: readonly string[];
	/** Nodes whose initial binary predates keyed streams: their config omits the keyed settings. */
	readonly legacyNodes?: readonly boolean[];
	/** Binary of the gateway (default URSULA_BIN). */
	readonly gatewayBin?: string;
	/** Start the keyed indexer (default true). */
	readonly indexer?: boolean;
	/** `--keyed-min-publish-interval-ms` (default 100). */
	readonly publishIntervalMs?: number;
	/** Raft groups (default 4). */
	readonly groupCount?: number;
	/** Feature level raised after start (default 1; 0 leaves the cluster at level 0). */
	readonly featureLevel?: number;
	/** Extra `[storage.cold]` lines. */
	readonly coldExtra?: readonly string[];
}

export interface NodeHandle {
	readonly id: number;
	readonly port: number;
	readonly adminPort: number;
	readonly url: string;
	readonly adminUrl: string;
	readonly dir: string;
	proc: Proc | undefined;
	bin: string;
	legacy: boolean;
}

export interface IndexerOptions {
	/** Hidden drill knob `--keyed-projection-format` (blue/green rebuild). */
	readonly format?: number;
	/** Binary (default URSULA_BIN). */
	readonly bin?: string;
}

export interface IndexerHandle {
	readonly proc: Proc;
	readonly port: number;
	readonly url: string;
}

const S3_ROOT = "ursula";
const ACCESS_ENV = (s3: S3Server): NodeJS.ProcessEnv => ({
	AWS_ACCESS_KEY_ID: s3.accessKey,
	AWS_SECRET_ACCESS_KEY: s3.secretKey,
	AWS_REGION: s3.region,
});

let stackCounter = 0;

export class Stack {
	readonly dir: string;
	readonly options: StackOptions;
	readonly nodes: NodeHandle[] = [];
	readonly groupCount: number;
	/** In front of the current indexer; nodes use it as `keyed_state_upstream` and drain URL. */
	indexerProxy!: FaultProxy;
	indexer: IndexerHandle | undefined;
	s3: S3Server | undefined;
	/** In front of S3 for every component. */
	s3Proxy: FaultProxy | undefined;
	/** The S3 bucket holding the cold root `ursula/`. */
	readonly s3Bucket: string;
	gateway: Proc | undefined;
	private gatewayPort = 0;
	private readonly extraProcs: Proc[] = [];

	private constructor(dir: string, options: StackOptions) {
		this.dir = dir;
		this.options = options;
		this.groupCount = options.groupCount ?? 4;
		this.s3Bucket = `ks-${process.pid}-${Date.now().toString(36)}-${stackCounter++}`;
	}

	/** Starts S3 (if asked), the nodes, the gateway, the indexer, and raises the feature level. */
	static async start(options: StackOptions = {}): Promise<Stack> {
		const dir = options.dir ?? mkdtempSync(join(tmpdir(), "ks-stack-"));
		mkdirSync(dir, { recursive: true });
		const stack = new Stack(dir, options);
		try {
			await stack.boot();
		} catch (error) {
			await stack.stop();
			throw error;
		}
		return stack;
	}

	/** The client entry point: the gateway, or the single node. */
	get url(): string {
		return this.gateway === undefined ? (this.nodes[0] as NodeHandle).url : `http://127.0.0.1:${this.gatewayPort}`;
	}

	/** The S3 cold root, also the indexer's `--keyed-s3-root`. */
	get s3Root(): string {
		return S3_ROOT;
	}

	s3Client(): S3Client {
		if (this.s3 === undefined) throw new Error("stack runs without S3");
		return new S3Client(this.s3.endpoint, this.s3);
	}

	/** Object keys under `{root}/{prefix}` in the stack's S3 bucket. */
	listS3(prefix: string): Promise<string[]> {
		return this.s3Client().list(this.s3Bucket, `${S3_ROOT}/${prefix}`);
	}

	private async boot(): Promise<void> {
		const n = this.options.nodes ?? 1;
		if (this.options.s3 === true) {
			this.s3 = await startS3(this.dir);
			await new S3Client(this.s3.endpoint, this.s3).createBucket(this.s3Bucket);
			this.s3Proxy = await FaultProxy.start(Number(new URL(this.s3.endpoint).port));
		}
		this.indexerProxy = await FaultProxy.start(await freePort());
		for (let i = 1; i <= n; i++) {
			const port = await freePort();
			const adminPort = await freePort();
			const nodeDir = join(this.dir, `node${i}`);
			mkdirSync(nodeDir, { recursive: true });
			this.nodes.push({
				id: i,
				port,
				adminPort,
				url: `http://127.0.0.1:${port}`,
				adminUrl: `http://127.0.0.1:${adminPort}`,
				dir: nodeDir,
				proc: undefined,
				bin: this.options.nodeBins?.[i - 1] ?? ursulaBinary(),
				legacy: this.options.legacyNodes?.[i - 1] ?? false,
			});
		}
		await Promise.all(this.nodes.map((node) => this.startNode(node.id, { wait: false })));
		await Promise.all(this.nodes.map((node) => waitReady(`${node.url}/__ursula/ready`, node.proc, 120_000)));
		if (n > 1) {
			this.gatewayPort = await freePort();
			this.startGateway(this.options.gatewayBin ?? ursulaBinary());
		}
		await this.waitWritable();
		if (this.options.indexer !== false) await this.startIndexer();
		const level = this.options.featureLevel ?? 1;
		if (level > 0) await this.raiseFeatureLevel(level);
	}

	private startGateway(bin: string): void {
		const args = ["gateway", "--listen", `127.0.0.1:${this.gatewayPort}`, "--raft-group-count", String(this.groupCount)];
		for (const node of this.nodes) args.push("--upstream", node.url);
		this.gateway = new Proc("gateway", bin, args, { cwd: this.dir, log: join(this.dir, "gateway.log") });
	}

	/** Replaces the gateway process (same port), for example with a new binary. */
	async restartGateway(bin: string = ursulaBinary()): Promise<void> {
		await this.gateway?.stop();
		this.startGateway(bin);
		await this.waitWritable();
	}

	private nodeConfig(node: NodeHandle): string {
		const n = this.nodes.length;
		const wal = this.options.wal ?? "memory";
		const lines = ["[server]", `listen = "127.0.0.1:${node.port}"`, `admin_listen = "127.0.0.1:${node.adminPort}"`];
		if (!node.legacy) lines.push(`keyed_state_upstream = "${this.indexerProxy.url}"`);
		lines.push("", "[runtime]", "core_count = 2", "", "[raft]", `node_id = ${node.id}`, `group_count = ${this.groupCount}`);
		if (n > 1) {
			lines.push(`init_membership = ${node.id === 1}`, "init_membership_per_group = false");
		}
		lines.push("", "[raft.wal]", `backend = "${wal}"`);
		if (wal === "disk") lines.push(`path = "${join(node.dir, "wal")}"`);
		else if (n > 1) lines.push("allow_volatile_multi_peer = true");
		if (n > 1) {
			for (const peer of this.nodes) lines.push("", "[[raft.peers]]", `node_id = ${peer.id}`, `url = "${peer.url}"`);
		}
		if (this.s3 !== undefined && this.s3Proxy !== undefined) {
			lines.push(
				"",
				"[storage.cold]",
				'backend = "s3"',
				`root = "${S3_ROOT}"`,
				'flush_interval = "200ms"',
				'flush_size = "64KiB"',
				'gc_interval = "500ms"',
				...(this.options.coldExtra ?? []),
				"",
				"[storage.cold.s3]",
				`bucket = "${this.s3Bucket}"`,
				`region = "${this.s3.region}"`,
				`endpoint = "${this.s3Proxy.url}"`,
				`access_key_id = "${this.s3.accessKey}"`,
				`secret_access_key = "${this.s3.secretKey}"`,
				'server_side_encryption = "none"',
				'timeout = "2s"',
				"max_retries = 1",
			);
		}
		if (!node.legacy) lines.push("", "[keyed_state]", `indexer_urls = ["${this.indexerProxy.url}"]`, 'drain_timeout = "10s"');
		return `${lines.join("\n")}\n`;
	}

	node(id: number): NodeHandle {
		const node = this.nodes.find((candidate) => candidate.id === id);
		if (node === undefined) throw new Error(`no node ${id}`);
		return node;
	}

	/** Starts node `id`, optionally with another binary (`legacy`: a binary without keyed settings). */
	async startNode(id: number, options: { bin?: string; legacy?: boolean; wait?: boolean } = {}): Promise<void> {
		const node = this.node(id);
		if (options.bin !== undefined) node.bin = options.bin;
		if (options.legacy !== undefined) node.legacy = options.legacy;
		const config = join(node.dir, "ursula.toml");
		writeFileSync(config, this.nodeConfig(node));
		node.proc = new Proc(`node${id}`, node.bin, ["server", "--config", config], {
			cwd: node.dir,
			log: join(node.dir, "node.log"),
			...(this.s3 === undefined ? {} : { env: ACCESS_ENV(this.s3) }),
		});
		if (options.wait !== false) await waitReady(`${node.url}/__ursula/ready`, node.proc, 120_000);
	}

	/** Stops node `id` (SIGTERM; `kill` for SIGKILL). */
	async stopNode(id: number, kill = false): Promise<void> {
		const proc = this.node(id).proc;
		if (proc === undefined) return;
		if (kill) await proc.kill();
		else await proc.stop(15_000);
	}

	/** Waits until a bucket create through the entry point succeeds (every group has a leader). */
	async waitWritable(timeoutMs = 120_000): Promise<void> {
		const deadline = Date.now() + timeoutMs;
		let last = "";
		while (Date.now() < deadline) {
			try {
				const r = await fetch(`${this.url}/ks-ready-probe`, { method: "PUT", signal: AbortSignal.timeout(5000) });
				last = `${r.status}`;
				if (r.ok || r.status === 409) {
					// Every group must accept a write, not only the bucket's.
					let ok = true;
					for (let i = 0; i < this.groupCount * 4 && ok; i++) {
						const s = await fetch(`${this.url}/ks-ready-probe/p${i}`, {
							method: "PUT",
							headers: { "content-type": "application/octet-stream" },
							signal: AbortSignal.timeout(5000),
						});
						ok = s.ok || s.status === 409;
						last = `stream ${s.status}`;
					}
					if (ok) return;
				}
			} catch (error) {
				last = String(error);
			}
			await sleep(250);
		}
		throw new Error(`cluster not writable within ${timeoutMs} ms: ${last}\n${this.nodes.map((n) => n.proc?.logs() ?? "").join("\n")}`);
	}

	/** Starts an indexer on a free port, makes it the proxy's target, and waits until ready. */
	async startIndexer(options: IndexerOptions = {}): Promise<IndexerHandle> {
		const handle = await this.spawnIndexer(options);
		this.indexer = handle;
		this.indexerProxy.retarget(handle.port, true);
		return handle;
	}

	/** Spawns an indexer without routing keyed-state to it (the green side of a cutover). */
	async spawnIndexer(options: IndexerOptions = {}, port?: number): Promise<IndexerHandle> {
		const listen = port ?? (await freePort());
		const tag = `indexer-${listen}`;
		const args = [
			"indexer",
			"--keyed-source-url",
			this.url,
			"--cache-dir",
			join(this.dir, `${tag}-cache`),
			"--listen",
			`127.0.0.1:${listen}`,
			"--keyed-min-publish-interval-ms",
			String(this.options.publishIntervalMs ?? 100),
		];
		if (this.s3 !== undefined && this.s3Proxy !== undefined) {
			args.push("--s3-bucket", this.s3Bucket, "--keyed-s3-root", S3_ROOT, "--s3-endpoint", this.s3Proxy.url, "--s3-region", this.s3.region);
		} else {
			const objects = join(this.dir, "objects");
			mkdirSync(objects, { recursive: true });
			args.push("--object-dir", objects);
		}
		if (options.format !== undefined) args.push("--keyed-projection-format", String(options.format));
		const proc = new Proc(tag, options.bin ?? ursulaBinary(), args, {
			cwd: this.dir,
			log: join(this.dir, `${tag}.log`),
			...(this.s3 === undefined ? {} : { env: ACCESS_ENV(this.s3) }),
		});
		const url = `http://127.0.0.1:${listen}`;
		await waitReady(`${url}/readyz`, proc);
		this.extraProcs.push(proc);
		return { proc, port: listen, url };
	}

	/** Stops the current indexer (SIGKILL with `kill`). The proxy keeps pointing at its port. */
	async stopIndexer(kill = false): Promise<void> {
		const proc = this.indexer?.proc;
		if (proc === undefined) return;
		if (kill) await proc.kill();
		else await proc.stop();
	}

	/** Restarts the indexer process on the same port and object store. */
	async restartIndexer(options: IndexerOptions = {}): Promise<void> {
		const port = this.indexer?.port;
		await this.stopIndexer();
		const handle = await this.spawnIndexer(options, port);
		this.indexer = handle;
		this.indexerProxy.retarget(handle.port, true);
	}

	/**
	 * Raises every group to `level` (C0): `POST /__ursula/feature-level` on every node until every
	 * hosted replica reports at least `level` (groups led elsewhere answer `not_leader`).
	 */
	async raiseFeatureLevel(level: number, timeoutMs = 60_000): Promise<void> {
		const deadline = Date.now() + timeoutMs;
		let last = "";
		while (Date.now() < deadline) {
			try {
				for (const node of this.nodes) {
					const r = await fetch(`${node.adminUrl}/__ursula/feature-level`, {
						method: "POST",
						headers: { "content-type": "application/json" },
						body: JSON.stringify({ level }),
						signal: AbortSignal.timeout(10_000),
					});
					last = `node ${node.id}: ${r.status} ${await r.text()}`;
				}
				if ((await this.minFeatureLevel()) >= level) return;
			} catch (error) {
				last = String(error);
			}
			await sleep(250);
		}
		throw new Error(`could not raise the feature level to ${level}: ${last}`);
	}

	/** The lowest replicated level over every hosted group replica of every node. */
	async minFeatureLevel(): Promise<number> {
		let min = Number.POSITIVE_INFINITY;
		for (const node of this.nodes) {
			const r = await fetch(`${node.adminUrl}/__ursula/feature-level`, { signal: AbortSignal.timeout(10_000) });
			if (!r.ok) return 0;
			const report = (await r.json()) as { groups: { hosted: boolean; level?: number; error?: string }[] };
			for (const group of report.groups) {
				if (!group.hosted) continue;
				min = Math.min(min, group.level ?? 0);
			}
		}
		return Number.isFinite(min) ? min : 0;
	}

	/** Creates a bucket through the entry point. */
	async createBucket(bucket: string): Promise<void> {
		const r = await fetch(`${this.url}/${bucket}`, { method: "PUT" });
		if (!r.ok && r.status !== 409) throw new Error(`create bucket ${bucket}: ${r.status} ${await r.text()}`);
	}

	/** Logs of every process, for failure messages. */
	logs(): string {
		const parts = this.nodes.map((node) => `--- node${node.id}\n${node.proc?.logs() ?? ""}`);
		if (this.gateway !== undefined) parts.push(`--- gateway\n${this.gateway.logs()}`);
		if (this.indexer !== undefined) parts.push(`--- indexer\n${this.indexer.proc.logs()}`);
		return parts.join("\n");
	}

	async stop(): Promise<void> {
		await Promise.all(this.extraProcs.map((proc) => proc.stop()));
		await this.gateway?.stop();
		await Promise.all(this.nodes.map((node) => node.proc?.stop()));
		await this.indexerProxy?.close();
		await this.s3Proxy?.close();
		await this.s3?.stop();
		if (process.env.KEEP_STACK !== "1" && this.options.dir === undefined) rmSync(this.dir, { recursive: true, force: true });
	}
}
