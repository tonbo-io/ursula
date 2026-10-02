// S3 for the e2e stack and the drills: a MinIO server (spawned from MINIO_BIN / `minio` on PATH, or an
// external one such as the CI container named by URSULA_S3_ENDPOINT), and a minimal SigV4 client the
// checks use to create buckets and list prefixes.
import { execFileSync } from "node:child_process";
import { createHash, createHmac } from "node:crypto";
import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { freePort, Proc, waitReady } from "./proc.ts";

export interface S3Credentials {
	readonly accessKey: string;
	readonly secretKey: string;
	readonly region: string;
}

/** One S3 endpoint plus credentials. */
export interface S3Server extends S3Credentials {
	readonly endpoint: string;
	stop(): Promise<void>;
}

const sha256 = (data: string | Uint8Array): string => createHash("sha256").update(data).digest("hex");
const hmac = (key: string | Buffer, data: string): Buffer => createHmac("sha256", key).update(data).digest();
/** RFC 3986 encoding, as SigV4 requires. */
const enc = (s: string): string => encodeURIComponent(s).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
const encPath = (key: string): string => key.split("/").map(enc).join("/");

function minioBinary(): string | undefined {
	const fromEnv = process.env.MINIO_BIN;
	if (fromEnv !== undefined && fromEnv.length > 0) return fromEnv;
	try {
		return execFileSync("which", ["minio"], { encoding: "utf8" }).trim() || undefined;
	} catch {
		return undefined;
	}
}

/** Whether the stack can reach S3: an external endpoint is configured or a MinIO binary exists. */
export function s3Available(): boolean {
	return (process.env.URSULA_S3_ENDPOINT ?? "") !== "" || minioBinary() !== undefined;
}

/**
 * The S3 server: URSULA_S3_ENDPOINT (with URSULA_S3_ACCESS_KEY / URSULA_S3_SECRET_KEY, default
 * `minioadmin`) when set, otherwise a MinIO spawned on a free port with its data under `dir`.
 */
export async function startS3(dir: string): Promise<S3Server> {
	const region = process.env.URSULA_S3_REGION ?? "us-east-1";
	const external = process.env.URSULA_S3_ENDPOINT ?? "";
	if (external !== "") {
		return {
			endpoint: external.replace(/\/+$/, ""),
			accessKey: process.env.URSULA_S3_ACCESS_KEY ?? "minioadmin",
			secretKey: process.env.URSULA_S3_SECRET_KEY ?? "minioadmin",
			region,
			stop: async () => {},
		};
	}
	const bin = minioBinary();
	if (bin === undefined) throw new Error("S3 needs URSULA_S3_ENDPOINT or a MinIO binary (MINIO_BIN or `minio` on PATH)");
	const port = await freePort();
	const consolePort = await freePort();
	const data = join(dir, "minio");
	mkdirSync(data, { recursive: true });
	const accessKey = "minioadmin";
	const secretKey = "minioadmin";
	const proc = new Proc("minio", bin, ["server", data, "--address", `127.0.0.1:${port}`, "--console-address", `127.0.0.1:${consolePort}`, "--quiet"], {
		cwd: dir,
		log: join(dir, "minio.log"),
		// Public Prometheus metrics: the soak reads S3 requests by API from /minio/v2/metrics/cluster.
		env: { MINIO_ROOT_USER: accessKey, MINIO_ROOT_PASSWORD: secretKey, MINIO_BROWSER: "off", MINIO_PROMETHEUS_AUTH_TYPE: "public" },
	});
	const endpoint = `http://127.0.0.1:${port}`;
	await waitReady(`${endpoint}/minio/health/live`, proc);
	return { endpoint, accessKey, secretKey, region, stop: () => proc.stop() };
}

/** A minimal path-style SigV4 S3 client: bucket create, prefix listing, object read. */
export class S3Client {
	private readonly endpoint: string;
	private readonly credentials: S3Credentials;

	constructor(endpoint: string, credentials: S3Credentials) {
		this.endpoint = endpoint;
		this.credentials = credentials;
	}

	private async request(method: string, path: string, query: Record<string, string> = {}): Promise<Response> {
		const url = new URL(`${this.endpoint}${path}`);
		for (const [k, v] of Object.entries(query)) url.searchParams.set(k, v);
		const amzDate = new Date().toISOString().replace(/[:-]|\.\d{3}/g, "");
		const date = amzDate.slice(0, 8);
		const payloadHash = sha256("");
		const headers: Record<string, string> = { host: url.host, "x-amz-content-sha256": payloadHash, "x-amz-date": amzDate };
		const names = Object.keys(headers).sort();
		const canonicalQuery = [...url.searchParams.entries()]
			.map(([k, v]) => [enc(k), enc(v)] as const)
			.sort(([a, av], [b, bv]) => (a === b ? (av < bv ? -1 : 1) : a < b ? -1 : 1))
			.map(([k, v]) => `${k}=${v}`)
			.join("&");
		const canonical = [method, url.pathname, canonicalQuery, names.map((n) => `${n}:${headers[n]}\n`).join(""), names.join(";"), payloadHash].join("\n");
		const scope = `${date}/${this.credentials.region}/s3/aws4_request`;
		const toSign = ["AWS4-HMAC-SHA256", amzDate, scope, sha256(canonical)].join("\n");
		const key = hmac(hmac(hmac(hmac(`AWS4${this.credentials.secretKey}`, date), this.credentials.region), "s3"), "aws4_request");
		const signature = createHmac("sha256", key).update(toSign).digest("hex");
		const { host: _host, ...sent } = headers;
		return fetch(url, {
			method,
			headers: {
				...sent,
				authorization: `AWS4-HMAC-SHA256 Credential=${this.credentials.accessKey}/${scope}, SignedHeaders=${names.join(";")}, Signature=${signature}`,
			},
			signal: AbortSignal.timeout(10_000),
		});
	}

	/** Creates `bucket` (an existing one is fine). */
	async createBucket(bucket: string): Promise<void> {
		const r = await this.request("PUT", `/${enc(bucket)}`);
		const body = await r.text();
		if (!r.ok && !body.includes("BucketAlreadyOwnedByYou")) throw new Error(`create S3 bucket ${bucket}: ${r.status} ${body}`);
	}

	/** Every object key in `bucket` under `prefix`. */
	async list(bucket: string, prefix: string): Promise<string[]> {
		const keys: string[] = [];
		let token: string | undefined;
		for (;;) {
			const r = await this.request("GET", `/${enc(bucket)}`, {
				"list-type": "2",
				prefix,
				...(token === undefined ? {} : { "continuation-token": token }),
			});
			const body = await r.text();
			if (!r.ok) throw new Error(`list s3://${bucket}/${prefix}: ${r.status} ${body}`);
			for (const m of body.matchAll(/<Key>([^<]*)<\/Key>/g)) keys.push(unescapeXml(m[1] ?? ""));
			const next = /<NextContinuationToken>([^<]*)<\/NextContinuationToken>/.exec(body)?.[1];
			if (!/<IsTruncated>true<\/IsTruncated>/.test(body) || next === undefined) return keys;
			token = unescapeXml(next);
		}
	}

	/** An object's bytes, or undefined when absent. */
	async get(bucket: string, key: string): Promise<Uint8Array | undefined> {
		const r = await this.request("GET", `/${enc(bucket)}/${encPath(key)}`);
		if (r.status === 404) return undefined;
		if (!r.ok) throw new Error(`get s3://${bucket}/${key}: ${r.status} ${await r.text()}`);
		return new Uint8Array(await r.arrayBuffer());
	}
}

function unescapeXml(s: string): string {
	return s.replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/&apos;/g, "'").replace(/&#39;/g, "'").replace(/&amp;/g, "&");
}
