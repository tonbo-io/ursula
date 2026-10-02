// A tiny local HTTP server for transport unit tests, plus an HTTP front for FakeUrsula.
import { createServer, type IncomingMessage, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import type { FakeUrsula } from "../src/fake/index.ts";
import { KEYED_ROWS_MEDIA_TYPE } from "../src/protocol.ts";
import type { HttpOutcome } from "../src/transport.ts";

export interface StubRequest {
	readonly method: string;
	readonly path: string;
	readonly query: URLSearchParams;
	readonly headers: Readonly<Record<string, string | string[] | undefined>>;
	readonly body: Uint8Array;
}

export interface StubResponse {
	readonly status: number;
	readonly headers?: Record<string, string>;
	readonly body?: Uint8Array | string;
	/** Wait before answering. */
	readonly delayMs?: number;
}

export type StubHandler = (request: StubRequest) => StubResponse | Promise<StubResponse>;

export interface Stub {
	readonly baseUrl: string;
	readonly requests: StubRequest[];
	handler: StubHandler;
	close(): Promise<void>;
}

async function readBody(req: IncomingMessage): Promise<Uint8Array> {
	const chunks: Buffer[] = [];
	for await (const chunk of req) chunks.push(chunk as Buffer);
	return new Uint8Array(Buffer.concat(chunks));
}

export async function startStub(handler: StubHandler): Promise<Stub> {
	const requests: StubRequest[] = [];
	const stub: { handler: StubHandler } = { handler };
	const server: Server = createServer((req, res) => {
		void (async () => {
			const url = new URL(req.url ?? "/", "http://stub");
			const request: StubRequest = {
				method: req.method ?? "GET",
				path: url.pathname,
				query: url.searchParams,
				headers: req.headers,
				body: await readBody(req),
			};
			requests.push(request);
			const response = await stub.handler(request);
			if (response.delayMs !== undefined) await new Promise((r) => setTimeout(r, response.delayMs));
			if (res.destroyed) return;
			res.writeHead(response.status, response.headers ?? {});
			res.end(response.body === undefined ? undefined : Buffer.from(response.body));
		})().catch((error: unknown) => {
			res.destroy(error instanceof Error ? error : new Error(String(error)));
		});
	});
	await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
	const { port } = server.address() as AddressInfo;
	return {
		baseUrl: `http://127.0.0.1:${port}`,
		requests,
		get handler() {
			return stub.handler;
		},
		set handler(h: StubHandler) {
			stub.handler = h;
		},
		close: () =>
			new Promise<void>((resolve) => {
				server.closeAllConnections();
				server.close(() => resolve());
			}),
	};
}

const toResponse = (o: HttpOutcome, body?: Uint8Array | string): StubResponse => ({
	status: o.status,
	headers: { ...o.headers },
	body: body ?? o.message ?? "",
});

const optInt = (q: URLSearchParams, name: string): number | undefined => {
	const v = q.get(name);
	return v === null ? undefined : Number(v);
};

/**
 * Serve a FakeUrsula over HTTP: `HEAD|PUT|POST|GET /{bucket}/{stream}` and
 * `GET /{bucket}/{stream}/keyed-state`. It goes through the fake's own transports, so fold and
 * record-match semantics are the fake's; this checks the HTTP clients' encoding and parsing.
 */
export function fakeHandler(fake: FakeUrsula): StubHandler {
	return async (req) => {
		const segments = req.path.split("/").filter((s) => s.length > 0).map(decodeURIComponent);
		const keyed = segments.at(-1) === "keyed-state";
		const path = `/${(keyed ? segments.slice(0, -1) : segments).join("/")}`;
		if (keyed) {
			const q = req.query;
			const scan = await fake.keyedStateTransport(path).scan({
				...(q.has("key") ? { key: q.get("key") as string } : {}),
				...(q.has("start") ? { start: q.get("start") as string } : {}),
				...(q.has("after") ? { after: q.get("after") as string } : {}),
				...(q.has("end") ? { end: q.get("end") as string } : {}),
				...(q.has("limit") ? { limit: optInt(q, "limit") as number } : {}),
				...(q.has("min_through_record") ? { minThroughRecord: optInt(q, "min_through_record") as number } : {}),
				...(q.has("timeout_ms") ? { timeoutMs: optInt(q, "timeout_ms") as number } : {}),
			});
			if (scan.status !== 200) return toResponse(scan);
			const body = scan.rows.map((r) => `{"key":"${r.key}","record":${r.record},"value":${r.value}}\n`).join("");
			return { status: 200, headers: { ...scan.headers, "content-type": KEYED_ROWS_MEDIA_TYPE }, body };
		}
		const log = fake.logTransport(path);
		switch (req.method) {
			case "HEAD":
				return toResponse(await log.head(), "");
			case "PUT":
				return toResponse(await log.create());
			case "POST":
				return toResponse(await log.append(req.body, Number(req.headers["stream-record-match"])));
			case "GET": {
				const q = req.query;
				const maxBytes = optInt(q, "max_bytes");
				const maxRecords = optInt(q, "max_records");
				const longPollMs = q.get("live") === "long-poll" ? (optInt(q, "timeout_ms") ?? 1000) : undefined;
				const read = await log.readRecords(Number(q.get("record")), {
					...(maxBytes === undefined ? {} : { maxBytes }),
					...(maxRecords === undefined ? {} : { maxRecords }),
					...(longPollMs === undefined ? {} : { longPollMs }),
					...(q.get("consistency") === "leader" ? { leader: true } : {}),
				});
				if (read.status !== 200) return toResponse(read);
				const body = Buffer.concat(read.records.flatMap((r) => [Buffer.from(r), Buffer.from("\n")]));
				return { status: 200, headers: { ...read.headers }, body: new Uint8Array(body) };
			}
			default:
				return { status: 405 };
		}
	};
}
