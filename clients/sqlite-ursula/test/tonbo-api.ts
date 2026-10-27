// A local stand-in for the two published Tonbo APIs `@tonbo/sqlite-ursula/tonbo` calls, as their
// contracts document them and no more: the Management API's `GET /v1/databases/{name}` and the
// token endpoint `POST /api/iam/token`. Errors are `application/problem+json` with `X-Request-Id`.
// The databases it serves point at streams of the suite's own server (test/stack.ts).
//
// Keys: `tbo_...` is a personal key the Management API takes directly, `agent_...` an Agent key it
// answers with 401 `exchange_required`, and any key in `revoked` is refused by both. Keys in
// `noStreams` get no Streams token. The token endpoint hands out `mgmt-<n>` with no audience and
// `streams-<n>` for the Streams audience, each for 600 s.
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";

const STREAMS_AUDIENCE = "https://streams.tonbo.dev";

/** A request the stand-in received: which API, its `Authorization`, and the name or audience it asked for. */
export interface Seen {
	readonly api: "management" | "token";
	readonly authorization: string | null;
	/** The database a lookup named. */
	readonly name?: string;
	/** The audience an exchange asked for, `null` for none. */
	readonly audience?: string | null;
	/** The token an exchange handed out. */
	readonly issued?: string;
}

/** A database the stand-in serves: its stream URL and state. `creating` turns `active` after `activeAfter` more lookups. */
export interface FixtureDatabase {
	url: string;
	state: "creating" | "active" | "deleting" | "deleted";
	activeAfter?: number;
	/** Answer 403: the key cannot read it. */
	forbidden?: boolean;
}

export class TonboApi {
	readonly seen: Seen[] = [];
	readonly databases = new Map<string, FixtureDatabase>();
	readonly revoked = new Set<string>(["tbo_revoked"]);
	readonly noStreams = new Set<string>(["tbo_nostreams"]);
	/** Answers the next lookups give first, in order: a status and a `Retry-After`. */
	readonly scripted: { status: number; retryAfter?: string }[] = [];
	/** The tokens handed out, and for which API. */
	readonly #issued = new Map<string, "management" | "streams">();
	#n = 0;
	#server: Server;
	origin = "";

	private constructor() {
		this.#server = createServer((req, res) => {
			this.#handle(req, res).catch((error: unknown) => {
				res.writeHead(500).end(String(error));
			});
		});
	}

	static async start(): Promise<TonboApi> {
		const api = new TonboApi();
		await new Promise<void>((resolve) => api.#server.listen(0, "127.0.0.1", resolve));
		api.origin = `http://127.0.0.1:${(api.#server.address() as AddressInfo).port}`;
		return api;
	}

	async stop(): Promise<void> {
		this.#server.closeAllConnections();
		await new Promise((resolve) => this.#server.close(resolve));
	}

	/** Forgets what it saw and what was scripted. */
	reset(): void {
		this.seen.length = 0;
		this.scripted.length = 0;
	}

	/** The requests of one API. */
	of(api: Seen["api"]): Seen[] {
		return this.seen.filter((s) => s.api === api);
	}

	#problem(res: ServerResponse, status: number, code: string, title: string, headers: Record<string, string> = {}, extra: Record<string, unknown> = {}): void {
		this.#n += 1;
		const requestId = `req-${this.#n}`;
		res.writeHead(status, { "content-type": "application/problem+json", "x-request-id": requestId, ...headers });
		res.end(JSON.stringify({ type: "about:blank", title, status, code, request_id: requestId, ...extra }));
	}

	async #handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
		const chunks: Buffer[] = [];
		for await (const chunk of req) chunks.push(chunk as Buffer);
		const authorization = req.headers.authorization ?? null;
		const bearer = authorization?.startsWith("Bearer ") === true ? authorization.slice("Bearer ".length) : "";
		const url = new URL(req.url ?? "/", this.origin);
		if (req.method === "POST" && url.pathname === "/api/iam/token") {
			const body: { audience?: string } = chunks.length === 0 ? {} : (JSON.parse(Buffer.concat(chunks).toString()) as { audience?: string });
			const audience = body.audience ?? null;
			const known = (bearer.startsWith("tbo_") || bearer.startsWith("agent_")) && !this.revoked.has(bearer);
			if (!known || (audience === STREAMS_AUDIENCE && this.noStreams.has(bearer))) {
				this.seen.push({ api: "token", authorization, audience });
				this.#problem(res, 401, "unauthenticated", "Unauthenticated");
				return;
			}
			if (audience !== null && audience !== STREAMS_AUDIENCE) {
				this.#problem(res, 400, "invalid_audience", "Invalid audience");
				return;
			}
			this.#n += 1;
			const kind = audience === null ? "management" : "streams";
			const issued = `${kind === "management" ? "mgmt" : "streams"}-${this.#n}`;
			this.#issued.set(issued, kind);
			this.seen.push({ api: "token", authorization, audience, issued });
			res.writeHead(200, { "content-type": "application/json" });
			res.end(JSON.stringify({ access_token: issued, token_type: "Bearer", expires_in: 600, expires_at: new Date(Date.now() + 600_000).toISOString() }));
			return;
		}
		const lookup = /^\/v1\/databases\/([^/]+)$/.exec(url.pathname);
		if (req.method === "GET" && lookup !== null) {
			const name = decodeURIComponent(lookup[1] as string);
			this.seen.push({ api: "management", authorization, name });
			const next = this.scripted.shift();
			if (next !== undefined) {
				this.#problem(res, next.status, next.status === 429 ? "rate_limited" : "unavailable", "Scripted", next.retryAfter === undefined ? {} : { "retry-after": next.retryAfter });
				return;
			}
			if (bearer.startsWith("agent_") && !this.revoked.has(bearer)) {
				this.#problem(res, 401, "exchange_required", "Exchange required", {}, { token_endpoint: `${this.origin}/api/iam/token` });
				return;
			}
			const personal = bearer.startsWith("tbo_") && !this.revoked.has(bearer);
			if (!personal && this.#issued.get(bearer) !== "management") {
				this.#problem(res, 401, "unauthenticated", "Unauthenticated");
				return;
			}
			const db = this.databases.get(name);
			if (db === undefined) {
				this.#problem(res, 404, "not_found", "Not found");
				return;
			}
			if (db.forbidden === true) {
				this.#problem(res, 403, "forbidden", "Forbidden");
				return;
			}
			if (db.state === "creating" && db.activeAfter !== undefined) {
				if (db.activeAfter <= 0) db.state = "active";
				else db.activeAfter -= 1;
			}
			const data = {
				name,
				kind: "sqlite",
				owner_id: "10000000-0000-4000-8000-000000000001",
				region: "local",
				url: db.url,
				incarnation: db.state === "creating" ? null : "1",
				state: db.state,
				public_read: false,
				created_at: "2026-10-28T00:00:00.000Z",
				created_by: "20000000-0000-4000-8000-000000000002",
				last_write_at: null,
				stored_bytes: { log: 0, snapshot: 0 },
			};
			res.writeHead(200, { "content-type": "application/json", etag: `"${data.incarnation ?? "0"}-${db.state}"` });
			res.end(JSON.stringify({ data }));
			return;
		}
		this.#problem(res, 404, "not_found", "Not found");
	}
}
