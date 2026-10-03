// The WAL stream: one Ursula `application/json` stream whose records are the database's commits.
// Only three operations are used: create (idempotent), conditional append (`Stream-Record-Match`),
// and record-aligned reads (`?record=N`, NDJSON body). The fake implements the same contract in memory.

/** Outcome of a conditional append. `next` is the stream's record tail after the call. */
export type AppendOutcome = { readonly ok: true; readonly next: number } | { readonly ok: false; readonly next: number };

export interface WalStream {
	/** Create the stream if it does not exist. */
	create(): Promise<void>;
	/** Append one record iff the tail is `match`. Throws when the outcome is unknown (no response). */
	append(record: string, match: number): Promise<AppendOutcome>;
	/** Records from ordinal `from` (a page; empty at the tail). */
	read(from: number): Promise<string[]>;
}

/** Thrown when the outcome of a request is unknown (connection error, timeout). */
export class UnknownOutcomeError extends Error {
	override readonly name = "UnknownOutcomeError";
}

export interface HttpWalStreamOptions {
	/** Node base URL, e.g. `http://127.0.0.1:4437`. */
	readonly baseUrl: string;
	/** `{bucket}/{stream}`. */
	readonly stream: string;
	readonly timeoutMs?: number;
	/** Records per read page (default 1000). */
	readonly pageRecords?: number;
}

export class HttpWalStream implements WalStream {
	readonly url: string;
	private readonly timeoutMs: number;
	private readonly pageRecords: number;

	constructor(options: HttpWalStreamOptions) {
		this.url = `${options.baseUrl.replace(/\/+$/, "")}/${options.stream.split("/").map(encodeURIComponent).join("/")}`;
		this.timeoutMs = options.timeoutMs ?? 10_000;
		this.pageRecords = options.pageRecords ?? 1000;
	}

	private async request(method: string, url: string, headers: Record<string, string> = {}, body?: string): Promise<Response> {
		try {
			const r = await fetch(url, { method, headers, signal: AbortSignal.timeout(this.timeoutMs), ...(body === undefined ? {} : { body }) });
			return r;
		} catch (error) {
			throw new UnknownOutcomeError(`${method} ${url}: ${String(error)}`, { cause: error });
		}
	}

	async create(): Promise<void> {
		const r = await this.request("PUT", this.url, { "content-type": "application/json" });
		await r.arrayBuffer();
		if (!r.ok && r.status !== 409) throw new Error(`create ${this.url}: ${r.status}`);
	}

	async append(record: string, match: number): Promise<AppendOutcome> {
		const r = await this.request("POST", this.url, { "content-type": "application/json", "stream-record-match": String(match) }, record);
		const text = await r.text();
		const next = Number(r.headers.get("stream-record-next") ?? Number.NaN);
		if (r.status === 412) return { ok: false, next };
		if (!r.ok) throw new Error(`append ${this.url}: ${r.status} ${text}`);
		return { ok: true, next };
	}

	async read(from: number): Promise<string[]> {
		const r = await this.request("GET", `${this.url}?record=${from}&max_records=${this.pageRecords}`);
		const text = await r.text();
		if (r.status === 204) return [];
		if (r.status !== 200) throw new Error(`read ${this.url} from ${from}: ${r.status} ${text}`);
		return text.split("\n").filter((line) => line.length > 0);
	}
}

/** In-memory fake: streams by path, with hooks to fail or lose an append. */
export class FakeUrsula {
	readonly streams = new Map<string, string[]>();
	/** Called before an append is applied; may throw to model a lost request. */
	beforeAppend: ((path: string, record: string) => void) | undefined;

	stream(path: string, pageRecords = 100): WalStream {
		return {
			create: async () => {
				if (!this.streams.has(path)) this.streams.set(path, []);
			},
			append: async (record, match) => {
				const log = this.log(path);
				this.beforeAppend?.(path, record);
				if (match !== log.length) return { ok: false, next: log.length };
				JSON.parse(record);
				log.push(record);
				return { ok: true, next: log.length };
			},
			read: async (from) => this.log(path).slice(from, from + pageRecords),
		};
	}

	private log(path: string): string[] {
		const log = this.streams.get(path);
		if (log === undefined) throw new Error(`no stream ${path}`);
		return log;
	}
}
