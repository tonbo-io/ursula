// A fault proxy in front of the server, for test/outage.e2e.ts: it forwards HTTP requests to the
// server until told to cut it off, in one of two ways an outage looks to a client:
// - `blackhole`: connections are accepted and requests read, but nothing is forwarded or answered
//   (a partition, a stalled path): each request waits for its own timeout.
// - `refuse`: the port stops listening and every open connection is reset, so a request fails at
//   once with ECONNREFUSED or a reset (a node or a gateway down).
// `forward` heals it. Requests held by a blackhole stay unanswered after the heal, as a partition's
// would, and time out on the client. For test/cluster.e2e.ts it also misroutes: the server it
// forwards to changes with `PUT /target`, as a routing mistake in front of a deployment would.
//
// It also scripts faults for the next requests it forwards, as the model's `/__model/faults` does
// (clients/sqlite-vfs/src/model/faults.rs), so the suites that hold an answer back or script a
// refusal run against a real server too (test/stack.ts puts a gate in front of it). Two faults are
// scripted: `delay <ms>`, the request forwarded at once and its answer held back for `ms`, and
// `status <code>`, the request answered with that status in place of the server and not forwarded,
// as a layer in front of it would.
//
// It records the method and the `Authorization` header of the last requests it received, so a
// suite sees which token went out on each request of a stream (test/token.e2e.ts).
//
// For test/token-change.e2e.ts and test/token.e2e.ts it also stands in for an authorizer: once told
// the tokens it accepts, it answers `401` to every request that does not carry one of them as
// `Authorization: Bearer <token>` before that token expires, and forwards nothing of it.
//
// argv: the server's URL. Prints {"port": n, "control": n} once it listens: the proxy's port and a
// control port of its own, which no mode affects. Control requests:
//   PUT /mode {"mode": "forward" | "blackhole" | "refuse"} -> 204
//   PUT /target {"url": "<server URL>"} -> 204: later requests go to that server
//   PUT /auth {"token": "<token>" | null} -> 204: later requests without that token get `401`
//     (null: none required)
//   PUT /auth {"tokens": [{"token": "<token>", "expiresAt": <ms since the epoch> | null}]} -> 204:
//     later requests get `401` unless they carry one of the tokens before its expiry (null: never)
//   GET /stats -> {"mode", "forwarded", "answered", "held", "refused", "unauthorized", "tokens"}
//   GET /requests?path=<path> -> [{"method", "authorization"}]: the recorded requests to `path` and
//     below it, oldest first, `authorization` null for a request without one
//   POST /faults[?times=<n>][&method=<METHOD>], body `delay <ms>` or `status <code>` -> 204: the
//     fault for the next `times` (1) requests with that method (any method when absent) that the
//     authorizer lets through
// It exits when its stdin closes, so it never outlives the test process that started it.
import { createServer, request } from "node:http";

let target = new URL(process.argv[2]);
/** `tokens`: the tokens the authorizer accepts, `{ token, expiresAt }` with `expiresAt` null for no expiry, or null: no authorizer. */
const state = { mode: "forward", forwarded: 0, answered: 0, held: 0, refused: 0, unauthorized: 0, tokens: null };
const sockets = new Set();
let port = 0;
/** Scripted faults, in order: `{ method, delayMs, status, times }`, with one of `delayMs` and `status`. */
const faults = [];
/** The last requests received, oldest first: `{ path, method, authorization }`. */
const seen = [];
const SEEN = 10_000;

/** Whether the authorizer lets a request with the `Authorization` header `authorization` through. */
function authorized(authorization) {
	if (state.tokens === null) return true;
	const now = Date.now();
	return state.tokens.some(({ token, expiresAt }) => authorization === `Bearer ${token}` && (expiresAt === null || now < expiresAt));
}

/** The fault for a request with `method`: the first scripted fault that matches, used once. */
function takeFault(method) {
	const at = faults.findIndex((f) => f.method === undefined || f.method === method);
	if (at < 0) return undefined;
	const fault = faults[at];
	fault.times -= 1;
	if (fault.times <= 0) faults.splice(at, 1);
	return fault;
}

const proxy = createServer((req, res) => {
	seen.push({ path: new URL(req.url, "http://gate").pathname, method: req.method, authorization: req.headers.authorization ?? null });
	if (seen.length > SEEN) seen.shift();
	if (state.mode === "blackhole") {
		state.held++;
		// Read the body and never answer.
		req.resume();
		return;
	}
	if (!authorized(req.headers.authorization)) {
		state.unauthorized++;
		// Answered once the body is read: an answer before it can close the connection under a
		// writer still sending, which then sees no answer instead of the `401`.
		req.on("end", () => res.writeHead(401, { "content-length": "0" }).end());
		req.resume();
		return;
	}
	const fault = takeFault(req.method);
	if (fault?.status !== undefined) {
		// Answered in place of the server once the body is read: nothing is forwarded.
		state.answered++;
		req.on("end", () => res.writeHead(fault.status, { "content-length": "0" }).end());
		req.resume();
		return;
	}
	state.forwarded++;
	const delayMs = fault?.delayMs ?? 0;
	const up = request({ host: target.hostname, port: target.port, method: req.method, path: req.url, headers: req.headers }, (answer) => {
		const send = () => {
			res.writeHead(answer.statusCode ?? 502, answer.headers);
			answer.pipe(res);
		};
		if (delayMs > 0) setTimeout(send, delayMs);
		else send();
	});
	up.on("error", () => res.destroy());
	req.pipe(up);
});
proxy.keepAliveTimeout = 60_000;
proxy.on("connection", (socket) => {
	if (state.mode === "refuse") {
		state.refused++;
		socket.destroy();
		return;
	}
	sockets.add(socket);
	socket.once("close", () => sockets.delete(socket));
});

/** Listens on `port` (any free one the first time), retrying while the old socket lingers. */
function listen() {
	return new Promise((resolve, reject) => {
		const attempt = (left) => {
			const fail = (error) => {
				proxy.off("listening", done);
				if (error.code === "EADDRINUSE" && left > 0) setTimeout(() => attempt(left - 1), 50);
				else reject(error);
			};
			const done = () => {
				proxy.off("error", fail);
				port = proxy.address().port;
				resolve();
			};
			proxy.once("error", fail);
			proxy.once("listening", done);
			proxy.listen(port, "127.0.0.1");
		};
		attempt(100);
	});
}

async function setMode(mode) {
	if (mode === state.mode) return;
	if (state.mode === "refuse") await listen();
	state.mode = mode;
	if (mode === "refuse") {
		await new Promise((resolve) => {
			proxy.close(() => resolve());
			for (const socket of sockets) socket.resetAndDestroy();
		});
	}
}

const control = createServer((req, res) => {
	if (req.method === "GET" && req.url === "/stats") {
		res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(state));
		return;
	}
	if (req.method === "GET" && req.url.startsWith("/requests?")) {
		const path = new URL(req.url, "http://gate").searchParams.get("path") ?? "";
		const of = seen.filter((r) => r.path === path || r.path.startsWith(`${path}/`)).map(({ method, authorization }) => ({ method, authorization }));
		res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(of));
		return;
	}
	let body = "";
	req.on("data", (chunk) => {
		body += chunk;
	});
	req.on("end", () => {
		const url = new URL(req.url, "http://gate");
		if (req.method === "POST" && url.pathname === "/faults") {
			const delay = /^delay (\d+)$/.exec(body.trim());
			const status = /^status ([1-5]\d\d)$/.exec(body.trim());
			const times = Number(url.searchParams.get("times") ?? 1);
			if ((delay === null && status === null) || !Number.isInteger(times) || times < 0) {
				res.writeHead(400).end(`the gate scripts only \`delay <ms>\` and \`status <code>\`: ${body}`);
				return;
			}
			const method = url.searchParams.get("method") ?? undefined;
			if (times > 0) faults.push(delay !== null ? { method, delayMs: Number(delay[1]), times } : { method, status: Number(status[1]), times });
			res.writeHead(204).end();
			return;
		}
		if (req.method === "PUT" && url.pathname === "/auth") {
			let parsed;
			try {
				parsed = JSON.parse(body);
			} catch {
				parsed = undefined;
			}
			const { token, tokens } = parsed ?? {};
			const expiring = (t) => typeof t?.token === "string" && (t.expiresAt === null || Number.isFinite(t.expiresAt));
			if (token !== undefined ? token !== null && typeof token !== "string" : !Array.isArray(tokens) || !tokens.every(expiring)) {
				res.writeHead(400).end(`not a JSON body with a token or null, or with tokens and their expiries: ${body}`);
				return;
			}
			state.tokens = token !== undefined ? (token === null ? null : [{ token, expiresAt: null }]) : tokens.map((t) => ({ token: t.token, expiresAt: t.expiresAt }));
			res.writeHead(204).end();
			return;
		}
		if (req.method === "PUT" && url.pathname === "/target") {
			try {
				target = new URL(JSON.parse(body).url);
			} catch {
				res.writeHead(400).end(`not a JSON body with a URL: ${body}`);
				return;
			}
			res.writeHead(204).end();
			return;
		}
		let mode;
		try {
			({ mode } = JSON.parse(body));
		} catch {
			res.writeHead(400).end(`not a JSON body: ${body}`);
			return;
		}
		if (req.method !== "PUT" || req.url !== "/mode" || !["forward", "blackhole", "refuse"].includes(mode)) {
			res.writeHead(400).end();
			return;
		}
		setMode(mode).then(
			() => res.writeHead(204).end(),
			(error) => res.writeHead(500).end(String(error)),
		);
	});
});

await listen();
control.listen(0, "127.0.0.1", () => {
	process.stdout.write(`${JSON.stringify({ port, control: control.address().port })}\n`);
});
process.stdin.on("end", () => process.exit(0));
process.stdin.resume();
