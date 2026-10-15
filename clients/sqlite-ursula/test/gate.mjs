// A fault proxy in front of the server, for test/outage.e2e.ts: it forwards HTTP requests to the
// server until told to cut it off, in one of two ways an outage looks to a client:
// - `blackhole`: connections are accepted and requests read, but nothing is forwarded or answered
//   (a partition, a stalled path): each request waits for its own timeout.
// - `refuse`: the port stops listening and every open connection is reset, so a request fails at
//   once with ECONNREFUSED or a reset (a node or a gateway down).
// `forward` heals it. Requests held by a blackhole stay unanswered after the heal, as a partition's
// would, and time out on the client.
//
// argv: the server's URL. Prints {"port": n, "control": n} once it listens: the proxy's port and a
// control port of its own, which no mode affects. Control requests:
//   PUT /mode {"mode": "forward" | "blackhole" | "refuse"} -> 204
//   GET /stats -> {"mode", "forwarded", "held", "refused"}
// It exits when its stdin closes, so it never outlives the test process that started it.
import { createServer, request } from "node:http";

const target = new URL(process.argv[2]);
const state = { mode: "forward", forwarded: 0, held: 0, refused: 0 };
const sockets = new Set();
let port = 0;

const proxy = createServer((req, res) => {
	if (state.mode === "blackhole") {
		state.held++;
		// Read the body and never answer.
		req.resume();
		return;
	}
	state.forwarded++;
	const up = request({ host: target.hostname, port: target.port, method: req.method, path: req.url, headers: req.headers }, (answer) => {
		res.writeHead(answer.statusCode ?? 502, answer.headers);
		answer.pipe(res);
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
	let body = "";
	req.on("data", (chunk) => {
		body += chunk;
	});
	req.on("end", () => {
		const { mode } = JSON.parse(body);
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
