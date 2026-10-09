// An HTTP layer in front of the node that refuses writes with 402 Payment Required on demand, the way
// an authorizer or a quota or billing service may. It runs in a process of its own, so the test process
// can attach and commit through it synchronously (node:sqlite blocks the event loop meanwhile).
//
// argv: the node's URL. Prints {"port": n} once it listens. Control requests, never forwarded:
//   PUT /__gate {"refuse": body | null, "limit"?: n, "methods"?: ["POST", "PUT"],
//                "dropNextPost"?: {"refuse": body, "limit"?: n} | null}
//   GET /__gate -> {"refused": n, "forwarded": n, "dropped": n}
// While `refuse` is set, requests with one of `methods` (writes: POST and PUT by default) are answered
// 402 with that body without being forwarded, `limit` times at most (then `refuse` clears). With
// `dropNextPost`, the next POST is forwarded but its answer is cut (its outcome is unknown to the
// client), and its refusal is armed at once.
import { createServer, request } from "node:http";

const target = new URL(process.argv[2]);
const state = { refuse: null, limit: Number.POSITIVE_INFINITY, methods: ["POST", "PUT"], dropNextPost: null, refused: 0, forwarded: 0, dropped: 0 };

const arm = ({ refuse, limit, methods }) => {
	state.refuse = refuse;
	state.limit = limit ?? Number.POSITIVE_INFINITY;
	state.methods = methods ?? ["POST", "PUT"];
};

const server = createServer((req, res) => {
	if (req.url === "/__gate") {
		if (req.method === "GET") {
			const { refused, forwarded, dropped } = state;
			res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ refused, forwarded, dropped }));
			return;
		}
		let body = "";
		req.on("data", (chunk) => {
			body += chunk;
		});
		req.on("end", () => {
			const control = JSON.parse(body);
			if ("refuse" in control) arm(control);
			if ("dropNextPost" in control) state.dropNextPost = control.dropNextPost;
			res.writeHead(204).end();
		});
		return;
	}
	if (state.refuse !== null && state.methods.includes(req.method)) {
		state.refused++;
		const body = state.refuse;
		state.limit--;
		if (state.limit <= 0) state.refuse = null;
		req.resume();
		res.writeHead(402, { "content-type": "application/json" }).end(body);
		return;
	}
	let drop = false;
	if (req.method === "POST" && state.dropNextPost !== null) {
		drop = true;
		arm(state.dropNextPost);
		state.dropNextPost = null;
	}
	state.forwarded++;
	const up = request({ host: target.hostname, port: target.port, method: req.method, path: req.url, headers: req.headers }, (answer) => {
		if (drop) {
			state.dropped++;
			answer.resume();
			answer.on("end", () => res.socket?.destroy());
			return;
		}
		res.writeHead(answer.statusCode ?? 502, answer.headers);
		answer.pipe(res);
	});
	up.on("error", () => res.destroy());
	req.pipe(up);
});

server.listen(0, "127.0.0.1", () => {
	process.stdout.write(`${JSON.stringify({ port: server.address().port })}\n`);
});
