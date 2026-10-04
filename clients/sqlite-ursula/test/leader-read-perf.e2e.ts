// TEMPORARY (PR10b A/B measurement, removed before merge; restored from #340's d1f38b6): HEAD and
// `consistency=leader` catch-up read latency against every node of the 3-node stack and through the
// gateway, sequential and 16 in flight on one stream; then a mixed run per target: appends at 16 in
// flight alone, and again with 16 concurrent HEAD loops on the same stream. Runs only with
// LEADER_READ_PERF=1.
import { appendFileSync } from "node:fs";
import { inject, it } from "vitest";

const pct = (xs: readonly number[], p: number): number => {
	const s = [...xs].sort((a, b) => a - b);
	return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))] as number;
};

async function timed(url: string, method: string, body?: Uint8Array): Promise<number> {
	const started = performance.now();
	const init: RequestInit = body === undefined ? { method } : { method, body, headers: { "content-type": "application/octet-stream" } };
	const r = await fetch(url, init);
	await r.arrayBuffer();
	const elapsed = performance.now() - started;
	if (!r.ok) throw new Error(`${method} ${url}: ${r.status}`);
	return elapsed;
}

// Appends at 16 in flight (125 each), alone and with 16 HEAD loops on the same stream that run
// until the appends finish.
async function mixed(url: string, withHeads: boolean): Promise<{ appends: number[]; wall: number; heads: number[]; headWall: number }> {
	const appends: number[] = [];
	const heads: number[] = [];
	let done = false;
	const started = performance.now();
	const headLoops = withHeads
		? Array.from({ length: 16 }, async () => {
				while (!done) heads.push(await timed(url, "HEAD"));
			})
		: [];
	await Promise.all(
		Array.from({ length: 16 }, async (_, worker) => {
			for (let i = 0; i < 125; i++) appends.push(await timed(url, "POST", new Uint8Array(256).fill(worker)));
		}),
	);
	const wall = (performance.now() - started) / 1000;
	done = true;
	await Promise.all(headLoops);
	const headWall = (performance.now() - started) / 1000;
	return { appends, wall, heads, headWall };
}

it.skipIf(process.env.LEADER_READ_PERF !== "1")(
	"leader read latency",
	async () => {
		const label = process.env.PERF_LABEL ?? "unlabelled";
		const gateway = inject("ursulaUrl");
		const nodes = inject("ursulaNodes");
		const path = `/sqlite-e2e/leader-read-perf-${process.pid}-${Date.now().toString(36)}`;
		const created = await fetch(gateway + path, { method: "PUT", headers: { "content-type": "application/octet-stream" } });
		if (!created.ok) throw new Error(`create: ${created.status}`);
		for (let i = 0; i < 16; i++) {
			const r = await fetch(gateway + path, { method: "POST", headers: { "content-type": "application/octet-stream" }, body: new Uint8Array(256).fill(i) });
			if (!r.ok) throw new Error(`append: ${r.status}`);
		}
		const targets: [string, string][] = [["gateway", gateway], ...nodes.map((n, i): [string, string] => [`node${i + 1}`, n.url])];
		const kinds: [string, string, string][] = [
			["HEAD", "HEAD", ""],
			["GET consistency=leader", "GET", "?offset=0&consistency=leader"],
			["GET local (control)", "GET", "?offset=0"],
		];
		const lines: string[] = [];
		for (const [target, base] of targets) {
			for (const [kind, method, query] of kinds) {
				const url = base + path + query;
				for (let i = 0; i < 50; i++) await timed(url, method);
				const seq: number[] = [];
				for (let i = 0; i < 1000; i++) seq.push(await timed(url, method));
				const conc: number[] = [];
				const started = performance.now();
				await Promise.all(
					Array.from({ length: 16 }, async () => {
						for (let i = 0; i < 125; i++) conc.push(await timed(url, method));
					}),
				);
				const wall = (performance.now() - started) / 1000;
				lines.push(
					`| ${label} | ${target} | ${kind} | ${pct(seq, 50).toFixed(3)} | ${pct(seq, 99).toFixed(3)} | ${pct(conc, 50).toFixed(3)} | ${pct(conc, 99).toFixed(3)} | ${(conc.length / wall).toFixed(0)} |`,
				);
			}
		}
		const header = ["| build | target | request | seq p50 ms | seq p99 ms | c16 p50 ms | c16 p99 ms | c16 req/s |", "|---|---|---|---|---|---|---|---|"];
		const mixedLines: string[] = [];
		for (const [target, base] of targets) {
			// A follower answers a write with 307 (or forwards it); `redirect: manual` tells which.
			const probe = await fetch(base + path, { method: "POST", redirect: "manual", headers: { "content-type": "application/octet-stream" }, body: new Uint8Array(1) });
			await probe.arrayBuffer();
			const role = target === "gateway" ? "gateway" : probe.ok ? "leader" : `follower (${probe.status})`;
			if (!probe.ok && target !== "gateway") continue;
			const url = base + path;
			for (let i = 0; i < 50; i++) await timed(url, "POST", new Uint8Array(256));
			for (const withHeads of [false, true]) {
				const run = await mixed(url, withHeads);
				const heads = withHeads
					? `${pct(run.heads, 50).toFixed(3)} | ${pct(run.heads, 99).toFixed(3)} | ${(run.heads.length / run.headWall).toFixed(0)}`
					: "- | - | -";
				mixedLines.push(
					`| ${label} | ${target} (${role}) | ${withHeads ? "appends + 16 HEAD loops" : "appends alone"} | ${pct(run.appends, 50).toFixed(3)} | ${pct(run.appends, 99).toFixed(3)} | ${(run.appends.length / run.wall).toFixed(0)} | ${heads} |`,
				);
			}
		}
		const mixedHeader = [
			"| build | target | run | append p50 ms | append p99 ms | append req/s | HEAD p50 ms | HEAD p99 ms | HEAD req/s |",
			"|---|---|---|---|---|---|---|---|---|",
		];
		const out = [...header, ...lines, "", ...mixedHeader, ...mixedLines].join("\n");
		console.log(out);
		const summary = process.env.GITHUB_STEP_SUMMARY;
		if (summary !== undefined && summary.length > 0) appendFileSync(summary, `\n${out}\n`);
	},
	900_000,
);
