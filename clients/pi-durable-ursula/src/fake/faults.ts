// Fault-hook builders for FakeUrsula.
import type { FakeOp, FakeRequest, Fault, FaultHook } from "./server.ts";

type Match = (request: FakeRequest) => boolean;

export const faults = {
	/** Apply `fault` to the first `times` requests matching `match` (default: every append). */
	nth(fault: Fault, times = 1, match: Match = (r) => r.op === "append"): FaultHook {
		let left = times;
		return (r) => {
			if (left <= 0 || !match(r)) return undefined;
			left--;
			return fault;
		};
	},
	/** Apply a sequence of faults to consecutive matching requests; `undefined` entries pass through. */
	sequence(list: readonly (Fault | undefined)[], match: Match = (r) => r.op === "append"): FaultHook {
		let i = 0;
		return (r) => (match(r) && i < list.length ? list[i++] : undefined);
	},
	/** First hook that returns a fault wins. */
	all(...hooks: FaultHook[]): FaultHook {
		return (r) => {
			for (const h of hooks) {
				const f = h(r);
				if (f !== undefined) return f;
			}
			return undefined;
		};
	},
	/** Random faults with probability `p` on matching ops, driven by a seeded PRNG. */
	random(seed: number, p: number, choices: readonly Fault[], ops: readonly FakeOp[] = ["append", "read"]): FaultHook {
		let s = seed >>> 0 || 1;
		const rnd = (): number => {
			s = (Math.imul(s, 1103515245) + 12345) & 0x7fffffff;
			return s / 0x7fffffff;
		};
		return (r) => {
			if (!ops.includes(r.op)) return undefined;
			if (rnd() >= p) return undefined;
			return choices[Math.floor(rnd() * choices.length)];
		};
	},
	status(status: number, headers?: Record<string, string>, apply = false): Fault {
		return { type: "respond", status, apply, ...(headers === undefined ? {} : { headers }) };
	},
	dropRequest: { type: "drop-request" } as Fault,
	dropResponse: { type: "drop-response" } as Fault,
	duplicate: { type: "duplicate" } as Fault,
	delay(ms: number): Fault {
		return { type: "delay", ms };
	},
	delayApply(ms: number): Fault {
		return { type: "delay-apply", ms };
	},
};
