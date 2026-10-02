// A sorted map over binary-string keys (octet order). Sorted-array implementation: O(log n)
// lookups, O(n) inserts. Adequate for the full-resident M1 store; the bounded owner (§7.2) swaps
// in a B+tree behind the same surface.

export class OrderedMap<V> {
	private keys: string[] = [];
	private readonly rows = new Map<string, V>();

	get size(): number {
		return this.keys.length;
	}

	/** Index of the first key ≥ k. */
	private lower(k: string): number {
		let lo = 0;
		let hi = this.keys.length;
		while (lo < hi) {
			const mid = (lo + hi) >>> 1;
			if ((this.keys[mid] as string) < k) lo = mid + 1;
			else hi = mid;
		}
		return lo;
	}

	get(k: string): V | undefined {
		return this.rows.get(k);
	}

	set(k: string, v: V): void {
		if (!this.rows.has(k)) this.keys.splice(this.lower(k), 0, k);
		this.rows.set(k, v);
	}

	delete(k: string): void {
		if (!this.rows.delete(k)) return;
		this.keys.splice(this.lower(k), 1);
	}

	/** Delete every key in [start, end). */
	deleteRange(start: string, end: string): void {
		if (!(start < end)) return;
		const a = this.lower(start);
		const b = this.lower(end);
		for (let i = a; i < b; i++) this.rows.delete(this.keys[i] as string);
		this.keys.splice(a, b - a);
	}

	/** Ascending iteration over [start, end). Tolerates no mutation during iteration. */
	*range(start: string, end: string | undefined): Generator<[string, V]> {
		for (let i = this.lower(start); i < this.keys.length; i++) {
			const k = this.keys[i] as string;
			if (end !== undefined && k >= end) return;
			yield [k, this.rows.get(k) as V];
		}
	}

	*entries(): Generator<[string, V]> {
		yield* this.range("", undefined);
	}
}
