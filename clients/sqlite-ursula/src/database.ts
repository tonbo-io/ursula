// The database `open` returns: a node:sqlite DatabaseSync, a subclass of it, whose methods that run
// SQL throw what `classify` (errors.ts) makes of their errors: an UrsulaError for a failure of the
// extension, and every other error unchanged. Its statements are node:sqlite's own StatementSync
// objects, whose run, get, all and iterate (and the iterator's next) do the same.
import { type ApplyChangesetOptions, DatabaseSync, type DatabaseSyncOptions, type StatementSync } from "node:sqlite";

/** Runs `f` with `args` on `self`, and throws `classify`'s error for what it throws. */
function guarded<A extends unknown[], R>(classify: (error: unknown) => unknown, self: object, f: (this: object, ...args: A) => R, args: A): R {
	try {
		return f.apply(self, args);
	} catch (error) {
		throw classify(error);
	}
}

/** `statement`, its methods that step it shadowed by ones that throw `classify`'s errors. */
function classifying(statement: StatementSync, classify: (error: unknown) => unknown): StatementSync {
	const { run, get, all, iterate } = statement;
	statement.run = ((...args: Parameters<StatementSync["run"]>) => guarded(classify, statement, run, args)) as StatementSync["run"];
	statement.get = ((...args: Parameters<StatementSync["get"]>) => guarded(classify, statement, get, args)) as StatementSync["get"];
	statement.all = ((...args: Parameters<StatementSync["all"]>) => guarded(classify, statement, all, args)) as StatementSync["all"];
	statement.iterate = ((...args: Parameters<StatementSync["iterate"]>) => {
		const iterator = guarded(classify, statement, iterate, args);
		const { next } = iterator;
		iterator.next = (...step: Parameters<typeof next>) => guarded(classify, iterator, next, step);
		return iterator;
	}) as StatementSync["iterate"];
	return statement;
}

export class UrsulaDatabase extends DatabaseSync {
	readonly #classify: (error: unknown) => unknown;

	constructor(path: string, options: DatabaseSyncOptions, classify: (error: unknown) => unknown) {
		super(path, options);
		this.#classify = classify;
	}

	override exec(sql: string): void {
		guarded(this.#classify, this, super.exec, [sql]);
	}

	override prepare(sql: string): StatementSync {
		return classifying(guarded(this.#classify, this, super.prepare, [sql]), this.#classify);
	}

	override applyChangeset(changeset: Uint8Array, options?: ApplyChangesetOptions): boolean {
		return guarded(this.#classify, this, super.applyChangeset, options === undefined ? [changeset] : [changeset, options]);
	}
}
