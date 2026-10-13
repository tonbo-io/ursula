// How a failure of the extension that a library wrapped maps to an error (`classifyWrapped`, which
// `openUrsulaPiStorage` uses): Pi wraps some failures in its own StorageRejected, a semantic
// rejection after which the session goes on, and the VFS's I/O error must surface as the Ursula error
// instead. A pure function: the VFS fails such a read only on a local file damaged mid-transaction,
// which is hard to reach.
import { StorageRejected } from "@earendil-works/pi-durable";
import { expect, it } from "vitest";
import { classifyWrapped, type FailureStatus, UrsulaError, UrsulaFencedError, UrsulaReattachError } from "../src/errors.ts";

/** An error node:sqlite throws for an I/O error of the VFS. */
const ioError = (errcode: number): Error => Object.assign(new Error("disk I/O error"), { code: "ERR_SQLITE_ERROR", errcode, errstr: "disk I/O error" });
const poisoned = (kind: string, action: "stop" | "reattach", fenced: boolean) => (): FailureStatus => ({
	poisoned: true,
	fenced,
	reason: "the first failure",
	payment_required: false,
	payment_reason: null,
	error: { kind, action, message: "the first failure" },
});

it("a failure of the extension that Pi wrapped in StorageRejected is the Ursula error, not a rejection", () => {
	const read = ioError(266);
	const rejected = new StorageRejected("Document copy 7 was rejected", { cause: read });
	const fenced = classifyWrapped(rejected, poisoned("fenced", "stop", true));
	expect(fenced).toBeInstanceOf(UrsulaFencedError);
	expect((fenced as UrsulaError).cause).toBe(read);
	// Behind the driver's failed ROLLBACK too, and for a damaged local file.
	const damaged = classifyWrapped(new AggregateError([rejected, new Error("cannot rollback - no transaction is active")]), poisoned("local_damage", "reattach", false));
	expect(damaged).toBeInstanceOf(UrsulaReattachError);
	expect(damaged).toMatchObject({ kind: "local_damage", action: "reattach" });
});

it("a rejection of Pi's own, or a wrapped error that is not the extension's, stays what it is", () => {
	const own = new StorageRejected("Fork source document 7 cannot be read", { cause: new Error("Fork source document 7 cannot be read") });
	expect(classifyWrapped(own, poisoned("fenced", "stop", true))).toBe(own);
	const constraint = new StorageRejected("Document copy 7 was rejected", {
		cause: Object.assign(new Error("UNIQUE constraint failed: documents.id"), { code: "ERR_SQLITE_ERROR", errcode: 2067 }),
	});
	expect(classifyWrapped(constraint, poisoned("fenced", "stop", true))).toBe(constraint);
	// An I/O error on a file that is not poisoned, nor refused, is SQLite's own.
	const healthy = (): FailureStatus => ({ poisoned: false, fenced: false, reason: null, payment_required: false, payment_reason: null, error: null });
	const local = new StorageRejected("Document copy 7 was rejected", { cause: ioError(522) });
	expect(classifyWrapped(local, healthy)).toBe(local);
});
