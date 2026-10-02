// Owner error classes (design §3.6, §7.6, §7.8). `StorageRejected` itself comes from pi-durable.

/** Another owner wrote to this harness's log: this storage is fenced. Terminal for the process. */
export class FencedError extends Error {
	constructor(message: string, options?: ErrorOptions) {
		super(message, options);
		this.name = "FencedError";
	}
}

/** `fail-if-active` open refused: the current owner is active. Nothing was written. */
export class OwnershipActive extends Error {
	constructor(message: string, options?: ErrorOptions) {
		super(message, options);
		this.name = "OwnershipActive";
	}
}

/** Two openers raced for the same tail and the other claim landed first. */
export class OwnershipContention extends Error {
	constructor(message: string, options?: ErrorOptions) {
		super(message, options);
		this.name = "OwnershipContention";
	}
}

/** The claim loop hit its deadline. Retryable; an ambiguous claim may still land and fence the previous owner. */
export class ClaimTimeout extends Error {
	constructor(message: string, options?: ErrorOptions) {
		super(message, options);
		this.name = "ClaimTimeout";
	}
}

/** Open refused because the node or the stream is incompatible (missing tokens, newer format, not a Pi log). */
export class OpenRefused extends Error {
	constructor(message: string, options?: ErrorOptions) {
		super(message, options);
		this.name = "OpenRefused";
	}
}
