// Waiting under an AbortSignal, as `open()` and the token provider (token.ts) both do.

/** `promise`, or `signal`'s reason when it fires first. */
export function raced<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
	return new Promise((resolve, reject) => {
		const abort = (): void => reject(signal.reason);
		if (signal.aborted) abort();
		signal.addEventListener("abort", abort, { once: true });
		promise.then(resolve, reject).finally(() => signal.removeEventListener("abort", abort));
	});
}

/** A signal's reason, or any other failure, as a message: its `message` when it has one. */
export function reasonOf(reason: unknown): string {
	if (reason instanceof Error) return reason.message;
	const message = (reason as { message?: unknown } | null | undefined)?.message;
	return typeof message === "string" ? message : String(reason);
}
