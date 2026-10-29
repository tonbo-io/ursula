// `@tonbo/sqlite-ursula/tonbo`: until 0.11.3 it opened a Tonbo database by name with a Tonbo API
// key. That is Tonbo's own, not Ursula's, and moved to the Tonbo SDK. The entry stays for the
// 0.11 line only to say so: `open` and `resolveDatabase` reject with an error that names where it
// moved. They forward nothing. The entry goes in 0.12.

/** Why this entry's functions reject. */
const MOVED =
	"@tonbo/sqlite-ursula/tonbo was removed in 0.11.3: opening a Tonbo database by name moved to the Tonbo SDK. To open it with this package, resolve the database's stream URL and a Streams token with Tonbo's own tooling, then pass both to open() of @tonbo/sqlite-ursula: open(url, { token })";

/**
 * Removed in 0.11.3: always rejects with an error that says where opening a Tonbo database by name
 * moved.
 *
 * @deprecated Use the Tonbo SDK, or `open(url, { token })` of `@tonbo/sqlite-ursula` with a stream
 * URL and a token you resolved. This entry goes in 0.12.
 */
export async function open(..._args: unknown[]): Promise<never> {
	throw new Error(MOVED);
}

/**
 * Removed in 0.11.3: always rejects with an error that says where resolving a Tonbo database by
 * name moved.
 *
 * @deprecated Use the Tonbo SDK. This entry goes in 0.12.
 */
export async function resolveDatabase(..._args: unknown[]): Promise<never> {
	throw new Error(MOVED);
}
