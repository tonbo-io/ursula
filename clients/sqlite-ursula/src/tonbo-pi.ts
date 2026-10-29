// `@tonbo/sqlite-ursula/tonbo/pi`: until 0.11.3 it opened Pi Durable's storage on a Tonbo database
// by name. That moved to the Tonbo SDK with the `/tonbo` entry (tonbo.ts). The entry stays for the
// 0.11 line only to say so: `openUrsulaPiStorage` rejects with an error that names where it moved,
// and forwards nothing. The entry goes in 0.12.

/** Why this entry's function rejects. */
const MOVED =
	"@tonbo/sqlite-ursula/tonbo/pi was removed in 0.11.3: Pi storage on a Tonbo database by name moved to the Tonbo SDK. To open it with this package, resolve the database's stream URL and a Streams token with Tonbo's own tooling, then pass both to openUrsulaPiStorage() of @tonbo/sqlite-ursula/pi: openUrsulaPiStorage(url, { token })";

/**
 * Removed in 0.11.3: always rejects with an error that says where Pi storage on a Tonbo database by
 * name moved.
 *
 * @deprecated Use the Tonbo SDK, or `openUrsulaPiStorage(url, { token })` of
 * `@tonbo/sqlite-ursula/pi` with a stream URL and a token you resolved. This entry goes in 0.12.
 */
export async function openUrsulaPiStorage(..._args: unknown[]): Promise<never> {
	throw new Error(MOVED);
}
