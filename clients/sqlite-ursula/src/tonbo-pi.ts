// `@tonbo/sqlite-ursula/tonbo/pi` (design: Tonbo): Pi Durable's storage on a Tonbo database opened
// by name, in one call. It is the `/tonbo` entry's `resolveDatabase` followed by the `/pi` entry's
// `openUrsulaPiStorage`, so the key sources, the lookup and the Streams token provider stay in
// tonbo.ts and the Pi adapter in pi.ts. An entry of its own, since `/tonbo` must not import
// `@earendil-works/pi-durable`, an optional peer dependency only Pi users install.
import type { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { DEFAULT_OPEN_MS } from "./abort.ts";
import { openUrsulaPiStorage as openStorage, type UrsulaPiStorageOptions } from "./pi.ts";
import { resolveDatabase, type TonboResolveOptions } from "./tonbo.ts";

export {
	TonboError,
	type TonboKind,
	UrsulaError,
	UrsulaResetError,
	UrsulaSetupError,
	UrsulaUnavailableError,
	UrsulaWriteRefusedError,
} from "./tonbo.ts";

/** {@link openUrsulaPiStorage}'s options: the `/pi` entry's, a Tonbo API key, and the Tonbo origins. */
export type TonboPiStorageOptions = UrsulaPiStorageOptions & Pick<TonboResolveOptions, "apiKey" | "apiOrigin" | "authOrigin">;

/**
 * Pi Durable's official node SqliteStorage on the Tonbo database `nameOrUrl` names: resolves it as
 * the `/tonbo` entry's `resolveDatabase` does, then opens it with the `/pi` entry's
 * `openUrsulaPiStorage`, whose open keeps the Streams tokens exchanged for the key fresh. Hand the
 * storage to `Harness.open`, whose `close` closes it.
 *
 * The key is `apiKey`, else TONBO_API_KEY, else the key `tonbo login` saved. A URL (`https://...`)
 * skips the lookup. A missing `dir` names the root entry's default cache root. The signal (30 s by
 * default) bounds the whole open, the lookup included.
 *
 * Throws as the `/tonbo` entry's `open` does: {@link TonboError} for a key or database to fix,
 * {@link UrsulaUnavailableError} when the signal fired while the Tonbo API did not answer,
 * {@link UrsulaSetupError} `invalid_argument` or `invalid_url`, and the `/pi` entry's errors from the
 * open of the stream.
 */
export async function openUrsulaPiStorage(nameOrUrl: string | URL, options: TonboPiStorageOptions = {}): Promise<SqliteStorage> {
	const { apiKey, apiOrigin, authOrigin, ...rest } = options;
	const signal = rest.signal ?? AbortSignal.timeout(DEFAULT_OPEN_MS);
	const { url, token } = await resolveDatabase(nameOrUrl, { apiKey, apiOrigin, authOrigin, token: rest.token, signal });
	return openStorage(url, { ...rest, token, signal });
}
