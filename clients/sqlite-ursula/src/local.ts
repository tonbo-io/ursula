// Where `open` keeps the local file of a stream when it is given no path: a file named after the
// stream URL, in a directory of its own. The local files are a cache of the stream, rebuilt from it
// whenever they are missing or cannot be trusted (another boot, another host).
import { createHash } from "node:crypto";
import { homedir } from "node:os";
import { isAbsolute, join } from "node:path";

/**
 * The directory `open` keeps derived files in unless it is given one: `sqlite-ursula` in the user's
 * cache directory, which is `$XDG_CACHE_HOME` when that is an absolute path, `~/Library/Caches` on
 * macOS and `~/.cache` elsewhere.
 */
export function defaultDir(): string {
	const xdg = process.env.XDG_CACHE_HOME;
	if (xdg !== undefined && isAbsolute(xdg)) return join(xdg, "sqlite-ursula");
	return join(homedir(), ...(process.platform === "darwin" ? ["Library", "Caches"] : [".cache"]), "sqlite-ursula");
}

/**
 * The name of the local file of `streamUrl`: `<label>-<hash>.db`.
 *
 * - `<hash>` is the first 32 hex digits (128 bits) of the SHA-256 of the URL as WHATWG URL parsing
 *   normalizes it (`new URL(streamUrl).href`: the scheme and host lowercased, the default port
 *   dropped, the path percent-encoded). Two spellings of the same URL share a file, and two different
 *   URLs never do, short of a SHA-256 collision on 128 bits.
 * - `<label>` only makes the name readable: the last two segments of the URL's path (the bucket and
 *   stream ids) joined by `-`, lowercased, with every run of characters other than `a-z`, `0-9`, `_`
 *   and `-` replaced by `_`, as is a leading `-`, cut to 64 characters, and `stream` when empty.
 *
 * So the name is at most 100 characters of `[a-z0-9_-]` and `.db`, never starts with `-` or `.`,
 * and does not depend on the file system's case sensitivity: a safe file name for any bucket and
 * stream id. SQLite's and the extension's files next to it add at most 12 characters
 * (`-ursula.lock`). Throws a `TypeError` (`ERR_INVALID_URL`) for a URL that does not parse.
 */
export function localName(streamUrl: string): string {
	const url = new URL(streamUrl);
	const hash = createHash("sha256").update(url.href).digest("hex").slice(0, 32);
	const segments = url.pathname.split("/").filter((segment) => segment.length > 0);
	const label = segments
		.slice(-2)
		.join("-")
		.toLowerCase()
		.replace(/[^a-z0-9_-]+/g, "_")
		.replace(/^-/, "_")
		.slice(0, 64);
	return `${label.length === 0 ? "stream" : label}-${hash}.db`;
}
