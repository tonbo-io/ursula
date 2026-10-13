// Where the local file of a stream is: the file a caller names, checked and resolved to an absolute
// path, or, when `open` is given none, a file named after the stream in a directory of its own. The
// local files are a cache of the stream, rebuilt from it whenever they are missing or cannot be
// trusted (another boot, another host).
import { createHash } from "node:crypto";
import { homedir, userInfo } from "node:os";
import { isAbsolute, join, resolve } from "node:path";
import { UrsulaSetupError } from "./errors.ts";

/**
 * `path` as an absolute path, which SQLite and the extension read the same way, whatever the
 * current directory is then. Refuses (`invalid_argument`) what SQLite would not open as that file:
 * `""` and `":memory:"` (a temporary or in-memory database) and a `file:` URI.
 */
export function localFile(path: string): string {
	if (path === "" || path === ":memory:" || path.startsWith("file:")) {
		const instead = path.startsWith("file:") ? "read it as a URI" : "open a temporary database";
		throw new UrsulaSetupError(
			`invalid_argument: ${JSON.stringify(path)} is not a file path: SQLite would ${instead}, not the attached file. Pass the path of a file on a local disk`,
			"invalid_argument",
			undefined,
		);
	}
	return resolve(path);
}

/**
 * The directory `open` keeps derived files in unless it is given one: `sqlite-ursula` in the user's
 * cache directory, which is `$XDG_CACHE_HOME` when that is an absolute path, `~/Library/Caches` on
 * macOS and `~/.cache` elsewhere. The home directory comes from `HOME`, or from the user database
 * when `HOME` is not an absolute path, and without either, `open` needs `dir` or `path`.
 */
export function defaultDir(): string {
	const xdg = process.env.XDG_CACHE_HOME;
	if (xdg !== undefined && isAbsolute(xdg)) return join(xdg, "sqlite-ursula");
	return join(home(), ...(process.platform === "darwin" ? ["Library", "Caches"] : [".cache"]), "sqlite-ursula");
}

/** The user's home directory, an absolute path. */
function home(): string {
	for (const lookup of [homedir, () => userInfo().homedir]) {
		let found: string | undefined;
		try {
			found = lookup();
		} catch {
			// No HOME, and no entry for this user in the user database (an arbitrary uid).
		}
		if (found !== undefined && isAbsolute(found)) return found;
	}
	throw new UrsulaSetupError(
		"local_setup: no home directory to keep the local file in (HOME is unset or not an absolute path, and the user database has none): pass `dir`, a writable directory on a local disk, or `path`",
		"local_setup",
		undefined,
	);
}

/** A path segment percent-decoded, or `undefined` when it does not decode. */
function decoded(segment: string): string | undefined {
	try {
		return decodeURIComponent(segment);
	} catch {
		return undefined;
	}
}

/**
 * The path of a parsed stream URL, spelled one way: every segment percent-decoded and encoded again
 * one fixed way (a segment that does not decode stays as it is), and trailing `/` dropped, as the
 * extension drops them. URL parsing has resolved `.` and `..` segments already.
 */
function canonicalPath(url: URL): string {
	return url.pathname
		.replace(/\/+$/, "")
		.split("/")
		.map((segment) => {
			const text = decoded(segment);
			return text === undefined ? segment : encodeURIComponent(text);
		})
		.join("/");
}

/**
 * The stream a parsed stream URL names, which the file name's hash covers: the scheme, the host
 * (lowercased by URL parsing), the port when it is not the scheme's default, and the
 * {@link canonicalPath}. Userinfo, a query and a fragment play no part: `open` refuses a URL with
 * either of the last two.
 */
export function streamIdentity(url: URL): string {
	return `${url.protocol}//${url.host}${canonicalPath(url)}`;
}

/**
 * The stream URL `open` attaches a file named after the URL with: the URL with its path spelled one
 * way ({@link canonicalPath}), its userinfo kept. The server reads every spelling as the same stream,
 * and the extension records this one in the file's sidecar, so every spelling the file name merges
 * attaches the file.
 */
export function canonicalUrl(url: URL): string {
	const userinfo = url.username === "" && url.password === "" ? "" : `${url.username}${url.password === "" ? "" : `:${url.password}`}@`;
	return `${url.protocol}//${userinfo}${url.host}${canonicalPath(url)}`;
}

/**
 * The name of the local file of a stream, from its parsed URL: `<label>-<hash><variant>.db`.
 *
 * - `<hash>` is the first 32 hex digits (128 bits) of the SHA-256 of {@link streamIdentity}. Every
 *   spelling of one stream URL (case of the scheme and host, a default port, percent-encoding, a
 *   trailing `/`, userinfo) maps to the same file, and two different stream URLs never do, short of
 *   a SHA-256 collision on 128 bits.
 * - `<label>` only makes the name readable: the last two segments of the URL's path (the bucket and
 *   stream ids), percent-decoded, joined by `-`, lowercased, with every run of characters other than
 *   `a-z`, `0-9`, `_` and `-` replaced by `_`, as is a leading `-`, cut to 64 characters, and
 *   `stream` when empty.
 * - `<variant>` is empty for an owner, and `-ro1`, `-ro2` and so on for read-only opens, which keep
 *   files of their own: a reader on the owner's host, and two readers on one host, do not hold each
 *   other's file.
 *
 * So the name is at most 104 characters of `[a-z0-9_-]` and `.db`, never starts with `-` or `.`, and
 * does not depend on the file system's case sensitivity: a safe file name for any bucket and stream
 * id. SQLite's and the extension's files next to it add at most 12 characters (`-ursula.lock`).
 */
export function localName(url: URL, variant = ""): string {
	const hash = createHash("sha256").update(streamIdentity(url)).digest("hex").slice(0, 32);
	const segments = url.pathname.split("/").filter((segment) => segment.length > 0);
	const label = segments
		.slice(-2)
		.map((segment) => decoded(segment) ?? segment)
		.join("-")
		.toLowerCase()
		.replace(/[^a-z0-9_-]+/g, "_")
		.replace(/^-/, "_")
		.slice(0, 64);
	return `${label.length === 0 ? "stream" : label}-${hash}${variant}.db`;
}
