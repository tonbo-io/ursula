// The prebuilt extension ships as one npm package per platform, `@tonbo/sqlite-ursula-<name>`, each an
// optional dependency of @tonbo/sqlite-ursula that npm installs only where its os, cpu and libc
// match. scripts/pack.mjs builds those packages from PREBUILT.
import { createRequire } from "node:module";

export interface Prebuilt {
	/** The package is `@tonbo/sqlite-ursula-<name>`. */
	readonly name: string;
	readonly os: NodeJS.Platform;
	readonly cpu: NodeJS.Architecture;
	/** Linux only: the C library the extension links against. */
	readonly libc?: "glibc" | "musl";
	/** The library, at the package's root. */
	readonly file: string;
}

export const PREBUILT: readonly Prebuilt[] = [
	{ name: "linux-x64-gnu", os: "linux", cpu: "x64", libc: "glibc", file: "libsqlite_ursula_vfs.so" },
	{ name: "linux-arm64-gnu", os: "linux", cpu: "arm64", libc: "glibc", file: "libsqlite_ursula_vfs.so" },
	{ name: "linux-x64-musl", os: "linux", cpu: "x64", libc: "musl", file: "libsqlite_ursula_vfs.so" },
	{ name: "darwin-arm64", os: "darwin", cpu: "arm64", file: "libsqlite_ursula_vfs.dylib" },
];

export const packageName = (p: Prebuilt): string => `@tonbo/sqlite-ursula-${p.name}`;

/** The C library of this (Linux) process: Node's report carries glibc's version, and none on musl. */
function libc(): "glibc" | "musl" {
	// Without the network section, which looks up the name of every open socket's peer.
	const report = process.report as NodeJS.ProcessReport & { excludeNetwork: boolean };
	const excludeNetwork = report.excludeNetwork;
	report.excludeNetwork = true;
	try {
		const { header } = report.getReport() as { header?: { glibcVersionRuntime?: string } };
		return header?.glibcVersionRuntime === undefined ? "musl" : "glibc";
	} finally {
		report.excludeNetwork = excludeNetwork;
	}
}

/** The path of this platform's prebuilt extension, from its package. */
export function prebuiltExtension(): string {
	const { platform, arch } = process;
	const c = platform === "linux" ? libc() : undefined;
	const p = PREBUILT.find((p) => p.os === platform && p.cpu === arch && p.libc === c);
	if (p === undefined) {
		const here = c === undefined ? `${platform}-${arch}` : `${platform}-${arch} (${c})`;
		throw new Error(`@tonbo/sqlite-ursula has no prebuilt extension for ${here}: build clients/sqlite-vfs and set SQLITE_URSULA_VFS to the library`);
	}
	const name = packageName(p);
	try {
		return createRequire(import.meta.url).resolve(`${name}/${p.file}`);
	} catch (error) {
		throw new Error(`${name} is not installed: reinstall @tonbo/sqlite-ursula without omitting optional dependencies, or set SQLITE_URSULA_VFS to the extension`, { cause: error });
	}
}
