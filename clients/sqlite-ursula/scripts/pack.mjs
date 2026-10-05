// Packs the npm packages of a release (.github/workflows/npm-publish.yml):
//
//   npm run build && node scripts/pack.mjs <extensions> <out>
//
// <extensions>/<name>/<file> is the library built for each entry of PREBUILT (src/platform.ts). Writes
// to <out> one tarball per platform, `@tonbo/sqlite-ursula-<name>` (package.json and the library,
// nothing else), the main package's tarball, and publish-order.txt: the tarballs in publish order,
// the main package last.
//
// The main package lists the platform packages as exact-version optionalDependencies. They are added
// here, not in package.json, because package-lock.json cannot lock a version before it is published.
import { execFileSync } from "node:child_process";
import { copyFileSync, cpSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { PREBUILT, packageName } from "../dist/platform.js";

const [extensions, out] = process.argv.slice(2).map((p) => resolve(p));
if (extensions === undefined || out === undefined) throw new Error("usage: node scripts/pack.mjs <extensions> <out>");
const root = fileURLToPath(new URL("..", import.meta.url));
const main = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
const { version, license, repository, homepage, bugs } = main;
mkdirSync(out, { recursive: true });

/** Packs the package in `dir` into `out`, checks that it holds exactly `files`, and returns the tarball's name. */
function pack(dir, files) {
	const json = execFileSync("npm", ["pack", "--json", "--ignore-scripts", "--pack-destination", out], { cwd: dir, encoding: "utf8" });
	const [{ name, filename, files: packed }] = JSON.parse(json);
	const got = packed.map((f) => f.path).sort();
	const want = [...files].sort();
	if (got.join("\n") !== want.join("\n")) throw new Error(`${name}: packed [${got.join(", ")}], expected [${want.join(", ")}]`);
	return filename;
}

const write = (file, json) => writeFileSync(file, `${JSON.stringify(json, null, 2)}\n`);
const order = [];

for (const p of PREBUILT) {
	const dir = mkdtempSync(join(tmpdir(), `${p.name}-`));
	copyFileSync(join(extensions, p.name, p.file), join(dir, p.file));
	write(join(dir, "package.json"), {
		name: packageName(p),
		version,
		description: `The sqlite-ursula-vfs SQLite extension for ${p.os} ${p.cpu}${p.libc === undefined ? "" : ` (${p.libc})`}, prebuilt for @tonbo/sqlite-ursula`,
		license,
		repository,
		homepage,
		bugs,
		os: [p.os],
		cpu: [p.cpu],
		...(p.libc === undefined ? {} : { libc: [p.libc] }),
		files: [p.file],
		preferUnplugged: true,
		publishConfig: { access: "public" },
	});
	order.push(pack(dir, ["package.json", p.file]));
}

const dir = mkdtempSync(join(tmpdir(), "sqlite-ursula-"));
cpSync(join(root, "dist"), join(dir, "dist"), { recursive: true });
for (const file of ["README.md", "LICENSE"]) copyFileSync(join(root, file), join(dir, file));
const { scripts: _scripts, devDependencies: _devDependencies, ...manifest } = main;
write(join(dir, "package.json"), { ...manifest, optionalDependencies: Object.fromEntries(PREBUILT.map((p) => [packageName(p), version])) });
const dist = readdirSync(join(dir, "dist")).map((f) => `dist/${f}`);
order.push(pack(dir, ["package.json", "README.md", "LICENSE", ...dist]));

writeFileSync(join(out, "publish-order.txt"), `${order.join("\n")}\n`);
console.log(order.join("\n"));
