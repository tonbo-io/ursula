// The package's two entries: the root (`@tonbo/sqlite-ursula`, src/index.ts) loads without
// `@earendil-works/pi-durable`, an optional peer dependency that only `@tonbo/sqlite-ursula/pi`
// (src/pi.ts) imports. Each entry is imported by a fresh Node process whose resolve hook records
// every specifier the module graph asks for, so a dynamic or transitive import counts too. The
// built entries (dist/) are checked as well when they exist.
import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { fileURLToPath, pathToFileURL } from "node:url";
import { expect, it } from "vitest";

const root = fileURLToPath(new URL("..", import.meta.url));

/** The bare specifiers (not `node:` or relative) that importing `entry` resolves. */
function bareImports(entry: string): string[] {
	const script = `
		import { registerHooks } from "node:module";
		const seen = new Set();
		registerHooks({
			resolve(specifier, context, next) {
				if (!specifier.startsWith("node:") && !specifier.startsWith(".") && !specifier.startsWith("file:")) seen.add(specifier);
				return next(specifier, context);
			},
		});
		await import(process.argv[1]);
		console.log(JSON.stringify([...seen]));
	`;
	const out = execFileSync(process.execPath, ["--no-warnings", "--input-type=module", "-e", script, pathToFileURL(entry).href], { cwd: root, encoding: "utf8" });
	return JSON.parse(out.trim().split("\n").at(-1) ?? "[]") as string[];
}

const entries = (name: string): string[] =>
	[`${root}src/${name}.ts`, `${root}dist/${name}.js`].filter((file) => file.endsWith(".ts") || existsSync(file));

it("the root entry imports nothing outside Node.js, so it loads without pi-durable", () => {
	for (const entry of entries("index")) {
		// The prebuilt extension is resolved by path when it is loaded, not imported.
		expect(bareImports(entry), entry).toEqual([]);
	}
});

it("the /pi entry is the one that imports pi-durable", () => {
	for (const entry of entries("pi")) {
		expect(bareImports(entry).some((s) => s.startsWith("@earendil-works/pi-durable")), entry).toBe(true);
	}
});
