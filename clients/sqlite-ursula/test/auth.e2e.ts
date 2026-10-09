// The extension over TLS, through an `ursula gateway` that checks bearer tokens (RFC 9068 access
// tokens from a JWKS this test serves), in front of the stack under test. An HTTPS proxy with a
// certificate from a throwaway CA terminates TLS in front of the gateway. Each owner runs in a
// child process, because the CA file and the token file are read from its environment.
import { type ChildProcess, execFileSync, spawn } from "node:child_process";
import { generateKeyPairSync, randomUUID, sign } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { type Server, createServer, request as httpRequest } from "node:http";
import { createServer as createHttpsServer } from "node:https";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { afterAll, beforeAll, expect, inject, it } from "vitest";
import { attach } from "../src/index.ts";
import { freshFile } from "./helpers.ts";
import { type Child, openPlain, runChild, streamPath, ursulaUrl } from "./kit.ts";

const dir = mkdtempSync(join(tmpdir(), "sqlite-ursula-auth-"));
const caFile = join(dir, "ca.pem");
const audience = "https://streams.e2e";
const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
let issuer = "";
let secureUrl = "";
const servers: Server[] = [];
let gateway: ChildProcess | undefined;

const listen = (server: Server): Promise<number> =>
	new Promise((res) => server.listen(0, "127.0.0.1", () => res((server.address() as AddressInfo).port)));

const b64 = (o: object): string => Buffer.from(JSON.stringify(o)).toString("base64url");

/** An access token the gateway accepts, valid for an hour. */
function mint(): string {
	const now = Math.floor(Date.now() / 1000);
	const head = b64({ alg: "RS256", typ: "at+jwt", kid: "e2e" });
	const claims = b64({ iss: issuer, sub: "vfs-e2e", aud: audience, client_id: "vfs-e2e", iat: now, exp: now + 3600, jti: randomUUID() });
	return `${head}.${claims}.${sign("sha256", Buffer.from(`${head}.${claims}`), privateKey).toString("base64url")}`;
}

/** A CA, and a server certificate for 127.0.0.1 it signed. */
function certificates(): { key: Buffer; cert: Buffer } {
	const openssl = (...args: string[]): void => {
		execFileSync("openssl", args, { cwd: dir, stdio: "pipe" });
	};
	openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key", "-out", "ca.pem", "-days", "1", "-subj", "/CN=sqlite-ursula e2e CA");
	openssl("req", "-newkey", "rsa:2048", "-nodes", "-keyout", "leaf.key", "-out", "leaf.csr", "-subj", "/CN=127.0.0.1");
	writeFileSync(join(dir, "leaf.ext"), "basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=IP:127.0.0.1\n");
	openssl("x509", "-req", "-in", "leaf.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-set_serial", "1", "-days", "1", "-extfile", "leaf.ext", "-out", "leaf.pem");
	return { key: readFileSync(join(dir, "leaf.key")), cert: readFileSync(join(dir, "leaf.pem")) };
}

beforeAll(async () => {
	const jwks = createServer((_, res) => {
		res.writeHead(200, { "content-type": "application/json" });
		res.end(JSON.stringify({ keys: [{ ...publicKey.export({ format: "jwk" }), kid: "e2e", alg: "RS256", use: "sig" }] }));
	});
	servers.push(jwks);
	const jwksPort = await listen(jwks);
	issuer = `http://127.0.0.1:${jwksPort}`;
	const policy = join(dir, "policy.toml");
	writeFileSync(policy, `[[bucket]]\nid = "sqlite-e2e"\nowners = [{ issuer = "${issuer}", subject = "vfs-e2e" }]\n`);
	const probe = createServer();
	const gatewayPort = await listen(probe);
	await new Promise((res) => probe.close(res));
	const args = ["gateway", "--listen", `127.0.0.1:${gatewayPort}`, "--raft-group-count", process.env.E2E_GROUPS ?? "4"];
	for (const node of inject("ursulaNodes")) args.push("--upstream", node.url);
	args.push("--auth-issuer", issuer, "--auth-audience", audience, "--auth-policy", policy, "--auth-jwks-url", `${issuer}/jwks.json`);
	gateway = spawn(resolve(process.env.URSULA_BIN ?? ""), args, { cwd: dir, stdio: "ignore" });
	for (let i = 0; ; i++) {
		const up = await fetch(`http://127.0.0.1:${gatewayPort}/`).then(
			() => true,
			() => false,
		);
		if (up) break;
		if (i > 100) throw new Error("the gateway did not start");
		await new Promise((r) => setTimeout(r, 100));
	}
	const proxy = createHttpsServer(certificates(), (req, res) => {
		const upstream = httpRequest({ host: "127.0.0.1", port: gatewayPort, method: req.method, path: req.url, headers: req.headers }, (answer) => {
			res.writeHead(answer.statusCode ?? 502, answer.headers);
			answer.pipe(res);
		});
		upstream.on("error", (e) => res.writeHead(502).end(String(e)));
		req.pipe(upstream);
	});
	servers.push(proxy);
	secureUrl = `https://127.0.0.1:${await listen(proxy)}`;
	writeFileSync(caFile, readFileSync(join(dir, "ca.pem")));
});

afterAll(() => {
	gateway?.kill("SIGKILL");
	for (const server of servers) server.close();
	rmSync(dir, { recursive: true, force: true });
});

const rows = (file: string): string[] => {
	const db = openPlain(file);
	try {
		return (db.prepare("SELECT x FROM t ORDER BY rowid").all() as { x: string }[]).map((r) => r.x);
	} finally {
		db.close();
	}
};

const tokenFile = (token: string): string => {
	const file = join(dir, `token-${randomUUID()}`);
	writeFileSync(file, `${token}\n`);
	return file;
};

const done = async (child: Child): Promise<void> => {
	const { code } = await child.exited;
	expect(code, child.stderr()).toBe(0);
};

it("over TLS, through a gateway that checks tokens, an owner with a token file commits", async () => {
	const path = streamPath();
	const env = { URSULA_VFS_TOKEN_FILE: tokenFile(mint()), URSULA_VFS_CA_FILE: caFile, CHILD_EXIT: "1" };
	const child = runChild(freshFile(), secureUrl + path, ["CREATE TABLE t(x TEXT)", "INSERT INTO t VALUES ('r1')", "INSERT INTO t VALUES ('r2')"], env);
	expect(await child.waitFor((l) => l.done === true)).toMatchObject({ poisoned: false });
	await done(child);
	const copy = freshFile();
	attach(copy, ursulaUrl() + path);
	expect(rows(copy)).toEqual(["r1", "r2"]);
});

// A refused token (401) is no fence: the commit is sent again, with the token file read again,
// until the file holds a token the gateway accepts, and the owner is not poisoned.
it("a token refused mid-run is read again until the file holds a good one", async () => {
	const path = streamPath();
	const token = tokenFile(mint());
	const env = { URSULA_VFS_TOKEN_FILE: token, URSULA_VFS_CA_FILE: caFile, URSULA_VFS_RETRY_MS: "20000", CHILD_EXIT: "1" };
	const child = runChild(freshFile(), secureUrl + path, ["CREATE TABLE t(x TEXT)", "INSERT INTO t VALUES ('r1')", "@sleep:500", "INSERT INTO t VALUES ('r2')"], env);
	await child.waitFor((l) => l.step === 2 && l.phase === "start");
	writeFileSync(token, "not-a-token\n");
	await child.waitFor((l) => l.step === 3 && l.phase === "start");
	await new Promise((r) => setTimeout(r, 1500));
	writeFileSync(token, `${mint()}\n`);
	const last = await child.waitFor((l) => l.done === true);
	expect(last).toMatchObject({ poisoned: false });
	expect(last.attempts?.at(-1)).toBeGreaterThan(1);
	await done(child);
	const copy = freshFile();
	attach(copy, ursulaUrl() + path);
	expect(rows(copy)).toEqual(["r1", "r2"]);
});

it("without a token the gateway hides the stream, and an untrusted CA fails the handshake", async () => {
	const path = streamPath();
	const anonymous = runChild(freshFile(), secureUrl + path, ["CREATE TABLE t(x TEXT)"], { URSULA_VFS_CA_FILE: caFile, CHILD_EXIT: "1" });
	expect((await anonymous.exited).code).not.toBe(0);
	expect(anonymous.stderr()).toMatch(/create https:\/\/127\.0\.0\.1:\d+\/sqlite-e2e\/\S+: 404/);
	expect((await fetch(ursulaUrl() + path, { method: "HEAD" })).status).toBe(404);
	const untrusting = runChild(freshFile(), secureUrl + path, ["CREATE TABLE t(x TEXT)"], { URSULA_VFS_TOKEN_FILE: tokenFile(mint()), CHILD_EXIT: "1" });
	expect((await untrusting.exited).code).not.toBe(0);
	expect(untrusting.stderr()).toMatch(/certificate|issuer/i);
});
