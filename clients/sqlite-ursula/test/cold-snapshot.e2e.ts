// A snapshot body above the 8 MiB cold-store part size round-trips through S3: on the cluster stack
// (S3 cold tier) a body of at least 1 MiB is staged in the cold tier, and a 12 MiB one goes up as a
// multipart upload (two parts) with the SSE header on every request. The single-node stack has no
// S3, so the test runs only with E2E_NODES=3.
import { randomBytes } from "node:crypto";
import { expect, inject, it } from "vitest";
import { streamPath, ursulaUrl } from "./kit.ts";

const MiB = 1 << 20;

it.runIf(inject("ursulaNodes").length === 3)("a 12 MiB snapshot body round-trips through the S3 cold tier", async () => {
	const url = ursulaUrl() + streamPath();
	const created = await fetch(url, { method: "PUT", headers: { "content-type": "application/octet-stream" } });
	expect(created.status).toBe(201);
	const appended = await fetch(url, { method: "POST", headers: { "content-type": "application/octet-stream" }, body: Buffer.from("record") });
	expect(appended.ok, `append: ${appended.status}`).toBe(true);
	const at = appended.headers.get("stream-next-offset");
	expect(at).not.toBeNull();

	const body = randomBytes(12 * MiB);
	const put = await fetch(`${url}/snapshot/${at}`, { method: "PUT", headers: { "content-type": "application/octet-stream" }, body });
	expect(put.ok, `publish: ${put.status} ${await put.text()}`).toBe(true);

	// A snapshot read confirms leadership first, which can answer 503 for a moment.
	let got: Response | undefined;
	for (let attempt = 0; attempt < 10; attempt++) {
		got = await fetch(`${url}/snapshot/${at}`);
		if (got.status !== 503) break;
		await got.arrayBuffer();
		await new Promise((r) => setTimeout(r, 500));
	}
	expect(got?.status).toBe(200);
	const read = Buffer.from(await (got as Response).arrayBuffer());
	expect(read.length).toBe(body.length);
	expect(read.equals(body)).toBe(true);
});
