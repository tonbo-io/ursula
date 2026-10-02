// A TCP proxy the drills put in front of S3 and the keyed indexer. `down()` resets every open
// connection and refuses new ones (an outage); `up()` restores service; `retarget()` points new
// connections at another upstream port (a blue/green cutover).
import { createServer, type Server, type Socket, connect } from "node:net";

export class FaultProxy {
	private readonly server: Server;
	private readonly sockets = new Set<Socket>();
	private target: number;
	private isDown = false;
	readonly port: number;

	private constructor(server: Server, port: number, target: number) {
		this.server = server;
		this.port = port;
		this.target = target;
	}

	/** Listens on a free loopback port and forwards to `127.0.0.1:target`. */
	static async start(target: number): Promise<FaultProxy> {
		let proxy: FaultProxy | undefined;
		const server = createServer((client) => proxy?.accept(client));
		const port = await new Promise<number>((res, rej) => {
			server.once("error", rej);
			server.listen(0, "127.0.0.1", () => {
				const address = server.address();
				res(typeof address === "object" && address !== null ? address.port : 0);
			});
		});
		proxy = new FaultProxy(server, port, target);
		return proxy;
	}

	get url(): string {
		return `http://127.0.0.1:${this.port}`;
	}

	get upstreamPort(): number {
		return this.target;
	}

	private accept(client: Socket): void {
		if (this.isDown) {
			client.resetAndDestroy();
			return;
		}
		const upstream = connect(this.target, "127.0.0.1");
		this.sockets.add(client);
		this.sockets.add(upstream);
		const close = (): void => {
			client.destroy();
			upstream.destroy();
			this.sockets.delete(client);
			this.sockets.delete(upstream);
		};
		client.on("error", close).on("close", close);
		upstream.on("error", close).on("close", close);
		client.pipe(upstream);
		upstream.pipe(client);
	}

	/** Outage: reset every connection and refuse new ones until `up()`. */
	down(): void {
		this.isDown = true;
		for (const socket of this.sockets) socket.resetAndDestroy();
		this.sockets.clear();
	}

	up(): void {
		this.isDown = false;
	}

	/** New connections go to `target`; with `drop`, open connections are closed too. */
	retarget(target: number, drop = false): void {
		this.target = target;
		if (drop) {
			for (const socket of this.sockets) socket.destroy();
			this.sockets.clear();
		}
	}

	/** Stops accepting, destroys every open connection and waits until the listener has closed. */
	async close(): Promise<void> {
		this.down();
		const closed = new Promise<void>((resolve) => this.server.close(() => resolve()));
		this.server.unref();
		await closed;
	}
}
