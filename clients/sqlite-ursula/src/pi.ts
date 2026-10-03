// Pi Durable on ReplicatedSqlite: the official SqliteStorage, unmodified, over the replicated facade.
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { ReplicatedSqlite, type ReplicatedSqliteOptions } from "./replicated.ts";
import type { WalStream } from "./stream.ts";

export async function openPiStorage(path: string, stream: WalStream, options: ReplicatedSqliteOptions = {}): Promise<SqliteStorage> {
	return SqliteStorage.open(await ReplicatedSqlite.open(path, stream, options));
}
