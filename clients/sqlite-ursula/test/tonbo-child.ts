// A process of its own for test/tonbo.e2e.ts, run by Node from this source (type stripping): opens
// the Tonbo database argv[2] by name in the cache root argv[3] with the `/tonbo` entry's `open`,
// under the environment its parent set (the origins, TONBO_API_KEY, URSULA_VFS_TOKEN_FILE), commits,
// awaits `synced()`, closes it and prints `synced`. No open of this process set a token first, so
// the extension's requests carry the token file's.
import { loadUrsulaVfs } from "../src/index.ts";
import { open } from "../src/tonbo.ts";

const [name, dir] = process.argv.slice(2) as [string, string];
loadUrsulaVfs();
const db = await open(name, { dir });
db.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
await db.synced();
db.close();
process.stdout.write("synced\n");
