# Publication vectors

Each `.trace` file is a log of the PHASETWO prototype (`proto-phasetwo`, `tracevfs.c` over the
"unix" VFS in Node's SQLite), transcribed by `transcribe.py`:

```sh
for n in basic spill multi checkpoint attachfail fault-shm_map_new fault-phasetwo_err pendingreader exclusive; do
  python3 transcribe.py proto-phasetwo/runs/v24.14.0/$n.log $n $n.trace
done
```

One line per call on a main db handle, or per WAL write of its connection:
`<trace line> <connection> <event>`.

| event | log line |
| --- | --- |
| `lock-write` | `main SHMLOCK lock excl WRITE rc=0` |
| `unlock-write` | `main SHMLOCK unlock excl WRITE` |
| `phasetwo` | `main FCNTL COMMIT_PHASETWO` |
| `close` | `main CLOSE` |
| `commit` | `wal WRITE data ... (COMMIT FRAME PAGE)` |
| `wal-write` | any other `wal WRITE` |
| `other` | any other `main` call, a failed WRITE lock and a `main FAULT` (the call the prototype failed) included |

Left out: `main OPEN`, the harness's `MARK` and `ARM` lines (its own PRAGMAs), `wal FAULT` (the
result of the write logged before it), other `wal` calls and other files. Runs of `other` and of
`wal-write` on one connection are kept as their first line, which the state machine cannot tell
apart from the whole run.
