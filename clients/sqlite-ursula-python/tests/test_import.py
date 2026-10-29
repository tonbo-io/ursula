"""``import_sqlite()``: a plain database becomes the first snapshot of a new stream, which then
opens as any other, and a file it cannot take is refused before anything is sent."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import sqlite_ursula
from sqlite_ursula import UrsulaSetupError


def plain_db(root: str, name: str, rows: int, page_size: int = 4096) -> str:
    """A plain database at ``root/name``, written by ``VACUUM INTO``."""
    source = sqlite3.connect(str(Path(root) / f"{name}.source"))
    source.executescript(
        f"PRAGMA page_size = {page_size}; CREATE TABLE t(i INTEGER PRIMARY KEY, b BLOB)"
    )
    source.executemany(
        "INSERT INTO t VALUES (?, randomblob(65536))", [(k + 1,) for k in range(rows)]
    )
    source.commit()
    file = str(Path(root) / name)
    source.execute("VACUUM INTO ?", (file,))
    source.close()
    return file


def test_imports_a_plain_database_as_the_first_snapshot_of_a_new_stream(stream, fresh):
    file = plain_db(fresh(), "plain.db", 3)
    with sqlite3.connect(file) as source:
        want = source.execute("SELECT i, hex(b) FROM t ORDER BY i").fetchall()
    imported = sqlite_ursula.import_sqlite(stream.url, file, timeout=120)
    assert imported.offset > 0
    assert imported.pages > 0 and imported.bytes > 0
    assert imported.digest
    db = sqlite_ursula.open(stream.url, dir=fresh())
    assert db.execute("SELECT i, hex(b) FROM t ORDER BY i").fetchall() == want
    db.close()


def test_refuses_a_file_with_8192_byte_pages_before_anything_is_sent(stream, fresh):
    file = plain_db(fresh(), "big-pages.db", 2, 8192)
    with pytest.raises(UrsulaSetupError) as refused:
        sqlite_ursula.import_sqlite(stream.url, file)
    assert (refused.value.kind, refused.value.action) == ("invalid_argument", "fix")
