"""Реестр состояний файлов (SQLite) — основа возобновляемости.

Одно состояние на файл, записанное в локальную базу, позволяет:

* после перезапуска продолжить с незавершённых задач, а не начинать заново;
* не перекачивать файлы, которые уже проверены и с тех пор не менялись;
* отличить «скачано, но не проверено» от «проверено» — незавершённый файл
  никогда не должен выглядеть как готовый;
* видеть в отчёте, что именно осталось неразрешённым.

Состояния (по ТЗ):
    pending      ожидает скачивания
    downloading  скачивается (файл в этот момент НЕ считается сохранённым)
    downloaded   скачан, но ещё не прошёл проверку
    verified     проверен: размер и контрольная сумма (если есть) совпали
    error        ошибка, попытки исчерпаны или требуется вмешательство
    recheck      требуется повторная проверка (например, файл изменился на диске)
    changed      изменился в облаке после инвентаризации
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator

from .manifest import Manifest, Resource

log = logging.getLogger(__name__)

PENDING = "pending"
DOWNLOADING = "downloading"
DOWNLOADED = "downloaded"
VERIFIED = "verified"
ERROR = "error"
RECHECK = "recheck"
CHANGED = "changed"

ALL_STATUSES = (PENDING, DOWNLOADING, DOWNLOADED, VERIFIED, ERROR, RECHECK, CHANGED)

#: Состояния, при которых файл считается сохранённым.
COMPLETE_STATUSES = (VERIFIED,)
#: Состояния, из которых файл имеет смысл качать.
WORK_STATUSES = (PENDING, ERROR, RECHECK, CHANGED, DOWNLOADING)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    api_path        TEXT PRIMARY KEY,
    path            TEXT,
    name            TEXT,
    size            INTEGER,
    modified        TEXT,
    md5             TEXT,
    sha256          TEXT,
    mime_type       TEXT,
    local_path      TEXT,
    local_size      INTEGER,
    status          TEXT NOT NULL DEFAULT 'pending',
    attempts        INTEGER NOT NULL DEFAULT 0,
    bytes_downloaded INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    last_error_kind TEXT,
    last_attempt_at REAL,
    verified_at     REAL,
    updated_at      REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_files_status ON files(status);
CREATE INDEX IF NOT EXISTS idx_files_local  ON files(local_path);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


@dataclass
class FileRecord:
    api_path: str
    path: str = ""
    name: str = ""
    size: int | None = None
    modified: str | None = None
    md5: str | None = None
    sha256: str | None = None
    mime_type: str | None = None
    local_path: str | None = None
    local_size: int | None = None
    status: str = PENDING
    attempts: int = 0
    bytes_downloaded: int = 0
    last_error: str | None = None
    last_error_kind: str | None = None
    last_attempt_at: float | None = None
    verified_at: float | None = None
    updated_at: float = 0.0

    @property
    def expected_checksum(self) -> tuple[str, str] | None:
        if self.md5:
            return ("md5", self.md5)
        if self.sha256:
            return ("sha256", self.sha256)
        return None

    @property
    def is_complete(self) -> bool:
        return self.status in COMPLETE_STATUSES


class StateStore:
    """Потокобезопасная обёртка над SQLite."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False, timeout=30.0)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            try:
                yield self._conn
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise

    # --- чтение --------------------------------------------------------
    def get(self, api_path: str) -> FileRecord | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM files WHERE api_path = ?", (api_path,)
            ).fetchone()
        return _row_to_record(row) if row else None

    def all_records(self) -> list[FileRecord]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM files ORDER BY api_path").fetchall()
        return [_row_to_record(r) for r in rows]

    def count_by_status(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) AS n FROM files GROUP BY status"
            ).fetchall()
        return {r["status"]: r["n"] for r in rows}

    def total_bytes_expected(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COALESCE(SUM(size), 0) AS n FROM files"
            ).fetchone()
        return int(row["n"] or 0)

    # --- массовые операции --------------------------------------------
    def sync_with_manifest(self, manifest: Manifest) -> dict[str, int]:
        """Приводит реестр в соответствие с новым манифестом.

        * новые файлы → pending;
        * файлы, чьи размер/дата/хеш изменились → changed (требует перекачки);
        * файлы, которых больше нет в манифесте, остаются в базе, но
          помечаются как требующие внимания (поле last_error), чтобы отчёт
          мог их показать — молча выбрасывать историю нельзя.
        """
        summary = {"new": 0, "changed": 0, "unchanged": 0, "gone": 0}
        now = time.time()
        rows: list[tuple] = []

        for res in manifest.files:
            record = self.get(res.api_path)
            if record is None:
                rows.append((
                    res.api_path, res.path, res.name, res.size, res.modified,
                    res.md5, res.sha256, res.mime_type, PENDING, now,
                ))
                summary["new"] += 1
                continue
            if _differs(record, res):
                rows.append((
                    res.api_path, res.path, res.name, res.size, res.modified,
                    res.md5, res.sha256, res.mime_type, CHANGED, now,
                ))
                summary["changed"] += 1
            else:
                rows.append((
                    res.api_path, res.path, res.name, res.size, res.modified,
                    res.md5, res.sha256, res.mime_type, record.status, now,
                ))
                summary["unchanged"] += 1

        with self._tx() as conn:
            conn.executemany(
                """
                INSERT INTO files
                    (api_path, path, name, size, modified, md5, sha256, mime_type,
                     status, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(api_path) DO UPDATE SET
                    path      = excluded.path,
                    name      = excluded.name,
                    size      = excluded.size,
                    modified  = excluded.modified,
                    md5       = excluded.md5,
                    sha256    = excluded.sha256,
                    mime_type = excluded.mime_type,
                    status    = excluded.status,
                    updated_at= excluded.updated_at
                """,
                rows,
            )

        known = {r.api_path for r in manifest.files}
        for record in self.all_records():
            if record.api_path not in known:
                summary["gone"] += 1
        return summary

    def reset_interrupted(self) -> int:
        """После перезапуска состояние «downloading» недействительно.

        Прерванный файл не считается сохранённым: временный остаток будет
        удалён при следующей попытке (см. docs/PART2.md).
        """
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE files SET status = ?, bytes_downloaded = 0, updated_at = ? "
                "WHERE status = ?",
                (PENDING, time.time(), DOWNLOADING),
            )
        return cur.rowcount

    # --- точечные операции ---------------------------------------------
    def bind_local_path(self, api_path: str, local_path: str) -> None:
        with self._tx() as conn:
            conn.execute(
                "UPDATE files SET local_path = ?, updated_at = ? WHERE api_path = ?",
                (local_path, time.time(), api_path),
            )

    def mark(self, api_path: str, status: str, **fields: Any) -> None:
        allowed = {
            "path", "name", "size", "modified", "md5", "sha256", "mime_type",
            "local_path", "local_size", "attempts", "bytes_downloaded",
            "last_error", "last_error_kind", "last_attempt_at", "verified_at",
        }
        sets = ["status = ?", "updated_at = ?"]
        args: list[Any] = [status, time.time()]
        for key, value in fields.items():
            if key in allowed:
                sets.append(f"{key} = ?")
                args.append(value)
        args.append(api_path)
        with self._tx() as conn:
            conn.execute(f"UPDATE files SET {', '.join(sets)} WHERE api_path = ?", args)

    def mark_downloading(self, api_path: str, local_path: str) -> None:
        with self._tx() as conn:
            conn.execute(
                "UPDATE files SET status = ?, local_path = ?, last_attempt_at = ?, "
                "updated_at = ? WHERE api_path = ?",
                (DOWNLOADING, local_path, time.time(), time.time(), api_path),
            )

    def mark_progress(self, api_path: str, bytes_downloaded: int) -> None:
        with self._tx() as conn:
            conn.execute(
                "UPDATE files SET bytes_downloaded = ?, updated_at = ? WHERE api_path = ?",
                (bytes_downloaded, time.time(), api_path),
            )

    def mark_verified(self, api_path: str, local_path: str, local_size: int) -> None:
        with self._tx() as conn:
            conn.execute(
                "UPDATE files SET status = ?, local_path = ?, local_size = ?, "
                "bytes_downloaded = ?, last_error = NULL, last_error_kind = NULL, "
                "verified_at = ?, updated_at = ? WHERE api_path = ?",
                (VERIFIED, local_path, local_size, local_size, time.time(), time.time(), api_path),
            )

    def mark_error(self, api_path: str, reason: str, kind: str = "unknown",
                   bump_attempt: bool = True) -> None:
        with self._tx() as conn:
            if bump_attempt:
                conn.execute(
                    "UPDATE files SET status = ?, last_error = ?, last_error_kind = ?, "
                    "attempts = attempts + 1, last_attempt_at = ?, updated_at = ? "
                    "WHERE api_path = ?",
                    (ERROR, reason[:500], kind, time.time(), time.time(), api_path),
                )
            else:
                conn.execute(
                    "UPDATE files SET status = ?, last_error = ?, last_error_kind = ?, "
                    "last_attempt_at = ?, updated_at = ? WHERE api_path = ?",
                    (ERROR, reason[:500], kind, time.time(), time.time(), api_path),
                )

    def bump_attempt(self, api_path: str) -> int:
        with self._tx() as conn:
            conn.execute(
                "UPDATE files SET attempts = attempts + 1, last_attempt_at = ?, "
                "updated_at = ? WHERE api_path = ?",
                (time.time(), time.time(), api_path),
            )
            row = conn.execute(
                "SELECT attempts FROM files WHERE api_path = ?", (api_path,)
            ).fetchone()
        return int(row["attempts"]) if row else 0

    # --- отбор работы ---------------------------------------------------
    def work_queue(self, statuses: Iterable[str] = WORK_STATUSES) -> list[FileRecord]:
        statuses = list(statuses)
        placeholders = ",".join("?" for _ in statuses)
        with self._lock:
            rows = self._conn.execute(
                f"SELECT * FROM files WHERE status IN ({placeholders}) ORDER BY api_path",
                statuses,
            ).fetchall()
        return [_row_to_record(r) for r in rows]

    # --- служебное ------------------------------------------------------
    def set_meta(self, key: str, value: str) -> None:
        with self._tx() as conn:
            conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    def get_meta(self, key: str) -> str | None:
        with self._lock:
            row = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else None

    def verify_local_state(self, backup_root: Path) -> dict[str, int]:
        """Сверяет «verified» с реальностью: файл могли удалить или испортить.

        Проверенный файл, которого нет на диске или размер которого не совпал,
        возвращается в работу. Это защита от отчёта, который врёт.
        """
        result = {"missing": 0, "size_mismatch": 0, "ok": 0}
        now = time.time()
        updates: list[tuple] = []
        for record in self.all_records():
            if record.status != VERIFIED or not record.local_path:
                continue
            local = backup_root / record.local_path
            try:
                size = local.stat().st_size
            except OSError:
                result["missing"] += 1
                updates.append((PENDING, f"локальный файл не найден при сверке {now}", now, record.api_path))
                continue
            if record.local_size is not None and size != record.local_size:
                result["size_mismatch"] += 1
                updates.append((RECHECK, f"размер изменился: {size} вместо {record.local_size}", now, record.api_path))
                continue
            result["ok"] += 1
        if updates:
            with self._tx() as conn:
                conn.executemany(
                    "UPDATE files SET status = ?, last_error = ?, updated_at = ? WHERE api_path = ?",
                    updates,
                )
        return result


def _differs(record: FileRecord, res: Resource) -> bool:
    """Изменился ли файл в облаке по сравнению с записью в реестре.

    Смысл имеет только для файлов, доведённых до `verified`: незавершённый
    файл в любом случае пойдёт в работу, и называть его «изменившимся»
    незачем — отчёт от этого становится только шумнее.
    """
    if record.status != VERIFIED:
        return False
    if record.size != res.size:
        return True
    if (record.modified or "") != (res.modified or ""):
        return True
    if res.checksum != record.expected_checksum:
        return True
    return False


def _row_to_record(row: sqlite3.Row) -> FileRecord:
    return FileRecord(
        api_path=row["api_path"],
        path=row["path"] or "",
        name=row["name"] or "",
        size=row["size"],
        modified=row["modified"],
        md5=row["md5"],
        sha256=row["sha256"],
        mime_type=row["mime_type"],
        local_path=row["local_path"],
        local_size=row["local_size"],
        status=row["status"],
        attempts=row["attempts"],
        bytes_downloaded=row["bytes_downloaded"] or 0,
        last_error=row["last_error"],
        last_error_kind=row["last_error_kind"],
        last_attempt_at=row["last_attempt_at"],
        verified_at=row["verified_at"],
        updated_at=row["updated_at"] or 0.0,
    )
