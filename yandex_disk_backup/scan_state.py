"""Прогресс обхода дерева (SQLite) — основа возобновляемости сканирования.

Обход гигантских каталогов (десятки тысяч файлов, сотни страниц листинга)
может оборваться в любой момент: таймаут, падение процесса, перезагрузка
компьютера. Чтобы работа не пропадала, каждое приращение прогресса сразу
пишется в базу:

* `folders`  — статус каждой папки: в очереди / листается с текущим
  `offset` / дочитана целиком / не читается (ошибка);
* `items`    — найденные ресурсы (файлы и подпапки), включая частичные
  результаты недочитанной папки;
* `meta`     — корень обхода, размер страницы, отметка завершения.

После перезапуска обход продолжается ровно с последнего сохранённого места:
недочитанная папка — с её offset, очередь папок — в исходном порядке.
Никаких префиксов имён и разбиений по буквам — только path + offset, как их
понимает REST API.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterator

log = logging.getLogger(__name__)

STATUS_QUEUED = "queued"      # папка ждёт листинга в очереди
STATUS_LISTING = "listing"    # листается сейчас; сохранён offset до последней страницы
STATUS_DONE = "done"          # дочитана целиком
STATUS_FAILED = "failed"      # не читается; причина в last_error

_TERMINAL_STATUSES = (STATUS_DONE, STATUS_FAILED)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS folders (
    api_path   TEXT PRIMARY KEY,
    parent     TEXT,
    depth      INTEGER NOT NULL DEFAULT 0,
    name       TEXT,
    status     TEXT NOT NULL DEFAULT 'queued',
    total      INTEGER,
    listed     INTEGER NOT NULL DEFAULT 0,
    offset     INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_folders_status ON folders(status);

CREATE TABLE IF NOT EXISTS items (
    api_path   TEXT PRIMARY KEY,
    parent     TEXT,
    name       TEXT,
    type       TEXT,
    size       INTEGER,
    md5        TEXT,
    modified   TEXT,
    payload    TEXT,
    updated_at REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_items_parent ON items(parent);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


@dataclass
class FolderRow:
    api_path: str
    parent: str | None = None
    depth: int = 0
    name: str | None = None
    status: str = STATUS_QUEUED
    total: int | None = None
    listed: int = 0
    offset: int = 0
    last_error: str | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "FolderRow":
        return cls(
            api_path=row["api_path"],
            parent=row["parent"],
            depth=int(row["depth"] or 0),
            name=row["name"],
            status=row["status"],
            total=row["total"],
            listed=int(row["listed"] or 0),
            offset=int(row["offset"] or 0),
            last_error=row["last_error"],
        )


class ScanProgressStore:
    """Потокобезопасное хранилище прогресса обхода.

    Все изменения фиксируются немедленно (SQLite WAL): даже если процесс
    убит между страницами листинга, потеряна будет не больше одной
    незаписанной страницы.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False, timeout=30.0)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
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

    # --- конфигурация сессии обхода ------------------------------------
    def configure(self, *, root: str, page_size: int) -> None:
        self.set_meta("root", root)
        self.set_meta("page_size", str(int(page_size)))

    def get_meta(self, key: str) -> str | None:
        with self._lock:
            row = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self._tx() as conn:
            conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    @property
    def root(self) -> str | None:
        return self.get_meta("root")

    @property
    def finished(self) -> bool:
        return self.get_meta("finished") == "1"

    def mark_finished(self) -> None:
        self.set_meta("finished", "1")
        self.set_meta("finished_at", str(time.time()))

    def reset(self) -> None:
        """Полностью очищает прогресс (новый обход с нуля)."""
        with self._tx() as conn:
            conn.execute("DELETE FROM folders")
            conn.execute("DELETE FROM items")
            conn.execute("DELETE FROM meta")

    # --- папки -----------------------------------------------------------
    def ensure_folder(
        self,
        api_path: str,
        *,
        parent: str | None = None,
        depth: int = 0,
        name: str | None = None,
        status: str = STATUS_QUEUED,
    ) -> None:
        """Регистрирует папку, не затирая прогресс уже листаемой."""
        with self._tx() as conn:
            conn.execute(
                """
                INSERT INTO folders (api_path, parent, depth, name, status, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(api_path) DO UPDATE SET
                    parent     = COALESCE(excluded.parent, folders.parent),
                    depth      = MIN(excluded.depth, folders.depth),
                    name       = COALESCE(excluded.name, folders.name),
                    updated_at = excluded.updated_at
                """,
                (api_path, parent, depth, name, status, time.time()),
            )

    def save_page_progress(
        self,
        api_path: str,
        *,
        next_offset: int,
        listed: int,
        total: int | None,
    ) -> None:
        """Сохраняет результат одной прочитанной страницы ДОКАЧИВАЮЩЕЙ папки.

        Вызывается сразу после записи элементов страницы в ту же транзакцию —
        так «что скачано» и «на каком offset остановились» всегда согласованы.
        """
        with self._tx() as conn:
            conn.execute(
                "UPDATE folders SET status = ?, offset = ?, listed = ?, total = ?, "
                "updated_at = ? WHERE api_path = ?",
                (STATUS_LISTING, int(next_offset), int(listed), total, time.time(), api_path),
            )

    def start_folder(self, api_path: str) -> None:
        with self._tx() as conn:
            conn.execute(
                "UPDATE folders SET status = ?, updated_at = ? WHERE api_path = ?",
                (STATUS_LISTING, time.time(), api_path),
            )

    def finish_folder(self, api_path: str, *, listed: int, total: int | None) -> None:
        with self._tx() as conn:
            conn.execute(
                "UPDATE folders SET status = ?, offset = ?, listed = ?, total = ?, "
                "last_error = NULL, updated_at = ? WHERE api_path = ?",
                (STATUS_DONE, int(listed), int(listed), total, time.time(), api_path),
            )

    def fail_folder(self, api_path: str, reason: str) -> None:
        """Папка не читается: обход это переживает, повторим при следующем запуске."""
        with self._tx() as conn:
            conn.execute(
                "UPDATE folders SET status = ?, last_error = ?, updated_at = ? "
                "WHERE api_path = ?",
                (STATUS_FAILED, reason[:500], time.time(), api_path),
            )

    def get_folder(self, api_path: str) -> FolderRow | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM folders WHERE api_path = ?", (api_path,)
            ).fetchone()
        return FolderRow.from_row(row) if row else None

    def pending_folders(self) -> list[FolderRow]:
        """Очередь на обработку: queued + listing (недочитанные) в исходном порядке."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM folders WHERE status IN (?, ?) "
                "ORDER BY depth, rowid, api_path",
                (STATUS_QUEUED, STATUS_LISTING),
            ).fetchall()
        return [FolderRow.from_row(r) for r in rows]

    def folder_count(self, status: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM folders WHERE status = ?", (status,)
            ).fetchone()
        return int(row["n"] or 0)

    # --- элементы ---------------------------------------------------------
    def add_items(self, parent: str, raw_items: list[dict[str, Any]]) -> None:
        """Пишет страницу найденных ресурсов (сырые dict'ы из ответа API)."""
        now = time.time()
        rows = []
        for raw in raw_items:
            api_path = str(raw.get("path") or "")
            if not api_path:
                continue
            rows.append((
                api_path,
                parent,
                raw.get("name"),
                raw.get("type") or "file",
                _as_int(raw.get("size")),
                _as_str(raw.get("md5")),
                _as_str(raw.get("modified")),
                json.dumps(raw, ensure_ascii=False),
                now,
            ))
        if not rows:
            return
        with self._tx() as conn:
            conn.executemany(
                """
                INSERT INTO items (api_path, parent, name, type, size, md5, modified,
                                   payload, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(api_path) DO UPDATE SET
                    parent     = excluded.parent,
                    name       = excluded.name,
                    type       = excluded.type,
                    size       = excluded.size,
                    md5        = excluded.md5,
                    modified   = excluded.modified,
                    payload    = excluded.payload,
                    updated_at = excluded.updated_at
                """,
                rows,
            )

    def all_items(self) -> list[dict[str, Any]]:
        """Сырые ответы элементов (уже собранные Resource'ы восстанавливаются
        вызывающим кодом через Resource.from_api)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT payload FROM items ORDER BY api_path"
            ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            try:
                out.append(json.loads(row["payload"]))
            except (TypeError, ValueError):
                log.warning("пропущен повреждённый элемент в базе прогресса")
        return out

    def items_of_folder(self, parent: str) -> list[dict[str, Any]]:
        """Сырые элементы, найденные в конкретной папке (по строке parent)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT payload FROM items WHERE parent = ? ORDER BY api_path",
                (parent,),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            try:
                out.append(json.loads(row["payload"]))
            except (TypeError, ValueError):
                log.warning("пропущен повреждённый элемент в базе прогресса")
        return out

    def parent_of(self, api_path: str) -> str | None:
        """Папка-лист, из которой пришёл элемент (для восстановления прогресса)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT parent FROM items WHERE api_path = ?", (api_path,)
            ).fetchone()
        return row["parent"] if row else None

    def item_count(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM items").fetchone()
        return int(row["n"] or 0)

    def stats(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        with self._lock:
            for row in self._conn.execute(
                "SELECT status, COUNT(*) AS n FROM folders GROUP BY status"
            ):
                counts[row["status"]] = int(row["n"])
        counts["items"] = self.item_count()
        return counts


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_str(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def progress_db_path(data_dir: Path) -> Path:
    """Файл базы прогресса обхода рядом с манифестом и реестром состояний."""
    return Path(data_dir) / "scan_progress.db"
