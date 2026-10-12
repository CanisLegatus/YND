"""Манифест инвентаризации: модель ресурса, статистика и атомарное сохранение.

Манифест — это «снимок» того, что мы увидели на Диске. Он должен быть
пригоден для:

* подсчёта файлов, папок и суммарного известного объёма;
* поиска самых больших файлов;
* честного ответа на вопрос «обход завершился полностью или нет»;
* последующей загрузки без повторного обхода.

Запись атомарная: сначала временный файл рядом, затем os.replace. При обрыве
или снятии питания основной манифест остаётся в целостном состоянии — либо
старый, либо новый.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

SCHEMA_VERSION = 1

STATUS_COMPLETE = "complete"
STATUS_INCOMPLETE = "incomplete"
STATUS_RUNNING = "running"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class Resource:
    """Один объект Диска: файл или папка."""

    path: str = ""            # путь от API, напр. "/Photos/2024"
    api_path: str = ""        # путь для запросов, напр. "disk:/Photos/2024"
    name: str = ""
    type: str = "file"        # "file" | "dir"
    size: int | None = None
    modified: str | None = None
    created: str | None = None
    mime_type: str | None = None
    media_type: str | None = None
    md5: str | None = None
    sha256: str | None = None
    resource_id: str | None = None
    file: str | None = None
    revision: int | None = None
    public_key: str | None = None
    public_url: str | None = None
    owner: str | None = None
    depth: int = 0
    parent_path: str | None = None

    # --- производные -------------------------------------------------
    @property
    def is_dir(self) -> bool:
        return self.type == "dir"

    @property
    def size_known(self) -> bool:
        return isinstance(self.size, int) and self.size >= 0

    @property
    def checksum(self) -> tuple[str, str] | None:
        """Есть ли у API контрольная сумма, и какого вида."""
        if self.md5:
            return ("md5", self.md5)
        if self.sha256:
            return ("sha256", self.sha256)
        return None

    @classmethod
    def from_api(cls, raw: dict[str, Any], *, parent_path: str | None = None,
                 depth: int = 0, root_name: str = "disk") -> "Resource":
        """Строит ресурс из ответа API, аккуратно относясь к отсутствующим полям.

        `root_name` — имя корня без двоеточия ("disk", "app"). Префикс вида
        `disk:/` собирается здесь ровно один раз: если API уже вернул путь с
        префиксом (например, `trash:/…`), повторно он не добавляется.
        """
        api_path = str(raw.get("path") or "")
        if not api_path:
            return cls(type=str(raw.get("type") or "file"))

        head = api_path.split(":", 1)[0]
        if ":" in api_path[:12] and head in ("disk", "app", "trash"):
            full = api_path
        else:
            full = f"{root_name}:{api_path}"

        name = raw.get("name")
        if not name:
            name = api_path.rstrip("/").rsplit("/", 1)[-1] or api_path

        return cls(
            path=api_path,
            api_path=full,
            name=str(name),
            type=str(raw.get("type") or "file"),
            size=_as_int(raw.get("size")),
            modified=_as_str(raw.get("modified")),
            created=_as_str(raw.get("created")),
            mime_type=_as_str(raw.get("mime_type")),
            media_type=_as_str(raw.get("media_type")),
            md5=_as_str(raw.get("md5")),
            sha256=_as_str(raw.get("sha256")),
            resource_id=_as_str(raw.get("resource_id")),
            file=_as_str(raw.get("file")),
            revision=_as_int(raw.get("revision")),
            public_key=_as_str(raw.get("public_key")),
            public_url=_as_str(raw.get("public_url")),
            owner=_as_str(raw.get("owner")),
            depth=depth,
            parent_path=parent_path,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Resource":
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in raw.items() if k in known})


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


@dataclass
class ScanError:
    """Ошибка обхода, из-за которой манифест нельзя считать полным."""

    path: str
    reason: str
    kind: str = "unknown"   # network | auth | permission | not_found | format | unknown
    attempts: int = 1
    at: str = field(default_factory=utc_now_iso)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ScanError":
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in raw.items() if k in known})


@dataclass
class ScanStats:
    folders: int = 0
    files: int = 0
    folders_failed: int = 0
    files_unknown_size: int = 0
    total_known_bytes: int = 0
    files_with_md5: int = 0
    files_with_sha256: int = 0
    api_requests: int = 0

    @property
    def is_complete(self) -> bool:
        return self.folders_failed == 0

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["is_complete"] = self.is_complete
        return data

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ScanStats":
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in raw.items() if k in known})


@dataclass
class Manifest:
    schema_version: int = SCHEMA_VERSION
    tool_version: str = ""
    generated_at: str = field(default_factory=utc_now_iso)
    started_at: str = ""
    finished_at: str = ""
    status: str = STATUS_RUNNING
    scan_root: str = "disk:/"
    account: dict[str, Any] = field(default_factory=dict)
    disk_info: dict[str, Any] = field(default_factory=dict)
    stats: ScanStats = field(default_factory=ScanStats)
    errors: list[ScanError] = field(default_factory=list)
    files: list[Resource] = field(default_factory=list)
    folders: list[Resource] = field(default_factory=list)

    # --- сводка -------------------------------------------------------
    def is_complete(self) -> bool:
        return self.status == STATUS_COMPLETE and not self.errors

    def iter_files(self) -> Iterator[Resource]:
        return iter(self.files)

    def largest_files(self, count: int = 10) -> list[Resource]:
        known = [f for f in self.files if f.size_known]
        return sorted(known, key=lambda r: r.size or 0, reverse=True)[:count]

    def top_extensions(self, count: int = 10) -> list[tuple[str, int]]:
        buckets: dict[str, int] = {}
        for res in self.files:
            _, ext = ("", res.name.rpartition(".")[2] if "." in res.name else "")
            buckets[ext.lower() or "(без расширения)"] = buckets.get(ext.lower() or "(без расширения)", 0) + 1
        return sorted(buckets.items(), key=lambda kv: kv[1], reverse=True)[:count]

    def files_without_checksum(self) -> list[Resource]:
        return [f for f in self.files if f.checksum is None]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "tool_version": self.tool_version,
            "generated_at": self.generated_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "status": self.status,
            "is_complete": self.is_complete(),
            "scan_root": self.scan_root,
            "account": self.account,
            "disk_info": self.disk_info,
            "stats": self.stats.to_dict(),
            "errors": [e.to_dict() for e in self.errors],
            "files": [f.to_dict() for f in self.files],
            "folders": [d.to_dict() for d in self.folders],
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Manifest":
        return cls(
            schema_version=int(raw.get("schema_version") or SCHEMA_VERSION),
            tool_version=raw.get("tool_version") or "",
            generated_at=raw.get("generated_at") or "",
            started_at=raw.get("started_at") or "",
            finished_at=raw.get("finished_at") or "",
            status=raw.get("status") or STATUS_INCOMPLETE,
            scan_root=raw.get("scan_root") or "disk:/",
            account=raw.get("account") or {},
            disk_info=raw.get("disk_info") or {},
            stats=ScanStats.from_dict(raw.get("stats") or {}),
            errors=[ScanError.from_dict(e) for e in (raw.get("errors") or [])],
            files=[Resource.from_dict(f) for f in (raw.get("files") or [])],
            folders=[Resource.from_dict(d) for d in (raw.get("folders") or [])],
        )

    # --- сохранение ---------------------------------------------------
    def save(self, path: Path) -> Path:
        """Атомарная запись: временный файл рядом + os.replace."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(self.to_dict(), ensure_ascii=False, indent=1) + "\n"
        # В том же каталоге — иначе os.replace может оказаться кросс-томным.
        fd, tmp_name = tempfile.mkstemp(
            prefix=path.name + ".", suffix=".tmp", dir=str(path.parent)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_name, path)
        except BaseException:
            # Убираем мусор, если что-то пошло не так.
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise
        return path

    @classmethod
    def load(cls, path: Path) -> "Manifest":
        path = Path(path)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise ManifestError(f"манифест не найден: {path}") from exc
        except json.JSONDecodeError as exc:
            raise ManifestError(
                f"манифест {path} повреждён: {exc}. "
                "Основной файл не должен быть повреждён при штатной работе — "
                "возможно, он редактировался вручную."
            ) from exc
        if not isinstance(raw, dict):
            raise ManifestError(f"манифест {path} имеет неожиданный формат")
        return cls.from_dict(raw)


class ManifestError(RuntimeError):
    pass


def manifest_path(data_dir: Path, name: str = "manifest.json") -> Path:
    return Path(data_dir) / name


def previous_manifest(data_dir: Path) -> Path | None:
    """Предыдущий манифест, если он уже сохранялся."""
    candidate = manifest_path(data_dir)
    return candidate if candidate.exists() else None
