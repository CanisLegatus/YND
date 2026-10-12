"""Отчёт о полноте резервной копии (часть II, раздел 16 ТЗ) и сверка манифестов.

Ключевое правило: отчёт не имеет права сказать «резервная копия полностью
проверена», если есть неразрешённые ошибки, если обход был неполным или
если обнаружены ресурсы, не сопоставленные с предыдущим состоянием.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

from .manifest import Manifest, Resource, utc_now_iso
from .state import (
    CHANGED,
    DOWNLOADED,
    DOWNLOADING,
    ERROR,
    PENDING,
    RECHECK,
    VERIFIED,
    FileRecord,
    StateStore,
)

STATUS_VERIFIED = "Резервная копия полностью проверена"
STATUS_INCOMPLETE = "Резервная копия неполна"
STATUS_NOT_STARTED = "Загрузка ещё не выполнялась"
STATUS_NO_MANIFEST = "Манифест отсутствует"

_MAX_LISTING = 50


def _bytes(value: int | None) -> str:
    if value is None:
        return "неизвестно"
    size = float(value)
    for unit in ("Б", "КБ", "МБ", "ГБ", "ТБ"):
        if size < 1024 or unit == "ТБ":
            return f"{int(size)} {unit}" if unit == "Б" else f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} ТБ"


def _num(value: int) -> str:
    return f"{value:,}".replace(",", " ")


@dataclass
class Report:
    generated_at: str = field(default_factory=utc_now_iso)
    tool_version: str = ""
    backup_root: str = ""
    manifest_status: str = "unknown"
    manifest_generated_at: str = ""
    scan_complete: bool = False

    files_found: int = 0
    folders_found: int = 0
    files_verified: int = 0
    files_pending: int = 0
    files_failed: int = 0
    files_changed: int = 0
    files_in_progress: int = 0

    expected_bytes: int = 0
    downloaded_bytes: int = 0
    bytes_verified: int = 0

    scan_errors: list[dict[str, Any]] = field(default_factory=list)
    download_errors: list[dict[str, Any]] = field(default_factory=list)
    changed_files: list[str] = field(default_factory=list)
    files_without_checksum: list[str] = field(default_factory=list)
    files_without_checksum_count: int = 0
    removed_since_previous: list[str] = field(default_factory=list)
    added_since_previous: list[str] = field(default_factory=list)
    changed_since_previous: list[str] = field(default_factory=list)
    diff_available: bool = False
    status_text: str = STATUS_NO_MANIFEST
    complete: bool = False

    def to_dict(self) -> dict[str, Any]:
        from dataclasses import asdict
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Report":
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in raw.items() if k in known})


def _decide_status(report: Report) -> tuple[str, bool]:
    """Единственное место, где выносится вердикт о полноте копии."""
    if report.manifest_status == "unknown":
        return STATUS_NO_MANIFEST, False

    unresolved_errors = len(report.scan_errors) + len(report.download_errors)
    pending = report.files_pending + report.files_in_progress

    if not report.scan_complete:
        return (
            f"{STATUS_INCOMPLETE}: обход был прерван или часть каталогов "
            f"не удалось прочитать ({len(report.scan_errors)} ошибок обхода)",
            False,
        )
    if report.files_verified == 0 and report.files_found > 0:
        return f"{STATUS_INCOMPLETE}: ничего не скачано", False
    if pending or report.files_changed:
        return (
            f"{STATUS_INCOMPLETE}: не сохранено {pending} файлов, "
            f"требуют внимания {report.files_changed}",
            False,
        )
    if unresolved_errors:
        return (
            f"{STATUS_INCOMPLETE}: неразрешённых ошибок — {unresolved_errors}",
            False,
        )
    if report.files_found == 0:
        return f"{STATUS_INCOMPLETE}: манифест не содержит ни одного файла", False
    if report.removed_since_previous:
        return (
            f"{STATUS_INCOMPLETE}: обнаружено {_num(len(report.removed_since_previous))} "
            "ресурсов, отсутствующих в предыдущем манифесте",
            False,
        )
    return STATUS_VERIFIED, True


def build_report(
    manifest: Manifest,
    store: StateStore | None,
    *,
    backup_root: Path | None = None,
    tool_version: str = "",
    previous: Manifest | None = None,
) -> Report:
    report = Report(
        tool_version=tool_version,
        backup_root=str(backup_root or ""),
        manifest_status=manifest.status,
        manifest_generated_at=manifest.generated_at,
        scan_complete=manifest.is_complete(),
        files_found=manifest.stats.files,
        folders_found=manifest.stats.folders,
        expected_bytes=manifest.stats.total_known_bytes,
    )
    report.scan_errors = [e.to_dict() for e in manifest.errors]

    if store is not None:
        records = store.all_records()
        by_status: dict[str, list[FileRecord]] = {}
        for rec in records:
            by_status.setdefault(rec.status, []).append(rec)

        report.files_verified = len(by_status.get(VERIFIED, []))
        report.files_pending = len(by_status.get(PENDING, [])) + len(by_status.get(RECHECK, []))
        report.files_failed = len(by_status.get(ERROR, []))
        report.files_changed = len(by_status.get(CHANGED, []))
        report.files_in_progress = len(by_status.get(DOWNLOADING, [])) + len(by_status.get(DOWNLOADED, []))
        report.bytes_verified = sum(r.local_size or 0 for r in by_status.get(VERIFIED, []))
        report.expected_bytes = sum(r.size or 0 for r in records if r.size is not None)
        report.downloaded_bytes = sum(
            r.local_size or 0 for r in records if r.status in (VERIFIED, DOWNLOADED, DOWNLOADING)
        )
        report.download_errors = [
            {"path": r.path or r.api_path, "reason": r.last_error or "", "attempts": r.attempts}
            for r in by_status.get(ERROR, [])
        ]
        report.changed_files = [r.path or r.api_path for r in by_status.get(CHANGED, [])]
        no_sum = [r for r in records if r.status == VERIFIED and not r.expected_checksum]
        report.files_without_checksum = [r.path or r.api_path for r in no_sum[:_MAX_LISTING]]
        report.files_without_checksum_count = len(no_sum)
    else:
        no_sum = [f for f in manifest.files if f.checksum is None]
        report.files_without_checksum = [f.path for f in no_sum[:_MAX_LISTING]]
        report.files_without_checksum_count = len(no_sum)

    if previous is not None:
        diff = diff_manifests(previous, manifest)
        report.diff_available = True
        report.added_since_previous = [d["path"] for d in diff["added"][:_MAX_LISTING]]
        report.removed_since_previous = [d["path"] for d in diff["removed"][:_MAX_LISTING]]
        report.changed_since_previous = [d["path"] for d in diff["changed"][:_MAX_LISTING]]

    report.status_text, report.complete = _decide_status(report)
    return report


# --- сверка двух манифестов ----------------------------------------------

def diff_manifests(previous: Manifest, current: Manifest) -> dict[str, list[dict[str, Any]]]:
    """Сравнивает два обхода: что появилось, пропало и изменилось."""
    prev = {f.path: f for f in previous.files}
    cur = {f.path: f for f in current.files}

    added = [_describe(f, None) for path, f in cur.items() if path not in prev]
    removed = [_describe(f, None) for path, f in prev.items() if path not in cur]

    changed: list[dict[str, Any]] = []
    for path, new in cur.items():
        old = prev.get(path)
        if old is None:
            continue
        reasons = _change_reasons(old, new)
        if reasons:
            changed.append(_describe(new, old, reasons))

    return {
        "added": added,
        "removed": removed,
        "changed": changed,
        "unchanged": len(set(prev) & set(cur)) - len(changed),
    }


def _change_reasons(old: Resource, new: Resource) -> list[str]:
    reasons: list[str] = []
    if old.size != new.size:
        reasons.append(f"размер {_bytes(old.size)} → {_bytes(new.size)}")
    if (old.modified or "") != (new.modified or ""):
        reasons.append(f"изменён {old.modified} → {new.modified}")
    if old.checksum != new.checksum:
        reasons.append("контрольная сумма отличается")
    return reasons


def _describe(res: Resource, other: Resource | None, reasons: list[str] | None = None) -> dict[str, Any]:
    return {
        "path": res.path,
        "name": res.name,
        "size": res.size,
        "modified": res.modified,
        "md5": res.md5,
        "sha256": res.sha256,
        "previous_size": other.size if other else None,
        "reasons": reasons or [],
    }


# --- вывод ---------------------------------------------------------------

def render_markdown(report: Report) -> str:
    lines: list[str] = []
    add = lines.append
    add(f"# Отчёт о резервной копии\n")
    add(f"**Статус: {report.status_text}**\n")
    add(f"- Сформирован: {report.generated_at}")
    add(f"- Каталог копии: `{report.backup_root or 'не задан'}`")
    add(f"- Манифест: {report.manifest_status} от {report.manifest_generated_at or '—'}")
    if report.tool_version:
        add(f"- Версия программы: {report.tool_version}")
    add("")

    add("## Цифры\n")
    add("| Показатель | Значение |")
    add("|---|---|")
    add(f"| Обнаружено файлов | {_num(report.files_found)} |")
    add(f"| Обнаружено папок | {_num(report.folders_found)} |")
    add(f"| Скачано и проверено | {_num(report.files_verified)} |")
    add(f"| Ожидает скачивания | {_num(report.files_pending)} |")
    add(f"| В работе / прервано | {_num(report.files_in_progress)} |")
    add(f"| Изменились в облаке | {_num(report.files_changed)} |")
    add(f"| Ошибок загрузки | {_num(report.files_failed)} |")
    add(f"| Ошибок обхода | {_num(len(report.scan_errors))} |")
    add(f"| Ожидаемый объём | {_bytes(report.expected_bytes)} |")
    add(f"| Фактически сохранено | {_bytes(report.bytes_verified)} |")
    add("")

    def listing(title: str, items: Sequence[str] | Sequence[dict], empty: str) -> None:
        add(f"## {title}\n")
        if not items:
            add(f"_{empty}_\n")
            return
        for item in list(items)[:_MAX_LISTING]:
            if isinstance(item, dict):
                extra = "; ".join(item.get("reasons") or [])
                add(f"- `{item.get('path')}`" + (f" — {extra}" if extra else ""))
            else:
                add(f"- `{item}`")
        if len(items) > _MAX_LISTING:
            add(f"- … и ещё {_num(len(items) - _MAX_LISTING)} (полный список — в report.json)")
        add("")

    listing("Ошибки обхода", report.scan_errors, "Ошибок нет")
    listing("Ошибки загрузки", report.download_errors, "Ошибок нет")
    listing("Изменились в облаке", report.changed_files, "Нет")
    if report.files_without_checksum_count:
        listing(
            f"Файлы без контрольной суммы (всего {_num(report.files_without_checksum_count)})",
            report.files_without_checksum,
            "",
        )
    else:
        add("## Файлы без контрольной суммы\n\n_Нет — API вернул хеш для всех сохранённых файлов_\n")

    if report.diff_available:
        listing("Появились с прошлого обхода", report.added_since_previous, "Нет")
        listing("Пропали с прошлого обхода", report.removed_since_previous, "Нет")
        listing("Изменились с прошлого обхода", report.changed_since_previous, "Нет")
    else:
        add("## Сверка с прошлым обходом\n\n_Предыдущий манифест не найден_\n")

    return "\n".join(lines) + "\n"


def save_report(report: Report, data_dir: Path) -> tuple[Path, Path]:
    """Атомарно сохраняет report.json и report.md."""
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    json_path = data_dir / "report.json"
    md_path = data_dir / "report.md"
    _atomic_write(json_path, json.dumps(report.to_dict(), ensure_ascii=False, indent=1) + "\n")
    _atomic_write(md_path, render_markdown(report))
    return json_path, md_path


def _atomic_write(path: Path, text: str) -> None:
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
