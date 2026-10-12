"""Сквозные сценарии: обход → реестр → загрузка → отчёт.

Отдельно от CLI, чтобы логику можно было запускать и из bat-файла, и из
Python, и покрыть тестами без ввода в терминал.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from pathlib import Path
from typing import Any, Callable, Sequence

from . import __version__
from .api import ApiError, DiskClient
from .config import Config
from .downloader import Downloader, DownloadOutcome, DownloadStats, _bytes
from .inventory import InventoryScanner, ScanProgress
from .manifest import Manifest, manifest_path, previous_manifest
from .naming import PathAllocator
from .report import Report, build_report, save_report
from .session import AuthRequired, AuthSession
from .state import StateStore, WORK_STATUSES

log = logging.getLogger(__name__)


class StopRequested(Exception):
    """Пользователь попросил остановиться; незавершённое остаётся для продолжения."""


class StopFlag:
    """Мягкая остановка: ловит Ctrl+C, чтобы не портить временные файлы."""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._previous: list[Any] = []

    def install(self) -> None:
        for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None)):
            if sig is None:
                continue
            try:
                self._previous.append((sig, signal.getsignal(sig)))
                signal.signal(sig, self._handle)
            except (ValueError, OSError):
                # Не в главном потоке — просто не ловим.
                pass

    def restore(self) -> None:
        for sig, handler in self._previous:
            try:
                signal.signal(sig, handler)
            except (ValueError, OSError):
                pass
        self._previous.clear()

    def _handle(self, signum, frame) -> None:  # noqa: ARG002
        if self._event.is_set():
            # Повторное Ctrl+C — выходим немедленно.
            raise KeyboardInterrupt
        self._event.set()
        print("\n  ⏹  Останавливаюсь… (ещё раз Ctrl+C — выход немедленно)")

    def __call__(self) -> bool:
        return self._event.is_set()


def make_client_factory(session: AuthSession, cfg: Config) -> Callable[[], DiskClient]:
    """Создаёт по клиенту на поток: requests.Session не потокобезопасен."""
    token = session.ensure_fresh().access_token

    def factory() -> DiskClient:
        return DiskClient(
            token,
            base_url=cfg.api_base,
            timeout=cfg.timeout,
            max_retries=cfg.max_retries,
            min_request_interval=cfg.min_request_interval,
            disable_service_logging=cfg.disable_service_logging,
        )

    return factory


def _allocate(state: StateStore, cfg: Config) -> PathAllocator:
    """Восстанавливает назначения имён из реестра, чтобы имена не «прыгали»."""
    allocator = PathAllocator(cfg.backup_dir, cfg.conflict_strategy)
    for record in state.all_records():
        if record.local_path:
            allocator.reserve(record.local_path, record.api_path)
    return allocator


def run_scan(
    cfg: Config,
    session: AuthSession,
    data_dir: Path,
    *,
    manifest: Manifest | None = None,
    stop: Callable[[], bool] | None = None,
    on_progress: Callable[[ScanProgress], None] | None = None,
) -> Manifest:
    """Этапы C–E + F: обход диска и сохранение манифеста.

    Даже если обход прерван (Ctrl+C) или упал с фатальной ошибки, уже
    собранная часть сохраняется как incomplete-манифест — она не пропадает
    и пригодна для загрузки; при следующем запуске обход продолжится с нуля
    и перепишет манифест целиком.
    """
    client = session.client()
    scanner = InventoryScanner(
        client,
        root=cfg.scan_root,
        page_size=cfg.page_size,
        sort=cfg.scan_sort,
        media_type=cfg.media_type,
        include_trash=cfg.include_trash,
        listing_read_timeout=getattr(cfg, "listing_read_timeout", None),
        should_stop=stop,
    )
    try:
        result = scanner.scan(on_progress=on_progress)
    except ApiError as exc:
        # Фатальная ошибка обхода (например, 401) — сохраняем то, что успели.
        log.error("обход прерван ошибкой API: %s", exc)
        result = scanner.save_partial(data_dir)
        result.errors.append(_as_scan_error(scanner, cfg.scan_root, exc))
        raise
    except KeyboardInterrupt:
        log.info("обход прерван пользователем (Ctrl+C)")
        scanner.stopped_by_user = True
        scanner.save_partial(data_dir)
        raise
    result.tool_version = __version__

    try:
        result.disk_info = client.get_disk_info()
        result.account = {"id": result.disk_info.get("id"), "type": result.disk_info.get("type")}
    except ApiError as exc:
        log.warning("не удалось получить сведения о диске: %s", exc)
        result.errors.append(_as_scan_error(scanner, "disk:/", exc))

    path = manifest_path(data_dir)
    result.save(path)
    log.info("манифест сохранён: %s (статус %s)", path, result.status)
    return result


def _as_scan_error(scanner, path: str, exc: BaseException):
    from .inventory import classify_error, human_error
    from .manifest import ScanError
    return ScanError(path=path, reason=human_error(exc), kind=classify_error(exc))


def sync_state(state: StateStore, manifest: Manifest, cfg: Config) -> dict[str, int]:
    state.reset_interrupted()
    summary = state.sync_with_manifest(manifest)
    local = state.verify_local_state(cfg.backup_dir)
    if local.get("missing") or local.get("size_mismatch"):
        log.info(
            "сверка с диском: отсутствует %d, размер не совпал у %d файлов",
            local.get("missing", 0), local.get("size_mismatch", 0),
        )
    state.set_meta("last_manifest", manifest.generated_at)
    state.set_meta("tool_version", __version__)
    return summary


def run_download(
    cfg: Config,
    session: AuthSession,
    data_dir: Path,
    manifest: Manifest,
    state: StateStore,
    *,
    dry_run: bool = False,
    stop: Callable[[], bool] | None = None,
    on_progress: Callable[[DownloadOutcome], None] | None = None,
    statuses: Sequence[str] = WORK_STATUSES,
) -> tuple[list[DownloadOutcome], DownloadStats]:
    records = state.work_queue(statuses)
    if not records:
        return [], DownloadStats()

    expected = sum(r.size or 0 for r in records)
    print(f"\n  К загрузке: {_num(len(records))} файлов, ≈{_bytes(expected)}")
    print(f"  Каталог: {cfg.backup_dir}")
    print(f"  Параллельных загрузок: {cfg.concurrency}\n")

    downloader = Downloader(
        cfg, state, make_client_factory(session, cfg),
        allocator=_allocate(state, cfg), should_stop=stop,
    )
    if on_progress is None:
        on_progress = _download_printer(downloader.stats)
    downloader.on_progress = on_progress

    outcomes = downloader.download_all(records, dry_run=dry_run)
    print()
    return outcomes, downloader.stats


def run_report(
    cfg: Config,
    data_dir: Path,
    manifest: Manifest,
    state: StateStore,
    previous: Manifest | None = None,
) -> Report:
    report = build_report(
        manifest, state,
        backup_root=cfg.backup_dir,
        tool_version=__version__,
        previous=previous,
    )
    json_path, md_path = save_report(report, data_dir)
    log.info("отчёт сохранён: %s и %s", json_path, md_path)
    return report


def run_full(
    cfg: Config,
    session: AuthSession,
    data_dir: Path,
    *,
    do_scan: bool = True,
    do_download: bool = True,
    dry_run: bool = False,
) -> Report:
    """Полный цикл: обход → загрузка → отчёт. Безопасен для повторного запуска."""
    stop = StopFlag()
    stop.install()
    try:
        state = StateStore(data_dir / "state.db")
        previous = _load_previous_manifest(data_dir)

        manifest: Manifest | None = None
        if do_scan:
            try:
                manifest = run_scan(
                    cfg, session, data_dir,
                    stop=stop, on_progress=_scan_printer(),
                )
            except AuthRequired:
                raise
        else:
            path = manifest_path(data_dir)
            if path.exists():
                manifest = Manifest.load(path)
            if manifest is None:
                raise RuntimeError("манифест не найден: сначала выполните обход диска")

        if manifest is None:
            raise RuntimeError("обход не дал результата")

        sync_state(state, manifest, cfg)

        if do_download:
            run_download(cfg, session, data_dir, manifest, state,
                         dry_run=dry_run, stop=stop)

        return run_report(cfg, data_dir, manifest, state, previous)
    finally:
        stop.restore()


def _load_previous_manifest(data_dir: Path) -> Manifest | None:
    path = previous_manifest(data_dir)
    if path is None:
        return None
    try:
        return Manifest.load(path)
    except Exception as exc:  # noqa: BLE001
        log.warning("предыдущий манифест не читается (%s), сверка отключена", exc)
        return None


# --- вывод прогресса ------------------------------------------------------

def _scan_printer() -> Callable[[ScanProgress], None]:
    def show(progress: ScanProgress) -> None:
        print(f"\r  {progress.render()}   {progress.current_folder[:44]}", end="", flush=True)
    return show


def _download_printer(stats: DownloadStats) -> Callable[[DownloadOutcome], None]:
    """Живая строка прогресса. Ошибки печатаются отдельными строками,
    чтобы не потеряться при перерисовке `\r`."""
    last_print = [0.0]

    def show(outcome: DownloadOutcome) -> None:
        if outcome.status == "error":
            print(f"\n  ✗ {outcome.api_path}: {outcome.note}")
            last_print[0] = 0.0
            return
        now = time.monotonic()
        if now - last_print[0] < 0.3:
            return
        last_print[0] = now
        print(f"\r  {stats.render()[:140]}", end="", flush=True)

    return show


def _num(value: int) -> str:
    return f"{value:,}".replace(",", " ")
