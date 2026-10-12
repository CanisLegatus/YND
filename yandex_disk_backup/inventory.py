"""Этапы D и E: листинг одной папки и рекурсивный обход Диска.

Обход устроен как обход в ширину: очередь каталогов, для каждого — все
страницы листинга. Обход не останавливается из-за одной плохой папки: ошибка
фиксируется, обход продолжается, а манифест в конце честно помечается
неполным. Это ровно то требование ТЗ «не объявлять сканирование успешным,
если часть каталогов не удалось обработать».
"""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from .api import (
    ApiError,
    AuthError,
    DiskClient,
    InsufficientScopeError,
    NetworkError,
    RateLimitedError,
    ResourceNotFoundError,
    ResponseFormatError,
)
from .manifest import (
    STATUS_COMPLETE,
    STATUS_INCOMPLETE,
    Manifest,
    Resource,
    ScanError,
    ScanStats,
    utc_now_iso,
)

log = logging.getLogger(__name__)

# Таймаут чтения при листинге каталогов. Для больших папок (десятки тысяч
# фотографий) Яндекс формирует ответ дольше обычных 60 с, и запрос обрывался
# как NetworkError ("connection timeout"). 300 с хватает даже на очень
# крупные каталоги; значение можно переопределить в конфиге
# (listing_read_timeout).
LISTING_READ_TIMEOUT = 300.0


@dataclass
class ScanProgress:
    """Состояние обхода для отображения прогресса."""

    current_folder: str = "disk:/"
    folders_done: int = 0
    folders_total: int | None = None
    folders_failed: int = 0
    files_found: int = 0
    total_known_bytes: int = 0
    started_at: float = field(default_factory=time.monotonic)

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.started_at

    @property
    def percent(self) -> float | None:
        """Достоверный процент известен только если известно число папок."""
        if not self.folders_total:
            return None
        done = self.folders_done + self.folders_failed
        return min(100.0, 100.0 * done / max(1, self.folders_total))

    def render(self) -> str:
        known_total = "?" if not self.folders_total else str(self.folders_total)
        percent = "" if self.percent is None else f" {self.percent:5.1f}%"
        return (
            f"Каталогов: {self.folders_done}/{known_total}{percent}  "
            f"Файлов: {self.files_found}  "
            f"Объём: {_bytes(self.total_known_bytes)}  "
            f"Ошибок: {self.folders_failed}  "
            f"Время: {int(self.elapsed)} с"
        )


def _bytes(value: int | None) -> str:
    if value is None:
        return "неизвестно"
    size = float(value)
    for unit in ("Б", "КБ", "МБ", "ГБ", "ТБ"):
        if size < 1024 or unit == "ТБ":
            return f"{int(size)} {unit}" if unit == "Б" else f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} ТБ"


def classify_error(exc: BaseException) -> str:
    if isinstance(exc, AuthError):
        return "auth"
    if isinstance(exc, InsufficientScopeError):
        return "permission"
    if isinstance(exc, ResourceNotFoundError):
        return "not_found"
    if isinstance(exc, NetworkError):
        return "network"
    if isinstance(exc, RateLimitedError):
        return "rate_limit"
    if isinstance(exc, ResponseFormatError):
        return "format"
    if isinstance(exc, ApiError):
        return "api"
    return "unknown"


def human_error(exc: BaseException) -> str:
    text = str(exc).strip() or type(exc).__name__
    return f"{type(exc).__name__}: {text}"


# --- Этап D: содержимое одной папки --------------------------------------

def list_folder(
    client: DiskClient,
    api_path: str,
    *,
    page_size: int = 200,
    sort: str = "path",
    media_type: str = "",
    include_trash: bool = False,
    max_items: int | None = None,
    read_timeout: float | None = None,
    on_page: Callable[[int, int], None] | None = None,
) -> tuple[Resource, list[Resource], int]:
    """Возвращает (сам ресурс-каталог, элементы, всего элементов по данным API).

    Пагинация: offset наращивается, пока страница не станет короче `page_size`
    либо пока offset не достигнет `_embedded.total`.

    `read_timeout` — увеличенный таймаут чтения для больших каталогов
    (тысячи файлов): Яндекс формирует листинг долго, и при стандартных 60 с
    запрос обрывается как NetworkError/timeout. Если не задан, используется
    LISTING_READ_TIMEOUT.

    `on_page(получено_на_странице, всего_страниц_с_начала)` — колбэк, чтобы
    обход большой папки не выглядел «зависшим»: прогресс обновляется после
    каждой страницы, а не только после целой папки.
    """
    items: list[Resource] = []
    offset = 0
    total: int | None = None
    folder_meta: dict[str, Any] = {}
    pages = 0

    while True:
        data = client.list_folder_page(
            api_path,
            limit=page_size,
            offset=offset,
            sort=sort or None,
            media_type=media_type or None,
            include_trash=include_trash,
            read_timeout=read_timeout if read_timeout is not None else LISTING_READ_TIMEOUT,
        )
        pages += 1
        if not folder_meta:
            folder_meta = data

        embedded = data.get("_embedded") or {}
        page = embedded.get("items")
        if not isinstance(page, list):
            raise ResponseFormatError(
                f"ответ листинга не содержит _embedded.items для {api_path}"
            )
        if total is None and isinstance(embedded.get("total"), int):
            total = embedded["total"]

        for raw in page:
            if not isinstance(raw, dict):
                raise ResponseFormatError("элемент листинга — не объект JSON")
            res = Resource.from_api(raw, parent_path=data.get("path") or api_path, depth=0)
            items.append(res)

        if on_page is not None:
            try:
                on_page(len(page), pages)
            except Exception:  # noqa: BLE001 — прогресс не должен ронять обход
                log.exception("ошибка в колбэке прогресса листинга")

        offset += len(page)
        if not page:
            break
        if len(page) < page_size:
            break
        if total is not None and offset >= total:
            break
        if max_items is not None and len(items) >= max_items:
            break

    folder = Resource.from_api(folder_meta, depth=0) if folder_meta else Resource(
        api_path=api_path, path=api_path.rstrip("/") or "/", name=api_path, type="dir"
    )
    return folder, items, total if total is not None else len(items)


# --- Этап E: рекурсивный обход -------------------------------------------

class InventoryScanner:
    """Обход дерева Диска с пагинацией, прогрессом и учётом ошибок."""

    def __init__(
        self,
        client: DiskClient,
        *,
        root: str = "disk:/",
        page_size: int = 200,
        sort: str = "path",
        media_type: str = "",
        include_trash: bool = False,
        max_depth: int | None = None,
        max_files: int | None = None,
        max_folders: int = 200_000,
        listing_read_timeout: float | None = None,
        should_stop: Callable[[], bool] | None = None,
    ) -> None:
        self.client = client
        self.root = root if root.endswith("/") or ":" in root else root + "/"
        self.page_size = max(1, page_size)
        self.sort = sort
        self.media_type = media_type
        self.include_trash = include_trash
        self.max_depth = max_depth
        self.max_files = max_files
        # Таймаут чтения листинга больших каталогов (см. LISTING_READ_TIMEOUT).
        self.listing_read_timeout = (
            LISTING_READ_TIMEOUT if listing_read_timeout is None else float(listing_read_timeout)
        )
        # Страховка от бесконечного обхода при сбое входных данных:
        # на диске физически не может быть миллион вложенных каталогов.
        self.max_folders = max_folders
        self._should_stop = should_stop or (lambda: False)

        self.progress = ScanProgress(current_folder=self.root)
        #: ISO-время начала обхода — для частичного манифеста.
        self.progress_started_iso = utc_now_iso()
        self.errors: list[ScanError] = []
        self._visited: set[str] = set()
        #: Собранные в ходе обхода файлы/папки — доступны и после прерывания,
        #: чтобы partial-манифест не потерял уже найденное.
        self._files: list[Resource] = []
        self._folders: list[Resource] = []
        #: Обход был прерван пользователем (Ctrl+C / stop-флаг).
        self.stopped_by_user = False
        #: Последний колбэк прогресса — вызывается после каждой страницы
        #: листинга, чтобы большая папка не выглядела «зависшей».
        self._on_progress: Callable[[ScanProgress], None] | None = None
        self._last_report_at = 0.0



    # --- основной проход ----------------------------------------------
    def scan(self, on_progress: Callable[[ScanProgress], None] | None = None) -> Manifest:
        self._on_progress = on_progress
        manifest = Manifest(
            started_at=utc_now_iso(),
            scan_root=self.root,
            status=STATUS_INCOMPLETE,
        )
        queue: deque[tuple[Resource, int]] = deque()
        seen_dirs: set[str] = set()
        # Файлы/папки копятся и в self._files/self._folders — это позволяет
        # сохранить частичный манифест, если обход прервут (Ctrl+C) или он
        # упадёт с фатальной ошибки.
        files = self._files
        folders = self._folders

        root_folder, root_items, _ = self._list_or_fail(self.root, depth=0)
        if root_folder is None:
            manifest.errors.extend(self.errors)
            manifest.stats = self._stats([], [])
            manifest.finished_at = utc_now_iso()
            return manifest

        root_folder.depth = 0
        if not root_folder.name:
            root_folder.name = "/"
        seen_dirs.add(root_folder.api_path)
        folders.append(root_folder)
        self.progress.folders_done = 1
        self._consume(root_items, files, 0)
        self._enqueue(root_items, seen_dirs, queue, depth=1)

        truncated = False
        while queue:
            if self._should_stop():
                log.info("обход остановлен по запросу пользователя")
                self.stopped_by_user = True
                break
            if self.max_files is not None and len(files) >= self.max_files:
                log.info("достигнут лимит файлов (%s), обход остановлен", self.max_files)
                break
            if self.progress.folders_done + len(queue) >= self.max_folders:
                log.error("достигнут предохранитель в %d каталогов — обход прерван", self.max_folders)
                self._record_error(None, kind="guard",
                                   reason=f"превышен предохранитель каталогов ({self.max_folders})")
                truncated = True
                break

            folder, depth = queue.popleft()
            self.progress.current_folder = folder.api_path
            child, items, _ = self._list_or_fail(folder.api_path, depth=depth)

            self.progress.folders_done += 1
            if child is not None:
                child.depth = depth
                folders.append(child)
                self._consume(items, files, depth)
                self._enqueue(items, seen_dirs, queue, depth=depth + 1)

            self._report(force=(not queue))

        if on_progress:
            on_progress(self.progress)

        # Порядок стабилен — так проще сверять манифесты между запусками.
        files.sort(key=lambda r: r.path)
        folders.sort(key=lambda r: r.path)

        manifest.files = files
        manifest.folders = folders
        manifest.errors.extend(self.errors)
        manifest.stats = self._stats(files, folders)
        manifest.finished_at = utc_now_iso()
        manifest.status = (
            STATUS_COMPLETE
            if manifest.stats.is_complete
            and not truncated
            and not self.stopped_by_user
            and not (self.max_files is not None and manifest.stats.files >= self.max_files)
            else STATUS_INCOMPLETE
        )
        return manifest

    def save_partial(self, data_dir, on_progress_note: Callable[[Manifest], None] | None = None) -> Manifest:
        """Сохраняет манифест с тем, что успели собрать (после остановки/исключения).

        Используется, когда пользователь прерывает обход (Ctrl+C) или случилась
        фатальная ошибка: собранные данные не должны пропадать — они пишутся
        как incomplete-манифест и пригодны для последующей загрузки.
        """
        from pathlib import Path as _Path

        from .manifest import manifest_path as _mpath

        manifest = self.build_partial_manifest()
        path = _mpath(_Path(data_dir))
        manifest.save(path)
        log.info("частичный манифест сохранён: %s (%d файлов)", path, manifest.stats.files)
        if on_progress_note is not None:
            on_progress_note(manifest)
        return manifest

    def build_partial_manifest(self) -> Manifest:
        """Собирает incomplete-манифест из текущего состояния обхода."""
        files = list(self._files)
        folders = list(self._folders)
        manifest = Manifest(
            started_at=self.progress_started_iso,
            scan_root=self.root,
            status=STATUS_INCOMPLETE,
        )
        files = sorted(files, key=lambda r: r.path)
        folders = sorted(folders, key=lambda r: r.path)
        manifest.files = files
        manifest.folders = folders
        manifest.errors.extend(self.errors)
        manifest.stats = self._stats(files, folders)
        manifest.finished_at = utc_now_iso()
        return manifest

    # --- вспомогательное ----------------------------------------------
    def _enqueue(self, items: Iterable[Resource], seen: set[str],
                 queue: deque, depth: int) -> None:
        """Ставит в очередь подкаталоги, которых ещё не видели и глубина в норме."""
        if self.max_depth is not None and depth >= self.max_depth:
            return
        for res in items:
            if not res.is_dir or res.api_path in seen:
                continue
            seen.add(res.api_path)
            queue.append((res, depth))

    def _list_or_fail(self, api_path: str, depth: int) -> tuple[Resource | None, list[Resource], int]:
        if api_path in self._visited:
            return None, [], 0
        self._visited.add(api_path)
        try:
            folder, items, total = list_folder(
                self.client,
                api_path,
                page_size=self.page_size,
                sort=self.sort,
                media_type=self.media_type,
                include_trash=self.include_trash,
                read_timeout=self.listing_read_timeout,
                on_page=lambda n, p: self._report(force=True),
            )
        except AuthError:
            # Проблема с доступом — дальше идти бессмысленно.
            self._record_error(api_path, exc=None, kind="auth",
                               reason="токен отклонён — дальнейший обход невозможен")
            raise
        except ApiError as exc:
            self._record_error(api_path, exc=exc, kind=classify_error(exc))
            return None, [], 0
        return folder, items, total

    def _report(self, force: bool = False) -> None:
        """Обновляет прогресс; force=True — сразу (после каждой страницы)."""
        if self._on_progress is None:
            return
        now = time.monotonic()
        if force or now - self._last_report_at >= 0.4:
            self._on_progress(self.progress)
            self._last_report_at = now

    def _record_error(self, path: str | None, exc: BaseException | None, kind: str,
                      reason: str | None = None) -> None:
        self.progress.folders_failed += 1
        detail = reason or (human_error(exc) if exc else "неизвестная ошибка")
        self.errors.append(ScanError(path=path or self.root, reason=detail, kind=kind))
        log.warning("каталог %s пропущен: %s", path or self.root, detail)

    def _consume(self, items: Iterable[Resource], files: list[Resource], depth: int) -> None:
        for res in items:
            res.depth = depth
            if res.is_dir:
                continue
            files.append(res)
            self.progress.files_found += 1
            if res.size_known:
                self.progress.total_known_bytes += res.size or 0

    def _stats(self, files: list[Resource], folders: list[Resource]) -> ScanStats:
        """Считает сводку по фактически собранным данным, а не по прогрессу.

        Так статистика всегда соответствует содержимому манифеста, даже если
        обход прервался на середине.
        """
        return ScanStats(
            folders=len(folders),
            files=len(files),
            folders_failed=self.progress.folders_failed,
            files_unknown_size=sum(1 for f in files if not f.size_known),
            total_known_bytes=sum(f.size or 0 for f in files if f.size_known),
            files_with_md5=sum(1 for f in files if f.md5),
            files_with_sha256=sum(1 for f in files if f.sha256),
            api_requests=self.client.request_count,
        )
