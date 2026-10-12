"""Этапы D и E: листинг одной папки и обход дерева Диска.

Обход устроен как обход в ширину: итеративная очередь каталогов (deque, а не
рекурсия), для каждого — все страницы листинга `limit=500`/`offset`, пока
offset не дойдёт до `_embedded.total`. После КАЖДОЙ страницы результаты и
прогресс (папка + текущий offset) сразу пишутся в SQLite (scan_state.py):
после падения или перезапуска обход продолжается с последнего сохранённого
места, а не с нуля.

Обход не останавливается из-за одной плохой папки: ошибка логируется и
фиксируется, обход продолжается, а манифест в конце честно помечается
неполным. Это ровно то требование ТЗ «не объявлять сканирование успешным,
если часть каталогов не удалось обработать».

Чего здесь сознательно нет: разбиения гигантских папок по префиксам имён
(name=glob) и параметра sort. REST API такой фильтрации не поддерживает —
подобные запросы только умножают нагрузку на сервер. Устойчивость к папкам с
десятками тысяч файлов обеспечивают короткие страницы (`fields` без превью-
ссылок), увеличенный таймаут чтения, повторы 429/5xx с экспоненциальной
паузой (см. api.DiskClient._request) и возобновляемый постраничный прогресс.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
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
from .scan_state import (
    STATUS_LISTING,
    STATUS_QUEUED,
    STATUS_DONE,
    FolderRow,
    ScanProgressStore,
)

log = logging.getLogger(__name__)

#: Размер страницы листинга по ТЗ: 500 элементов за запрос.
PAGE_SIZE_DEFAULT = DiskClient.LISTING_PAGE_LIMIT

#: Таймаут чтения при листинге каталогов. Для папок с десятками тысяч файлов
#: Яндекс формирует страницу дольше обычных 60 с, и запрос обрывался как
#: NetworkError ("connection timeout"). Значение можно переопределить в
#: конфиге (listing_read_timeout).
LISTING_READ_TIMEOUT = 300.0


def _parse_page(data: dict[str, Any], api_path: str) -> tuple[list[dict[str, Any]], int | None]:
    """Разбирает одну страницу листинга: сырые элементы + total из ответа."""
    embedded = data.get("_embedded") or {}
    page = embedded.get("items")
    if not isinstance(page, list):
        raise ResponseFormatError(
            f"ответ листинга не содержит _embedded.items для {api_path}"
        )
    for raw in page:
        if not isinstance(raw, dict):
            raise ResponseFormatError("элемент листинга — не объект JSON")
    total: int | None = embedded.get("total") if isinstance(embedded.get("total"), int) else None
    return page, total


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
    #: Живой статус текущей фазы (например, «disk:/Photos: 500/30000»).
    #: Короткое сообщение в строке прогресса — чтобы было видно, что большая
    #: папка дочитывается страницами, а не зависла.
    phase: str = ""

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
        line = (
            f"Каталогов: {self.folders_done}/{known_total}{percent}  "
            f"Файлов: {self.files_found}  "
            f"Объём: {_bytes(self.total_known_bytes)}  "
            f"Ошибок: {self.folders_failed}  "
            f"Время: {int(self.elapsed)} с"
        )
        if self.phase:
            line += f"  [{self.phase}]"
        return line


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


def _overflow_tail(
    client: DiskClient,
    api_path: str,
    overflow: int,
    page_size: int,
    next_offset: int,
    read_timeout: float,
    media_type: str,
    include_trash: bool,
) -> list[dict[str, Any]]:
    """Хвост последней страницы, вылезший за лимит (overflow элементов).

    Листинг Яндекса стабилен по offset, поэтому хвост — это срез
    `items[-overflow:]` следующей страницы. Элементы не сохраняются в базу
    прогресса и offset не двигается: при возобновлении страница будет
    дочитана заново целиком, а здесь мы лишь не теряем их в текущем проходе.
    Сбой добора хвоста не должен ронять обход — возвращаем пустой список.
    """
    if overflow <= 0:
        return []
    extra: dict[str, Any] = {}
    if media_type:
        extra["media_type"] = media_type
    if include_trash:
        extra["include_trash"] = "true"
    try:
        data = client.list_folder_page(api_path, offset=next_offset,
                                       limit=page_size, read_timeout=read_timeout,
                                       **extra)
    except Exception as exc:  # noqa: BLE001 — хвост опционален, обход не роняем
        log.warning("не удалось добрать хвост листинга %s: %s", api_path, human_error(exc))
        return []
    page, _ = _parse_page(data, api_path)
    return page[-overflow:] if len(page) >= overflow else page


# --- Этап D: содержимое одной папки ----------------------------------------

def list_folder(
    client: DiskClient,
    api_path: str,
    *,
    page_size: int = PAGE_SIZE_DEFAULT,
    media_type: str = "",
    include_trash: bool = False,
    max_items: int | None = None,
    read_timeout: float | None = None,
    start_offset: int = 0,
    on_page: Callable[[int, int], None] | None = None,
    store: ScanProgressStore | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> tuple[Resource, list[Resource], int]:
    """Возвращает (сам ресурс-каталог, элементы, всего элементов по данным API).

    Пагинация: страницы по `page_size` идут одна за другой, `offset`
    наращивается, пока не достигнет `_embedded.total` (или пока страница не
    станет короче лимита). Параметр `sort` не используется; фильтров по
    префиксам имён нет — API такой фильтрации не поддерживает, и это только
    умножает нагрузку.

    `read_timeout` — увеличенный таймаут чтения для больших каталогов
    (тысячи файлов): Яндекс формирует листинг долго, и при стандартных 60 с
    запрос обрывается как NetworkError/timeout. Если не задан, используется
    LISTING_READ_TIMEOUT.

    `start_offset` — продолжение дочитывания с середины (возобновление после
    падения). Элементы возвращаются начиная с этого offset; всё уже
    прочитанное лежит в базе `store`.

    `on_page(элементов_на_странице, всего_элементов_в_папке)` — колбэк
    прогресса после каждой страницы, чтобы обход большой папки не выглядел
    «зависшим».

    `store` — база прогресса обхода: каждая дочитанная страница сразу
    сохраняется (элементы + новый offset), поэтому перезапуск продолжает с
    того же места, а не с нуля.
    """
    items: list[Resource] = []
    offset = max(0, int(start_offset))
    listed = offset
    total: int | None = None
    folder_meta: dict[str, Any] = {}
    rt = read_timeout if read_timeout is not None else LISTING_READ_TIMEOUT

    while True:
        extra: dict[str, Any] = {}
        if media_type:
            extra["media_type"] = media_type
        if include_trash:
            extra["include_trash"] = "true"
        data = client.list_folder_page(api_path, offset=offset, limit=page_size,
                                       read_timeout=rt, **extra)
        if not folder_meta:
            folder_meta = data

        page, ptotal = _parse_page(data, api_path)
        if ptotal is not None:
            # total может уточняться на последующих страницах — берём максимум.
            total = ptotal if total is None else max(total, ptotal)

        for raw in page:
            items.append(Resource.from_api(raw, parent_path=data.get("path") or api_path,
                                           depth=0))

        listed += len(page)
        next_offset = offset + len(page)

        if store is not None:
            try:
                store.add_items(api_path, page)
                store.save_page_progress(api_path, next_offset=next_offset,
                                         listed=listed, total=total)
            except Exception:  # noqa: BLE001 — сбой записи прогресса не роняет обход
                log.exception("не удалось сохранить прогресс листинга %s", api_path)

        if on_page is not None:
            try:
                on_page(len(page), listed)
            except Exception:  # noqa: BLE001 — прогресс не должен ронять обход
                log.exception("ошибка в колбэке прогресса листинга")

        offset = next_offset
        if not page:
            break
        if len(page) < page_size:
            break
        if total is not None and offset >= total:
            break
        # Стоп-флаг/лимит файлов проверяем ДО добора хвоста и до ветки
        # max_items: обрыв посреди гигантской папки должен сохранить offset
        # в базе, а не дочитывать её целиком «на автопилоте».
        stop_now = should_stop is not None and should_stop()
        if stop_now and max_items is None:
            break
        if max_items is not None and listed >= max_items:
            # Дочитали контрольный объём (или лимит файлов сканера): выходим,
            # сохранив offset — при возобновлении продолжим отсюда. Если это
            # был именно стоп-флаг (listed > max_items), добираем хвост
            # последней страницы, чтобы ни один элемент не потерялся.
            if stop_now and (overflow := listed - max_items):
                items.extend(_overflow_tail(client, api_path, overflow,
                                            page_size, offset, rt, media_type,
                                            include_trash))
            break

    folder = Resource.from_api(folder_meta, depth=0) if folder_meta else Resource(
        api_path=api_path, path=api_path.rstrip("/") or "/", name=api_path, type="dir"
    )
    return folder, items, total if total is not None else listed


# --- Этап E: обход дерева ---------------------------------------------------

class InventoryScanner:
    """Итеративный обход дерева Диска с возобновляемым прогрессом в SQLite.

    Каждой прочитанной странице соответствует запись в базе прогресса:
    элементы + новый offset папки. Поэтому процесс можно убить в любой
    момент — следующий запуск продолжит с последнего сохранённого места,
    а не с нуля. Подпапки (`type=dir`) добавляются в очередь (BFS, без
    рекурсии), файлы собираются в манифест и далее в реестр состояний.

    После каждой страницы в лог пишется строка вида
    «disk:/Photos: 500/30000» — по ней видно, что обход идёт, а не завис.
    """

    def __init__(
        self,
        client: DiskClient,
        *,
        root: str = "disk:/",
        page_size: int = PAGE_SIZE_DEFAULT,
        media_type: str = "",
        include_trash: bool = False,
        max_depth: int | None = None,
        max_files: int | None = None,
        max_folders: int = 200_000,
        listing_read_timeout: float | None = None,
        should_stop: Callable[[], bool] | None = None,
        progress: ScanProgressStore | None = None,
        reset_progress: bool = False,
        stop_check_interval: int = 0,
    ) -> None:
        self.client = client
        self.root = root if root.endswith("/") or ":" in root else root + "/"
        self.page_size = max(1, int(page_size))
        self.media_type = media_type
        self.include_trash = include_trash
        self.max_depth = max_depth
        self.max_files = max_files
        # Страховка от бесконечного обхода при сбое входных данных:
        # на диске физически не может быть миллион вложенных каталогов.
        self.max_folders = max_folders
        # Таймаут чтения листинга больших каталогов (см. LISTING_READ_TIMEOUT).
        self.listing_read_timeout = (
            LISTING_READ_TIMEOUT if listing_read_timeout is None else float(listing_read_timeout)
        )
        self._should_stop = should_stop or (lambda: False)
        #: Каждые N отлистованных элементов проверять стоп-флаг/лимит файлов
        #: прямо внутри листинга папки — иначе обрыв посреди гигантской папки
        #: сохранит offset только после её полного дочитывания. 0 = выкл.
        self.stop_check_interval = max(0, int(stop_check_interval))
        #: База прогресса обхода (SQLite). Если передана — обход становится
        #: возобновляемым: каждая страница фиксируется немедленно.
        self.store = progress
        self.reset_progress = reset_progress

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
        #: Колбэк прогресса — вызывается после каждой страницы листинга,
        #: чтобы большая папка не выглядела «зависшей».
        self._on_progress: Callable[[ScanProgress], None] | None = None
        self._last_report_at = 0.0
        self._queue: deque[FolderRow] = deque()

    def _listing_should_stop(self) -> bool:
        """Стоп-условие для проверки прямо внутри листинга папки:
        флаг пользователя или достигнутый лимит файлов сканера."""
        if self._should_stop():
            return True
        if self.max_files is not None and len(self._files) >= self.max_files:
            return True
        return False

    # --- основной проход ----------------------------------------------
    def scan(self, on_progress: Callable[[ScanProgress], None] | None = None) -> Manifest:
        self._on_progress = on_progress
        manifest = Manifest(
            started_at=utc_now_iso(),
            scan_root=self.root,
            status=STATUS_INCOMPLETE,
        )

        files = self._files
        folders = self._folders
        truncated = False

        self._prepare_queue()
        self._report(force=True)

        while self._queue:
            if self._should_stop():
                log.info("обход остановлен по запросу пользователя: %d каталогов "
                         "осталось в очереди", len(self._queue))
                self.stopped_by_user = True
                break
            if self.max_files is not None and len(files) >= self.max_files:
                log.info("достигнут лимит файлов (%s), обход остановлен", self.max_files)
                break
            if self.progress.folders_done + len(self._queue) >= self.max_folders:
                log.error("достигнут предохранитель в %d каталогов — обход прерван",
                          self.max_folders)
                self._record_error(None, exc=None, kind="guard",
                                   reason=f"превышен предохранитель каталогов ({self.max_folders})")
                truncated = True
                break

            row = self._queue.popleft()
            self._process_folder(row, files, folders)
            self._report(force=True)

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
        if self.store is not None and manifest.is_complete():
            # Отметка о завершении: при следующем запуске возобновлять нечего.
            self.store.mark_finished()
        return manifest

    def save_partial(self, data_dir, on_progress_note: Callable[[Manifest], None] | None = None) -> Manifest:
        """Сохраняет манифест с тем, что успели собрать (после остановки/исключения).

        Используется, когда пользователь прерывает обход (Ctrl+C) или случилась
        фатальная ошибка: собранные данные не должны пропадать — они пишутся
        как incomplete-манифест и пригодны для последующей загрузки.
        Прогресс в базе при этом остаётся — возобновляемый обход продолжится
        с последней сохранённой страницы.
        """
        from .manifest import manifest_path as _mpath

        manifest = self.build_partial_manifest()
        path = _mpath(Path(data_dir))
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

    # --- очередь папок (итеративный обход, без рекурсии) ----------------
    def _prepare_queue(self) -> None:
        """Заполняет очередь обхода; при наличии базы — возобновляет прогресс.

        Возобновляемость: недочитанные папки возвращаются в очередь вместе
        со своим offset'ом, поэтому дочитывание продолжается ровно с
        последнего сохранённого места, а не с нуля. Уже собранные файлы
        восстанавливаются из базы, чтобы не потерять их из манифеста.
        """
        if self.store is None:
            self._queue.append(FolderRow(api_path=self.root, depth=0))
            return

        if self.reset_progress:
            log.info("прогресс обхода сброшен — начинаем с нуля")
            self.store.reset()

        if self.store.get_folder(self.root) is None:
            self.store.ensure_folder(self.root, depth=0, status=STATUS_QUEUED)

        pending = self.store.pending_folders()
        resumed = [r for r in pending if r.status == STATUS_LISTING and r.offset > 0]
        for r in resumed:
            log.info("возобновление: каталог %s продолжен с offset=%d "
                     "(прочитано %d/%s)", r.api_path, r.offset, r.listed,
                     r.total if r.total is not None else "?")
        if resumed:
            self.progress.phase = f"возобновлено {len(resumed)} недочитанных каталогов"

        # Файлы и подпапки, найденные до падения, поднимаем из базы: они не
        # должны потеряться из манифеста. Элементы ещё не дочитанных папок
        # исключаем — их дочитает цикл обхода (offset уже сохранён).
        self._restore_from_store(pending)

        # Папки, дочитанные в прошлых проходах, повторно не листим: их
        # содержимое уже поднято из базы выше. Отмечаем их обработанными,
        # иначе на первом же лимите файлов scan() завершится «пустым»
        # манифестом и потеряет возобновленные данные.
        for r in pending:
            if r.status == STATUS_DONE:
                self._visited.add(r.api_path)
                self.progress.folders_done += 1
        self._queue.extend(r for r in pending if r.status != STATUS_DONE)

    def _restore_from_store(self, pending: list[FolderRow]) -> None:
        """Восстанавливает собранные ранее ресурсы из базы прогресса."""
        assert self.store is not None
        unfinished = {r.api_path for r in pending}
        seen_files = {f.api_path for f in self._files}
        for raw in self.store.all_items():
            res = Resource.from_api(raw, depth=0)
            parent = self.store.parent_of(res.api_path)
            if parent in unfinished:
                continue
            if res.is_dir:
                continue  # папки вернутся через pending-очередь/повторную регистрацию
            if res.api_path in seen_files:
                continue
            seen_files.add(res.api_path)
            self._files.append(res)
            self.progress.files_found += 1
            if res.size_known:
                self.progress.total_known_bytes += res.size or 0

    # --- одна папка -------------------------------------------------------
    def _process_folder(self, row: FolderRow, files: list[Resource],
                        folders: list[Resource]) -> None:
        api_path = row.api_path
        self.progress.current_folder = api_path
        if api_path in self._visited:
            return
        self._visited.add(api_path)
        if self.store is not None:
            self.store.start_folder(api_path)

        common = dict(
            page_size=self.page_size,
            media_type=self.media_type,
            include_trash=self.include_trash,
            read_timeout=self.listing_read_timeout,
            start_offset=row.offset,
            on_page=self._on_page,
            store=self.store,
            # Каждые N файлов проверяем стоп-флаг/лимит: обрыв посреди
            # большой папки сохраняет offset в базе, а не дочитывает её
            # целиком «на автопилоте».
            max_items=(self.stop_check_interval or None),
            # Стоп-флаг + лимит файлов сканера проверяются прямо внутри
            # листинга: обрыв посреди гигантской папки сохраняет offset
            # в базе, а не дочитывает её целиком «на автопилоте».
            should_stop=self._listing_should_stop if self.stop_check_interval else None,
        )
        try:
            child, items, total = list_folder(self.client, api_path, **common)
        except AuthError:
            # Проблема с доступом — дальше идти бессмысленно: падает весь обход.
            self._record_error(api_path, exc=None, kind="auth",
                               reason="токен отклонён — дальнейший обход невозможен")
            raise
        except ApiError as exc:
            # Остальные ошибки логируются и НЕ роняют обход целиком.
            self._record_error(api_path, exc=exc, kind=classify_error(exc))
            return
        except Exception as exc:  # noqa: BLE001 — обход переживает даже неожиданное
            self._record_error(api_path, exc=exc, kind="unknown")
            return

        if self._should_stop():
            # Прервано пользователем: offset уже сохранён постранично —
            # папка осталась в статусе listing и продолжится при следующем запуске.
            return

        listed_all = (self.store.get_folder(api_path) if self.store is not None else None)
        stopped_midway = (
            self.max_files is not None and len(self._files) >= self.max_files
            and listed_all is not None and listed_all.status != STATUS_DONE
            and listed_all.total is not None and listed_all.offset < listed_all.total
        )
        if stopped_midway:
            # Достигнут лимит файлов посреди большой папки: она остаётся
            # недочитанной (offset сохранён), возобновление продолжит с него.
            return

        self.progress.folders_done += 1
        if self.store is not None:
            self.store.finish_folder(api_path, listed=total, total=total)

        if child is not None:
            child.depth = row.depth
            if not child.name:
                child.name = api_path.rstrip("/").rpartition("/")[2] or "/"
            folders.append(child)

        # При возобновлении (start_offset > 0) `items` содержат только новые
        # страницы; полный набор дочитанной папки берём из базы прогресса.
        if self.store is not None and row.offset > 0:
            items = [Resource.from_api(raw, depth=0)
                     for raw in self.store.items_of_folder(api_path)]

        self._consume(items, files, row.depth)
        self._enqueue(items, row.depth + 1)

    # --- вспомогательное ----------------------------------------------
    def _enqueue(self, items: Iterable[Resource], depth: int) -> None:
        """Ставит в очередь подпапки, которых ещё не видели (BFS, не рекурсия).

        В `_visited` помечаются только реально обработанные папки
        (`_process_folder`), поэтому здесь проверяем очередь на дубли сами —
        иначе подпапка, помеченная «увиденной» при постановке в очередь, была
        бы пропущена при извлечении и весь её содержимое терялось.
        """
        if self.max_depth is not None and depth >= self.max_depth:
            return
        queued = {row.api_path for row in self._queue}
        for res in items:
            if not res.is_dir or res.api_path in self._visited:
                continue
            if res.api_path in queued:
                continue  # уже ждёт своей очереди
            queued.add(res.api_path)
            if self.store is not None:
                self.store.ensure_folder(res.api_path, parent=self.progress.current_folder,
                                         depth=depth, name=res.name, status=STATUS_QUEUED)
            self._queue.append(FolderRow(api_path=res.api_path,
                                         parent=self.progress.current_folder,
                                         depth=depth, name=res.name))

    def _on_page(self, page_items: int, listed: int) -> None:
        """Прогресс листинга: лог «папка: 500/30000» после каждой страницы."""
        api_path = self.progress.current_folder
        total: int | str = "?"
        if self.store is not None:
            row = self.store.get_folder(api_path)
            if row is not None and row.total is not None:
                total = row.total
        log.info("%s: %d/%s", api_path, listed, total)
        self.progress.phase = f"{api_path}: {listed}/{total}"
        self._report(force=True)

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
        if self.store is not None and path:
            try:
                self.store.fail_folder(path, detail)
            except Exception:  # noqa: BLE001
                log.exception("не удалось записать ошибку каталога %s в базу прогресса", path)

    def _consume(self, items: Iterable[Resource], files: list[Resource], depth: int) -> None:
        known_paths = {f.api_path for f in files}
        for res in items:
            res.depth = depth
            if res.is_dir:
                continue
            if res.api_path in known_paths:
                continue  # защита от дублей при возобновлении обхода
            known_paths.add(res.api_path)
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
