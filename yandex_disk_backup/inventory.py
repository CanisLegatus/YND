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
    ResourceLockedError,
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

# --- Стратегия «alphabet-splitting» для гигантских папок -----------------
# Когда обычный постраничный листинг огромного каталога (~десятки тысяч
# файлов, ~120 ГБ фотографий) упирается в таймаут или выглядит «зависшим»,
# мы делим пространство имён рекурсивно по префиксу имени файла:
# сначала одной буквой/цифрой, затем двумя и т.д. Каждый запрос становится
# лёгким (фильтр name=glob:<папка>/<префикс>*), страницы мелкие, а прогресс
# обновляется после каждого префикса — обход остаётся живым.
SPLIT_THRESHOLD_DEFAULT = 5000   # порог total, при котором включается сплит
MAX_PREFIX_DEPTH_DEFAULT = 4     # максимальная длина префикса при делении
# Символы первого уровня деления: цифры, латиница, кириллица + «прочее».
SPLIT_ALPHABET = (
    "0123456789"
    "abcdefghijklmnopqrstuvwxyz"
    "абвгдеёжзийклмнопрстуфхцчшщъыьэюя"
)
SPLIT_OTHER = "\u0000"  # служебный маркер сегмента «всё, что не попало в алфавит»


def _folder_dir_path(api_path: str) -> str:
    """Каталог ресурса в форме path-параметра API ('/Photos' для disk:/Photos)."""
    p = api_path
    if ":" in p[:12] and p.split(":", 1)[0] in ("disk", "app", "trash"):
        p = p.split(":", 1)[1]
    return p.rstrip("/") or "/"


def _glob_for_prefix(dir_path: str, prefix: str) -> str:
    if dir_path.endswith("/"):
        return f"{dir_path}{prefix}*"
    return f"{dir_path}/{prefix}*"


def _parse_page(data: dict[str, Any], api_path: str) -> tuple[list[Resource], int | None]:
    """Разбирает одну страницу листинга в список ресурсов (+total из ответа)."""
    embedded = data.get("_embedded") or {}
    page = embedded.get("items")
    if not isinstance(page, list):
        raise ResponseFormatError(
            f"ответ листинга не содержит _embedded.items для {api_path}"
        )
    total: int | None = embedded.get("total") if isinstance(embedded.get("total"), int) else None
    items = []
    for raw in page:
        if not isinstance(raw, dict):
            raise ResponseFormatError("элемент листинга — не объект JSON")
        items.append(Resource.from_api(raw, parent_path=data.get("path") or api_path, depth=0))
    return items, total


def _is_split_candidate(exc: BaseException) -> bool:
    """Стоит ли пробовать alphabet-splitting после падения обычного листинга.

    Только временные сетевые/серверные ошибки: таймауты, обрывы, 5xx, 423.
    Ошибки доступа (401/403/404) сплитом не лечатся.
    """
    if isinstance(exc, NetworkError):
        return True
    return isinstance(exc, ApiError) and exc.status in (408, 423, 425, 500, 502, 503, 504)


def split_listing_by_prefix(
    client: DiskClient,
    api_path: str,
    *,
    page_size: int,
    sort: str = "",
    media_type: str = "",
    include_trash: bool = False,
    read_timeout: float | None = None,
    max_prefix_len: int = MAX_PREFIX_DEPTH_DEFAULT,
    on_segment: Callable[[str, int], None] | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> tuple[Resource, list[Resource], int]:
    """Листинг гигантской папки через рекурсивное деление по префиксам имён.

    Вместо одного тяжёлого запроса «отдай весь каталог» делаем много лёгких:
    `GET /v1/disk/resources?path=<папка>&name=glob:<папка>/<префикс>*`.
    Если сегмент всё ещё слишком велик (total > page_size) — рекурсивно
    делим его по следующей букве. Сегмент «\\x00» собирает имена, начинающиеся
    с символов вне алфавита (служебные, редкие юникод-символы).

    Возвращает (папка-ресурс, элементы, всего элементов) — тот же контракт,
    что и `list_folder`, поэтому вызывающий код может подменить стратегию
    прозрачно. `on_segment(prefix, collected)` даёт живой прогресс: вызывается
    после каждого завершённого префикса.
    """
    stop = should_stop or (lambda: False)
    rt = read_timeout if read_timeout is not None else LISTING_READ_TIMEOUT
    dir_path = _folder_dir_path(api_path)
    folder_meta: dict[str, Any] = {}

    def fetch(extra: dict[str, Any] | None) -> tuple[list[Resource], int | None]:
        nonlocal folder_meta
        data = client.list_folder_page(
            api_path,
            limit=page_size,
            offset=0,
            sort=sort or None,
            media_type=media_type or None,
            include_trash=include_trash,
            read_timeout=rt,
            extra_params=extra,
        )
        if not folder_meta:
            folder_meta = data
        return _parse_page(data, api_path)

    def drain(extra: dict[str, Any] | None, first_page: list[Resource]) -> list[Resource]:
        """Дочитывает сегмент страницами до конца (первая страница уже есть)."""
        collected = list(first_page)
        offset = len(first_page)
        page_items = first_page
        while len(page_items) >= page_size:
            if stop():
                break
            more, _ = fetch({**(extra or {}), "offset": offset})
            page_items = more
            collected.extend(page_items)
            offset += len(page_items)
        return collected

    def walk(prefix: str, parent_count: int = -1) -> list[Resource]:
        """Собирает все элементы сегмента `prefix` (или всего каталога для '').

        `parent_count` — размер родительского сегмента; нужен для защиты от
        немонотонного деления: если фильтр по префиксу почему-то не уменьшил
        выборку (сервер игнорирует glob или матчит его иначе), дальнейшая
        рекурсия бессмысленна — вместо неё докачиваем сегмент страницами.
        """
        if stop():
            return []
        extra = {"name": _glob_for_prefix(dir_path, prefix)} if prefix else None
        glob_failed = False
        try:
            page_items, seg_total = fetch(extra)
        except (NetworkError, ResourceLockedError):
            # Глоб-фильтр не принят сервером либо сеть легла — пробуем без него.
            if prefix == "":
                raise
            log.debug("сегмент %r: запрос с глобом не удался, беру весь листинг", prefix)
            glob_failed = True
            extra = None
            page_items, seg_total = fetch(None)

        # Пустой сегмент не из чего делить и нечего собирать.
        if not page_items:
            return []

        seg_len = len(page_items)
        # Полная страница => за ней могут быть ещё элементы: сегмент велик.
        # Неполная страница => сегмент дочитан целиком независимо от total:
        # при фильтре name=glob Яндекс может возвращать total по всему
        # каталогу, а не по сегменту, поэтому слепое доверие total ведёт к
        # ложному решению «делить дальше» и кратному дублированию данных.
        seg_full = seg_len >= page_size
        has_more = seg_full and isinstance(seg_total, int) and seg_total > seg_len
        n = seg_total if has_more else seg_len

        # Деление должно уменьшать пространство имён: если дочерний сегмент
        # вернулся такого же размера, что и родитель, glob не работает как
        # фильтр — рекурсия уйдёт в бесконечность, качаем pages напрямую.
        monotonic = parent_count < 0 or n < parent_count
        can_split = (not glob_failed and prefix != ""
                     and len(prefix) < max_prefix_len)
        if seg_full and has_more and can_split and monotonic:
            split_children: list[Resource] = []
            for ch in SPLIT_ALPHABET + SPLIT_OTHER:
                if stop():
                    break
                split_children.extend(walk(prefix + ch, n))
            return split_children

        collected = drain(extra, page_items)

        if on_segment is not None:
            try:
                on_segment(prefix, len(collected))
            except Exception:  # noqa: BLE001 — прогресс не должен ронять обход
                log.exception("ошибка в колбэке прогресса сегмента")
        return collected

    items = walk("")
    folder = Resource.from_api(folder_meta, depth=0) if folder_meta else Resource(
        api_path=api_path, path=api_path.rstrip("/") or "/", name=api_path, type="dir"
    )
    return folder, items, len(items)


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
    #: Живой статус текущей фазы (например, «листинг по префиксам: 'IMG' → 340»).
    #: Короткое сообщение, которое видно в строке прогресса, пока большая папка
    #: обрабатывается — чтобы было очевидно, что данные продолжают приходить.
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
    probe_only: bool = False,
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

    `probe_only=True` — быстрый режим разведки: берётся только первая
    страница и `_embedded.total`, по которым вызывающий код решает, нужна ли
    гигантской папке стратегия alphabet-splitting (см. split_listing_by_prefix).
    Полный цикл list_folder на десятках тысяч файлов сам по себе стоит
    многих тяжёлых страниц, поэтому при большом total дальше читаем сплитом.
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

        if probe_only:
            break

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
        split_threshold: int | None = SPLIT_THRESHOLD_DEFAULT,
        max_prefix_len: int = MAX_PREFIX_DEPTH_DEFAULT,
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
        # Стратегия «alphabet-splitting» (план А): папки, в которых total
        # превышает этот порог, обходятся не тяжёлым постраничным листингом,
        # а рекурсивным делением по префиксам имён — лёгкими запросами с
        # живым прогрессом. 0/None — отключить явное деление (fallback при
        # сетевых ошибках остаётся включённым).
        self.split_threshold = max(0, int(split_threshold)) if split_threshold else 0
        self.max_prefix_len = max(1, int(max_prefix_len))
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
        common = dict(
            page_size=self.page_size,
            sort=self.sort,
            media_type=self.media_type,
            include_trash=self.include_trash,
            read_timeout=self.listing_read_timeout,
        )
        try:
            try:
                if self.split_threshold:
                    # Разведка: одна страница + total. Если папка гигантская —
                    # не выкачиваем её тяжёлыми постраничными запросами (это и
                    # выглядит как «зависание» на 120 ГБ фотоархиве), а сразу
                    # переходим к делению по префиксам имён (план А).
                    folder, items, total = list_folder(
                        self.client,
                        api_path,
                        probe_only=True,
                        on_page=lambda n, p: self._report(force=True),
                        **common,
                    )
                    if total > self.split_threshold and len(items) < total:
                        log.info(
                            "каталог %s: %d элементов (> порога %d) — обход по префиксам имён",
                            api_path, total, self.split_threshold,
                        )
                        # Первый запрос (разведка) уже лёгкий и полный —
                        # начинаем сразу с деления по одной букве, не
                        # повторяя тяжёлый «весь каталог» без фильтра.
                        sfolder, sitems = self._split_segments(
                            api_path, common, first_page=items,
                        )
                        if sfolder is not None:
                            folder = sfolder
                        items = sitems
                        total = len(items)
                    else:
                        # Папка обычная — дочитываем оставшиеся страницы.
                        rest, rtotal = self._read_rest(api_path, items, total, common)
                        items.extend(rest)
                        if rtotal:
                            total = rtotal
                else:
                    folder, items, total = list_folder(
                        self.client,
                        api_path,
                        on_page=lambda n, p: self._report(force=True),
                        **common,
                    )
            except ApiError as exc:
                # План А (страховка): обычный листинг гигантской папки упал на
                # таймауте/обрыве сети — пробуем деление по префиксам имён.
                if not _is_split_candidate(exc):
                    raise
                log.warning(
                    "каталог %s не читается обычным листингом (%s) — "
                    "переключаюсь на обход по префиксам имён", api_path, exc,
                )
                self.progress.phase = f"сплит-листинг {api_path}"
                self._report(force=True)
                folder, items, total = split_listing_by_prefix(
                    self.client,
                    api_path,
                    max_prefix_len=self.max_prefix_len,
                    on_segment=self._on_split_segment,
                    should_stop=self._should_stop,
                    **common,
                )
        except AuthError:
            # Проблема с доступом — дальше идти бессмысленно.
            self._record_error(api_path, exc=None, kind="auth",
                               reason="токен отклонён — дальнейший обход невозможен")
            raise
        except ApiError as exc:
            self._record_error(api_path, exc=exc, kind=classify_error(exc))
            return None, [], 0
        finally:
            if self.progress.phase:
                self.progress.phase = ""
        return folder, items, total

    def _split_segments(
        self,
        api_path: str,
        common: dict[str, Any],
        *,
        first_page: list[Resource] | None = None,
    ) -> tuple[Resource | None, list[Resource]]:
        """Рекурсивный обход сегментов «первой буквы» гигантской папки.

        `first_page` — элементы уже выполненного разведочного запроса всего
        каталога; они покрывают начало пространства имён, поэтому сегменты,
        целиком уместившиеся в одну страницу, повторно не запрашиваются.
        """
        dir_path = _folder_dir_path(api_path)
        rt = common.get("read_timeout")
        page_size = common.get("page_size", self.page_size)
        collected: list[Resource] = []
        seen: set[str] = set()
        folder: Resource | None = None
        remaining = list(first_page or [])
        # Размер родительского каталога (из разведки) — для проверки, что
        # деление по префиксу действительно уменьшает выборку.
        parent_total = len(remaining) if remaining else -1

        def take(items: list[Resource]) -> None:
            for r in items:
                if r.api_path not in seen:
                    seen.add(r.api_path)
                    collected.append(r)

        for ch in SPLIT_ALPHABET + SPLIT_OTHER:
            if self._should_stop():
                break
            low = ch.lower()
            seg_items: list[Resource] | None = None
            while remaining and remaining[0].name[:1].lower() == low:
                head, remaining = remaining[0], remaining[1:]
                if seg_items is None:
                    seg_items = []
                seg_items.append(head)
            if seg_items is not None and len(seg_items) < page_size:
                # Сегмент весь влез в разведочную страницу — запрос не нужен.
                take(seg_items)
                self._on_split_segment(ch, len(seg_items))
                continue

            try:
                data = self.client.list_folder_page(
                    api_path,
                    limit=page_size,
                    sort=self.sort or None,
                    media_type=self.media_type or None,
                    include_trash=self.include_trash,
                    read_timeout=rt,
                    extra_params={"name": _glob_for_prefix(dir_path, ch)},
                )
            except (NetworkError, ResourceLockedError):
                # Глоб-фильтр не принят сервером — fallback: берём весь
                # листинг без фильтра (медленно, но надёжно).
                data = self.client.list_folder_page(
                    api_path,
                    limit=page_size,
                    sort=self.sort or None,
                    media_type=self.media_type or None,
                    include_trash=self.include_trash,
                    read_timeout=rt,
                )
            if folder is None:
                folder = Resource.from_api(data, depth=0)
            seg_items, seg_total = _parse_page(data, api_path)
            if not seg_items:
                continue
            # Оценка размера ИМЕННО этого сегмента. При glob-фильтре Яндекс
            # может возвращать total по всему каталогу, поэтому доверяем ему
            # только когда страница полна (иначе сегмент дочитан целиком).
            seg_full = len(seg_items) >= page_size
            n = len(seg_items)
            if seg_full and isinstance(seg_total, int) and seg_total > n:
                n = seg_total

            # Деление обязано уменьшать пространство имён; если нет (сервер
            # игнорирует glob) — рекурсия опасна, докачиваем pages напрямую.
            monotonic = parent_total < 0 or n < parent_total

            if (seg_full and n > page_size and self.split_threshold
                    and n <= self.split_threshold and monotonic):
                # Сегмент средний: дочитываем его страницами напрямую.
                take(seg_items)
                offset = len(seg_items)
                while len(seg_items) >= page_size:
                    if self._should_stop():
                        break
                    more, _ = _parse_page(self.client.list_folder_page(
                        api_path, limit=page_size, offset=offset,
                        sort=self.sort or None,
                        media_type=self.media_type or None,
                        include_trash=self.include_trash,
                        read_timeout=rt,
                        extra_params={"name": _glob_for_prefix(dir_path, ch)},
                    ), api_path)
                    take(more)
                    offset += len(more)
                    seg_items = more
                    self._report(force=True)
                self._on_split_segment(ch, offset)
            elif seg_full and n > page_size:
                # Сегмент всё ещё гигантский — делим по следующей букве.
                sub_folder, sub_items = self._split_deeper(
                    api_path, dir_path, ch, common, parent_total=n,
                )
                if folder is None:
                    folder = sub_folder
                take(sub_items)
                self._on_split_segment(ch, len(sub_items))
            else:
                take(seg_items)
                self._on_split_segment(ch, len(seg_items))

        take(remaining)  # хвост разведочной страницы (если он был неполным)
        return folder, collected

    def _split_deeper(
        self,
        api_path: str,
        dir_path: str,
        prefix: str,
        common: dict[str, Any],
        *,
        parent_total: int = -1,
    ) -> tuple[Resource | None, list[Resource]]:
        """Деление сегмента по второй и последующим буквам имени.

        `parent_total` — размер родительского сегмента; если деление не
        уменьшает выборку (сервер игнорирует glob), рекурсия опасна —
        вместо неё сегмент докачивается страницами (`_drain_segment`).
        """
        rt = common.get("read_timeout")
        page_size = common.get("page_size", self.page_size)
        if len(prefix) >= self.max_prefix_len:
            # Дальше делить некуда — выкачиваем сегмент страницами как есть.
            return self._drain_segment(api_path, dir_path, prefix, common)
        collected: list[Resource] = []
        seen: set[str] = set()
        folder: Resource | None = None
        for ch2 in SPLIT_ALPHABET + SPLIT_OTHER:
            if self._should_stop():
                break
            nxt = prefix + ch2
            try:
                data = self.client.list_folder_page(
                    api_path,
                    limit=page_size,
                    sort=self.sort or None,
                    media_type=self.media_type or None,
                    include_trash=self.include_trash,
                    read_timeout=rt,
                    extra_params={"name": _glob_for_prefix(dir_path, nxt)},
                )
            except (NetworkError, ResourceLockedError):
                return self._drain_segment(api_path, dir_path, prefix, common)
            if folder is None:
                folder = Resource.from_api(data, depth=0)
            seg_items, seg_total = _parse_page(data, api_path)
            if not seg_items:
                continue
            # Оценка размера сегмента (см. комментарий про «лживый» total).
            seg_full = len(seg_items) >= page_size
            n = len(seg_items)
            if seg_full and isinstance(seg_total, int) and seg_total > n:
                n = seg_total
            monotonic = parent_total < 0 or n < parent_total
            if seg_full and n > page_size and monotonic:
                sub_folder, sub_items = self._split_deeper(
                    api_path, dir_path, nxt, common, parent_total=n,
                )
                if folder is None:
                    folder = sub_folder
                for r in sub_items:
                    if r.api_path not in seen:
                        seen.add(r.api_path)
                        collected.append(r)
            else:
                for r in seg_items:
                    if r.api_path not in seen:
                        seen.add(r.api_path)
                        collected.append(r)
                self._on_split_segment(nxt, len(seg_items))
        return folder, collected

    def _drain_segment(
        self,
        api_path: str,
        dir_path: str,
        prefix: str,
        common: dict[str, Any],
    ) -> tuple[Resource | None, list[Resource]]:
        """Полностью выкачивает один сегмент постранично (последняя инстанция)."""
        rt = common.get("read_timeout")
        page_size = common.get("page_size", self.page_size)
        extra = {"name": _glob_for_prefix(dir_path, prefix)} if prefix else None
        collected: list[Resource] = []
        seen: set[str] = set()
        folder: Resource | None = None
        offset = 0
        while True:
            if self._should_stop():
                break
            data = self.client.list_folder_page(
                api_path,
                limit=page_size,
                offset=offset,
                sort=self.sort or None,
                media_type=self.media_type or None,
                include_trash=self.include_trash,
                read_timeout=rt,
                extra_params=dict(extra) if extra else None,
            )
            if folder is None:
                folder = Resource.from_api(data, depth=0)
            page_items, _ = _parse_page(data, api_path)
            for r in page_items:
                if r.api_path not in seen:
                    seen.add(r.api_path)
                    collected.append(r)
            offset += len(page_items)
            self._report(force=True)
            if not page_items or len(page_items) < page_size:
                break
        self._on_split_segment(prefix, len(collected))
        return folder, collected

    def _read_rest(
        self,
        api_path: str,
        already: list[Resource],
        total: int,
        common: dict[str, Any],
    ) -> tuple[list[Resource], int]:
        """Дочитывает страницы обычной (не гигантской) папки после разведки.

        `list_folder(probe_only=True)` вернул только первую страницу; здесь
        продолжаем с offset = len(already) теми же параметрами. Возвращает
        (остаток элементов, уточнённый total).
        """
        rest: list[Resource] = []
        offset = len(already)
        rt = common.get("read_timeout")
        while True:
            if offset and offset >= max(total, 1):
                break
            data = self.client.list_folder_page(
                api_path,
                limit=self.page_size,
                offset=offset,
                sort=self.sort or None,
                media_type=self.media_type or None,
                include_trash=self.include_trash,
                read_timeout=rt,
            )
            page_items, ptotal = _parse_page(data, api_path)
            if ptotal:
                total = ptotal
            rest.extend(page_items)
            self._report(force=True)
            offset += len(page_items)
            if not page_items or len(page_items) < self.page_size:
                break
        return rest, total

    def _on_split_segment(self, prefix: str, collected: int) -> None:
        """Живой прогресс сплит-обхода: видно, какой префикс дочитали."""
        shown = repr(prefix)[1:-1] if prefix else "(весь каталог)"
        self.progress.phase = f"префикс '{shown}' → {collected} эл."
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
