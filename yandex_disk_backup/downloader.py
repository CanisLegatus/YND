"""Скачивание оригинальных файлов (часть II ТЗ, разделы 13–15 и 17).

Инварианты, которые код обязан соблюдать:

1. Файл сначала пишется во временный `.part` рядом с целью и только после
   проверки перемещается на место. Незавершённый файл не может оказаться
   под финальным именем.
2. Каждый файл скачивается отдельно, по временной ссылке из официального API.
   Никакого общего ZIP-архива, никакого перекодирования: байты сохраняются
   как есть.
3. Проверка: размер сверяется всегда (если известен), контрольная сумма — если
   API её вернул. Файл без контрольной суммы помечается в отчёте, а не
   молча считается идеально проверенным.
4. Существующий проверенный файл не перезаписывается и не удаляется.
   При конфликте применяется заранее заданная стратегия.
5. Истёкшая временная ссылка — не ошибка, а повод взять новую: ссылка
   получается заново, файл скачивается с нуля (или с Range, если сервер
   подтвердил поддержку).
"""

from __future__ import annotations

import faulthandler
import hashlib
import logging
import os
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

import requests
from requests.adapters import HTTPAdapter

from .api import DiskClient
from .config import CONFLICT_KEEP, CONFLICT_OVERWRITE, CONFLICT_REPLACE, Config
from .inventory import human_error
from .naming import PathAllocator
from .state import (
    ERROR,
    RECHECK,
    VERIFIED,
    FileRecord,
    StateStore,
)
from .throttle import ThrottleBreaker

log = logging.getLogger(__name__)

PART_SUFFIX = ".part"


class ThrottledInterrupt(RuntimeError):
    """Скачивание прервано глобальным выключателем (429 от любого воркера)."""


class DownloadError(RuntimeError):
    """Ошибка скачивания конкретного файла."""

    def __init__(self, message: str, kind: str = "unknown", permanent: bool = False) -> None:
        super().__init__(message)
        self.kind = kind
        self.permanent = permanent


@dataclass
class DownloadOutcome:
    api_path: str
    local_path: str
    status: str
    bytes_written: int = 0
    seconds: float = 0.0
    skipped: bool = False
    note: str = ""

    @property
    def ok(self) -> bool:
        return self.status == VERIFIED


@dataclass
class DownloadStats:
    files_total: int = 0
    files_done: int = 0
    files_skipped: int = 0
    files_failed: int = 0
    bytes_done: int = 0
    started_at: float = field(default_factory=time.monotonic)
    last_note: str = ""

    def reset(self, files_total: int) -> None:
        """Обнуляет счётчики, сохраняя объект.

        Объект переживает переиспользование: на нём же висит живая строка
        прогресса, и подмена его на новый оставила бы строку «замёрзшей».
        """
        self.files_total = files_total
        self.files_done = 0
        self.files_skipped = 0
        self.files_failed = 0
        self.bytes_done = 0
        self.started_at = time.monotonic()
        self.last_note = ""

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.started_at

    @property
    def speed(self) -> float:
        elapsed = self.elapsed
        return (self.bytes_done / elapsed) if elapsed > 0 else 0.0

    def render(self) -> str:
        total = self.files_total or 1
        pct = 100.0 * (self.files_done + self.files_skipped) / total
        return (
            f"Файлов: {self.files_done + self.files_skipped}/{self.files_total} ({pct:5.1f}%)  "
            f"Загружено: {_bytes(self.bytes_done)}  "
            f"Скорость: {_bytes(int(self.speed))}/с  "
            f"Ошибок: {self.files_failed}  "
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


def _parse_iso(value: str | None) -> float | None:
    if not value:
        return None
    text = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def free_space(path: Path) -> int:
    """Свободные байты на диске, где лежит каталог (или его ближайший родитель)."""
    probe = path
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    return shutil.disk_usage(probe).free


def _default_http_get(method: str, href: str, **kwargs: Any) -> Any:
    """Скачивание по временной ссылке. Ответ — контекстный менеджер.

    Собственный Session с pool_maxsize не меньше числа воркеров: без явного
    размера пула urllib3 держит 10 соединений на хост, и при стриминге
    (ответы долго не освобождают слот) лишние запросы могли бы ждать
    освобождения соединения. Вызов всегда идёт внутри `with response:` —
    соединение возвращается в пул сразу после закрытия ответа.
    """
    session = getattr(_default_http_get, "_session", None)
    if session is None:
        workers = max(4, int(getattr(threading, "_ydb_download_workers", 4)))
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=workers, pool_maxsize=workers)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        _default_http_get._session = session
    return session.request(method, href, **kwargs)


class Downloader:
    """Параллельное скачивание с проверкой и безопасной записью."""

    def __init__(
        self,
        cfg: Config,
        store: StateStore,
        client_factory: Callable[[], DiskClient],
        *,
        allocator: PathAllocator | None = None,
        on_progress: Callable[[DownloadOutcome], None] | None = None,
        should_stop: Callable[[], bool] | None = None,
        http_get: Callable[..., Any] | None = None,
        breaker: ThrottleBreaker | None = None,
    ) -> None:
        self.cfg = cfg
        self.store = store
        self.client_factory = client_factory
        self.allocator = allocator or PathAllocator(cfg.backup_dir, cfg.conflict_strategy)
        self.on_progress = on_progress
        self._should_stop = should_stop or (lambda: False)
        # Отдельная точка для скачивания: временные ссылки ведут на CDN,
        # а не на API, и подменяются в тестах.
        self._http_get = http_get or _default_http_get
        self._local = threading.local()
        self._stats_lock = threading.Lock()
        self.stats = DownloadStats()
        # Диагностический счётчик «в очереди» (для heartbeat-лога раз в 10 с).
        self._pending = 0
        # Глобальный выключатель против 429: общий флаг для всех воркеров.
        if breaker is not None:
            self.breaker = breaker
        elif getattr(cfg, "throttle_enabled", True):
            self.breaker = ThrottleBreaker(
                max_workers=max(1, int(cfg.concurrency)),
                base_sleep=getattr(cfg, "throttle_base_sleep", 2.0),
                max_sleep=getattr(cfg, "throttle_max_sleep", 60.0),
                recovery_window=getattr(cfg, "throttle_recovery_window", 120.0),
            )
        else:
            self.breaker = None

    # --- клиент на поток ----------------------------------------------
    def _client(self) -> DiskClient:
        client = getattr(self._local, "client", None)
        if client is None:
            client = self.client_factory()
            # Пул воркеров делит один выключатель: 429 любого клиента
            # усыпляет всех, успех пробника будит всех.
            if self.breaker is not None and getattr(client, "breaker", None) is None:
                client.breaker = self.breaker
            self._local.client = client
        return client

    # --- основной проход ----------------------------------------------
    def download_all(
        self,
        records: Sequence[FileRecord],
        *,
        dry_run: bool = False,
    ) -> list[DownloadOutcome]:
        self.stats.reset(len(records))
        outcomes: list[DownloadOutcome] = []
        if not records:
            return outcomes

        self.cfg.backup_dir.mkdir(parents=True, exist_ok=True)
        if not dry_run:
            self._check_free_space(records)

        workers = max(1, min(self.cfg.concurrency, len(records)))
        if workers == 1:
            for record in records:
                if self._should_stop():
                    break
                outcomes.append(self._handle(record, dry_run))
            return outcomes

        # Диагностика «молчаливой остановки»: если поток зависнет (например,
        # на заблокированном семафоре/замке), через 120 с трейсбек всех потоков
        # будет выписан в stderr автоматически, без падения процесса.
        try:
            faulthandler.dump_traceback_later(120, repeat=True)
        except (ValueError, OSError):  # например, нет stderr
            pass
        heartbeat_stop = threading.Event()

        def _heartbeat() -> None:
            """Раз в 10 секунд — сводка: байты, файлы, воркеры, очередь."""
            while not heartbeat_stop.wait(10.0):
                with self._stats_lock:
                    done = self.stats.files_done + self.stats.files_skipped
                    failed = self.stats.files_failed
                    total = self.stats.files_total
                    bts = self.stats.bytes_done
                    note = self.stats.last_note
                breaker = self.breaker
                if breaker is not None:
                    allowed = breaker.allowed_workers
                    busy, permits = breaker.snapshot()
                    throttled = "да" if breaker.is_throttled else "нет"
                    waves = breaker.throttle_waves
                else:
                    allowed, throttled, waves = workers, "нет", 0
                    busy, permits = self._pending and min(self._pending, workers) or 0, 0
                log.info(
                    "heartbeat: скачано %s байт, файлов %d/%d (ошибок %d), "
                    "активных задач %d, в очереди %d, воркеров занято/свободно %d/%d "
                    "(разрешено %d из %d), троттлинг=%s (волн %d), последний: %s",
                    bts, done, total, failed, self._pending,
                    max(0, total - done - failed - self._pending),
                    busy, permits, allowed, workers, throttled, waves, note,
                )

        hb = threading.Thread(target=_heartbeat, name="ydb-dl-heartbeat", daemon=True)
        hb.start()
        # Размер пула соединений CDN-сессии должен покрывать число воркеров.
        threading._ydb_download_workers = workers
        try:
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="ydb-dl") as pool:
                with self._stats_lock:
                    self._pending = len(records)
                futures = {pool.submit(self._handle, record, dry_run): record for record in records}
                for future in as_completed(futures):
                    record = futures[future]
                    with self._stats_lock:
                        self._pending -= 1
                    try:
                        outcomes.append(future.result())
                    except ThrottledInterrupt:
                        # Файл прерван выключателем: не ошибка, вернётся в очередь.
                        outcomes.append(DownloadOutcome(record.api_path,
                                                       record.local_path or "", RECHECK,
                                                       note="прервано выключателем (429)"))
                    except DownloadError as exc:
                        outcomes.append(self._fail(record, exc))
                    except Exception as exc:  # noqa: BLE001 - не теряем ни одной задачи
                        # Логуем полный traceback: проглоченная в пуле ошибка
                        # иначе выглядит как «процесс просто молчит».
                        log.error("непредвиденная ошибка при загрузке %s",
                                  record.api_path, exc_info=True)
                        outcomes.append(self._fail(record, DownloadError(human_error(exc), "internal")))
        finally:
            heartbeat_stop.set()
            try:
                faulthandler.cancel_dump_traceback_later()
            except (ValueError, RuntimeError):
                pass
        if self.breaker is not None and self.breaker.throttle_waves:
            log.info("выключатель срабатывал %d раз(а); финальный параллелизм: %d из %d",
                     self.breaker.throttle_waves, self.breaker.allowed_workers, workers)
        return outcomes

    # --- один файл -----------------------------------------------------
    def _handle(self, record: FileRecord, dry_run: bool) -> DownloadOutcome:
        started = time.monotonic()
        log.info("начало загрузки: %s", record.api_path)
        breaker = self.breaker
        if breaker is not None and not dry_run:
            # Динамический параллелизм: воркер занимает «место» семафора.
            # После волн 429 мест становится меньше — лишние воркеры ждут,
            # и одновременных запросов к серверу физически не может быть
            # больше, чем разрешено выключателем.
            breaker.note_task_start()
        try:
            outcome = self._handle_inner(record, dry_run, started)
            log.info("загрузка завершена (%s): %s — статус %s, %s байт, %.1f с",
                     "пропущен" if outcome.skipped else "ок" if outcome.ok else "ошибка",
                     record.api_path, outcome.status, outcome.bytes_written,
                     time.monotonic() - started)
            return outcome
        finally:
            if breaker is not None and not dry_run:
                breaker.note_task_end()

    def _handle_inner(self, record: FileRecord, dry_run: bool, started: float) -> DownloadOutcome:
        allocation = self.allocator.allocate(record.api_path, record.path or record.name)
        self.store.bind_local_path(record.api_path, allocation.local_relpath)
        target = allocation.absolute(self.cfg.backup_dir)

        if dry_run:
            note = "пробный запуск" + (f", имя изменено: {allocation.reason}" if allocation.renamed else "")
            return self._record(record, allocation, VERIFIED, 0, started, skipped=True, note=note)

        # Уже проверенный файл не трогаем — но только если он лежит там же,
        # куда положен сейчас: переименованный путь требует новой проверки.
        if self._is_already_done(record, allocation, target):
            return self._record(record, allocation, VERIFIED, record.local_size or 0,
                                started, skipped=True, note="уже проверен, пропущен")

        # Чужой локальный файл, о котором реестр ничего не знает: не затираем.
        # Если же реестр знает этот путь — файл наш, и испорченный/устаревший
        # экземпляр нужно перекачать, а не беречь.
        known_to_registry = bool(record.local_path) and record.local_path == allocation.local_relpath
        if target.exists() and not known_to_registry and self.cfg.conflict_strategy == CONFLICT_KEEP:
            return self._record(record, allocation, RECHECK, 0, started, skipped=True,
                                note="файл уже есть на диске, оставлен как есть")

        self._prepare_target(target, record)

        target.parent.mkdir(parents=True, exist_ok=True)
        part = target.with_name(target.name + PART_SUFFIX)
        self.store.mark_downloading(record.api_path, allocation.local_relpath)

        try:
            written = self._fetch_with_retries(record, part, target)
        except DownloadError as exc:
            self._cleanup(part)
            if exc.permanent and exc.kind in ("auth", "permission"):
                raise
            return self._fail(record, exc, allocation=allocation)
        except ThrottledInterrupt:
            # Выключатель поднялся посреди передачи: прерываемся немедленно,
            # но файл НЕ считаем ошибкой — он встанет в очередь заново.
            self._cleanup(part)
            self.store.mark(record.api_path, RECHECK)
            log.info("скачивание %s прервано выключателем (429), будет повтор после сна",
                     record.name)
            raise

        # Проверка до перемещения — это и есть смысл временного файла.
        problem = self._verify(written, record)
        if problem:
            self._cleanup(part)
            self.store.mark(record.api_path, ERROR, last_error=problem, last_error_kind="verify")
            with self._stats_lock:
                self.stats.files_failed += 1
            return DownloadOutcome(record.api_path, allocation.local_relpath, ERROR,
                                   note=problem)

        self._finalize(part, target, record)
        final_size = target.stat().st_size
        self.store.mark_verified(record.api_path, allocation.local_relpath, final_size)
        # Переименование — это не деталь реализации: пользователь должен
        # знать, почему файл лёг под другим именем.
        return self._record(record, allocation, VERIFIED, final_size, started,
                            note=allocation.reason if allocation.renamed else "")

    # --- скачивание -----------------------------------------------------
    def _fetch_with_retries(self, record: FileRecord, part: Path, target: Path) -> Path:
        attempts = max(1, self.cfg.max_attempts_per_file)
        last: DownloadError | None = None

        for attempt in range(1, attempts + 1):
            if self._should_stop():
                raise DownloadError("остановлено пользователем", "stopped", permanent=True)
            breaker = self.breaker
            if breaker is not None:
                # Глобальный выключатель: пока кто-то поймал 429 и флаг поднят,
                # воркер не начинает новый запрос — коротко спит и проверяет
                # снова. Это резко обнуляет трафик.
                breaker.before_request()
            self._cleanup(part)  # каждая попытка начинается с чистого файла
            try:
                self._download_once(record, part, target, attempt)
                if breaker is not None:
                    # Успешная загрузка — признак того, что лимит отпустил.
                    breaker.on_success()
                return part
            except ThrottledInterrupt:
                # Обрыв посреди передачи из-за выключателя: попытку НЕ расходуем
                # (файл ни в чём не виноват), даём волне закончиться и пробуем
                # снова — до throttle_max_attempts дополнительных циклов.
                extra = getattr(self.cfg, "throttle_max_attempts", 60)
                self._recovered_after_throttle(extra)
                continue
            except DownloadError as exc:
                if breaker is not None and self._is_rate_limit(exc):
                    # 429 прямого запроса на CDN: поднимаем общий флаг,
                    # все засыпают; попытка этого файла не расходуется.
                    breaker.on_throttled(None)
                    extra = getattr(self.cfg, "throttle_max_attempts", 60)
                    self._recovered_after_throttle(extra)
                    continue
                last = exc
                if exc.permanent:
                    raise
                self.store.bump_attempt(record.api_path)
                if attempt >= attempts:
                    break
                delay = min(30.0, 1.5 * (2 ** (attempt - 1)))
                log.info("повтор %d/%d для %s через %.1f с: %s",
                         attempt, attempts, record.name, delay, exc)
                time.sleep(delay)

        assert last is not None
        self.store.mark_error(record.api_path, str(last), last.kind)
        raise last

    @staticmethod
    def _is_rate_limit(exc: DownloadError) -> bool:
        text = str(exc)
        return "(429" in text or "HTTP 429" in text or " 429)" in text

    def _recovered_after_throttle(self, max_cycles: int) -> None:
        """Ждём окончания волны 429: сон короткими интервалами до сброса флага."""
        breaker = self.breaker
        assert breaker is not None
        for _ in range(max(1, int(max_cycles))):
            if self._should_stop():
                raise DownloadError("остановлено пользователем", "stopped", permanent=True)
            if not breaker.is_throttled:
                return
            breaker.before_request()
        # Волна так и не закончилась — прерываем файл, он вернётся в очередь.
        raise ThrottledInterrupt("выключатель не сбросился за отведённое время")

    def _download_once(self, record: FileRecord, part: Path, target: Path, attempt: int) -> None:
        client = self._client()
        href, method = client.get_download_link(record.api_path)

        resume_from = 0
        headers: dict[str, str] = {}
        if self.cfg.allow_range_resume and attempt > 1 and part.exists():
            resume_from = part.stat().st_size
            headers["Range"] = f"bytes={resume_from}-"

        tmp_headers = {"User-Agent": "YandexDiskBackup/0.3 (read-only backup tool)"}
        tmp_headers.update(headers)
        # Таймаут скачивания: (соединение, чтение). 10 с на коннект и 60 с без
        # байт на чтение — «мёртвое» соединение не вешает воркер навсегда,
        # а ошибка NetworkError превращается в обычную повторную попытку.
        timeout = self.cfg.download_timeout
        try:
            connect_t, read_t = float(timeout[0]), float(timeout[1])
        except (TypeError, ValueError, IndexError):
            connect_t, read_t = 10.0, 60.0
        download_timeout = (min(connect_t, 10.0), min(read_t, 60.0))

        try:
            response = self._http_get(
                method, href, stream=True, timeout=download_timeout,
                headers=tmp_headers, allow_redirects=True,
            )
        except requests.RequestException as exc:
            raise DownloadError(
                f"не удалось скачать: {type(exc).__name__}", "network"
            ) from exc

        with response:
            self._check_link_response(response, resume_from)
            mode = "r+b" if resume_from and part.exists() else "wb"
            self.store.mark_progress(record.api_path, resume_from)
            written = self._stream_to_file(response, part, record, mode, resume_from)

        expected = record.size
        if expected is not None and resume_from and written < expected:
            # Сервер вернул меньше, чем просили: это не «готово».
            raise DownloadError(
                f"получено {written} байт при ожидаемых {expected}", "incomplete"
            )

    def _check_link_response(self, response: requests.Response, resume_from: int) -> None:
        status = response.status_code
        if status in (401, 403, 404, 410):
            raise DownloadError(
                f"временная ссылка не сработала (HTTP {status}); будет запрошена новая",
                "link_expired",
            )
        if resume_from:
            if status != 206:
                # Сервер проигнорировал Range — докачка невозможна, начинаем заново.
                log.info("сервер не поддержал Range для %s, файл будет скачан заново", response.url)
                return
            content_range = response.headers.get("Content-Range", "")
            if not content_range.startswith(f"bytes {resume_from}-"):
                raise DownloadError(
                    f"сервер вернул неожиданный Content-Range: {content_range!r}", "bad_range"
                )
        if status >= 400:
            raise DownloadError(f"HTTP {status} при скачивании", "http")

    def _stream_to_file(self, response: requests.Response, part: Path, record: FileRecord,
                        mode: str, resume_from: int) -> int:
        need_md5 = bool(record.md5)
        need_sha = bool(record.sha256) and not record.md5
        md5 = hashlib.md5() if need_md5 else None
        sha = hashlib.sha256() if need_sha else None
        digest_name = "md5" if need_md5 else ("sha256" if need_sha else "")
        digest = md5 or sha

        written = resume_from
        last_report = 0.0
        with open(part, mode) as fh:
            for chunk in response.iter_content(chunk_size=self.cfg.chunk_size):
                if not chunk:
                    continue
                fh.write(chunk)
                if digest is not None:
                    digest.update(chunk)
                written += len(chunk)
                now = time.monotonic()
                if now - last_report > 1.0:
                    self.store.mark_progress(record.api_path, written)
                    last_report = now
            fh.flush()
            os.fsync(fh.fileno())

        if digest is not None:
            part.with_name(part.name + f".{digest_name}").write_text(digest.hexdigest(), encoding="ascii")

        with self._stats_lock:
            self.stats.bytes_done += written - resume_from
        return written

    # --- проверка и перемещение ------------------------------------------
    def _verify(self, part: Path, record: FileRecord) -> str | None:
        """Возвращает текст проблемы или None, если файл в порядке."""
        if not part.exists():
            return "временный файл исчез до проверки"
        actual = part.stat().st_size

        if record.size is not None and actual != record.size:
            return f"размер не совпал: получено {actual}, ожидалось {record.size}"

        expected = record.expected_checksum
        if expected:
            kind, value = expected
            side = part.with_name(f"{part.name}.{kind}")
            got = side.read_text(encoding="ascii").strip() if side.exists() else None
            side.unlink(missing_ok=True)
            if got is None:
                return f"не удалось вычислить {kind} для проверки"
            if got.lower() != value.lower():
                return f"{kind} не совпал: получено {got}, ожидалось {value}"
        return None

    def _prepare_target(self, target: Path, record: FileRecord) -> None:
        """Освобождает путь по стратегии конфликта. Ничего не удаляет молча."""
        if not target.exists():
            return
        strategy = self.cfg.conflict_strategy
        if strategy == CONFLICT_KEEP:
            return
        if strategy == CONFLICT_OVERWRITE:
            log.info("перезаписываю %s (стратегия overwrite)", target)
            return
        if strategy == CONFLICT_REPLACE:
            # Заменяем лишь если локальный файл сам по себе не подтверждён.
            existing = self.store.get(record.api_path)
            if existing and existing.status == VERIFIED and existing.local_size == target.stat().st_size:
                raise DownloadError(
                    "на диске уже есть проверенный файл этого размера; "
                    "измените стратегию конфликта, если нужна перезапись",
                    "conflict", permanent=True,
                )
            log.info("заменяю непроверенный локальный файл %s", target)

    def _finalize(self, part: Path, target: Path, record: FileRecord) -> None:
        """Перемещает проверенный временный файл на место."""
        ts = _parse_iso(record.modified) if self.cfg.preserve_times else None
        if ts is not None:
            try:
                os.utime(part, (ts, ts))
            except OSError as exc:
                log.debug("не удалось выставить время файла %s: %s", target.name, exc)
        try:
            os.replace(part, target)
        except OSError as exc:
            # Разные тома / занятый файл — копируем с сохранением содержимого.
            log.debug("os.replace не удался (%s), копирую", exc)
            shutil.copy2(part, target)
            part.unlink(missing_ok=True)
        for leftover in part.parent.glob(part.name + ".*"):
            if leftover.suffix in (".md5", ".sha256"):
                leftover.unlink(missing_ok=True)

    # --- вспомогательное --------------------------------------------------
    def _is_already_done(self, record: FileRecord, allocation, target: Path) -> bool:
        """Можно ли не трогать файл: он проверен, лежит на месте и цел."""
        if record.status != VERIFIED:
            return False
        if record.local_path != allocation.local_relpath:
            return False  # путь изменился — надо заново скачать и проверить
        try:
            size = target.stat().st_size
        except OSError:
            return False
        return record.local_size is None or size == record.local_size

    def _cleanup(self, part: Path) -> None:
        for path in [part, *part.parent.glob(part.name + ".*")]:
            if path.suffix in (".md5", ".sha256", ".part") and path.exists():
                try:
                    path.unlink()
                except OSError:
                    pass

    def _check_free_space(self, records: Sequence[FileRecord]) -> None:
        needed = sum(r.size or 0 for r in records if r.status != VERIFIED)
        if needed <= 0:
            return
        have = free_space(self.cfg.backup_dir)
        if have < needed + self.cfg.free_space_margin:
            raise DownloadError(
                f"недостаточно места: нужно ≈{_bytes(needed)}, доступно {_bytes(have)} "
                f"плюс запас {_bytes(self.cfg.free_space_margin)}. Скачивание не начато.",
                "no_space", permanent=True,
            )
        log.info("места достаточно: нужно ≈%s, доступно %s", _bytes(needed), _bytes(have))

    def _record(self, record: FileRecord, allocation, status: str, size: int,
                started: float, *, skipped: bool = False, note: str = "") -> DownloadOutcome:
        outcome = DownloadOutcome(
            api_path=record.api_path,
            local_path=allocation.local_relpath,
            status=status,
            bytes_written=size,
            seconds=time.monotonic() - started,
            skipped=skipped,
            note=note,
        )
        with self._stats_lock:
            if skipped:
                self.stats.files_skipped += 1
            else:
                self.stats.files_done += 1
            self.stats.last_note = f"{record.name}: {note}" if note else record.name
        if self.on_progress:
            self.on_progress(outcome)
        return outcome

    def _fail(self, record: FileRecord, exc: DownloadError, allocation=None) -> DownloadOutcome:
        local_path = allocation.local_relpath if allocation else (record.local_path or "")
        if exc.kind not in ("auth", "permission"):
            self.store.mark_error(record.api_path, str(exc), exc.kind)
        with self._stats_lock:
            self.stats.files_failed += 1
            self.stats.last_note = f"{record.name}: {exc}"
        if self.on_progress:
            self.on_progress(DownloadOutcome(record.api_path, local_path, ERROR, note=str(exc)))
        return DownloadOutcome(record.api_path, local_path, ERROR, note=str(exc))
