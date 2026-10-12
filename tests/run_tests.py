"""Тесты без сети: поддельные API и поддельный CDN.

Запуск:  python tests/run_tests.py
Ничего не скачивается и никуда не ходит — все ответы заданы здесь.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from yandex_disk_backup.api import DiskClient  # noqa: E402
from yandex_disk_backup.config import Config  # noqa: E402
from yandex_disk_backup.downloader import Downloader  # noqa: E402
from yandex_disk_backup.inventory import InventoryScanner, list_folder  # noqa: E402
from yandex_disk_backup.manifest import Manifest, Resource  # noqa: E402
from yandex_disk_backup.naming import PathAllocator, sanitize_component  # noqa: E402
from yandex_disk_backup.report import build_report, diff_manifests  # noqa: E402
from yandex_disk_backup.state import CHANGED, ERROR, PENDING, VERIFIED, StateStore  # noqa: E402


# --- поддельные ответы -----------------------------------------------------

class FakeResponse:
    """Минимальный аналог requests.Response для API-клиента."""

    def __init__(self, status_code: int = 200, payload: Any = None, headers: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    @property
    def text(self) -> str:
        return json.dumps(self._payload) if self._payload is not None else ""

    def json(self) -> Any:
        if self._payload is None:
            raise ValueError("empty")
        return self._payload


def _resource(name: str, path: str, *, type_: str = "file", size: int | None = None,
              md5: str | None = None, modified: str = "2026-01-02T03:04:05+00:00") -> dict:
    res: dict[str, Any] = {"name": name, "path": path, "type": type_, "modified": modified,
                           "created": modified}
    if type_ == "file":
        res["size"] = size
        res["mime_type"] = "image/jpeg"
        res["media_type"] = "image"
        if md5:
            res["md5"] = md5
    return res


class FakeDisk:
    """Поддельный REST API Диска: листинг папок, метаданные, ссылка на файл."""

    def __init__(self, tree: dict[str, list[dict]], contents: dict[str, bytes] | None = None) -> None:
        self.tree = tree
        self.contents = contents or {}
        self.requests = 0
        self.fail: dict[str, tuple[int, str]] = {}   # api_path -> (код, сообщение)

    def session(self):
        outer = self

        class _Session:
            def request(self, method, url, params=None, headers=None, timeout=None,
                        allow_redirects=None):
                outer.requests += 1
                path = (params or {}).get("path", "")
                if path in outer.fail:
                    code, message = outer.fail[path]
                    return FakeResponse(code, {"message": message})

                if url.endswith("/v1/disk"):
                    return FakeResponse(200, {"id": "d1", "type": "user",
                                              "total_space": 200 * 1024 ** 3,
                                              "used_space": 180 * 1024 ** 3,
                                              "free_space": 20 * 1024 ** 3})
                if url.endswith("/v1/disk/resources/download"):
                    body = outer.contents.get(path, b"payload")
                    return FakeResponse(200, {"href": f"https://cdn.local/get?path={path}",
                                              "method": "GET"})
                if url.endswith("/v1/disk/resources"):
                    return FakeResponse(200, outer._listing(path, params or {}))
                return FakeResponse(404, {"message": "unexpected url"})

        return _Session()

    def _listing(self, path: str, params: dict) -> dict:
        items = self.tree.get(path, [])
        limit = int(params.get("limit") or 100)
        offset = int(params.get("offset") or 0)
        page = items[offset:offset + limit]
        return {
            "name": path.rstrip("/").rsplit("/", 1)[-1] or "/",
            "path": path[5:] if path.startswith("disk:") else path,
            "type": "dir",
            "modified": "2026-01-01T00:00:00+00:00",
            "_embedded": {"items": page, "limit": limit, "offset": offset,
                          "sort": params.get("sort", "path"), "total": len(items)},
        }


def make_cdn(contents: dict[str, bytes], *, chunk: int = 16) -> Callable[..., Any]:
    """Возвращает подмену requests.request для временных ссылок."""

    def fetch(method, href, **kwargs):
        path = href.split("path=", 1)[1]
        body = contents.get(path, b"payload")
        return _CdnResponse(200, body, chunk, dict(kwargs.get("headers") or {}))

    return fetch


class _CdnResponse:
    def __init__(self, status: int, body: bytes, chunk: int, request_headers: dict) -> None:
        self.status_code = status
        self.headers = {"Content-Length": str(len(body)), "Accept-Ranges": "bytes"}
        self._body = body
        self._chunk = chunk
        self._req_headers = request_headers
        self.url = "https://cdn.local/get"

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def iter_content(self, chunk_size=None):
        step = self._chunk
        for i in range(0, len(self._body), step):
            yield self._body[i:i + step]


# --- проверки ---------------------------------------------------------------

_results: list[tuple[str, bool, str]] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    _results.append((name, bool(condition), detail))
    mark = "✓" if condition else "✗"
    print(f"  {mark} {name}" + (f" — {detail}" if detail and not condition else ""))


def section(title: str) -> None:
    print(f"\n{title}")
    print("-" * len(title))


def make_tree() -> tuple[dict[str, list[dict]], dict[str, bytes]]:
    """Дерево из 3 папок и 5 файлов, у части — разные контрольные суммы."""
    a = b"photo-one" * 3
    b = b"video-two" * 40
    c = b"doc-three"
    d = b"doc-four-longer"
    e = b"without-hash"

    def md5(data: bytes) -> str:
        return hashlib.md5(data).hexdigest()

    tree = {
        "disk:/": [
            _resource("Photos", "/Photos", type_="dir"),
            _resource("Videos", "/Videos", type_="dir"),
            _resource("Docs", "/Docs", type_="dir"),
            _resource("readme.txt", "/readme.txt", size=len(c), md5=md5(c)),
        ],
        "disk:/Photos": [
            _resource("2024", "/Photos/2024", type_="dir"),
            _resource("photo1.jpg", "/Photos/photo1.jpg", size=len(a), md5=md5(a)),
        ],
        "disk:/Photos/2024": [
            _resource("photo2.jpg", "/Photos/2024/photo2.jpg", size=len(b), md5=md5(b)),
        ],
        "disk:/Videos": [
            _resource("clip.mp4", "/Videos/clip.mp4", size=len(d), md5=md5(d)),
        ],
        "disk:/Docs": [
            _resource("notes.txt", "/Docs/notes.txt", size=len(e)),  # без хеша
        ],
    }
    contents = {
        "disk:/Photos/photo1.jpg": a,
        "disk:/Photos/2024/photo2.jpg": b,
        "disk:/Videos/clip.mp4": d,
        "disk:/Docs/notes.txt": e,
        "disk:/readme.txt": c,
    }
    return tree, contents


# --- тесты ------------------------------------------------------------------

def test_naming() -> None:
    section("Имена файлов и коллизии (Windows)")
    check("недопустимые символы заменены", sanitize_component('a<b>c:d"e/f\\g|h?i*j') == "a_b_c_d_e_f_g_h_i_j",
          sanitize_component('a<b>c:d"e/f\\g|h?i*j'))
    check("зарезервированное имя обезличено", sanitize_component("CON") != "CON")
    check("CON.txt обезличено", not sanitize_component("CON.txt").upper().startswith("CON"),
          sanitize_component("CON.txt"))
    check("хвостовые точки убраны", sanitize_component("file. .") == "file", sanitize_component("file. ."))
    check("пустое имя заменено", sanitize_component("") == "_")
    check("длинное имя обрезано с сохранением расширения",
          len(sanitize_component("x" * 400 + ".jpg")) <= 200)

    root = Path(tempfile.mkdtemp())
    try:
        alloc = PathAllocator(root, "keep")
        # Один и тот же облачный путь, но API-путь другой — как при переименовании.
        first = alloc.allocate("disk:/A/файл.txt", "/A/файл.txt")
        same = alloc.allocate("disk:/A/файл.txt", "/A/файл.txt")
        check("повторное назначение стабильно", same.local_relpath == first.local_relpath)

        # Два РАЗНЫХ облачных пути, сходящиеся к одному локальному имени.
        root2 = Path(tempfile.mkdtemp())
        try:
            alloc2 = PathAllocator(root2, "keep")
            alloc2.reserve(os.path.join("A", "файл.txt"), "disk:/A/файл.txt")
            second = alloc2.allocate("disk:/B/файл.txt", "/A/файл.txt")
            check("коллизия разрешена суффиксом", second.renamed, second.local_relpath)
            check("исходное имя сохранено за первым файлом",
                  alloc2.assigned("disk:/A/файл.txt") == os.path.join("A", "файл.txt"))
        finally:
            shutil.rmtree(root2, ignore_errors=True)

        # Существующий локальный файл, неизвестный реестру, не затирается.
        (root / "C").mkdir()
        (root / "C" / "занято.txt").write_text("x", encoding="utf-8")
        clash = alloc.allocate("disk:/C/занято.txt", "/C/занято.txt")
        check("существующий локальный файл не перезаписан молча",
              clash.local_relpath != os.path.join("C", "занято.txt") and clash.renamed,
              clash.local_relpath)
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_pagination_and_scan() -> None:
    section("Этап D/E: пагинация и рекурсивный обход")
    tree, _ = make_tree()
    fake = FakeDisk(tree)
    client = DiskClient("token-abcdefghijklmnop", session=fake.session(), min_request_interval=0)

    folder, items, total = list_folder(client, "disk:/", page_size=2, max_items=2)
    check("max_items ограничивает выборку", len(items) == 2, f"получено {len(items)}")
    check("total из _embedded прочитан", total == 4, f"total={total}")
    check("type ресурса определён", folder.type == "dir")

    # Маленький limit заставляет клиент пройти несколько страниц.
    fake.requests = 0
    folder, items, total = list_folder(client, "disk:/", page_size=1)
    check("все страницы обработаны", len(items) == 4, f"получено {len(items)}")
    check("сделано по одному запросу на страницу", fake.requests == 4, f"запросов {fake.requests}")

    scanner = InventoryScanner(client, root="disk:/", page_size=2)
    manifest = scanner.scan()
    check("обход завершён полностью", manifest.is_complete())
    check("найдено 5 файлов", manifest.stats.files == 5, f"{manifest.stats.files}")
    check("найдено 5 папок (включая корень)", manifest.stats.folders == 5, f"{manifest.stats.folders}")
    expected = sum(r.size for r in manifest.files)
    check("суммарный размер посчитан", manifest.stats.total_known_bytes == expected)
    check("md5 посчитаны для 4 файлов", manifest.stats.files_with_md5 == 4,
          f"{manifest.stats.files_with_md5}")
    check("файл без хеша найден", len(manifest.files_without_checksum()) == 1)
    paths = {f.path for f in manifest.files}
    check("вложенный путь сохранён полностью", "/Photos/2024/photo2.jpg" in paths)
    check("api_path пригоден для запросов",
          all(f.api_path.startswith("disk:/") for f in manifest.files))
    check("папки отличины от файлов",
          len(manifest.folders) == 5 and all(d.is_dir for d in manifest.folders))
    check("запросы к API посчитаны", manifest.stats.api_requests >= 5, f"{manifest.stats.api_requests}")


def test_scan_with_error() -> None:
    section("Ошибка обхода: манифест не должен врать")
    tree, _ = make_tree()
    fake = FakeDisk(tree)
    fake.fail["disk:/Videos"] = (403, "Access denied")
    client = DiskClient("token-abcdefghijklmnop", session=fake.session(), min_request_interval=0)

    manifest = InventoryScanner(client, root="disk:/", page_size=10).scan()
    check("статус — incomplete", manifest.status == "incomplete", manifest.status)
    check("is_complete() == False", not manifest.is_complete())
    check("ошибка записана", len(manifest.errors) == 1, f"{len(manifest.errors)}")
    check("причина указана", "403" in manifest.errors[0].reason, manifest.errors[0].reason)
    check("тип ошибки определён", manifest.errors[0].kind == "permission", manifest.errors[0].kind)
    check("остальные файлы всё же собраны", manifest.stats.files == 4, f"{manifest.stats.files}")


def test_manifest_atomic() -> None:
    section("Манифест: атомарная запись")
    root = Path(tempfile.mkdtemp())
    try:
        m = Manifest(status="incomplete", scan_root="disk:/")
        m.stats.files = 3
        path = root / "manifest.json"
        m.save(path)
        check("манифест создан", path.exists())
        check("временных файлов не осталось", not list(root.glob("*.tmp")),
              str(list(root.glob("*.tmp"))))
        again = Manifest.load(path)
        check("манифест читается обратно", again.stats.files == 3)

        # Повреждённый файл должен давать понятную ошибку, а не исключение JSON.
        path.write_text("{не json", encoding="utf-8")
        try:
            Manifest.load(path)
            check("повреждённый манифест даёт понятную ошибку", False)
        except Exception as exc:
            check("повреждённый манифест даёт понятную ошибку", "повреждён" in str(exc).lower(), str(exc))
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_state_store() -> None:
    section("Реестр состояний: возобновляемость")
    root = Path(tempfile.mkdtemp())
    try:
        store = StateStore(root / "state.db")
        _, contents = make_tree()
        manifest = _manifest_for(contents)

        summary = store.sync_with_manifest(manifest)
        check("все файлы зарегистрированы", summary["new"] == 5, f"{summary}")
        check("все в очереди", store.count_by_status().get(PENDING) == 5)

        queue = store.work_queue()
        first = queue[0]
        store.mark_downloading(first.api_path, "local/x.bin")
        store.mark_progress(first.api_path, 500)
        check("состояние downloading записано", store.get(first.api_path).status == "downloading")

        reset = store.reset_interrupted()
        check("прерванные файлы вернулись в очередь", reset == 1, f"{reset}")
        check("bytes_downloaded обнулён", store.get(first.api_path).bytes_downloaded == 0)

        store.mark_verified(first.api_path, "local/x.bin", 123)
        check("проверенный файл не попадает в очередь",
              first.api_path not in {r.api_path for r in store.work_queue()})

        # Файл удалили с диска — сверка должна это заметить.
        local = root.parent / "fake_backup"
        local.mkdir(exist_ok=True)
        (local / "x.bin").write_bytes(b"1234567890123")
        result = store.verify_local_state(local)
        check("сверка заметила пропавший файл", result["missing"] == 1, f"{result}")

        # Возвращаем файл и подтверждаем заново, чтобы проверить отслеживание
        # изменений в облаке (это касается только файлов в статусе verified).
        store.mark_verified(first.api_path, "x.bin", 13)
        manifest2 = _manifest_for(contents)
        for f in manifest2.files:
            if f.api_path == first.api_path:
                f.size = (f.size or 0) + 1
        s2 = store.sync_with_manifest(manifest2)
        check("изменение в облаке замечено", s2["changed"] == 1, f"{s2}")
        check("состояние changed выставлено", store.get(first.api_path).status == CHANGED)
        store.close()
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _manifest_for(contents: dict[str, bytes]) -> Manifest:
    tree, _ = make_tree()
    fake = FakeDisk(tree)
    client = DiskClient("token-abcdefghijklmnop", session=fake.session(), min_request_interval=0)
    return InventoryScanner(client, root="disk:/", page_size=10).scan()


def test_downloader() -> None:
    section("Загрузка: временный файл, проверка, безопасное перемещение")
    root = Path(tempfile.mkdtemp())
    backup = root / "backup"
    backup.mkdir()
    try:
        _, contents = make_tree()
        manifest = _manifest_for(contents)
        store = StateStore(root / "state.db")
        store.sync_with_manifest(manifest)

        cfg = Config(client_id="1", backup_root=str(backup), concurrency=2,
                     free_space_margin=0, max_attempts_per_file=2)
        clients: list[DiskClient] = []
        factory = lambda: clients.append(DiskClient("token-abcdefghijklmnop",
                                                   session=FakeDisk(make_tree()[0]).session(),
                                                   min_request_interval=0)) or clients[-1]
        downloader = Downloader(cfg, store, factory, http_get=make_cdn(contents))
        records = store.work_queue()
        outcomes = downloader.download_all(records)

        check("все файлы обработаны", len(outcomes) == 5, f"{len(outcomes)}")
        check("все в статусе verified", store.count_by_status().get(VERIFIED) == 5,
              f"{store.count_by_status()}")
        check("ошибок нет", downloader.stats.files_failed == 0, f"{downloader.stats.files_failed}")

        # Файлы на диске совпадают с исходными байтами.
        by_api = {o.api_path: o for o in outcomes}
        ok = True
        for api_path, body in contents.items():
            rel = by_api[api_path].local_path
            if (backup / rel).read_bytes() != body:
                ok = False
        check("байты сохранены без изменений", ok)

        check("структура папок сохранена", (backup / "Photos" / "2024" / "photo2.jpg").exists())
        check("временных .part не осталось", not list(backup.rglob("*.part")),
              str(list(backup.rglob("*.part"))))
        check("временных файлов хешей не осталось", not list(backup.rglob("*.md5")))

        # Повторный запуск: ничего не перекачивает.
        before = sum(1 for _ in backup.rglob("*") if _.is_file())
        again = Downloader(cfg, store, factory, http_get=make_cdn(contents))
        outcomes2 = again.download_all(store.work_queue())
        check("повторный запуск не качает заново", all(o.skipped for o in outcomes2),
              f"скачано: {[o.api_path for o in outcomes2 if not o.skipped]}")
        check("файлы на месте", sum(1 for _ in backup.rglob("*") if _.is_file()) == before)
        store.close()
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_download_integrity() -> None:
    section("Загрузка: битые данные и истёкшая ссылка не проходят незаметно")
    root = Path(tempfile.mkdtemp())
    backup = root / "backup"
    backup.mkdir()
    try:
        tree, contents = make_tree()
        manifest = _manifest_for(contents)
        store = StateStore(root / "state.db")
        store.sync_with_manifest(manifest)
        cfg = Config(client_id="1", backup_root=str(backup), concurrency=1,
                     free_space_margin=0, max_attempts_per_file=1)

        # 1. CDN отдаёт испорченные байты при верном размере.
        corrupt = dict(contents)
        target = "disk:/Photos/photo1.jpg"
        original = corrupt[target]
        corrupt[target] = b"X" * len(original)

        factory = lambda: DiskClient("t-abcdefghijklmnopqrst", session=FakeDisk(tree).session(),
                                     min_request_interval=0)
        dl = Downloader(cfg, store, factory, http_get=make_cdn(corrupt))
        outcomes = dl.download_all(store.work_queue(), )
        failed = [o for o in outcomes if o.status == ERROR]
        check("испорченный файл НЕ помечен как verified", len(failed) == 1, f"{failed}")
        check("причина — несовпадение md5", "md5" in (failed[0].note if failed else ""),
              failed[0].note if failed else "")
        check("на диске не осталось битого файла",
              not (backup / "Photos" / "photo1.jpg").exists())
        check("ошибка записана в реестр",
              store.get(target).status == ERROR, store.get(target).status)
        store.close()
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_conflict_strategy() -> None:
    section("Коллизия на локальном диске: чужой файл не затирается")
    root = Path(tempfile.mkdtemp())
    backup = root / "backup"
    (backup / "Photos").mkdir(parents=True)
    try:
        tree, contents = make_tree()
        manifest = _manifest_for(contents)
        store = StateStore(root / "state.db")
        store.sync_with_manifest(manifest)
        # Кладём «пользовательский» файл на место будущего.
        (backup / "Photos" / "photo1.jpg").write_bytes("МОЙ ЛИЧНЫЙ ФАЙЛ".encode())

        cfg = Config(client_id="1", backup_root=str(backup), concurrency=1,
                     free_space_margin=0, conflict_strategy="keep")
        factory = lambda: DiskClient("t-abcdefghijklmnopqrst", session=FakeDisk(tree).session(),
                                     min_request_interval=0)
        dl = Downloader(cfg, store, factory, http_get=make_cdn(contents))
        outcomes = dl.download_all(store.work_queue())
        kept = (backup / "Photos" / "photo1.jpg").read_bytes()
        check("существующий файл не перезаписан", kept == "МОЙ ЛИЧНЫЙ ФАЙЛ".encode(), str(kept[:20]))
        check("файл не удалён с диска", (backup / "Photos" / "photo1.jpg").exists())

        conflict = [o for o in outcomes if o.api_path == "disk:/Photos/photo1.jpg"][0]
        check("облачный файл сохранён под другим именем",
              conflict.status == VERIFIED and conflict.local_path != "Photos/photo1.jpg",
              conflict.local_path)
        check("причина переименования сообщена пользователю",
              bool(conflict.note), conflict.note)
        check("оригинал и копия оба на диске",
              (backup / conflict.local_path).exists() and (backup / "Photos" / "photo1.jpg").exists())
        store.close()
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_report_verdicts() -> None:
    section("Отчёт: вердикт о полноте копии")
    tree, contents = make_tree()
    manifest = _manifest_for(contents)

    empty_store = StateStore(Path(tempfile.mkdtemp()) / "s.db")
    rep = build_report(manifest, None, backup_root=Path("/tmp"), tool_version="test")
    check("без загрузки копия не «полностью проверена»", not rep.complete, rep.status_text)

    root = Path(tempfile.mkdtemp())
    backup = root / "backup"
    backup.mkdir()
    try:
        store = StateStore(root / "state.db")
        store.sync_with_manifest(manifest)
        factory = lambda: DiskClient("t-abcdefghijklmnopqrst", session=FakeDisk(tree).session(),
                                     min_request_interval=0)
        cfg = Config(client_id="1", backup_root=str(backup), concurrency=4, free_space_margin=0)
        Downloader(cfg, store, factory, http_get=make_cdn(contents)).download_all(store.work_queue())

        rep = build_report(manifest, store, backup_root=backup, tool_version="test")
        check("после полной загрузки — verified", rep.complete, rep.status_text)
        check("все файлы учтены", rep.files_verified == 5, f"{rep.files_verified}")
        check("объём совпадает", rep.bytes_verified == manifest.stats.total_known_bytes)
        check("файл без хеша отмечен", rep.files_without_checksum_count == 1,
              f"{rep.files_without_checksum_count}")

        # Ошибка загрузки → копия больше не «полная».
        store.mark_error("disk:/readme.txt", "диск отвалился", "network")
        rep2 = build_report(manifest, store, backup_root=backup, tool_version="test")
        check("ошибка загрузки ломает вердикт", not rep2.complete, rep2.status_text)

        # Неполный обход → тоже не «полная».
        manifest.status = "incomplete"
        store2 = StateStore(Path(tempfile.mkdtemp()) / "s2.db")
        store2.sync_with_manifest(manifest)
        rep3 = build_report(manifest, store2, backup_root=backup, tool_version="test")
        check("неполный обход ломает вердикт", not rep3.complete, rep3.status_text)
        store.close()
        store2.close()
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_diff() -> None:
    section("Сверка двух обходов")
    _, contents = make_tree()
    prev = _manifest_for(contents)

    cur = _manifest_for(contents)
    for f in cur.files:
        if f.path == "/readme.txt":
            f.path = "/readme-v2.txt"
            f.name = "readme-v2.txt"
    cur.files = [f for f in cur.files if f.path != "/Videos/clip.mp4"]
    for f in cur.files:
        if f.path == "/Photos/photo1.jpg":
            f.size = (f.size or 0) + 10
            f.modified = "2026-05-05T00:00:00+00:00"

    diff = diff_manifests(prev, cur)
    removed = {d["path"] for d in diff["removed"]}
    check("удалённый файл найден", removed == {"/Videos/clip.mp4", "/readme.txt"}, str(removed))
    check("новый файл найден", "/readme-v2.txt" in {d["path"] for d in diff["added"]})
    changed = {d["path"]: d["reasons"] for d in diff["changed"]}
    check("изменённый файл найден", "/Photos/photo1.jpg" in changed, str(changed))
    check("причина изменения указана", any("размер" in r for r in changed["/Photos/photo1.jpg"]))
    check("остальные отмечены без изменений", diff["unchanged"] == 2, f"{diff['unchanged']}")


def test_api_errors() -> None:
    section("Клиент API: классификация ошибок")
    class One:
        def __init__(self, code, payload=None, headers=None):
            self.code, self.payload, self.headers = code, payload, headers or {}
            self.calls = 0
        def request(self, *a, **k):
            self.calls += 1
            return FakeResponse(self.code, self.payload or {"message": "e"}, self.headers)

    slept: list[float] = []
    def mk(code, payload=None, headers=None, retries=3):
        s = One(code, payload, headers)
        c = DiskClient("t-abcdefghijklmnopqrst", session=s, min_request_interval=0,
                       max_retries=retries, sleep=slept.append)
        return c, s

    c, s = mk(401)
    try:
        c.get_disk_info()
        check("401 → понятная ошибка", False)
    except Exception as exc:
        check("401 → понятная ошибка", "повторная авторизация" in str(exc).lower(), str(exc))
    check("401 не повторяется", s.calls == 1, f"{s.calls}")

    c, s = mk(404, {"message": "Not found"})
    try:
        c.get_disk_info()
        check("404 → ResourceNotFound", False)
    except Exception as exc:
        check("404 → ResourceNotFound", type(exc).__name__ == "ResourceNotFoundError", type(exc).__name__)

    slept.clear()
    c, s = mk(429, {"message": "slow down"}, {"Retry-After": "7"})
    try:
        c.get_disk_info()
        check("429 исчерпал попытки", False)
    except Exception as exc:
        check("429 исчерпал попытки", "429" in str(exc), str(exc))
    check("Retry-After уважен", all(x == 7.0 for x in slept) and len(slept) == 3, f"{slept}")

    # Заголовки лимита Яндекса.
    slept.clear()
    reset_at = int(time.time() + 42)
    c, s = mk(429, {"message": "limit"}, {"X-Rate-Limit-Degree-Remaining": "0",
                                           "X-Rate-Limit-Degree-Reset": str(reset_at)}, retries=1)
    try:
        c.get_disk_info()
    except Exception:
        pass
    check("учтён X-Rate-Limit-Degree-Reset", any(x > 30 for x in slept), f"{slept}")


def test_secrets_not_leaked() -> None:
    section("Секреты не утекают")
    from yandex_disk_backup.secrets_guard import SECRETS, redact, token_preview
    secret = "y0_AgAAAABlongsecrettoken0123456789ABCDEFGH"
    SECRETS.add(secret)
    check("токен маскируется в тексте", secret not in redact(f"Authorization: OAuth {secret}"))
    check("маска на месте", "REDACTED" in redact(f"токен={secret}"))
    preview = token_preview(secret)
    check("preview не содержит токен", secret not in preview and len(preview) < 40, preview)
    check("похожие длинные строки тоже маскируются",
          "REDACTED" in redact("value=abcdefghij0123456789ABCDEFGHIJKL"), redact("value=abcdefghij0123456789ABCDEFGHIJKL"))


def main() -> int:
    print("=" * 70)
    print(" Yandex Disk Backup — тесты без сети")
    print("=" * 70)
    for test in (
        test_naming,
        test_pagination_and_scan,
        test_scan_with_error,
        test_manifest_atomic,
        test_state_store,
        test_downloader,
        test_download_integrity,
        test_conflict_strategy,
        test_report_verdicts,
        test_diff,
        test_api_errors,
        test_secrets_not_leaked,
    ):
        test()

    passed = sum(1 for _, ok, _ in _results if ok)
    total = len(_results)
    print("\n" + "=" * 70)
    print(f" Пройдено {passed} из {total}")
    if passed != total:
        print("\n НЕ ПРОЙДЕНО:")
        for name, ok, detail in _results:
            if not ok:
                print(f"   ✗ {name}" + (f" — {detail}" if detail else ""))
    print("=" * 70)
    return 0 if passed == total else 1


if __name__ == "__main__":
    raise SystemExit(main())
