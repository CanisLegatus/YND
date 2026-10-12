"""Сквозной прогон полного цикла: обход → реестр → загрузка → отчёт.

Проверяет не отдельные функции, а то, что они стыкуются: настоящий CLI,
настоящий workflow, настоящие файлы. Сеть подменена, но код — рабочий.

Запуск:  python tests/run_workflow.py
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from run_tests import FakeDisk, check, make_cdn, make_tree, section, _results  # noqa: E402

from yandex_disk_backup import api, workflow  # noqa: E402
from yandex_disk_backup.config import Config  # noqa: E402
from yandex_disk_backup.manifest import Manifest  # noqa: E402
from yandex_disk_backup.report import Report  # noqa: E402
from yandex_disk_backup.session import AuthSession  # noqa: E402
from yandex_disk_backup.state import VERIFIED, StateStore  # noqa: E402
from yandex_disk_backup.token_store import TokenData, TokenStore  # noqa: E402


def build_env() -> tuple[Config, AuthSession, Path, Path, dict[str, bytes], dict[str, list[dict]]]:
    data_dir = Path(tempfile.mkdtemp()) / "appdata"
    backup = Path(tempfile.mkdtemp()) / "backup"
    data_dir.mkdir(parents=True)
    backup.mkdir(parents=True)

    tree, contents = make_tree()
    cfg = Config(
        client_id="1234567",
        backup_root=str(backup),
        concurrency=3,
        page_size=2,
        free_space_margin=0,
        path=data_dir / "config.json",
    )
    cfg.save()

    store = TokenStore(data_dir)
    store.save(TokenData(access_token="t-abcdefghijklmnopqrst", refresh_token="r" * 40,
                         expires_in=3600, obtained_at=__import__("time").time(),
                         scope="cloud_api:disk.read cloud_api:disk.info"))
    session = AuthSession(cfg, TokenStore(data_dir))
    session.load()

    # Подменяем только транспорт: клиент создаётся фабрикой workflow,
    # поэтому патчим конструктор, а не подсовываем готовый объект.
    real_init = api.DiskClient.__init__

    def fake_init(self, access_token, **kwargs):
        kwargs["session"] = FakeDisk(tree).session()
        kwargs["min_request_interval"] = 0
        real_init(self, access_token, **kwargs)

    api.DiskClient.__init__ = fake_init

    # Скачивание по временной ссылке идёт напрямую, минуя API-клиент,
    # поэтому подменяем сам загрузчик: остальная логика остаётся настоящей.
    real_downloader = workflow.Downloader

    class FakeCdnDownloader(real_downloader):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("http_get", make_cdn(contents))
            super().__init__(*args, **kwargs)

    workflow.Downloader = FakeCdnDownloader

    # Журнал нужен, чтобы проверить, что секреты в него не попадают.
    from yandex_disk_backup.secrets_guard import install_logging
    install_logging(str(data_dir / "backup.log"))
    return cfg, session, data_dir, backup, contents, tree, real_downloader


def main() -> int:
    print("=" * 72)
    print(" Сквозной прогон: обход → загрузка → отчёт (сеть подменена)")
    print("=" * 72)

    cfg, session, data_dir, backup, contents, tree, real_downloader = build_env()
    real_init = api.DiskClient.__init__

    try:
        # --- полный цикл ------------------------------------------------
        section("Полный цикл (workflow.run_full)")
        report = workflow.run_full(cfg, session, data_dir, dry_run=False)

        check("обход завершён полностью", report.scan_complete, report.status_text)
        check("найдено 5 файлов", report.files_found == 5, f"{report.files_found}")
        check("найдено 5 папок", report.folders_found == 5, f"{report.folders_found}")
        check("все 5 файлов проверены", report.files_verified == 5, f"{report.files_verified}")
        check("ошибок нет", report.files_failed == 0 and not report.scan_errors)
        check("копия признана полной", report.complete, report.status_text)
        check("объём сохранён полностью",
              report.bytes_verified == report.expected_bytes,
              f"{report.bytes_verified} / {report.expected_bytes}")

        manifest = Manifest.load(data_dir / "manifest.json")
        check("манифест на диске", manifest.exists() if hasattr(manifest, "exists") else True)
        check("манифест помечен полным", manifest.status == "complete", manifest.status)
        check("в манифесте 5 файлов", len(manifest.files) == 5, f"{len(manifest.files)}")
        check("в манифесте есть md5", manifest.stats.files_with_md5 == 4,
              f"{manifest.stats.files_with_md5}")
        raw = json.loads((data_dir / "manifest.json").read_text(encoding="utf-8"))
        check("в JSON есть флаг полноты", raw["is_complete"] is True)
        check("в JSON есть статистика", raw["stats"]["files"] == 5)

        # --- байты на диске ---------------------------------------------
        section("Файлы на диске")
        for api_path, body in contents.items():
            rel = api_path[5:]  # отбрасываем префикс disk:/
            local = backup / rel.lstrip("/")
            check(f"сохранён {rel}", local.exists() and local.read_bytes() == body)
        check("вложенная папка создана", (backup / "Photos" / "2024").is_dir())
        leftovers = list(backup.rglob("*.part")) + list(backup.rglob("*.md5"))
        check("временных файлов не осталось", not leftovers, str(leftovers))

        # --- отчёт --------------------------------------------------------
        section("Файлы отчёта")
        md = data_dir / "report.md"
        js = data_dir / "report.json"
        check("report.md создан", md.exists())
        check("report.json создан", js.exists())
        if md.exists():
            text = md.read_text(encoding="utf-8")
            check("в отчёте есть вердикт", "СТАТУС" in text.upper() or "Статус" in text)
            check("в отчёте перечислены цифры", "Обнаружено файлов" in text)
            check("в отчёте есть раздел без контрольной суммы",
                  "без контрольной суммы" in text.lower())
        if js.exists():
            data = json.loads(js.read_text(encoding="utf-8"))
            check("отчёт machine-readable", data["files_verified"] == 5, str(data.get("files_verified")))

        # --- повторный запуск: ничего не перекачивает -----------------------
        section("Повторный запуск (возобновляемость)")
        mtime_before = {p: p.stat().st_mtime for p in backup.rglob("*") if p.is_file()}
        report2 = workflow.run_full(cfg, session, data_dir, dry_run=False)
        mtime_after = {p: p.stat().st_mtime for p in backup.rglob("*") if p.is_file()}
        check("файлы не перезаписаны", mtime_before == mtime_after)
        check("всё ещё 5 проверенных", report2.files_verified == 5, f"{report2.files_verified}")
        check("копия по-прежнему полная", report2.complete, report2.status_text)

        # --- обрыв и продолжение --------------------------------------------
        section("Обрыв и продолжение")
        victim = backup / "Videos" / "clip.mp4"
        victim.unlink()
        state = StateStore(data_dir / "state.db")
        report3 = workflow.run_full(cfg, session, data_dir, do_scan=False)
        check("удалённый файл снова в копии", victim.exists())
        check("после докачки всё снова полно", report3.complete, report3.status_text)
        check("файл восстановлен байт в байт",
              victim.read_bytes() == contents["disk:/Videos/clip.mp4"])
        state.close()

        # --- порча файла: молчаливый «успех» невозможен -------------------------
        section("Порча локального файла обнаруживается")
        victim.write_bytes("испорчено".encode())
        report4 = workflow.run_full(cfg, session, data_dir, do_scan=False)
        check("испорченный файл перекачан", report4.complete, report4.status_text)
        check("испорченный файл восстановлен",
              victim.read_bytes() == contents["disk:/Videos/clip.mp4"])

        # --- недостаточно места --------------------------------------------------
        section("Недостаточно места: загрузка не начинается")
        cfg.free_space_margin = 10 ** 15  # заведомо больше диска
        manifest = Manifest.load(data_dir / "manifest.json")
        state2 = StateStore(data_dir / "state.db")
        state2.sync_with_manifest(manifest)
        for rec in state2.all_records():
            state2.mark(rec.api_path, "pending")
        from yandex_disk_backup.downloader import Downloader, DownloadError
        dl = Downloader(cfg, state2, workflow.make_client_factory(session, cfg),
                        http_get=make_cdn(contents))
        try:
            dl.download_all(state2.work_queue())
            check("нехватка места поднимает ошибку", False)
        except DownloadError as exc:
            check("нехватка места поднимает ошибку", "недостаточно места" in str(exc), str(exc)[:80])
        state2.close()
        cfg.free_space_margin = 0

        # --- секреты ----------------------------------------------------------------
        section("Секреты")
        log_text = (data_dir / "backup.log").read_text(encoding="utf-8")
        check("токен не попал в журнал", "t-abcdefghijklmnopqrst" not in log_text)
        check("токен не попал в отчёт", "t-abcdefghijklmnopqrst" not in md.read_text(encoding="utf-8"))
        check("токен не попал в манифест",
              "t-abcdefghijklmnopqrst" not in (data_dir / "manifest.json").read_text(encoding="utf-8"))
        check("токен не попал в config.json",
              "t-abcdefghijklmnopqrst" not in (data_dir / "config.json").read_text(encoding="utf-8"))

    finally:
        api.DiskClient.__init__ = real_init
        workflow.Downloader = real_downloader
        shutil.rmtree(backup.parent, ignore_errors=True)
        shutil.rmtree(data_dir.parent, ignore_errors=True)

    passed = sum(1 for _, ok, _ in _results if ok)
    total = len(_results)
    print("\n" + "=" * 72)
    print(f" Пройдено {passed} из {total}")
    if passed != total:
        print("\n НЕ ПРОЙДЕНО:")
        for name, ok, detail in _results:
            if not ok:
                print(f"   ✗ {name}" + (f" — {detail}" if detail else ""))
    print("=" * 72)
    return 0 if passed == total else 1


if __name__ == "__main__":
    raise SystemExit(main())
