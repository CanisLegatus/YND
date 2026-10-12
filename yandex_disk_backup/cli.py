"""Консольный интерфейс.

Меню соответствует ТЗ и покрывает все этапы: подключение, сведения о диске,
инвентаризацию, сохранение манифеста, скачивание, проверку полноты и отчёт.
Пункты, которые не реализованы, помечены честно — лучше явная заглушка, чем
тихий «успешный» вывод.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable

from . import __version__
from .api import ApiError, AuthError, NetworkError
from .config import APP_TITLE, Config, ConfigError, CONFLICT_STRATEGIES, default_data_dir
from .manifest import Manifest, manifest_path
from .report import render_markdown
from .secrets_guard import install_logging
from .session import AuthRequired, AuthSession
from .state import StateStore
from .token_store import TokenStore
from .workflow import (
    StopFlag,
    run_download,
    run_full,
    run_report,
    run_scan,
    sync_state,
    _num,
)

log = logging.getLogger(__name__)

MENU = f"""
{APP_TITLE}

  [1] Авторизоваться (получить OAuth-токен)
  [2] Проверить OAuth
  [3] Сведения о диске
  [4] Обойти Диск и сохранить манифест
  [5] Показать статистику манифеста
  [6] Скачать файлы (по манифесту и реестру)
  [7] Полный цикл: обход + скачивание + отчёт
  [8] Отчёт о полноте резервной копии
  [9] Сравнить с предыдущим манифестом
  [A] Все дела (обход + скачивание + отчёт)
  [B] Пробный запуск загрузки (ничего не качать)
  [C] Проверить свободное место
  [D] Настройки и параметры
  [Q] Выйти и удалить локальный токен
  [0] Выход
"""


def _fmt_bytes(value: int | None) -> str:
    if value is None:
        return "неизвестно"
    size = float(value)
    for unit in ("Б", "КБ", "МБ", "ГБ", "ТБ"):
        if size < 1024 or unit == "ТБ":
            return f"{int(size)} {unit}" if unit == "Б" else f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} ТБ"


def _explain_unknown(value) -> str:
    if value is None or value == "":
        return "неизвестно (поле не вернул API)"
    return str(value)


def _as_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class ConsoleApp:
    def __init__(self, cfg: Config, data_dir: Path) -> None:
        self.cfg = cfg
        self.data_dir = data_dir
        self.store = TokenStore(data_dir)
        self.session = AuthSession(cfg, self.store)
        self.session.load()

    # --- состояние ------------------------------------------------------
    @property
    def state_store(self) -> StateStore:
        return StateStore(self.data_dir / "state.db")

    def _manifest(self) -> Manifest | None:
        path = manifest_path(self.data_dir)
        return Manifest.load(path) if path.exists() else None

    # --- главный цикл ----------------------------------------------------
    def run(self) -> int:
        self._banner()
        while True:
            print(MENU)
            choice = self._ask("Выберите пункт: ", default="0").upper()
            if choice in ("0", "Q", "ВЫХОД"):
                print("Выход. Локальный токен остаётся на диске — удалить: пункт Q.")
                return 0
            handler = HANDLERS.get(choice)
            if handler is None:
                print("  Нет такого пункта.")
                continue
            try:
                handler(self)
            except AuthRequired as exc:
                print(f"\n  ✗ Требуется авторизация: {exc}")
            except KeyboardInterrupt:
                print("\n  Прервано. Незавершённое можно продолжить — пункт 6 или A.")
            except Exception as exc:  # noqa: BLE001 - единая точка для ошибок
                log.debug("непредвиденная ошибка", exc_info=True)
                print(f"\n  ✗ {type(exc).__name__}: {exc}")

    def _banner(self) -> None:
        print("=" * 68)
        print(f" {APP_TITLE}  v{__version__}")
        print("=" * 68)
        print(f" Данные программы: {self.data_dir}")
        print(f" Резервная копия: {self.cfg.backup_dir}")
        reason = self.cfg.missing_reason()
        if reason:
            print(f" ⚠ Настройка не завершена: {reason}")
        for line in self.session.status_lines():
            print(" " + line)
        manifest = None
        try:
            manifest = self._manifest()
        except Exception:  # noqa: BLE001
            pass
        if manifest is not None:
            print(f" Манифест: {manifest.status}, файлов {_num(manifest.stats.files)}, "
                  f"папок {_num(manifest.stats.folders)}, "
                  f"объём {_fmt_bytes(manifest.stats.total_known_bytes)}")
        else:
            print(" Манифест: отсутствует — сначала пункт 4.")
        print("=" * 68)

    @staticmethod
    def _ask(prompt: str, default: str | None = None) -> str:
        try:
            value = input(f" {prompt}").strip()
        except EOFError:
            return default if default is not None else "0"
        return value or (default or "")

    def _confirm(self, prompt: str, default: bool = False) -> bool:
        answer = self._ask(f"{prompt} (y/N): ", default="n" if not default else "y")
        return answer.lower() in ("y", "yes", "д", "да")

    def _require_manifest(self) -> Manifest:
        manifest = self._manifest()
        if manifest is None:
            raise RuntimeError("манифест не найден. Сначала выполните пункт 4 (обход диска).")
        return manifest

    # --- 1. авторизация ---------------------------------------------------
    def action_authorize(self) -> None:
        reason = self.cfg.missing_reason()
        if reason:
            print(f"\n  ✗ Авторизация невозможна: {reason}")
            print("     Задайте client_id: python main.py --setup")
            return
        print("\n  Запускаю авторизацию…")
        token = self.session.authorize()
        print("\n  ✓ Токен получен и сохранён локально.")
        for line in self.session.status_lines():
            print("  " + line)
        log.info("авторизация завершена, scope=%s", token.scope)

    def action_check_oauth(self) -> None:
        print("\n  Проверяю OAuth…")
        client = self.session.client()  # сам обновит токен при необходимости
        info = _call(client.get_disk_info)
        print("  ✓ Авторизация работает, API отвечает.")
        print(f"    Объём диска: {_fmt_bytes(_as_int(info.get('total_space')))}")
        for line in self.session.status_lines():
            print("  " + line)

    def action_disk_info(self) -> None:
        client = self.session.client()
        info = _call(client.get_disk_info)
        size_keys = {"total_space", "used_space", "free_space", "trash_size", "photoslice_size"}
        print("\n  Сведения о диске (GET /v1/disk):")
        for title, key in (
            ("ID диска", "id"),
            ("Тип", "type"),
            ("Всего места", "total_space"),
            ("Занято", "used_space"),
            ("Свободно", "free_space"),
            ("В корзине", "trash_size"),
            ("Фото и видео", "photoslice_size"),
        ):
            raw = info.get(key)
            if raw is None or raw == "":
                value = "неизвестно (поле не вернул API)"
            elif key in size_keys:
                value = _fmt_bytes(_as_int(raw))
            else:
                value = str(raw)
            print(f"    {title + ':':<17}{value}")
        for title, key in (("Лимит на пакет", "max_items_download"),
                           ("Макс. загрузок", "max_items_upload")):
            print(f"    {title + ':':<17}{_explain_unknown(info.get(key))}")
        print("\n    Полный набор ключей ответа записан в журнал (без токена).")
        log.info("GET /v1/disk keys: %s", sorted(info.keys()))

    # --- 4. обход --------------------------------------------------------
    def action_scan(self) -> None:
        self._require_config()
        print(f"\n  Обход диска с корня {self.cfg.scan_root}…")
        print("  Ctrl+C — мягкая остановка, уже собранное сохранится в манифест.")
        print(f"  Пагинация: по {self.cfg.page_size} элементов; таймаут больших"
              f" каталогов: {int(getattr(self.cfg, 'listing_read_timeout', 300))} с.\n")
        stop = StopFlag()
        stop.install()
        try:
            manifest = run_scan(
                self.cfg, self.session, self.data_dir,
                stop=stop, on_progress=_scan_line(),
            )
        except KeyboardInterrupt:
            print("\n  ⏹ Обход прерван. Уже найденное сохранено как неполный манифест.")
            stored = manifest_path(self.data_dir)
            if stored.exists():
                try:
                    m = Manifest.load(stored)
                    print(f"    Файлов собрано: {_num(m.stats.files)}, "
                          f"папок: {_num(m.stats.folders)}")
                except Exception:  # noqa: BLE001
                    pass
            return
        except ApiError as exc:
            print(f"\n  ✗ Обход прерван ошибкой API: {exc}")
            print("  Частичный манифест сохранён (статус incomplete)." if
                  manifest_path(self.data_dir).exists() else "")
            return
        finally:
            stop.restore()
        print()
        _print_scan_summary(manifest)
        print(f"\n  Манифест сохранён: {manifest_path(self.data_dir)}")
        if not manifest.is_complete():
            print("  ⚠ Обход НЕполный — состояние записано как incomplete.")
            print("    Повторите пункт 4 после устранения причин (см. report.md).")

    # --- 5. статистика ----------------------------------------------------
    def action_stats(self) -> None:
        manifest = self._require_manifest()
        print(f"\n  Манифест: {manifest_path(self.data_dir)}")
        print(f"  Статус обхода: {'полный' if manifest.is_complete() else 'НЕПОЛНЫЙ'}")
        print(f"  Создан: {manifest.generated_at}")
        print(f"  Корень: {manifest.scan_root}")
        print()
        print(f"  Файлов:  {_num(manifest.stats.files)}")
        print(f"  Папок:   {_num(manifest.stats.folders)}")
        print(f"  Объём:   {_fmt_bytes(manifest.stats.total_known_bytes)}")
        print(f"  Файлов без известного размера: {_num(manifest.stats.files_unknown_size)}")
        print(f"  С md5:   {_num(manifest.stats.files_with_md5)}")
        print(f"  С sha256:{_num(manifest.stats.files_with_sha256)}")
        print(f"  Ошибок обхода: {_num(len(manifest.errors))}")
        print(f"  Обращений к API: {_num(manifest.stats.api_requests)}")

        if manifest.errors:
            print("\n  Пропущенные каталоги:")
            for err in manifest.errors[:20]:
                print(f"    ✗ [{err.kind}] {err.path} — {err.reason}")
            if len(manifest.errors) > 20:
                print(f"    … и ещё {_num(len(manifest.errors) - 20)}")

        largest = manifest.largest_files(10)
        if largest:
            print("\n  10 самых больших файлов:")
            for res in largest:
                print(f"    {_fmt_bytes(res.size):>12}  {res.path}")

        exts = manifest.top_extensions(8)
        if exts:
            print("\n  Расширения:")
            for ext, count in exts:
                print(f"    {_num(count):>8}  {ext}")

    # --- 6. скачивание ----------------------------------------------------
    def action_download(self) -> None:
        self._require_config()
        manifest = self._require_manifest()
        state = self.state_store
        summary = sync_state(state, manifest, self.cfg)
        print(f"\n  Реестр синхронизирован: новых {_num(summary['new'])}, "
              f"изменилось {_num(summary['changed'])}, без изменений {_num(summary['unchanged'])}")
        if summary["gone"]:
            print(f"  ⚠ {_num(summary['gone'])} файлов есть в реестре, но отсутствуют в манифесте.")
        _print_state_table(state)
        if not self._confirm("  Начать скачивание?", default=True):
            print("  Отменено.")
            return
        run_download(self.cfg, self.session, self.data_dir, manifest, state)
        print()
        print("  Готово. Итоговый отчёт — пункт 8 или сформируется при пункте A.")

    # --- 7/A. полный цикл --------------------------------------------------
    def action_full(self, dry_run: bool = False, do_scan: bool = True) -> None:
        self._require_config()
        label = "ПРОБНЫЙ ЗАПУСК (файлы не качаются)" if dry_run else "ПОЛНЫЙ ЦИКЛ"
        print(f"\n  === {label} ===")
        print("  Шаг 1. Обход диска и сохранение манифеста")
        if do_scan and not dry_run and not self._confirm(
            "  Обойти диск заново перед загрузкой?", default=True
        ):
            print("  Пропускаю обход, беру существующий манифест.")
            do_scan = False
        report = run_full(self.cfg, self.session, self.data_dir,
                          dry_run=dry_run, do_scan=do_scan)
        print()
        _print_report_summary(report)
        print(f"\n  Отчёт: {self.data_dir / 'report.md'}")
        print(f"         {self.data_dir / 'report.json'}")

    # --- 8. отчёт ----------------------------------------------------------
    def action_report(self) -> None:
        manifest = self._require_manifest()
        state = self.state_store
        # Без синхронизации реестр может быть пустым или расходиться с
        # манифестом — и отчёт тогда покажет неправду.
        sync_state(state, manifest, self.cfg)
        previous = self._previous_manifest()
        report = run_report(self.cfg, self.data_dir, manifest, state, previous)
        _print_report_summary(report)
        print()
        print(render_markdown(report))

    def _previous_manifest(self) -> Manifest | None:
        backup = self.data_dir / "manifest.prev.json"
        if not backup.exists():
            return None
        try:
            return Manifest.load(backup)
        except Exception:  # noqa: BLE001
            return None

    # --- 9. сверка ---------------------------------------------------------
    def action_diff(self) -> None:
        from .report import diff_manifests

        current = self._require_manifest()
        previous = self._previous_manifest()
        if previous is None:
            print("\n  Предыдущего манифеста нет. Сверка появится после следующего обхода:")
            print("  текущий манифест будет сохранён как manifest.prev.json.")
            _archive_manifest(self.data_dir, current)
            return
        diff = diff_manifests(previous, current)
        print(f"\n  Сверка манифестов ({previous.generated_at} → {current.generated_at}):")
        print(f"    Без изменений:  {_num(diff['unchanged'])}")
        print(f"    Добавлено:      {_num(len(diff['added']))}")
        print(f"    Удалено:        {_num(len(diff['removed']))}")
        print(f"    Изменено:       {_num(len(diff['changed']))}")
        for item in diff["changed"][:20]:
            print(f"      ~ {item['path']} — {'; '.join(item['reasons'])}")
        for item in diff["removed"][:20]:
            print(f"      - {item['path']}")
        for item in diff["added"][:20]:
            print(f"      + {item['path']}")
        _archive_manifest(self.data_dir, current)

    # --- C. место -----------------------------------------------------------
    def action_free_space(self) -> None:
        import shutil
        from .downloader import free_space

        manifest = None
        try:
            manifest = self._manifest()
        except Exception:  # noqa: BLE001
            pass
        target = self.cfg.backup_dir
        target.mkdir(parents=True, exist_ok=True)
        usage = shutil.disk_usage(target)
        print(f"\n  Каталог: {target}")
        print(f"  Всего:   {_fmt_bytes(usage.total)}")
        print(f"  Занято:  {_fmt_bytes(usage.used)}")
        print(f"  Свободно:{_fmt_bytes(usage.free)}")
        if manifest is not None:
            need = manifest.stats.total_known_bytes
            print(f"  Нужно под манифест: {_fmt_bytes(need)}")
            if usage.free < need:
                print("  ⚠ Места не хватит на полный объём манифеста.")
            elif usage.free < need + self.cfg.free_space_margin:
                print("  ⚠ Места хватит без учёта запаса — загрузка не начнётся.")
            else:
                print("  ✓ Места достаточно с учётом запаса.")

    # --- D. настройки --------------------------------------------------------
    def action_settings(self) -> None:
        cfg = self.cfg
        print("\n  Текущие параметры:")
        for key, value in cfg.to_public_dict().items():
            print(f"    {key + ':':<24}{value}")
        print(f"    {'preserve_times:':<24}{cfg.preserve_times}")
        print(f"    {'max_attempts_per_file:':<24}{cfg.max_attempts_per_file}")
        print(f"    {'include_trash:':<24}{cfg.include_trash}")
        print(f"    {'media_type:':<24}{cfg.media_type or '(все типы)'}")
        print("\n  Изменить: 1 — каталог копии, 2 — параллельные загрузки, "
              "3 — стратегия конфликтов, 4 — докачка через Range, 0 — назад")
        answer = self._ask("Что меняем?", default="0")
        if answer == "1":
            cfg.backup_root = self._ask(f"  Каталог копии [{cfg.backup_dir}]: ") or cfg.backup_root
        elif answer == "2":
            value = self._ask(f"  Параллельных загрузок [{cfg.concurrency}]: ")
            cfg.concurrency = max(1, int(value or cfg.concurrency))
        elif answer == "3":
            print(f"    варианты: {', '.join(CONFLICT_STRATEGIES)}")
            value = self._ask(f"  Стратегия [{cfg.conflict_strategy}]: ")
            if value:
                if value not in CONFLICT_STRATEGIES:
                    print("  ✗ недопустимое значение")
                    return
                cfg.conflict_strategy = value
        elif answer == "4":
            cfg.allow_range_resume = not cfg.allow_range_resume
            print(f"    докачка через Range: {'включена' if cfg.allow_range_resume else 'выключена'}")
        else:
            return
        cfg.save()
        print(f"  ✓ Сохранено: {self.cfg.path}")

    # --- Q. выход с отзывом ---------------------------------------------------
    def action_logout(self) -> None:
        if not self._confirm("Отозвать доступ у Яндекса и удалить локальный токен?"):
            print("  Отменено.")
            return
        revoked = self.session.logout(revoke_remote=True)
        print("  ✓ Локальный токен удалён.")
        print("  ✓ Доступ отозван на стороне Яндекса." if revoked
              else "  ⚠ Не удалось отозвать доступ у Яндекса (возможно, токен уже недействителен).")

    # --- служебное -----------------------------------------------------------
    def _require_config(self) -> None:
        reason = self.cfg.missing_reason()
        if reason:
            raise RuntimeError(f"Настройка не завершена: {reason}. Выполните: python main.py --setup")


#: Соответствие пункта меню действию. Клавичи в верхнем регистре.
HANDLERS: dict[str, Callable[[ConsoleApp], None]] = {
    "1": ConsoleApp.action_authorize,
    "2": ConsoleApp.action_check_oauth,
    "3": ConsoleApp.action_disk_info,
    "4": ConsoleApp.action_scan,
    "5": ConsoleApp.action_stats,
    "6": ConsoleApp.action_download,
    "7": lambda app: app.action_full(dry_run=False, do_scan=True),
    "8": ConsoleApp.action_report,
    "9": ConsoleApp.action_diff,
    "A": lambda app: app.action_full(dry_run=False, do_scan=True),
    "B": lambda app: app.action_full(dry_run=True, do_scan=True),
    "C": ConsoleApp.action_free_space,
    "D": ConsoleApp.action_settings,
    "Q": ConsoleApp.action_logout,
}


def _archive_manifest(data_dir: Path, manifest: Manifest) -> None:
    """Сохраняет текущий манифест как «предыдущий» для следующей сверки."""
    try:
        manifest.save(data_dir / "manifest.prev.json")
    except OSError as exc:
        log.warning("не удалось сохранить предыдущий манифест: %s", exc)


def _scan_line():
    from .workflow import _scan_printer
    return _scan_printer()


def _print_scan_summary(manifest: Manifest) -> None:
    print(f"  Файлов:  {_num(manifest.stats.files)}")
    print(f"  Папок:   {_num(manifest.stats.folders)}")
    print(f"  Объём:   {_fmt_bytes(manifest.stats.total_known_bytes)}")
    print(f"  С md5:   {_num(manifest.stats.files_with_md5)}")
    print(f"  С sha256:{_num(manifest.stats.files_with_sha256)}")
    print(f"  Ошибок:  {_num(len(manifest.errors))}")
    print(f"  Статус:  {'полный обход' if manifest.is_complete() else 'НЕПОЛНЫЙ'}")


def _print_state_table(state: StateStore) -> None:
    counts = state.count_by_status()
    if not counts:
        print("  Реестр пуст.")
        return
    labels = {
        "verified": "проверено", "pending": "ожидает", "downloading": "в работе",
        "downloaded": "скачано, не проверено", "error": "ошибка",
        "recheck": "требует проверки", "changed": "изменилось в облаке",
    }
    print("  Состояние файлов:")
    for status, count in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"    {labels.get(status, status):<24}{_num(count)}")


def _print_report_summary(report) -> None:
    print()
    print("=" * 68)
    print(f"  СТАТУС: {report.status_text}")
    print("=" * 68)
    print(f"  Обнаружено файлов:   {_num(report.files_found)}")
    print(f"  Проверено и сохранено: {_num(report.files_verified)}")
    print(f"  Ожидает:             {_num(report.files_pending)}")
    print(f"  Изменилось:          {_num(report.files_changed)}")
    print(f"  Ошибок загрузки:     {_num(report.files_failed)}")
    print(f"  Ошибок обхода:       {_num(len(report.scan_errors))}")
    print(f"  Ожидаемый объём:     {_fmt_bytes(report.expected_bytes)}")
    print(f"  Сохранено:           {_fmt_bytes(report.bytes_verified)}")
    print(f"  Без контрольной суммы: {_num(report.files_without_checksum_count)}")


class _Handled(Exception):
    """Ошибка уже показана пользователю; продолжаем работу."""


def _call(func, *args, **kwargs):
    """Выполняет запрос к API и переводит ошибки в понятный текст."""
    try:
        return func(*args, **kwargs)
    except AuthError as exc:
        raise AuthRequired(str(exc)) from exc
    except NetworkError as exc:
        print(f"\n  ✗ Нет соединения с интернетом: {exc}")
        print("    Проверьте подключение и повторите попытку.")
    except ApiError as exc:
        print(f"\n  ✗ Ошибка API: {exc}")
    raise _Handled()


# --- настройка -------------------------------------------------------------

def _setup(cfg: Config) -> int:
    """Интерактивное заполнение конфигурации. Секреты сюда не вводятся."""
    print(f"\nНастройка. Файл конфигурации: {cfg.path}\n")
    print("Приложение создаётся здесь: https://oauth.yandex.ru/client/new")
    print("  Платформы: «Веб-сервисы»")
    print("  Доступы: cloud_api:disk.read, cloud_api:disk.info (только чтение)")
    print("  Redirect URI: https://oauth.yandex.ru/verification_code (значение по умолчанию)\n")

    client_id = input(" ClientID приложения: ").strip()
    if not client_id:
        print(" ✗ ClientID обязателен.")
        return 2
    cfg.client_id = client_id

    print("\n Режим получения кода подтверждения:")
    print("   1 — вручную: код показывается на странице Яндекса, вы вводите его сюда (рекомендуется)")
    print("   2 — локальный сервер на 127.0.0.1 (нужно, чтобы этот Redirect URI был в настройках приложения)")
    answer = input(" Выберите [1]: ").strip() or "1"

    if answer == "2":
        cfg.auth_mode = "loopback"
        port = input(f" Порт локального сервера [{cfg.loopback_port}]: ").strip()
        cfg.loopback_port = int(port) if port else cfg.loopback_port
        cfg.redirect_uri = f"http://127.0.0.1:{cfg.loopback_port}/callback"
        print(f"   В настройках приложения должен быть ровно этот Redirect URI:\n     {cfg.redirect_uri}")
    else:
        cfg.auth_mode = "manual"
        cfg.redirect_uri = "https://oauth.yandex.ru/verification_code"

    print("\n Куда складывать резервную копию:")
    from .config import default_backup_root
    print(f"   По умолчанию: {cfg.backup_dir}")
    root = input(f" Каталог [{cfg.backup_dir}]: ").strip()
    cfg.backup_root = root or str(default_backup_root())

    path = cfg.save()
    print(f"\n ✓ Конфигурация сохранена: {path}")
    print("   Токен будет сохранён отдельно и не попадёт в этот файл.")
    return 0


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        prog="yandex-disk-backup",
        description=f"{APP_TITLE} v{__version__} — резервное копирование Яндекс Диска.",
    )
    parser.add_argument("--data-dir", type=Path, default=None, help="каталог конфига, токена, логов, манифеста")
    parser.add_argument("--verbose", action="store_true", help="подробный журнал (без токенов)")
    parser.add_argument("--setup", action="store_true", help="первоначальная настройка")
    parser.add_argument("--check", action="store_true", help="проверка OAuth и API, затем выход")
    parser.add_argument(
        "--backup", action="store_true",
        help="полный цикл без вопросов: обход + скачивание + отчёт",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="с планированием загрузки, но без скачивания файлов",
    )
    parser.add_argument("--only-download", action="store_true", help="скачать по существующему манифесту")
    args = parser.parse_args(argv)

    data_dir = Path(args.data_dir) if args.data_dir else default_data_dir()
    data_dir.mkdir(parents=True, exist_ok=True)
    install_logging(str(data_dir / "backup.log"), verbose=args.verbose)

    from .config import load_config

    try:
        cfg = load_config(data_dir / "config.json")
    except ConfigError as exc:
        print(f"✗ {exc}")
        return 2
    cfg.path = data_dir / "config.json"

    if args.setup:
        return _setup(cfg)

    app = ConsoleApp(cfg, data_dir)
    if args.check:
        return _run_check(app)
    if args.backup or args.dry_run or args.only_download:
        try:
            app.action_full(dry_run=args.dry_run, do_scan=not args.only_download)
        except AuthRequired as exc:
            print(f"\n✗ {exc}")
            return 1
        return 0
    try:
        return app.run()
    except _Handled:
        return 1


def _run_check(app: ConsoleApp) -> int:
    """Неинтерактивная проверка для критериев приёмки."""
    print(f"{APP_TITLE} v{__version__} — проверка\n")
    for line in app.session.status_lines():
        print("  " + line)
    try:
        client = app.session.client()
        info = _call(client.get_disk_info)
    except AuthRequired as exc:
        print(f"\n  ✗ {exc}")
        return 1
    except _Handled:
        return 1
    print("\n  ✓ GET /v1/disk — успешно")
    print(f"    Всего:   {_fmt_bytes(_as_int(info.get('total_space')))}")
    print(f"    Занято:  {_fmt_bytes(_as_int(info.get('used_space')))}")
    print(f"    Свободно: {_fmt_bytes(_as_int(info.get('free_space')))}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
