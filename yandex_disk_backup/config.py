"""Конфигурация приложения.

Конфиг НЕ содержит секретов: только идентификатор приложения и параметры
работы. OAuth-токен хранится отдельно, в закрытом файле (см. token_store).
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

APP_TITLE = "Yandex Disk Backup"
APP_SLUG = "yandex-disk-backup"

# --- Эндпоинты Яндекс OAuth (актуально на 2026-10, см. docs/STAGE_A.md) ---
OAUTH_AUTHORIZE_URL = "https://oauth.yandex.ru/authorize"
OAUTH_TOKEN_URL = "https://oauth.yandex.ru/token"
OAUTH_REVOKE_URL = "https://oauth.yandex.ru/revoke_token"

# --- Эндпоинты REST API Яндекс Диска ---
API_BASE_URL = "https://cloud-api.yandex.net/v1"

# Минимально необходимые права: чтение содержимого Диска + сведения о нём.
# Права на запись и удаление намеренно НЕ запрашиваются.
DEFAULT_SCOPES = ["cloud_api:disk.read", "cloud_api:disk.info"]

# Redirect URI, который Яндекс предлагает «из коробки» для отладочного
# сценария: код подтверждения показывается пользователю на странице.
DEFAULT_MANUAL_REDIRECT_URI = "https://oauth.yandex.ru/verification_code"

DEVICE_NAME = APP_TITLE

# Стратегии при конфликте имён на локальном диске.
CONFLICT_KEEP = "keep"        # не трогать существующий файл, новый сохранить с суффиксом
CONFLICT_REPLACE = "replace"  # заменить существующий неподтверждённый файл
CONFLICT_OVERWRITE = "overwrite"  # перезаписать безусловно (осторожно!)
CONFLICT_STRATEGIES = (CONFLICT_KEEP, CONFLICT_REPLACE, CONFLICT_OVERWRITE)

FILE_CHUNK_SIZE = 1024 * 1024  # 1 МиБ


def default_data_dir() -> Path:
    """Каталог для конфига, токена, логов, манифеста и реестра состояний."""
    if sys.platform == "win32":
        base = os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA")
        if base:
            return Path(base) / APP_TITLE
        return Path.home() / f"AppData\\Local\\{APP_TITLE}"
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / APP_TITLE
    return Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share")) / APP_SLUG


def default_backup_root() -> Path:
    """Куда складывать скачанные данные по умолчанию."""
    if sys.platform == "win32":
        return Path(os.environ.get("USERPROFILE", str(Path.home()))) / "YandexDiskBackup"
    return Path.home() / "YandexDiskBackup"


@dataclass
class Config:
    # --- OAuth ---
    client_id: str = ""
    # Секрет приложения НЕ хранится здесь и не попадает в репозиторий.
    # Он нужен только если выбран режим без PKCE; см. docs/STAGE_A.md.
    redirect_uri: str = DEFAULT_MANUAL_REDIRECT_URI
    auth_mode: str = "manual"  # "manual" | "loopback"
    loopback_port: int = 53682
    scopes: list[str] = field(default_factory=lambda: list(DEFAULT_SCOPES))
    # Стабильный идентификатор устройства: Яндекс ограничивает 30 токенов
    # на одно устройство, поэтому переиспользуем один и тот же device_id.
    device_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    # --- API ---
    api_base: str = API_BASE_URL
    min_request_interval: float = 0.25   # минимальный интервал между запросами, с
    timeout_connect: float = 15.0
    timeout_read: float = 60.0
    max_retries: int = 5

    # --- Обход (этапы D/E) ---
    scan_root: str = "disk:/"            # с чего начинаем обход
    page_size: int = 200                 # параметр limit в /v1/disk/resources
    scan_sort: str = "path"              # поле сортировки листинга
    include_trash: bool = False          # включать ли корзину
    media_type: str = ""                 # например "image" или "video"; "" = все

    # --- Скачивание (часть II) ---
    backup_root: str = ""                # "" = default_backup_root()
    concurrency: int = 4                 # одновременных загрузок
    download_read_timeout: float = 900.0
    chunk_size: int = FILE_CHUNK_SIZE
    max_attempts_per_file: int = 3
    preserve_times: bool = True          # выставлять mtime из поля modified
    conflict_strategy: str = CONFLICT_KEEP
    # Докачка через Range — только при подтверждённой поддержке сервером
    # (см. docs/PART2.md). По умолчанию выключено: дешевле перекачать файл.
    allow_range_resume: bool = False
    free_space_margin: int = 2 * 1024 ** 3  # не начинать, если запас меньше

    # Заголовок X-Data-Log-Enabled отключает логирование запросов на стороне
    # сервиса. Неизвестные заголовки сервер игнорирует, на работу не влияет.
    disable_service_logging: bool = True

    # --- служебное ---
    path: Path | None = None

    # --- производные свойства ---
    @property
    def timeout(self) -> tuple[float, float]:
        return (self.timeout_connect, self.timeout_read)

    @property
    def download_timeout(self) -> tuple[float, float]:
        return (self.timeout_connect, self.download_read_timeout)

    @property
    def is_configured(self) -> bool:
        return bool(self.client_id.strip())

    @property
    def backup_dir(self) -> Path:
        return Path(self.backup_root).expanduser() if self.backup_root else default_backup_root()

    def missing_reason(self) -> str | None:
        if not self.is_configured:
            return "не задан client_id (идентификатор приложения Яндекс OAuth)"
        if self.auth_mode not in ("manual", "loopback"):
            return f"неизвестный auth_mode={self.auth_mode!r}"
        if self.auth_mode == "loopback" and not self.loopback_port:
            return "для auth_mode=loopback нужно задать loopback_port"
        if self.concurrency < 1:
            return "concurrency должен быть не меньше 1"
        if self.page_size < 1:
            return "page_size должен быть не меньше 1"
        if self.conflict_strategy not in CONFLICT_STRATEGIES:
            return f"conflict_strategy должен быть одним из {CONFLICT_STRATEGIES}"
        return None

    def to_public_dict(self) -> dict[str, Any]:
        """Представление для вывода/логов — без секретов по построению."""
        return {
            "client_id": self.client_id,
            "auth_mode": self.auth_mode,
            "scopes": self.scopes,
            "api_base": self.api_base,
            "scan_root": self.scan_root,
            "page_size": self.page_size,
            "backup_root": str(self.backup_dir),
            "concurrency": self.concurrency,
            "conflict_strategy": self.conflict_strategy,
            "allow_range_resume": self.allow_range_resume,
        }

    def save(self, path: Path | None = None) -> Path:
        target = Path(path) if path else (self.path or (default_data_dir() / "config.json"))
        target.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "auth_mode": self.auth_mode,
            "loopback_port": self.loopback_port,
            "scopes": self.scopes,
            "device_id": self.device_id,
            "api_base": self.api_base,
            "min_request_interval": self.min_request_interval,
            "timeout_connect": self.timeout_connect,
            "timeout_read": self.timeout_read,
            "max_retries": self.max_retries,
            "scan_root": self.scan_root,
            "page_size": self.page_size,
            "scan_sort": self.scan_sort,
            "include_trash": self.include_trash,
            "media_type": self.media_type,
            "backup_root": self.backup_root,
            "concurrency": self.concurrency,
            "download_read_timeout": self.download_read_timeout,
            "chunk_size": self.chunk_size,
            "max_attempts_per_file": self.max_attempts_per_file,
            "preserve_times": self.preserve_times,
            "conflict_strategy": self.conflict_strategy,
            "allow_range_resume": self.allow_range_resume,
            "free_space_margin": self.free_space_margin,
            "disable_service_logging": self.disable_service_logging,
        }
        _atomic_write_json(target, data)
        self.path = target
        return target


def _atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def load_config(path: Path | None = None) -> Config:
    target = Path(path) if path else (default_data_dir() / "config.json")
    if not target.exists():
        return Config(path=target)
    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError(f"не удалось прочитать конфиг {target}: {exc}") from exc

    known = {f for f in Config.__dataclass_fields__ if f != "path"}
    kwargs = {k: v for k, v in raw.items() if k in known}
    cfg = Config(**kwargs)
    cfg.path = target
    return cfg


class ConfigError(RuntimeError):
    pass
