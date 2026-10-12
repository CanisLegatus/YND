"""Защита секретов в логах и выводе.

Правило простое: токен не должен попасть ни в лог, ни в консоль, ни в
диагностическое сообщение. Здесь собраны механизмы, которые это гарантируют:

* реестр активных секретов — значение токена маскируется до `***`;
* фильтр логирования, применяющий маскирование к record.message и args;
* эвристика для «похожих на токен» строк (OAuth-токены Яндекса —
  длинные буквенно-цифровые последовательности).
"""

from __future__ import annotations

import logging
import re
from typing import Any, Iterable

_MASK = "***REDACTED***"

# Токены Яндекса — длинные base64url-подобные строки. Эвристика нужна на
# случай, если значение попало в текст из внешнего источника и ещё не
# зарегистрировано в реестре.
_TOKEN_LIKE = re.compile(r"(?<![A-Za-z0-9_\-])[A-Za-z0-9_\-]{28,}(?![A-Za-z0-9_\-])")

# Строки, которые похожи на токены, но ими не являются: хеши, идентификаторы
# из документации, названия полей. Их маскировать не нужно.
_ALLOWLIST = {
    "cloud_api:disk.read",
    "cloud_api:disk.info",
    "cloud_api:disk.write",
    "cloud_api:disk.app_folder",
    "X-Rate-Limit-Degree-Remaining",
    "X-Rate-Limit-Degree-Reset",
}


class SecretRegistry:
    """Хранит значения, которые нельзя показывать, и умеет их маскировать."""

    def __init__(self) -> None:
        self._secrets: set[str] = set()

    def add(self, secret: str | None) -> None:
        # Слишком короткие значения маскировать бессмысленно: иначе
        # замаскируется половина обычного текста.
        if secret and len(secret) >= 12:
            self._secrets.add(secret)

    def clear(self) -> None:
        self._secrets.clear()

    def scrub(self, text: str) -> str:
        for secret in self._secrets:
            if secret in text:
                text = text.replace(secret, _MASK)
        return _TOKEN_LIKE.sub(self._mask_if_suspicious, text)

    @staticmethod
    def _mask_if_suspicious(match: re.Match[str]) -> str:
        value = match.group(0)
        if value in _ALLOWLIST:
            return value
        if any(ch.isdigit() for ch in value) and any(ch.isalpha() for ch in value) and len(value) >= 28:
            return _MASK
        return value


#: Глобальный реестр. Токен добавляется сразу после получения.
SECRETS = SecretRegistry()


class RedactingFilter(logging.Filter):
    """Маскирует секреты во всех строковых полях записи журнала."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:  # noqa: BLE001 - логирование не должно ломать программу
            return True
        scrubbed = SECRETS.scrub(message)
        if scrubbed != message:
            record.msg = scrubbed
            record.args = ()
        return True


def redact(value: Any) -> str:
    """Безопасное строковое представление произвольного значения."""
    return SECRETS.scrub(str(value))


def token_preview(token: str | None, keep: int = 4) -> str:
    """Короткая несекретная отметка о токене для подтверждения «это тот же».

    Никакого полного значения здесь нет и быть не может.
    """
    if not token:
        return "<нет токена>"
    tail = token[-keep:] if len(token) > keep else ""
    return f"{'*' * 8}{tail} (длина {len(token)})"


def install_logging(log_file: str | None = None, verbose: bool = False) -> logging.Logger:
    """Настраивает корневой логгер: консоль + (опционально) файл."""
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    for handler in list(root.handlers):
        root.removeHandler(handler)

    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-7s %(message)s", "%H:%M:%S"))
    console.addFilter(RedactingFilter())
    root.addHandler(console)

    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s  %(levelname)-7s %(name)s: %(message)s")
        )
        file_handler.addFilter(RedactingFilter())
        root.addHandler(file_handler)

    # Сторонние библиотеки не должны сыпать DEBUG в консоль.
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    return root


def safe_headers_for_log(headers: dict[str, str] | None) -> Iterable[tuple[str, str]]:
    """Заголовки запроса в виде, безопасном для лога."""
    for key, value in (headers or {}).items():
        if key.lower() == "authorization":
            yield key, "OAuth <скрыто>"
        else:
            yield key, value
