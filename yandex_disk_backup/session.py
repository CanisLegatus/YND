"""Сессия авторизации: токен + автоматическое обновление.

Правила ТЗ:
* при отсутствующем/просроченном/отозванном токене — понятное сообщение;
* если обновить токен невозможно — требовать повторную авторизацию;
* никогда не печатать токен.
"""

from __future__ import annotations

import logging
from pathlib import Path

from .api import DiskClient
from .config import Config
from .oauth import OAuthError, authorize_interactively, refresh_token, revoke
from .secrets_guard import token_preview
from .token_store import TokenData, TokenStore, TokenStoreError

log = logging.getLogger(__name__)


class AuthRequired(RuntimeError):
    """Нужна повторная авторизация: пользователь должен её подтвердить."""


class AuthSession:
    def __init__(self, cfg: Config, store: TokenStore) -> None:
        self.cfg = cfg
        self.store = store
        self.token: TokenData | None = None
        self.notice: str | None = None

    # --- состояние ----------------------------------------------------
    def load(self) -> TokenData | None:
        try:
            self.token = self.store.load()
        except TokenStoreError as exc:
            self.notice = str(exc)
            self.token = None
        return self.token

    @property
    def is_authorized(self) -> bool:
        return self.token is not None

    # --- действия -----------------------------------------------------
    def authorize(self, open_browser: bool = True) -> TokenData:
        self.token = authorize_interactively(self.cfg, timeout=self.cfg.timeout, open_browser=open_browser)
        self.store.save(self.token)
        self.notice = None
        return self.token

    def ensure_fresh(self) -> TokenData:
        """Возвращает рабочий токен, обновляя его при необходимости."""
        if self.token is None:
            self.load()
        if self.token is None:
            raise AuthRequired(
                "токен не найден. Запустите пункт «Авторизоваться»."
                + (f" {self.notice}" if self.notice else "")
            )
        if not self.token.is_expired():
            return self.token
        log.info("срок действия токена истёк, выполняю обновление…")
        try:
            refreshed = refresh_token(self.token, self.cfg, timeout=self.cfg.timeout)
        except OAuthError as exc:
            raise AuthRequired(
                f"не удалось обновить токен ({exc}). Требуется повторная авторизация."
            ) from exc
        self.token = refreshed
        self.store.save(refreshed)
        return refreshed

    def client(self) -> DiskClient:
        token = self.ensure_fresh()
        return DiskClient(
            token.access_token,
            base_url=self.cfg.api_base,
            timeout=self.cfg.timeout,
            max_retries=self.cfg.max_retries,
            min_request_interval=self.cfg.min_request_interval,
            disable_service_logging=self.cfg.disable_service_logging,
        )

    def logout(self, revoke_remote: bool) -> bool:
        """Удаляет локальный токен и по желанию отзывает доступ у Яндекса."""
        data = self.token or (self.store.load() if self.store.exists() else None)
        revoked = False
        if data and revoke_remote:
            revoked = revoke(data, self.cfg)
        self.store.delete()
        self.token = None
        return revoked

    def status_lines(self) -> list[str]:
        if self.token is None:
            lines = ["Авторизация: нет токена"]
            if self.notice:
                lines.append(f"  {self.notice}")
            return lines
        exp = self.token.expires_at
        if exp is None:
            ttl = "бессрочный (API не вернул срок)"
        elif self.token.is_expired():
            ttl = "ИСТЁК — нужна повторная авторизация"
        else:
            import time as _time

            ttl = f"осталось ~{int(exp - _time.time())} с"
        return [
            "Авторизация: токен получен",
            f"  Значение: {token_preview(self.token.access_token)}",
            f"  Срок: {ttl}",
            f"  Refresh-токен: {'есть' if self.token.refresh_token else 'нет'}",
            f"  Права: {self.token.scope or 'не указаны API'}",
            f"  Файл: {self.store.path}",
        ]


def default_store(directory: Path) -> TokenStore:
    return TokenStore(directory)
