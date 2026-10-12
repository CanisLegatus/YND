"""OAuth 2.0 Яндекс ID: получение, обновление и отзыв токена.

Сценарий, выбранный на этапе A (обоснование — docs/STAGE_A.md):

    Authorization Code Flow + PKCE (code_challenge_method=S256)

Почему именно он:
* это официально рекомендуемый сценарий для консольных приложений;
* PKCE позволяет вообще не использовать client_secret, то есть в коде и
  конфиге нет секрета приложения (требование ТЗ);
* код подтверждения живёт 10 минут — риск подхвата ограничен.

Два режима получения кода:
* `manual`   — код показывается на странице Яндекса, пользователь вводит
               его в консоль. Работает с Redirect URI
               `https://oauth.yandex.ru/verification_code`, который
               предлагается Яндексом по умолчанию, без регистрации своего.
* `loopback` — локальный HTTP-сервер на 127.0.0.1 перехватывает redirect.
               Требует, чтобы точный Redirect URI был прописан в настройках
               приложения. Если он не прописан — Яндекс его проигнорирует,
               поэтому режим включается явно.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import secrets
import time
import webbrowser
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from typing import Any
from urllib.parse import urlencode

import requests

from .config import (
    DEVICE_NAME,
    OAUTH_AUTHORIZE_URL,
    OAUTH_REVOKE_URL,
    OAUTH_TOKEN_URL,
    Config,
)
from .secrets_guard import SECRETS
from .token_store import TokenData

log = logging.getLogger(__name__)

CODE_LIFETIME_SECONDS = 600  # 10 минут, как указано в документации
_LOCALHOST = "127.0.0.1"


class OAuthError(RuntimeError):
    """Ошибка, после которой повторять тот же шаг бессмысленно."""


class OAuthTimeout(OAuthError):
    """Пользователь не завершил авторизацию вовремя."""


# --- PKCE --------------------------------------------------------------

def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def make_code_verifier() -> str:
    return _b64url(secrets.token_bytes(64))


def make_code_challenge(verifier: str) -> str:
    return _b64url(hashlib.sha256(verifier.encode("ascii")).digest())


def make_state() -> str:
    return _b64url(secrets.token_bytes(16))


# --- Шаг 1: страница согласия -----------------------------------------

def build_authorize_url(cfg: Config, code_verifier: str, state: str) -> str:
    """Собирает URL, который нужно открыть в браузере."""
    params = {
        "response_type": "code",
        "client_id": cfg.client_id,
        "redirect_uri": cfg.redirect_uri,
        "scope": " ".join(cfg.scopes),
        "device_id": cfg.device_id,
        "device_name": DEVICE_NAME[:100],
        "state": state,
        "code_challenge": make_code_challenge(code_verifier),
        "code_challenge_method": "S256",
    }
    return f"{OAUTH_AUTHORIZE_URL}?{urlencode(params)}"


# --- Локальный приём кода (режим loopback) ----------------------------

class _CallbackHandler(BaseHTTPRequestHandler):
    server_version = "YandexDiskBackup/0.3"

    def do_GET(self) -> None:  # noqa: N802 - имя задано BaseHTTPRequestHandler
        from urllib.parse import parse_qs, urlparse

        query = parse_qs(urlparse(self.path).query)
        code = (query.get("code") or [None])[0]
        state = (query.get("state") or [None])[0]
        error = (query.get("error") or [None])[0]

        if error:
            body = "Авторизация отклонена. Закройте окно и повторите."
            status = 200
        elif not code:
            body = "Код подтверждения не найден в ссылке. Закройте окно и повторите."
            status = 400
        else:
            body = (
                "<!doctype html><meta charset='utf-8'>"
                "<h3>Готово</h3>"
                "<p>Авторизация Яндекс Диска подтверждена. "
                "Вернитесь в консоль — можно закрыть это окно.</p>"
            )
            status = 200

        payload = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

        self.server.result = {  # type: ignore[attr-defined]
            "code": code,
            "state": state,
            "error": error,
        }

    def log_message(self, fmt: str, *args: Any) -> None:  # silence stderr access log
        log.debug("loopback: " + fmt, *args)


def _wait_for_code_loopback(port: int, expected_state: str, timeout: float) -> str:
    server = HTTPServer((_LOCALHOST, port), _CallbackHandler)
    server.result = None  # type: ignore[attr-defined]
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = getattr(server, "result", None)
            if result:
                if result.get("error"):
                    raise OAuthError(f"Яндекс OAuth вернул ошибку: {result['error']}")
                if result.get("state") != expected_state:
                    raise OAuthError("не совпал параметр state — возможна подмена ответа")
                return result["code"]  # type: ignore[return-value]
            time.sleep(0.2)
        raise OAuthTimeout(
            f"Код подтверждения не получен за {int(timeout)} с. "
            "Проверьте, что в браузере выдан доступ, и повторите авторизацию."
        )
    finally:
        server.shutdown()
        server.server_close()


def _prompt_code_manual(expected_state: str) -> str:
    print()
    print("  Откроется страница Яндекса. Нажмите «Разрешить» и скопируйте код,")
    print("  который покажет страница. Код живёт 10 минут.")
    if expected_state:
        print(f"  (state для проверки: {expected_state})")
    print()
    try:
        raw = input("  Введите код подтверждения: ").strip()
    except EOFError as exc:
        raise OAuthError("ввод кода прерван") from exc
    if not raw:
        raise OAuthError("код подтверждения не введён")
    return raw


# --- Шаг 2: обмен кода на токен ---------------------------------------

def _post_form(url: str, data: dict[str, str], timeout: tuple[float, float]) -> dict[str, Any]:
    try:
        response = requests.post(
            url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise OAuthError(f"не удалось соединиться с сервером авторизации Яндекса: {exc}") from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise OAuthError(
            f"сервер авторизации вернул не-JSON ответ (HTTP {response.status_code})"
        ) from exc

    if response.status_code != 200 or "error" in payload:
        error = payload.get("error", "unknown_error")
        description = payload.get("error_description") or "описание отсутствует"
        raise OAuthError(f"Яндекс OAuth: {error} — {description}")
    return payload


def exchange_code(
    code: str,
    code_verifier: str,
    cfg: Config,
    timeout: tuple[float, float] = (15.0, 60.0),
) -> TokenData:
    """Меняет код подтверждения на токен.

    client_secret не передаётся: при PKCE он не требуется
    (см. docs/STAGE_A.md, раздел «Обмен кода подтверждения на OAuth-токен»).
    """
    payload = _post_form(
        OAUTH_TOKEN_URL,
        {
            "grant_type": "authorization_code",
            "code": code,
            "client_id": cfg.client_id,
            "device_id": cfg.device_id,
            "device_name": DEVICE_NAME[:100],
            "code_verifier": code_verifier,
        },
        timeout,
    )
    return _to_token_data(payload)


def _to_token_data(payload: dict[str, Any]) -> TokenData:
    token = TokenData(
        access_token=payload["access_token"],
        refresh_token=payload.get("refresh_token"),
        token_type=payload.get("token_type", "bearer"),
        expires_in=payload.get("expires_in"),
        obtained_at=time.time(),
        scope=payload.get("scope"),
    )
    SECRETS.add(token.access_token)
    SECRETS.add(token.refresh_token)
    return token


def refresh_token(
    current: TokenData,
    cfg: Config,
    client_secret: str | None = None,
    timeout: tuple[float, float] = (15.0, 60.0),
) -> TokenData:
    """Обновляет токен. Без refresh-токена и без client_secret — явная ошибка."""
    if not current.refresh_token:
        raise OAuthError(
            "refresh-токен не был выдан. Требуется повторная авторизация "
            "(запустите «Авторизоваться заново»)."
        )
    data = {
        "grant_type": "refresh_token",
        "refresh_token": current.refresh_token,
        "client_id": cfg.client_id,
        "device_id": cfg.device_id,
        "device_name": DEVICE_NAME[:100],
    }
    if client_secret:
        data["client_secret"] = client_secret
    else:
        log.debug("client_secret не задан; обновление без секрета (PKCE-сценарий)")
    return _to_token_data(_post_form(OAUTH_TOKEN_URL, data, timeout))


def revoke(current: TokenData, cfg: Config, client_secret: str | None = None) -> bool:
    """Отзывает токен на стороне Яндекса. Ошибки не поднимаются."""
    data = {
        "access_token": current.access_token,
        "client_id": cfg.client_id,
        "device_id": cfg.device_id,
    }
    if client_secret:
        data["client_secret"] = client_secret
    try:
        response = requests.post(OAUTH_REVOKE_URL, data=data, timeout=(15.0, 30.0))
    except requests.RequestException as exc:
        log.warning("не удалось отозвать токен: %s", exc)
        return False
    return response.status_code == 200


# --- Сценарий целиком --------------------------------------------------

def authorize_interactively(
    cfg: Config,
    timeout: tuple[float, float] = (15.0, 60.0),
    wait_seconds: float = 300.0,
    open_browser: bool = True,
) -> TokenData:
    """Полный цикл: открыть браузер → получить код → обменять на токен."""
    verifier = make_code_verifier()
    state = make_state()
    url = build_authorize_url(cfg, verifier, state)

    print()
    print("  Открываю страницу авторизации Яндекса…")
    print(f"  Если браузер не открылся, откройте вручную:\n    {url}")
    print()
    if open_browser:
        try:
            webbrowser.open(url)
        except Exception as exc:  # noqa: BLE001 - webbrowser зависит от ОС
            log.debug("не удалось открыть браузер автоматически: %s", exc)

    if cfg.auth_mode == "loopback":
        print(f"  Ожидаю код на {_LOCALHOST}:{cfg.loopback_port} (до {int(wait_seconds)} с)…")
        code = _wait_for_code_loopback(cfg.loopback_port, state, wait_seconds)
    else:
        code = _prompt_code_manual(state)

    token = exchange_code(code, verifier, cfg, timeout=timeout)
    log.info("токен получен; refresh-токен: %s", "есть" if token.refresh_token else "не выдан")
    return token
