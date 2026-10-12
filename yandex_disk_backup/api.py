"""HTTP-клиент REST API Яндекс Диска.

Ответственность:
* корректные заголовки (`Accept`, `Content-Type`, `Authorization`);
* таймауты соединения и чтения;
* ограниченные повторные попытки с растущим интервалом для временных ошибок;
* учёт заголовков ограничения запросов, если сервер их присылает;
* понятные исключения вместо сырых traceback;
* ни одного запроса, который изменяет данные на Диске.
"""

from __future__ import annotations

import json
import logging
import random
import time
from email.utils import parsedate_to_datetime
from typing import Any, Callable

import requests

from .secrets_guard import SECRETS

log = logging.getLogger(__name__)

RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})


class ApiError(RuntimeError):
    """Базовая ошибка API с HTTP-статусом и понятным описанием."""

    def __init__(self, message: str, status: int | None = None, payload: Any = None) -> None:
        super().__init__(message)
        self.status = status
        self.payload = payload


class NetworkError(ApiError):
    """Нет соединения с интернетом / таймаут / обрыв."""


class AuthError(ApiError):
    """401 — токен недействителен, отозван или истёк."""


class InsufficientScopeError(ApiError):
    """403 — недостаточно прав."""


class ResourceNotFoundError(ApiError):
    """404 — ресурс не найден или нет доступа к нему."""


class ResourceLockedError(ApiError):
    """423 — ресурс занят операцией на стороне Диска."""


class RateLimitedError(ApiError):
    """429 — превышен лимит запросов."""

    def __init__(self, message: str, retry_after: float | None = None, status: int = 429) -> None:
        super().__init__(message, status=status)
        self.retry_after = retry_after


class ResponseFormatError(ApiError):
    """Ответ не удалось разобрать как JSON."""


class DiskClient:
    """Тонкая обёртка над REST API Яндекс Диска. Только чтение."""

    def __init__(
        self,
        access_token: str,
        *,
        base_url: str = "https://cloud-api.yandex.net/v1",
        timeout: tuple[float, float] = (15.0, 60.0),
        max_retries: int = 5,
        min_request_interval: float = 0.25,
        disable_service_logging: bool = True,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not access_token:
            raise ValueError("access_token обязателен")
        SECRETS.add(access_token)
        self._token = access_token
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max(0, int(max_retries))
        self.min_request_interval = max(0.0, float(min_request_interval))
        self.disable_service_logging = disable_service_logging
        self._session = session or requests.Session()
        self._sleep = sleep
        self._clock = clock
        self._last_request_at = 0.0
        #: Счётчик реально выполненных HTTP-запросов (включая повторы).
        #: Нужен, чтобы в манифесте указывать честное число обращений к API.
        self.request_count = 0

    # --- публичный интерфейс ------------------------------------------
    def get_disk_info(self) -> dict[str, Any]:
        """GET /v1/disk — сведения о диске пользователя."""
        return self._request("GET", "/disk")

    def get_resource(self, path: str, **params: Any) -> dict[str, Any]:
        """GET /v1/disk/resources — метаданные ресурса / листинг папки.

        На этапе B используется только для самопроверки; полноценный обход
        появится на этапах D–E.
        """
        params = {k: v for k, v in params.items() if v is not None}
        params["path"] = path
        return self._request("GET", "/disk/resources", params=params)

    def list_folder_page(
        self,
        path: str,
        *,
        limit: int,
        offset: int = 0,
        sort: str | None = None,
        media_type: str | None = None,
        include_trash: bool = False,
        read_timeout: float | None = None,
        extra_params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Одна страница листинга папки (GET /v1/disk/resources).

        `read_timeout` — увеличенный таймаут чтения: для больших каталогов
        (тысячи фотографий) Яндекс формирует ответ заметно дольше обычных 60 с,
        и при стандартном таймауте запрос обрывается с ConnectTimeout/
        ReadTimeout. Здесь он поднимается по умолчанию до 300 с.

        `extra_params` — дополнительные параметры запроса; используются
        стратегией «alphabet-splitting» (см. inventory.split_listing_by_prefix)
        для фильтров вида `name=glob:<папка>/<префикс>*`.
        """
        params: dict[str, Any] = {"path": path, "limit": max(1, int(limit))}
        if offset > 0:
            params["offset"] = offset
        if sort:
            params["sort"] = sort
        if media_type:
            params["media_type"] = media_type
        if include_trash:
            params["include_trash"] = "true"
        if extra_params:
            params.update(extra_params)
        timeout = self.timeout
        if read_timeout is not None:
            timeout = (timeout[0], float(read_timeout))
        return self._request("GET", "/disk/resources", params=params, timeout=timeout)

    def get_download_link(self, path: str) -> tuple[str, str]:
        """GET /v1/disk/resources/download — временная ссылка на скачивание.

        Ссылка живёт ограниченное время и может быть использована один раз.
        Возвращает (href, method).
        """
        data = self._request("GET", "/disk/resources/download", params={"path": path})
        href = data.get("href")
        if not isinstance(href, str) or not href:
            raise ResponseFormatError("API не вернул ссылку на скачивание", payload=data)
        method = str(data.get("method") or "GET").upper()
        log.debug("получена временная ссылка для %s (метод %s)", path, method)
        return href, method

    # --- транспорт ----------------------------------------------------
    def _headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"OAuth {self._token}",
            "User-Agent": "YandexDiskBackup/0.3 (read-only backup tool)",
        }
        if self.disable_service_logging:
            headers["X-Data-Log-Enabled"] = "false"
        return headers

    def _throttle(self) -> None:
        if self.min_request_interval <= 0:
            return
        elapsed = self._clock() - self._last_request_at
        if 0 <= elapsed < self.min_request_interval:
            self._sleep(self.min_request_interval - elapsed)

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        timeout: tuple[float, float] | None = None,
    ) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        if timeout is None:
            timeout = self.timeout
        last_error: Exception | None = None

        for attempt in range(self.max_retries + 1):
            self._throttle()
            try:
                self._last_request_at = self._clock()
                self.request_count += 1
                response = self._session.request(
                    method,
                    url,
                    params=params,
                    headers=self._headers(),
                    timeout=timeout,
                    allow_redirects=False,
                )
            except requests.exceptions.RequestException as exc:
                last_error = NetworkError(
                    f"нет связи с API Яндекс Диска ({type(exc).__name__}). Проверьте интернет-соединение."
                )
                if attempt >= self.max_retries:
                    break
                delay = self._backoff(attempt)
                log.warning(
                    "сеть недоступна (%s), повтор через %.1f с [%d/%d]",
                    type(exc).__name__, delay, attempt + 1, self.max_retries,
                )
                self._sleep(delay)
                continue

            status = response.status_code
            if status in (200, 201, 202, 203, 204):
                return self._parse_json(response)

            if status in (401, 403, 404, 423):
                raise self._fatal(response)

            if status == 429 or status in (500, 502, 503, 504, 408, 425):
                last_error = self._transient(response)
                if attempt >= self.max_retries:
                    break
                delay = self._retry_delay(response, attempt)
                log.warning(
                    "HTTP %s от API Диска, повтор через %.1f с [%d/%d]: %s",
                    status, delay, attempt + 1, self.max_retries, last_error,
                )
                self._sleep(delay)
                continue

            raise self._fatal(response)

        assert last_error is not None
        raise last_error

    # --- разбор ответов ----------------------------------------------
    def _parse_json(self, response: requests.Response) -> dict[str, Any]:
        if not (response.text or "").strip():
            raise ResponseFormatError("API вернул пустой ответ", status=response.status_code)
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError as exc:
            raise ResponseFormatError(
                f"не удалось разобрать ответ API как JSON: {exc}",
                status=response.status_code,
            ) from exc
        if not isinstance(data, dict):
            raise ResponseFormatError(
                "API вернул не объект JSON, а другой тип", status=response.status_code, payload=data
            )
        return data

    def _message(self, response: requests.Response) -> str:
        try:
            data = response.json()
        except ValueError:
            text = (response.text or "").strip()
            return text[:300] or "<пустое тело ответа>"
        if isinstance(data, dict):
            for key in ("message", "error_description", "error", "status"):
                value = data.get(key)
                if value:
                    return str(value)
        return json.dumps(data, ensure_ascii=False)[:300]

    def _fatal(self, response: requests.Response) -> ApiError:
        message = self._message(response)
        status = response.status_code
        if status == 401:
            return AuthError(
                "токен отклонён (401). Токен истёк или был отозван — нужна повторная авторизация.",
                status=status,
            )
        if status == 403:
            return InsufficientScopeError(
                f"недостаточно прав (403): {message}. Проверьте выданные доступы приложения.",
                status=status,
            )
        if status == 404:
            return ResourceNotFoundError(
                f"ресурс не найден или недоступен (404): {message}", status=status
            )
        if status == 423:
            return ResourceLockedError(
                f"ресурс заблокирован операцией на Диске (423): {message}", status=status
            )
        return ApiError(f"API Диска вернул HTTP {status}: {message}", status=status)

    def _transient(self, response: requests.Response) -> ApiError:
        message = self._message(response)
        status = response.status_code
        if status == 429:
            return RateLimitedError(
                f"превышен лимит запросов к API (429): {message}",
                retry_after=self._retry_after(response),
                status=status,
            )
        return ApiError(f"временная ошибка API Диска (HTTP {status}): {message}", status=status)

    # --- backoff ------------------------------------------------------
    @staticmethod
    def _backoff(attempt: int, base: float = 1.0, cap: float = 60.0) -> float:
        delay = min(cap, base * (2**attempt))
        return delay * (0.75 + random.random() * 0.5)  # джиттер против «стада»

    def _retry_delay(self, response: requests.Response, attempt: int) -> float:
        header_delay = self._retry_after(response)
        if header_delay is not None:
            return min(300.0, max(0.5, header_delay))
        # Заголовки лимита Яндекса, если сервер их присылает:
        # сколько «степеней» осталось и когда они сбросятся.
        remaining = _header_int(response, "X-Rate-Limit-Degree-Remaining")
        reset = _header_int(response, "X-Rate-Limit-Degree-Reset")
        if remaining is not None and remaining <= 0 and reset:
            return min(300.0, max(1.0, float(reset) - time.time()))
        return self._backoff(attempt)

    @staticmethod
    def _retry_after(response: requests.Response) -> float | None:
        raw = response.headers.get("Retry-After")
        if not raw:
            return None
        raw = raw.strip()
        try:
            return float(raw)
        except ValueError:
            pass
        try:
            target = parsedate_to_datetime(raw)
        except (TypeError, ValueError):
            return None
        if target is None:
            return None
        import datetime as _dt

        if target.tzinfo is None:
            target = target.replace(tzinfo=_dt.timezone.utc)
        return max(0.0, target.timestamp() - time.time())


def _header_int(response: requests.Response, name: str) -> int | None:
    raw = response.headers.get(name)
    if raw is None:
        return None
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return None
