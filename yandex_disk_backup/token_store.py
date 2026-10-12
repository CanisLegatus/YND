"""Локальное хранилище OAuth-токена.

Требования ТЗ:
* токен хранится локально с ограничением доступа;
* токен не попадает в код, логи и консольный вывод;
* при невозможности обновления требуется повторная авторизация.

Файл `token.json` содержит сам токен и НЕ должен попадать в git
(см. .gitignore). На POSIX выставляются права 0600, на Windows —
пытаемся снять наследование ACL через icacls; если это не удалось,
программа честно предупреждает пользователя, а не делает вид, что всё хорошо.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .secrets_guard import SECRETS, token_preview

TOKEN_FILENAME = "token.json"


class TokenStoreError(RuntimeError):
    pass


@dataclass
class TokenData:
    access_token: str
    refresh_token: str | None = None
    token_type: str = "bearer"
    expires_in: int | None = None
    obtained_at: float = 0.0
    scope: str | None = None

    @property
    def expires_at(self) -> float | None:
        if not self.expires_in or not self.obtained_at:
            return None
        return self.obtained_at + float(self.expires_in)

    def is_expired(self, leeway: float = 60.0) -> bool:
        exp = self.expires_at
        if exp is None:
            return False
        return time.time() >= (exp - leeway)

    def to_dict(self) -> dict[str, Any]:
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "token_type": self.token_type,
            "expires_in": self.expires_in,
            "obtained_at": self.obtained_at,
            "scope": self.scope,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "TokenData":
        access = raw.get("access_token")
        if not isinstance(access, str) or not access:
            raise TokenStoreError("в файле токена отсутствует поле access_token")
        expires = raw.get("expires_in")
        obtained = raw.get("obtained_at")
        return cls(
            access_token=access,
            refresh_token=raw.get("refresh_token"),
            token_type=raw.get("token_type") or "bearer",
            expires_in=int(expires) if isinstance(expires, (int, float)) else None,
            obtained_at=float(obtained) if isinstance(obtained, (int, float)) else 0.0,
            scope=raw.get("scope"),
        )


class TokenStore:
    def __init__(self, directory: Path) -> None:
        self.directory = Path(directory)
        self.path = self.directory / TOKEN_FILENAME

    # --- чтение/запись -------------------------------------------------
    def exists(self) -> bool:
        return self.path.exists()

    def load(self) -> TokenData | None:
        if not self.path.exists():
            return None
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise TokenStoreError(
                f"файл токена {self.path} повреждён: {exc}. "
                "Удалите файл и выполните авторизацию заново."
            ) from exc
        except OSError as exc:
            raise TokenStoreError(f"не удалось прочитать файл токена: {exc}") from exc
        data = TokenData.from_dict(raw)
        SECRETS.add(data.access_token)
        SECRETS.add(data.refresh_token)
        return data

    def save(self, token: TokenData) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + ".tmp")
        payload = json.dumps(token.to_dict(), ensure_ascii=False, indent=2) + "\n"
        # Сначала restrictive права, потом запись: иначе файл мигнёт
        # с дефолтными правами в момент создания.
        _precreate_secure(tmp)
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        _harden_permissions(tmp)
        os.replace(tmp, self.path)
        _harden_permissions(self.path)
        SECRETS.add(token.access_token)
        SECRETS.add(token.refresh_token)

    def delete(self) -> bool:
        existed = self.path.exists()
        if existed:
            self.path.unlink()
        return existed

    def describe(self) -> str:
        if not self.path.exists():
            return f"Файл токена: {self.path} (отсутствует)"
        data = self.load()
        assert data is not None
        state = "действителен"
        if data.is_expired():
            state = "истёк по сроку"
        elif data.expires_at is None:
            state = "бессрочный (срок API не вернул)"
        refresh = "есть" if data.refresh_token else "нет (потребуется повторная авторизация)"
        return (
            f"Файл токена: {self.path}\n"
            f"  Значение: {token_preview(data.access_token)}\n"
            f"  Состояние: {state}\n"
            f"  Refresh-токен: {refresh}\n"
            f"  Права: {data.scope or 'не указаны API'}"
        )


# --- работа с правами доступа -----------------------------------------

def _precreate_secure(path: Path) -> None:
    if sys.platform != "win32" and not path.exists():
        fd = os.open(path, os.O_CREAT | os.O_WRONLY, 0o600)
        os.close(fd)


def _harden_permissions(path: Path) -> None:
    """Ограничивает доступ к файлу. Возвращает предупреждение или None."""
    if sys.platform == "win32":
        return _harden_windows(path)
    try:
        os.chmod(path, stat.S_IRUSR | stat.S_IWRITE)
        return None
    except OSError as exc:  # pragma: no cover - зависит от ФС
        return f"не удалось выставить права 0600 на {path}: {exc}"


def _harden_windows(path: Path) -> str | None:
    # os.chmod в Windows умеет только read-only, поэтому снимаем наследование
    # ACL и оставляем доступ только текущему пользователю.
    try:
        completed = subprocess.run(
            ["icacls", str(path), "/inheritance:r", "/grant:r", f"{os.environ.get('USERNAME', '')}:F"],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return (
            f"не удалось ограничить доступ к {path} через icacls: {exc}. "
            "Проверьте права вручную: файл с токеном должен быть доступен только вам."
        )
    if completed.returncode != 0:
        return (
            f"icacls вернул код {completed.returncode} для {path}. "
            "Убедитесь вручную, что файл токена доступен только текущему пользователю."
        )
    return None
