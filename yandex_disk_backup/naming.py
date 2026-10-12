"""Безопасное сопоставление имён облака с именами на локальном диске.

Задачи, которые решает модуль:

* привести имя файла из Диска к виду, допустимому на Windows
  (зарезервированные имена, недопустимые символы, хвостовые точки/пробелы,
  чрезмерная длина);
* разрулить коллизии: два разных облачных файла не должны превратиться в
  один локальный, и уже скачанный файл не должен быть молча перезаписан;
* сохранить стабильность имён между запусками, чтобы докачка после обрыва
  попадала в тот же самый файл.

Стратегии коллизий (см. config.CONFLICT_*):
  keep       — существующий файл не трогаем, новый получа суффикс «(2)»;
  replace    — заменяем существующий файл, если он не подтверждён как верный;
  overwrite  — безусловная перезапись (включать осознанно).
"""

from __future__ import annotations

import os
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

#: Символы, запрещённые в именах файлов Windows.
_ILLEGAL = '<>:"/\\|?*'
_ILLEGAL_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f\x7f]')

#: Зарезервированные имена Windows (нельзя использовать даже с расширением).
_RESERVED = {
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}

#: Разумный предел длины одного компонента пути.
MAX_COMPONENT = 200


def sanitize_component(name: str, replacement: str = "_") -> str:
    """Приводит один компонент пути к безопасному имени файла/папки."""
    if not name:
        return "_"

    cleaned = _ILLEGAL_RE.sub(replacement, name)
    # Убираем хвостовые точки и пробелы: Windows их не допускает.
    cleaned = cleaned.rstrip(" .")
    # Скрытые «системные» имена остаются валидными, но пустой хвост — нет.
    if not cleaned:
        return "_"

    stem, dot, ext = cleaned.rpartition(".")
    if dot and stem.upper() in _RESERVED:
        cleaned = f"{replacement}{cleaned}"
    elif cleaned.upper() in _RESERVED:
        cleaned = f"{replacement}{cleaned}"

    if len(cleaned) > MAX_COMPONENT:
        # Обрезаем так, чтобы расширение (если есть) сохранилось.
        stem, dot, ext = cleaned.rpartition(".")
        if dot and 0 < len(ext) <= 20:
            keep = MAX_COMPONENT - len(ext) - 1
            cleaned = f"{stem[:max(1, keep)]}.{ext}"
        else:
            cleaned = cleaned[:MAX_COMPONENT]
        cleaned = cleaned.rstrip(" .") or "_"
    return cleaned


def sanitize_relpath(relpath: str, replacement: str = "_") -> str:
    """`/Photos/Лето/снимок.jpg` → безопасный относительный путь для ОС."""
    parts = [p for p in PurePosixPath(relpath).parts if p not in ("/", "")]
    if not parts:
        return ""
    return os.path.join(*(sanitize_component(p, replacement) for p in parts))


def split_name(name: str) -> tuple[str, str]:
    """`archive.tar.gz` → (`archive.tar`, `.gz`). Без точки — пустая строка."""
    stem, dot, ext = name.rpartition(".")
    if not dot or not stem:
        return name, ""
    return stem, f".{ext}"


def numbered_name(name: str, number: int) -> str:
    """`снимок.jpg` + 2 → `снимок (2).jpg`."""
    stem, ext = split_name(name)
    return f"{stem} ({number}){ext}"


def _fold_key(path: str) -> str:
    """Ключ сравнения имён для регистронезависимых и Unicode-нестрогих ФС."""
    decomposed = unicodedata.normalize("NFC", path)
    return decomposed.casefold().rstrip(" .")


@dataclass
class Allocation:
    """Результат сопоставления облачного пути с локальным."""

    api_path: str
    relpath: str          # относительный путь без корня, «как в облаке»
    local_relpath: str    # безопасный относительный путь на диске
    renamed: bool = False  # True, если имя пришлось менять из-за коллизии
    reason: str = ""       # чем именно пришлось помечать

    def absolute(self, root: Path) -> Path:
        return root / self.local_relpath


class PathAllocator:
    """Назначает локальные пути, не давая двум файлам получить один адрес.

    Реестр намеренно персистентен в пределах запуска и может быть предзаполнен
    из реестра состояний (state.db), чтобы после перезапуска уже известные
    файлы получали те же имена, что и раньше.
    """

    def __init__(self, root: Path, strategy: str = "keep") -> None:
        self.root = Path(root)
        self.strategy = strategy
        self._taken: dict[str, str] = {}       # fold_key -> api_path
        self._assigned: dict[str, str] = {}    # api_path -> local_relpath
        self._missing: set[str] = set()        # относительные пути, не найденные на диске

    # --- предварительное заполнение -----------------------------------
    def reserve(self, local_relpath: str, api_path: str) -> None:
        """Занять адрес заранее (например, из предыдущего запуска)."""
        key = _fold_key(local_relpath)
        self._taken.setdefault(key, api_path)
        self._assigned.setdefault(api_path, local_relpath)

    def note_absent(self, local_relpath: str) -> None:
        """Пометить путь как отсутствующий на диске, но занятый в реестре."""
        self._missing.add(_fold_key(local_relpath))

    # --- назначение ----------------------------------------------------
    def assigned(self, api_path: str) -> str | None:
        return self._assigned.get(api_path)

    def allocate(self, api_path: str, relpath: str) -> Allocation:
        """Возвращает безопасный локальный относительный путь для ресурса."""
        if api_path in self._assigned:
            return Allocation(api_path, relpath, self._assigned[api_path])

        base = sanitize_relpath(relpath) or "_"
        candidate = base

        key = _fold_key(candidate)
        owner = self._taken.get(key)
        if owner is None and self._exists(candidate):
            # Файл есть на диске, но неизвестен реестру: считаем занятым,
            # чтобы не потерять данные пользователя.
            self._missing.add(key)
            reason = "локальный файл уже существует (неизвестен реестру)"
        elif owner is None:
            self._taken[key] = api_path
            self._assigned[api_path] = candidate
            return Allocation(api_path, relpath, candidate)
        elif owner == api_path:
            self._assigned[api_path] = candidate
            return Allocation(api_path, relpath, candidate)
        else:
            reason = f"коллизия с облачным путём {owner}"

        number = 2
        while True:
            directory, filename = os.path.split(candidate)
            tried = numbered_name(filename, number)
            new_rel = os.path.join(directory, tried) if directory else tried
            new_key = _fold_key(new_rel)
            new_owner = self._taken.get(new_key)
            if new_owner is None and not self._exists(new_rel):
                self._taken[new_key] = api_path
                self._assigned[api_path] = new_rel
                return Allocation(api_path, relpath, new_rel, renamed=True, reason=reason)
            if new_owner == api_path:
                self._assigned[api_path] = new_rel
                return Allocation(api_path, relpath, new_rel, renamed=True, reason=reason)
            number += 1
            if number > 9999:  # защита от бесконечного цикла
                raise RuntimeError(f"не удалось разрешить коллизию имени для {relpath}")

    # --- вспомогательное ------------------------------------------------
    def _exists(self, relpath: str) -> bool:
        if not relpath:
            return False
        if _fold_key(relpath) in self._missing:
            return False
        return (self.root / relpath).exists()

    @property
    def assignments(self) -> dict[str, str]:
        return dict(self._assigned)
