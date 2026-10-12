"""Глобальный «выключатель» (Circuit Breaker) против 429 Too Many Requests.

Проблема: при достижении определённого количества скачиваний Яндекс начинает
отвечать 429, воркеры зацикливаются на повторах и только усугубляют нагрузку —
в итоге «засыпают» посреди прохода.

Решение (три уровня):

1. Общий флаг ``is_throttled``. Любой воркер, получивший 429, поднимает флаг;
   все остальные видят его и немедленно прекращают работу, коротко засыпая и
   проверяя флаг снова. Трафик резко обнуляется, серверу дают «забыть» о нас.
2. Экспоненциальная пауза и пробный запрос. Пока флаг поднят, право делать
   API-запросы получает только один воркер-«пробник». Если пробный запрос
   успешен — флаг снимается и все просыпаются; если снова 429 — время сна
   удваивается (с ограничением сверху).
3. Динамическое снижение параллелизма. Каждая волна троттлинга уменьшает
   число активных воркеров (минимум до 1); после продолжительной спокойной
   работы параллелизм постепенно возвращается к исходному.

Потоки в ThreadPoolExecutor не удаляются на лету, поэтому снижение
параллелизма реализовано через семафор: активным остаётся ровно
``allowed_workers`` воркеров, остальные ждут освобождения места.
"""

from __future__ import annotations

import random
import threading
import time
from typing import Callable


class ThrottleBreaker:
    """Circuit breaker для 429: общий флаг, экспоненциальный backoff, лимит воркеров."""

    def __init__(
        self,
        max_workers: int,
        *,
        base_sleep: float = 2.0,
        max_sleep: float = 60.0,
        probe_timeout: float = 30.0,
        recovery_window: float = 120.0,
        sleep_fn: Callable[[float], None] | None = None,
        time_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        self.max_workers = max(1, int(max_workers))
        self.base_sleep = max(0.05, float(base_sleep))
        self.max_sleep = max(self.base_sleep, float(max_sleep))
        self.probe_timeout = max(1.0, float(probe_timeout))
        # Как долго нужно работать без 429, чтобы вернуть одного воркера.
        self.recovery_window = max(1.0, float(recovery_window))

        self._sleep_fn = sleep_fn or time.sleep
        self._time = time_fn

        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self.is_throttled = False
        # True, пока кто-то делает пробный запрос (остальные в это время спят).
        self._probing = False
        self._wave_started_at: float | None = None
        self._waves = 0
        self._consecutive_successes = 0
        self._calm_since = self._time()
        self._active_slots = self.max_workers
        self._permits = self.max_workers  # сколько мест семафора сейчас свободно

    # ------------------------------------------------------------------
    # Публичный интерфейс
    # ------------------------------------------------------------------
    @property
    def throttle_waves(self) -> int:
        """Сколько раз за проход срабатывал выключатель."""
        with self._lock:
            return self._waves

    @property
    def allowed_workers(self) -> int:
        """Сколько воркеров имеют право работать прямо сейчас."""
        with self._lock:
            return self._active_slots

    def note_task_start(self) -> None:
        """Воркер берёт место в семaphore перед началом задачи."""
        with self._cond:
            while self._permits <= 0:
                self._cond.wait(1.0)
            self._permits -= 1

    def note_task_end(self) -> None:
        """Воркер освобождает место по окончании задачи."""
        with self._cond:
            self._permits = min(self.max_workers, self._permits + 1)
            self._cond.notify()

    def wait_for_slot(self) -> bool:
        """Вернуть False, если задачу пора прервать (используется извне)."""
        return not self.should_stop()

    # --- точки проверки внутри воркера ---------------------------------
    def before_request(self) -> None:
        """Вызывается ПЕРЕД каждым HTTP-запросом воркера.

        Пока флаг поднят, сон короче, чем осталось до конца волны, — это
        «короткие интервалы 1–2 секунды» из ТЗ. Право сделать запрос
        получает только пробник; остальные продолжают дремать.
        """
        while True:
            with self._lock:
                if not self.is_throttled:
                    return
                remaining = self._remaining_wave_locked()
                nap = min(self.check_interval(), remaining)
            self._sleep_fn(max(0.05, nap))

    def can_probe(self) -> bool:
        """Может ли этот воркер сейчас сделать запрос (флаг снят или он пробник)."""
        with self._lock:
            return (not self.is_throttled) or self._probing

    def acquire_probe(self) -> bool:
        """Попытаться стать пробником: единственным, кто делает запросы во время сна.

        Возвращает True, если роль получена (не забудьте release_probe()).
        """
        deadline = self._time() + self.probe_timeout
        with self._cond:
            while True:
                if not self.is_throttled and not self._probing:
                    self._probing = True
                    return True
                if self._time() >= deadline:
                    return False
                self._cond.wait(0.5)

    def release_probe(self) -> None:
        with self._cond:
            self._probing = False
            self._cond.notify_all()

    # --- сигналы от ответов ------------------------------------------------
    def on_throttled(self, retry_after: float | None = None) -> None:
        """Воркер получил 429: поднимаем общий флаг, все остальные засыпают."""
        with self._cond:
            first_wave = not self.is_throttled
            self.is_throttled = True
            if first_wave:
                self._waves += 1
                self._wave_started_at = self._time()
                self._reduce_parallelism_locked()
                duration = self._wave_duration_locked(retry_after)
                self._wave_deadline = self._wave_started_at + duration
            else:
                # Пробник тоже поймал 429: продлеваем волну с удвоенным шагом.
                duration = self._wave_duration_locked(retry_after)
                self._wave_deadline = self._time() + duration
            self._consecutive_successes = 0
            self._calm_since = None
            self._probing = False
            self._cond.notify_all()

    def on_success(self) -> None:
        """Запрос прошёл успешно: снимаем флаг, все просыпаются."""
        with self._lock:
            was_throttled = self.is_throttled
            self.is_throttled = False
            self._probing = False
            self._wave_deadline = None
            now = self._time()
            if not was_throttled:
                self._consecutive_successes += 1
                calm_since = self._calm_since
                if calm_since is None:
                    self._calm_since = now
                    calm_since = now
                if (self._active_slots < self.max_workers
                        and now - calm_since >= self.recovery_window):
                    self._active_slots += 1
                    self._permits = min(self.max_workers, self._permits + 1)
                    self._calm_since = now
            else:
                # Первая же волна обратно перевела счётчик спокойствия.
                self._calm_since = now
            self._cond.notify_all()

    # --- вспомогательное ----------------------------------------------------
    def check_interval(self) -> float:
        """Короткий интервал опроса флага во сне (1–2 секунды)."""
        with self._lock:
            base = min(self.base_sleep, 2.0)
        return base * (0.75 + 0.5 * random.random())

    def should_stop(self) -> bool:
        """Нужно ли воркеру прервать текущую передачу данных.

        Проверяется в цикле записи чанков: при поднятом флаге соединение
        рвётся как можно быстрее, чтобы обнулить трафик. Пробник не
        останавливается — иначе волна никогда не закончится.
        """
        with self._lock:
            return self.is_throttled and not self._probing

    # --- внутреннее (под замком) --------------------------------------------
    def _wave_duration_locked(self, retry_after: float | None) -> float:
        if retry_after is not None and retry_after > 0:
            return min(self.max_sleep, max(self.base_sleep, float(retry_after)))
        waves = max(0, self._waves - 1)  # первая волна = base_sleep
        return min(self.max_sleep, self.base_sleep * (2 ** waves))

    def _remaining_wave_locked(self) -> float:
        deadline = getattr(self, "_wave_deadline", None)
        if deadline is None:
            return self.base_sleep
        return max(0.0, deadline - self._time())

    def _reduce_parallelism_locked(self) -> None:
        if self._active_slots > 1:
            new_slots = max(1, self._active_slots // 2)
            # Отнимаем столько мест, сколько реально свободно прямо сейчас.
            reduction = self._active_slots - new_slots
            taken = min(reduction, self._permits)
            self._permits -= taken
            self._active_slots -= taken
