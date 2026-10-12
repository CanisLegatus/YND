#!/usr/bin/env python3
"""Точка входа: Yandex Disk Backup (MVP, этапы A/B).

    python main.py --setup     # первоначальная настройка
    python main.py             # меню
    python main.py --check     # неинтерактивная проверка OAuth и API
"""

from __future__ import annotations

import sys

from yandex_disk_backup.cli import main

if __name__ == "__main__":
    sys.exit(main())
