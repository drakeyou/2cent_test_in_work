"""Мелкие утилиты: тики, время, идентификаторы.

Цены хранятся внутри как ЦЕЛЫЕ ТИКИ (шаг 0.001, диапазон [1, 999]).
Плавающая точка на ценах уровня книги — источник тихих ошибок сравнения,
а весь проект держится на сравнении «на этом ли уровне торговали».
"""
from __future__ import annotations

import time
import uuid
from typing import Any

TICK = 0.001
MIN_TICK = 1
MAX_TICK = 999

SIZE_EPS = 1e-6


def now_ms() -> int:
    return int(time.time() * 1000)


def to_tick(price: float | str | None) -> int | None:
    """0.02 -> 20. Вне диапазона [0.001, 0.999] -> None."""
    if price is None:
        return None
    try:
        p = float(price)
    except (TypeError, ValueError):
        return None
    t = int(round(p * 1000.0))
    if t < MIN_TICK or t > MAX_TICK:
        return None
    return t


def to_price(tick: int | None) -> float | None:
    if tick is None:
        return None
    return round(tick / 1000.0, 3)


def clamp_tick(tick: int) -> int:
    return max(MIN_TICK, min(MAX_TICK, tick))


def fsize(value: Any) -> float:
    """Размер из строки WS в float; мусор -> 0.0."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return 0.0
    if v != v or v in (float("inf"), float("-inf")):
        return 0.0
    return v


def ms_from_any(value: Any, default: int | None = None) -> int:
    """Таймстемп из WS приходит строкой в мс; Gamma отдаёт ISO."""
    if value is None:
        return default if default is not None else now_ms()
    if isinstance(value, (int, float)):
        v = float(value)
        # секунды -> мс
        return int(v * 1000) if v < 1e11 else int(v)
    s = str(value).strip()
    if s.isdigit():
        v = float(s)
        return int(v * 1000) if v < 1e11 else int(v)
    return iso_to_ms(s, default)


def iso_to_ms(value: str | None, default: int | None = None) -> int | None:
    if not value:
        return default
    s = str(value).strip().replace("Z", "+00:00")
    try:
        from datetime import datetime

        return int(datetime.fromisoformat(s).timestamp() * 1000)
    except Exception:
        return default


def ms_to_iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    from datetime import datetime, timezone

    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat(timespec="milliseconds")


def new_event_id() -> str:
    return uuid.uuid4().hex[:16]


def safe_div(numer: float | None, denom: float | None) -> float | None:
    """Деление с явным None вместо inf/NaN.

    Нужно не для красоты: internal_dislocation делит на bid_after, который
    равен нулю ровно в самых глубоких обвалах — то есть в самых интересных
    событиях. inf в колонке предиктора хуже, чем честный NULL.
    """
    if numer is None or denom is None:
        return None
    if abs(denom) < 1e-12:
        return None
    return numer / denom


def dense_grid(before_s: int, after_s: int, step_s: int) -> list[int]:
    """Узлы плотной сетки относительно филла.

    -30..+120 с шагом 2 даёт 76 узлов, а не 75: (120 + 30) / 2 + 1.
    """
    return list(range(-abs(before_s), after_s + 1, step_s))
