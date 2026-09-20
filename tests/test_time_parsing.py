"""Разбор времени из Gamma. Форматы взяты из живого ответа API."""
from __future__ import annotations

from ptsim.util import iso_to_ms, ms_from_any


def test_end_date_format():
    assert iso_to_ms("2027-01-01T04:59:00Z") == 1798779540000


def test_game_start_time_format_with_space_and_short_tz():
    """'2025-12-19 23:40:00+00' — реальный формат gameStartTime.

    fromisoformat принимает его только с Python 3.11. Без явной нормализации
    на 3.10 вернулся бы None, и рынок молча получил бы
    game_start_source='none' — то есть подписку не в момент матча.
    """
    assert iso_to_ms("2025-12-19 23:40:00+00") == 1766187600000


def test_broken_value_returns_default_not_an_exception():
    assert iso_to_ms("не дата", default=-1) == -1
    assert iso_to_ms(None) is None


def test_trade_timestamps_seconds_and_millis():
    # data-api отдаёт секунды, WS — миллисекунды
    assert ms_from_any(1730906435) == 1730906435000
    assert ms_from_any("1789895532824") == 1789895532824
