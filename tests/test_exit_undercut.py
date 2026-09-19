"""Политика выхода undercut."""
from __future__ import annotations

import pytest

from ptsim.book import Book
from ptsim.exit_policy import UndercutExit
from ptsim.tape import Trade


def mk(bids, asks, ts=10_000, **kw):
    b = Book("a")
    b.apply_snapshot(
        [{"price": p, "size": s} for p, s in bids],
        [{"price": p, "size": s} for p, s in asks],
        ts,
    )
    x = UndercutExit("ev", b, position_size=1000.0, fill_ms=ts, sweep_window_ms=1500, **kw)
    return b, x


def buy(tick, size, ts):
    return Trade(tick=tick, size=size, ts_ms=ts, side_hit="ASK", reported_side="BUY")


def test_places_one_tick_below_the_best_ask():
    b, x = mk([("0.03", "100")], [("0.10", "400")])
    moves = x.on_book_update(10_000)
    assert len(moves) == 1
    assert moves[0].action == "place" and moves[0].our_ask == pytest.approx(0.099)
    assert moves[0].prior_size_at_our_ask == pytest.approx(0), "подрезали в пустой уровень"


def test_one_tick_spread_joins_the_ask_instead_of_crossing():
    """Подрезать при спреде в тик значит продать по биду. Это другая политика."""
    b, x = mk([("0.090", "100")], [("0.091", "400")])
    moves = x.on_book_update(10_000)
    assert moves[0].our_ask == pytest.approx(0.091)
    assert moves[0].prior_size_at_our_ask == pytest.approx(400), "встали в конец очереди"


def test_follows_the_ask_down_and_loses_queue_priority():
    b, x = mk([("0.03", "100")], [("0.10", "400")])
    x.on_book_update(10_000)
    b.apply_price_change(
        [{"price": "0.10", "side": "SELL", "size": "0"},
         {"price": "0.06", "side": "SELL", "size": "250"}], 11_000,
    )
    moves = x.on_book_update(11_000)
    assert moves[0].action == "move" and moves[0].reason == "follow_down"
    assert moves[0].our_ask == pytest.approx(0.059)


def test_does_not_follow_the_ask_up_by_default():
    b, x = mk([("0.03", "100")], [("0.10", "400")])
    x.on_book_update(10_000)
    b.apply_price_change([{"price": "0.10", "side": "SELL", "size": "0"}], 11_000)
    assert x.on_book_update(11_000) == []
    assert x.our_ask_tick == 99


def test_fill_subtracts_the_queue_ahead_at_our_ask():
    b, x = mk([("0.090", "100")], [("0.091", "400")])
    x.on_book_update(10_000)
    fills, _ = x.on_trade(buy(91, 400, 11_000))
    assert fills == [], "первые 400 достались тем, кто уже стоял"
    fills, _ = x.on_trade(buy(91, 300, 11_100))
    assert fills[0].size == pytest.approx(300)
    assert fills[0].prior_size_at_our_ask == pytest.approx(400)


def test_partial_exits_are_separate_fills_and_close_the_position():
    b, x = mk([("0.03", "100")], [("0.10", "400")])
    x.on_book_update(10_000)
    f1, _ = x.on_trade(buy(99, 600, 11_000))
    f2, _ = x.on_trade(buy(99, 900, 40_000))
    assert f1[0].size == pytest.approx(600) and f2[0].size == pytest.approx(400)
    assert x.filled == pytest.approx(1000) and x.closed_by == "ask_filled"
    assert x.exit_vwap == pytest.approx(0.099)
    assert x.n_fills == 2


def test_trade_below_our_ask_does_not_fill_us():
    b, x = mk([("0.03", "100")], [("0.10", "400")])
    x.on_book_update(10_000)
    assert x.on_trade(buy(50, 5000, 11_000)) == ([], [])


def test_max_participation_caps_our_share_of_the_flow():
    """Наши 1000 долей в пустой после обвала книге не бесплатны."""
    b, x = mk([("0.03", "100")], [("0.10", "400")], max_participation=0.5)
    x.on_book_update(10_000)
    fills, _ = x.on_trade(buy(99, 800, 11_000))
    assert fills[0].size == pytest.approx(400), "половина потока, не весь"


def test_dust_remainder_stops_quoting():
    b, x = mk([("0.03", "100")], [("0.10", "400")], min_clip_shares=5.0)
    x.on_book_update(10_000)
    x.on_trade(buy(99, 997, 11_000))
    assert x.remaining == pytest.approx(3)
    assert not x.sellable and x.closed_by == "dust"


def test_quote_moves_are_capped():
    b, x = mk([("0.03", "100")], [("0.90", "400")], max_quote_moves=3)
    x.on_book_update(10_000)
    n = 1
    for i, price in enumerate(["0.80", "0.70", "0.60", "0.50"]):
        b.apply_price_change(
            [{"price": "0.90", "side": "SELL", "size": "0"} if i == 0 else
             {"price": ["0.80", "0.70", "0.60", "0.50"][i - 1], "side": "SELL", "size": "0"},
             {"price": price, "side": "SELL", "size": "100"}], 11_000 + i * 100,
        )
        n += len(x.on_book_update(11_000 + i * 100))
    assert n == 3 and x.moves_capped
