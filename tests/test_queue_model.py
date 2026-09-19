"""Модель очереди на входе: price-time priority.

Главный тест здесь — test_prior_size_is_frozen_at_sweep_start. Формула ТЗ
верна, но реализация, смешивающая prior_size из середины прохода с объёмом за
весь проход, завышает филл в разы. Это ровно та ошибка, которая делает
симуляцию красивой и бесполезной.
"""
from __future__ import annotations

import pytest

from ptsim.book import Book
from ptsim.tape import Trade
from ptsim.virtual_orders import AssetOrderManager


def setup(prior_at_002: float = 3000.0, best_bid: str = "0.08"):
    b = Book("a")
    levels = [{"price": best_bid, "size": "500"}]
    if prior_at_002 > 0:
        levels.append({"price": "0.02", "size": str(prior_at_002)})
    b.apply_snapshot(levels, [{"price": "0.12", "size": "200"}], 1_000)
    m = AssetOrderManager(
        "a", b,
        entry_tick=20, entry_size=1000.0, min_best_bid_tick=80,
        cooldown_s=300, cancel_on_break=False, sweep_window_ms=1500,
        max_events_per_hour=12,
    )
    m.on_book_update(1_000)
    return b, m


def sell(tick: int, size: float, ts: int) -> Trade:
    return Trade(tick=tick, size=size, ts_ms=ts, side_hit="BID", reported_side="SELL")


def test_empty_level_gives_a_full_fill():
    """На 2 центах пусто в половине случаев — весь вынос достаётся нам."""
    b, m = setup(prior_at_002=0)
    evs = [e for e in m.on_trade(sell(20, 5000, 2_000)) if not e.is_shadow]
    assert len(evs) == 1
    assert evs[0].our_fill == pytest.approx(1000)
    assert evs[0].prior_size_at_002 == pytest.approx(0)
    assert evs[0].trigger == "trade_at_level"


def test_queue_ahead_eats_most_of_the_sweep():
    b, m = setup(prior_at_002=3000)
    evs = [e for e in m.on_trade(sell(20, 3200, 2_000)) if not e.is_shadow]
    assert evs[0].our_fill == pytest.approx(200)
    assert evs[0].level_traded_size == pytest.approx(3200)
    assert evs[0].prior_size_at_002 == pytest.approx(3000)


def test_queue_ahead_can_eat_everything():
    b, m = setup(prior_at_002=3000)
    assert m.on_trade(sell(20, 2500, 2_000)) == []
    assert m.order is not None and m.order.filled == pytest.approx(0)


def test_prior_size_is_frozen_at_sweep_start():
    """Ловушка реализации: prior_size из середины прохода + объём за весь проход.

    Правильный ответ 200. Наивная реализация, перечитывающая prior_size после
    того, как уровень уже съеден (0), получила бы 3200 и упёрлась бы в наш
    размер 1000 — завышение в пять раз на ровном месте.
    """
    b, m = setup(prior_at_002=3000)
    assert m.on_trade(sell(20, 3000, 2_000)) == [], "очередь впереди съела весь объём"
    b.apply_price_change(
        [{"price": "0.02", "side": "BUY", "size": "0"},
         {"price": "0.08", "side": "BUY", "size": "0"}], 2_010,
    )
    m.on_book_update(2_010)
    evs = [e for e in m.on_trade(sell(10, 200, 2_020)) if not e.is_shadow]
    assert len(evs) == 1
    assert evs[0].our_fill == pytest.approx(200), "должно быть 200, а не 1000"
    assert evs[0].prior_size_at_002 == pytest.approx(3000)
    assert evs[0].is_new_event is True, "событие открывается на первом ФИЛЛЕ"


def test_non_contiguous_sweep_counts_only_at_or_below_entry():
    """Бид на 0.03 и 0.01, на 0.02 пусто. Объём выше нашей цены не наш."""
    b = Book("a")
    b.apply_snapshot(
        [{"price": "0.08", "size": "500"}, {"price": "0.03", "size": "500"},
         {"price": "0.01", "size": "500"}],
        [{"price": "0.12", "size": "200"}], 1_000,
    )
    m = AssetOrderManager(
        "a", b, entry_tick=20, entry_size=1000.0, min_best_bid_tick=80,
        cooldown_s=300, cancel_on_break=False, sweep_window_ms=1500,
        max_events_per_hour=12,
    )
    m.on_book_update(1_000)
    assert m.on_trade(sell(30, 500, 2_000)) == [], "0.03 выше нашей цены"
    evs = [e for e in m.on_trade(sell(10, 500, 2_010)) if not e.is_shadow]
    assert evs[0].our_fill == pytest.approx(500)


def test_partial_fills_accumulate_within_one_event():
    b, m = setup(prior_at_002=0)
    e1 = [e for e in m.on_trade(sell(20, 300, 2_000)) if not e.is_shadow][0]
    e2 = [e for e in m.on_trade(sell(20, 400, 2_200)) if not e.is_shadow][0]
    assert e1.event_id == e2.event_id, "пачка сделок = одно событие"
    assert (e1.our_fill, e2.our_fill) == (pytest.approx(300), pytest.approx(400))
    assert e2.our_fill_cum == pytest.approx(700)
    assert (e1.n_partial, e2.n_partial) == (1, 2)


def test_no_order_without_the_precondition():
    b, m = setup(prior_at_002=0, best_bid="0.03")
    assert m.order is None and m.shadow is None
    assert m.on_trade(sell(20, 5000, 2_000)) == []


def test_precondition_break_does_not_cancel_by_default():
    """Реальный пассивный мейкер не успевает снять заявку при обвале."""
    b, m = setup(prior_at_002=0)
    b.apply_price_change([{"price": "0.08", "side": "BUY", "size": "0"}], 2_000)
    m.on_book_update(2_000)
    assert m.order is not None
    evs = [e for e in m.on_trade(sell(20, 5000, 2_010)) if not e.is_shadow]
    assert evs[0].precondition_held_at_fill is False, "факт пишется, заявка остаётся"


def test_cooldown_suppresses_primary_but_shadow_still_fires():
    """Теневая заявка без кулдауна делает частоту пересчитываемой офлайн."""
    b, m = setup(prior_at_002=0)
    assert [e for e in m.on_trade(sell(20, 5000, 2_000)) if not e.is_shadow]
    b.apply_price_change([{"price": "0.08", "side": "BUY", "size": "500"},
                          {"price": "0.02", "side": "BUY", "size": "0"}], 3_000)
    m.on_book_update(3_000)
    assert m.order is None, "кулдаун"
    evs = m.on_trade(sell(20, 5000, 3_010))
    assert len(evs) == 1 and evs[0].is_shadow
    assert m.suppression_reason(3_010) == "cooldown"


def test_cancels_at_our_level_shrink_the_optimistic_queue_estimate():
    from ptsim.tape import Classification

    b, m = setup(prior_at_002=3000)
    assert m.order.queue_ahead_at_placement == pytest.approx(3000)
    m.on_classification(Classification(
        asset_id="a", side="BID", tick=20, reduced=1200, traded=0, cancelled=1200,
        unknown=0, evidence="none", ts_ms=2_000, settled_ms=3_500, batch_id=1,
        opposite_side_changed_ms=None,
    ))
    assert m.order.queue_ahead_est == pytest.approx(1800)
    assert m.order.queue_ahead_at_placement == pytest.approx(3000), "консервативная не трогается"
