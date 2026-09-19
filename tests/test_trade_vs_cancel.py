"""Тесты классификатора сделка-vs-отмена.

Сценарии выбраны по тому, чем эти два события реально путаются в проде:
гонка порядка сообщений, свип через уровни и полный снапшот посреди события.
"""
from __future__ import annotations

import pytest

from ptsim.book import Book
from ptsim.tape import SideCalibrator, TapeClassifier, Trade


def make(**kw):
    cal = SideCalibrator(target=10_000)  # не фиксируем калибровку в тестах
    b = Book("asset")
    tc = TapeClassifier(
        "asset", window_ms=1500, credit_ttl_ms=3000, calibrator=cal, book=b, **kw
    )
    b.apply_snapshot(
        [
            {"price": "0.08", "size": "500"},
            {"price": "0.05", "size": "800"},
            {"price": "0.03", "size": "400"},
            {"price": "0.02", "size": "3000"},
        ],
        [{"price": "0.12", "size": "200"}],
        1_000,
    )
    return b, tc


def test_pure_trade_is_not_a_cancel():
    b, tc = make()
    tc.on_trade(Trade(tick=20, size=3000, ts_ms=2_000, side_hit="BID", reported_side="SELL"))
    out = tc.on_deltas(b.apply_price_change([{"price": "0.02", "side": "BUY", "size": "0"}], 2_050))
    assert len(out) == 1
    assert out[0].traded == pytest.approx(3000)
    assert out[0].cancelled == pytest.approx(0)
    assert out[0].evidence == "ws_trade"


def test_pure_cancel_is_not_a_trade():
    b, tc = make()
    tc.on_deltas(b.apply_price_change([{"price": "0.02", "side": "BUY", "size": "0"}], 2_000))
    assert tc.tick(2_100) == [], "решение не должно приниматься до дедлайна"
    out = tc.tick(3_600)
    assert len(out) == 1
    assert out[0].cancelled == pytest.approx(3000)
    assert out[0].traded == pytest.approx(0)
    assert out[0].evidence == "none"


def test_book_update_before_trade_message():
    """Порядок сообщений не гарантирован: книга пришла раньше сделки."""
    b, tc = make()
    out = tc.on_deltas(b.apply_price_change([{"price": "0.02", "side": "BUY", "size": "0"}], 2_000))
    assert out == [], "сразу классифицировать нельзя, сделка может ещё прийти"
    out = tc.on_trade(Trade(tick=20, size=3000, ts_ms=2_400, side_hit="BID", reported_side="SELL"))
    assert len(out) == 1 and out[0].traded == pytest.approx(3000)
    assert tc.tick(9_000) == []


def test_trade_outside_window_does_not_rescue_a_cancel():
    b, tc = make()
    tc.on_deltas(b.apply_price_change([{"price": "0.02", "side": "BUY", "size": "0"}], 2_000))
    out = tc.tick(3_600)
    assert out[0].cancelled == pytest.approx(3000)


def test_partial_trade_leaves_the_rest_as_cancel():
    """Съели 1000, мейкер снял остальные 2000 — обе части должны быть видны."""
    b, tc = make()
    tc.on_trade(Trade(tick=20, size=1000, ts_ms=2_000, side_hit="BID", reported_side="SELL"))
    tc.on_deltas(b.apply_price_change([{"price": "0.02", "side": "BUY", "size": "0"}], 2_050))
    out = tc.tick(3_700)
    assert len(out) == 1
    assert out[0].traded == pytest.approx(1000)
    assert out[0].cancelled == pytest.approx(2000)


def test_sweep_through_levels_is_attributed_to_the_trade():
    """Тейкер прошёл книгу, last_trade_price отдал одну цену на весь проход."""
    b, tc = make()
    tc.on_trade(Trade(tick=80, size=500, ts_ms=2_000, side_hit="BID", reported_side="SELL"))
    deltas = b.apply_price_change(
        [
            {"price": "0.08", "side": "BUY", "size": "0"},
            {"price": "0.05", "side": "BUY", "size": "0"},
            {"price": "0.03", "side": "BUY", "size": "0"},
            {"price": "0.02", "side": "BUY", "size": "1200"},
        ],
        2_020,
    )
    tc.on_deltas(deltas)
    out = sorted(tc.tick(3_600), key=lambda c: -c.tick)
    assert [c.tick for c in out] == [50, 30, 20]
    assert all(c.cancelled == pytest.approx(0) for c in out)
    assert all(c.evidence == "sweep_inferred" for c in out)
    assert out[-1].traded == pytest.approx(1800)


def test_two_sided_pull_is_not_inferred_as_a_sweep():
    """Мейкер снял котировки с обеих сторон — это отмена, а не свип."""
    b, tc = make()
    deltas = b.apply_price_change(
        [
            {"price": "0.08", "side": "BUY", "size": "0"},
            {"price": "0.05", "side": "BUY", "size": "0"},
            {"price": "0.12", "side": "SELL", "size": "0"},
        ],
        2_000,
    )
    tc.on_deltas(deltas)
    out = tc.tick(3_600)
    assert all(c.traded == pytest.approx(0) for c in out)
    assert all(c.evidence == "none" for c in out)
    bid = [c for c in out if c.side == "BID"]
    assert all(c.opposite_side_changed_ms == 0 for c in bid), "признак синхронной отмены"


def test_sweep_inference_needs_a_real_trade_credit():
    """Без кредита сделки полоса сокращений остаётся отменой."""
    b, tc = make()
    tc.on_deltas(
        b.apply_price_change(
            [
                {"price": "0.08", "side": "BUY", "size": "0"},
                {"price": "0.05", "side": "BUY", "size": "0"},
                {"price": "0.03", "side": "BUY", "size": "0"},
            ],
            2_000,
        )
    )
    out = tc.tick(3_600)
    assert all(c.traded == pytest.approx(0) for c in out)


def test_snapshot_reset_yields_unknown_not_a_guess():
    b, tc = make()
    tc.on_deltas(b.apply_price_change([{"price": "0.02", "side": "BUY", "size": "0"}], 2_000))
    b.apply_snapshot([{"price": "0.01", "size": "100"}], [], 2_100)
    out = tc.on_snapshot_reset(2_100)
    assert len(out) == 1
    assert out[0].evidence == "snapshot_reset"
    assert out[0].unknown == pytest.approx(3000)
    assert out[0].cancelled == pytest.approx(0)
    assert tc.stat_unknown == pytest.approx(3000)


def test_adds_are_not_classified():
    """Добавление в очередь — это очередь ПОЗАДИ нас, классифицировать нечего."""
    b, tc = make()
    out = tc.on_deltas(b.apply_price_change([{"price": "0.02", "side": "BUY", "size": "9000"}], 2_000))
    assert out == []
    assert tc.tick(4_000) == []


def test_calibration_votes_for_taker_semantics():
    cal = SideCalibrator(target=4)
    tc = TapeClassifier("a", window_ms=1000, credit_ttl_ms=2000, calibrator=cal)
    b = Book("a")
    for i in range(4):
        t0 = 10_000 + i * 10_000
        b.apply_snapshot([{"price": "0.02", "size": "1000"}], [{"price": "0.9", "size": "50"}], t0)
        tc.on_trade(Trade(tick=20, size=400, ts_ms=t0 + 10, side_hit="BID", reported_side="SELL"))
        tc.on_deltas(b.apply_price_change([{"price": "0.02", "side": "BUY", "size": "600"}], t0 + 20))
        tc.tick(t0 + 5_000)
    assert cal.locked and cal.taker_semantics
    assert cal.side_hit("SELL") == "BID"


def test_sweep_inference_rejects_a_gapped_band():
    """Между «выметенными» уровнями остался живой бид — проход так не выглядит."""
    b, tc = make()
    tc.on_trade(Trade(tick=80, size=500, ts_ms=2_000, side_hit="BID", reported_side="SELL"))
    tc.on_deltas(
        b.apply_price_change(
            [
                {"price": "0.08", "side": "BUY", "size": "0"},
                {"price": "0.03", "side": "BUY", "size": "0"},
            ],
            2_020,
        )
    )
    out = tc.tick(3_600)
    assert [c.tick for c in out] == [30]
    assert out[0].cancelled == pytest.approx(400), "0.05 живой -> это не свип"


def test_sweep_inference_rejects_when_higher_bids_survive():
    """Свип начинается с лучшего бида; отмена середины книги — нет."""
    b, tc = make()
    tc.on_trade(Trade(tick=50, size=800, ts_ms=2_000, side_hit="BID", reported_side="SELL"))
    tc.on_deltas(
        b.apply_price_change(
            [
                {"price": "0.05", "side": "BUY", "size": "0"},
                {"price": "0.03", "side": "BUY", "size": "0"},
            ],
            2_020,
        )
    )
    out = tc.tick(3_600)
    assert [c.tick for c in out] == [30]
    assert out[0].cancelled == pytest.approx(400), "бид 0.08 жив -> проход не оттуда"
