"""Сквозной прогон: обвал книги -> вход -> окно -> выход -> резолюция.

Проверяет не отдельные модули, а то, что в базе оказалось ровно то, что
описано в ТЗ: одна строка события, плотное окно нужной длины, перестановки
аска, закрытая позиция с множителем.
"""
from __future__ import annotations

import os
import tempfile

import pytest

from ptsim.config import load
from ptsim.discovery import MarketRec, MarketRegistry
from ptsim.engine import Engine
from ptsim.storage import SqliteStore

T0 = 1_760_000_000_000  # опорный момент, мс
A = "tokenA"
B = "tokenB"


@pytest.fixture()
def rig():
    path = tempfile.mktemp(suffix=".db")
    store = SqliteStore(path, commit_interval_s=999)
    store.open()
    cfg = load("config.yaml")
    reg = MarketRegistry(cfg, store)
    reg.markets["cond1"] = MarketRec(
        condition_id="cond1", asset_id_a=A, asset_id_b=B,
        question="Will OG win Map 1?", slug="dota2-og-vs-spirit-map-1",
        sport="dota2", market_level="map", kind="map_winner", segment_no=1,
        volume=4200.0, liquidity=310.0,
        game_start_ms=T0 - 600_000, game_start_source="gamma",
        end_date_ms=T0 + 3_600_000, first_seen_ms=T0 - 900_000, last_seen_ms=T0,
    )
    eng = Engine(cfg, store, reg)
    eng.sync_assets(reg.desired_assets(T0))
    yield cfg, store, eng
    store.close() if False else None
    store.flush()
    if store._conn:
        store._conn.close()
    os.unlink(path)


def book_msg(asset, bids, asks, ts):
    return {"event_type": "book", "asset_id": asset, "timestamp": str(ts),
            "buys": [{"price": p, "size": s} for p, s in bids],
            "sells": [{"price": p, "size": s} for p, s in asks]}


def change(asset, changes, ts):
    return {"event_type": "price_change", "asset_id": asset, "timestamp": str(ts),
            "changes": [{"price": p, "side": sd, "size": s} for p, sd, s in changes]}


def trade(asset, price, size, side, ts):
    return {"event_type": "last_trade_price", "asset_id": asset, "timestamp": str(ts),
            "price": price, "size": size, "side": side}


def test_full_event_lifecycle(rig):
    cfg, store, eng = rig

    # Здоровая книга: лучший бид 0.10 (предусловие выполнено), на 0.02 пусто.
    eng.on_ws_message(book_msg(A, [("0.10", "500"), ("0.08", "400")],
                               [("0.15", "300")], T0 - 40_000))
    eng.on_ws_message(book_msg(B, [("0.80", "1000")], [("0.90", "500")], T0 - 40_000))

    # Кольцевой буфер: 20 тиков по 2 секунды до события.
    for i in range(20):
        eng.tick(T0 - 40_000 + i * 2_000)

    st = eng.assets[A]
    assert st.orders.order is not None, "заявка должна лежать при лучшем биде 0.10"
    assert st.orders.order.queue_ahead_at_placement == 0.0, "на 0.02 пусто"

    # Обвал: рыночная продажа проходит книгу до дна.
    eng.on_ws_message(trade(A, "0.02", "5000", "SELL", T0))
    eng.on_ws_message(change(A, [("0.10", "BUY", "0"), ("0.08", "BUY", "0")], T0 + 20))

    ev = store.query_one("SELECT * FROM paper_events")
    assert ev is not None, "событие должно быть записано"
    assert ev["trigger"] == "trade_at_level"
    assert ev["our_fill"] == pytest.approx(1000), "пусто на уровне -> полный филл"
    assert ev["prior_size_at_002"] == pytest.approx(0)
    assert ev["bid_before"] == pytest.approx(0.10)
    assert ev["our_entry_price"] == pytest.approx(0.02)
    assert ev["is_suppressed"] == 0
    assert ev["precondition_held_at_fill"] == 1
    assert ev["market_level"] == "map" and ev["sport"] == "dota2"
    # Пол по близнецу: 1 - 0.90 = 0.10, то есть в пять раз выше нашей цены.
    assert ev["fair_lower_bound"] == pytest.approx(0.10)
    assert ev["fair_upper_bound"] == pytest.approx(0.20)
    assert ev["dislocation_vs_entry"] == pytest.approx(5.0)
    assert ev["paired_ask"] == pytest.approx(0.90)
    assert ev["paired_stale_seconds"] is not None
    assert ev["minutes_from_game_start"] == pytest.approx(10.0)
    assert ev["prior_size_staleness_ms"] >= 0

    # Предсобытийная часть окна пришла из кольцевого буфера.
    pre = store.query("SELECT * FROM paper_book WHERE seconds_from_fill < 0")
    assert len(pre) == 15, "узлы -30..-2 с шагом 2"
    assert all(r["paired_bid"] == pytest.approx(0.80) for r in pre)

    # Аск на нашей стороне появился -> подрезаем на тик.
    eng.on_ws_message(change(A, [("0.10", "SELL", "400")], T0 + 3_000))
    for i in range(0, 125, 2):
        eng.tick(T0 + i * 1_000)

    q = store.query("SELECT * FROM paper_quotes ORDER BY ts_ms")
    assert q[0]["action"] == "place"
    assert q[0]["our_ask"] == pytest.approx(0.099), "тик ниже лучшего аска 0.10"

    # bid_after дописан отложенно: на момент филла книги после сделки не было.
    # Здесь он равен НУЛЮ, а не NULL: бид-сторона выметена целиком. Это самое
    # содержательное наблюдение события, и оно обязано отличаться от пропуска.
    ev = store.query_one("SELECT * FROM paper_events")
    assert ev["bid_after"] == pytest.approx(0.0)
    assert ev["internal_dislocation"] is None, "деление на ноль -> NULL, не inf"
    assert ev["book_sum"] == pytest.approx(0.80), "у книги отсутствует сторона"

    dense = store.query("SELECT * FROM paper_book WHERE is_checkpoint = 0")
    assert len(dense) == cfg.window.expected_dense_rows == 76, "76 узлов, не 75"
    assert ev["window_complete"] == 1

    # Выход: тейкер покупает вверх и снимает наш аск.
    eng.on_ws_message(trade(A, "0.099", "1500", "BUY", T0 + 130_000))
    pos = store.query_one("SELECT * FROM paper_positions")
    assert pos["exit_size"] == pytest.approx(1000)
    assert pos["exit_vwap"] == pytest.approx(0.099)
    assert pos["closed_by"] == "ask_filled"
    assert pos["multiple"] == pytest.approx(4.95), "0.099 / 0.02"
    assert pos["pnl"] == pytest.approx(79.0), "1000 * (0.099 - 0.02)"
    assert pos["hold_seconds"] == pytest.approx(130.0)


def test_cancelled_book_does_not_open_a_position(rig):
    """Мейкер снял заявки — нас бы не залило, события быть не должно."""
    cfg, store, eng = rig
    eng.on_ws_message(book_msg(A, [("0.10", "500"), ("0.02", "3000")],
                               [("0.15", "300")], T0 - 10_000))
    eng.on_ws_message(book_msg(B, [("0.80", "1000")], [("0.90", "500")], T0 - 10_000))
    eng.tick(T0 - 10_000)

    # Книга на дне исчезла, но сделки не было.
    eng.on_ws_message(change(A, [("0.02", "BUY", "0"), ("0.10", "BUY", "0")], T0))
    eng.tick(T0 + 5_000)

    assert store.query_one("SELECT * FROM paper_events") is None
    vanished = store.query("SELECT * FROM book_vanished")
    assert len(vanished) == 1, "отмена логируется отдельно, без открытия позиции"
    assert vanished[0]["cancelled"] == pytest.approx(3000)
    assert vanished[0]["traded"] == pytest.approx(0)


def test_queue_ahead_limits_the_fill_end_to_end(rig):
    cfg, store, eng = rig
    eng.on_ws_message(book_msg(A, [("0.10", "500"), ("0.02", "4000")],
                               [("0.15", "300")], T0 - 10_000))
    eng.on_ws_message(book_msg(B, [("0.80", "1000")], [("0.90", "500")], T0 - 10_000))
    eng.tick(T0 - 10_000)
    eng.on_ws_message(trade(A, "0.02", "4300", "SELL", T0))

    ev = store.query_one("SELECT * FROM paper_events")
    assert ev["our_fill"] == pytest.approx(300), "4300 - 4000 очереди впереди"
    assert ev["prior_size_at_002"] == pytest.approx(4000)
    assert ev["queue_ahead_at_placement"] == pytest.approx(4000)


def test_resolution_closes_the_position_at_payout(rig):
    cfg, store, eng = rig
    eng.on_ws_message(book_msg(A, [("0.10", "500")], [("0.15", "300")], T0 - 10_000))
    eng.on_ws_message(book_msg(B, [("0.80", "1000")], [("0.90", "500")], T0 - 10_000))
    eng.tick(T0 - 10_000)
    eng.on_ws_message(trade(A, "0.02", "5000", "SELL", T0))
    eng.tick(T0 + 1_000)

    eng.on_resolution("cond1", {A: True, B: False})
    pos = store.query_one("SELECT * FROM paper_positions")
    assert pos["closed_by"] == "resolution"
    assert pos["resolution_winner"] == 1
    assert pos["resolution_payout"] == pytest.approx(1.0)
    # Не продали ничего, токен выиграл: 1000 долей по $1 против затрат $20.
    assert pos["pnl"] == pytest.approx(980.0)
    assert pos["multiple"] == pytest.approx(50.0)


def test_position_row_exists_before_any_exit(rig):
    """74% позиций сгорают в ноль и выхода не видят.

    Если строка позиции появляется только при первом филле на выходе, у трёх
    четвертей записей не будет даже цены и размера входа.
    """
    cfg, store, eng = rig
    eng.on_ws_message(book_msg(A, [("0.10", "500")], [("0.15", "300")], T0 - 10_000))
    eng.on_ws_message(book_msg(B, [("0.80", "1000")], [("0.90", "500")], T0 - 10_000))
    eng.tick(T0 - 10_000)
    eng.on_ws_message(trade(A, "0.02", "5000", "SELL", T0))

    pos = store.query_one("SELECT * FROM paper_positions")
    assert pos is not None, "строка позиции должна быть сразу после входа"
    assert pos["entry_price"] == pytest.approx(0.02)
    assert pos["entry_size"] == pytest.approx(1000)
    assert pos["exit_size"] == pytest.approx(0)
    assert pos["closed_by"] is None, "ещё не закрыта"
    assert pos["pnl"] is None, "без выхода и резолюции PnL неизвестен, а не ноль"


def test_crossed_book_joins_the_ask_rather_than_crossing(rig):
    """Пересечённая книга: подрезать нельзя, иначе продаём по биду."""
    cfg, store, eng = rig
    eng.on_ws_message(book_msg(A, [("0.10", "500")], [("0.15", "300")], T0 - 10_000))
    eng.on_ws_message(book_msg(B, [("0.80", "1000")], [("0.90", "500")], T0 - 10_000))
    eng.tick(T0 - 10_000)
    eng.on_ws_message(trade(A, "0.02", "5000", "SELL", T0))
    eng.on_ws_message(change(A, [("0.10", "SELL", "400")], T0 + 1_000))
    eng.tick(T0 + 2_000)

    ctx = list(eng.events.values())[0]
    assert ctx.exit.our_ask_tick == 100, "встаём В аск 0.10, а не под него"


def test_engine_constructs_before_the_store_is_open():
    """Регрессия: движок читал калибровку в конструкторе, до open() базы,
    и коллектор падал на старте, не дойдя до первого сообщения."""
    import tempfile

    from ptsim.storage import SqliteStore

    store = SqliteStore(tempfile.mktemp(suffix=".db"), 999)
    cfg = load("config.yaml")
    reg = MarketRegistry(cfg, store)
    eng = Engine(cfg, store, reg)  # не должно бросать
    with pytest.raises(RuntimeError, match="до open"):
        store.query("SELECT 1")
    store.open()
    eng.load_state()
    assert eng.calibrator.pending is True
    store._conn.close()
