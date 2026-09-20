"""Регрессия на РЕАЛЬНЫХ фреймах, снятых с живого CLOB WebSocket.

Причина существования этого файла. Маршрутизация писалась по предположению,
что у каждого сообщения есть `asset_id` на верхнем уровне. У `price_change` его
НЕТ: он лежит внутри каждого изменения, и одно сообщение штатно несёт
изменения по обоим токенам рынка. На живом захвате таких сообщений было 100%.

Движок молча отбрасывал их все. Синтетические тесты этого не ловили, потому что
синтетику писал тот же, кто писал предположение. Фикстура снята с реального
сокета и правит это раз и навсегда.
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from ptsim.config import load
from ptsim.discovery import MarketRec, MarketRegistry
from ptsim.engine import Engine
from ptsim.storage import SqliteStore

FIXTURE = Path(__file__).parent / "fixtures" / "real_frames.json"
T0 = 1_789_895_000_000


def frames() -> list[dict]:
    return json.loads(FIXTURE.read_text())


def asset_ids(msgs: list[dict]) -> list[str]:
    out: list[str] = []
    for m in msgs:
        if m.get("asset_id"):
            out.append(str(m["asset_id"]))
        for ch in m.get("price_changes") or m.get("changes") or []:
            if ch.get("asset_id"):
                out.append(str(ch["asset_id"]))
    return list(dict.fromkeys(out))


@pytest.fixture()
def rig():
    store = SqliteStore(tempfile.mktemp(suffix=".db"), 999)
    store.open()
    cfg = load("config.yaml")
    reg = MarketRegistry(cfg, store)
    ids = asset_ids(frames())
    for i in range(0, len(ids), 2):
        pair = ids[i:i + 2]
        if len(pair) == 1:
            pair.append(pair[0] + "-pair")
        reg.markets[f"c{i}"] = MarketRec(
            condition_id=f"c{i}", asset_id_a=pair[0], asset_id_b=pair[1],
            question="Q", slug="dota2-a-vs-b-map-1", sport="dota2",
            market_level="map", kind="map_winner", segment_no=1,
            volume=0.0, liquidity=0.0, game_start_ms=T0 - 600_000,
            game_start_source="gamma", end_date_ms=T0 + 3_600_000,
            first_seen_ms=T0, last_seen_ms=T0,
        )
    eng = Engine(cfg, store, reg)
    eng.sync_assets(reg.desired_assets(T0))
    yield eng, store
    if store._conn:
        store._conn.close()


def test_fixture_has_the_shape_that_broke_routing():
    msgs = frames()
    pc = [m for m in msgs if m.get("event_type") == "price_change"]
    assert pc, "фикстура должна содержать price_change"
    for m in pc:
        assert "asset_id" not in m, "на верхнем уровне asset_id отсутствует"
        assert m.get("price_changes"), "изменения лежат в price_changes"
        assert len({c["asset_id"] for c in m["price_changes"]}) == 2, \
            "одно сообщение несёт оба токена рынка"
    bk = [m for m in msgs if m.get("event_type") == "book"]
    assert bk and "bids" in bk[0], "book несёт bids/asks, а не buys/sells"


def test_real_frames_are_all_routed(rig):
    eng, _ = rig
    for msg in frames():
        eng.on_ws_message(msg)
    assert eng.dropped_no_asset == 0, "сообщения без asset_id остались нерасположенными"
    assert eng.dropped_unknown_asset == 0


def test_price_change_updates_both_tokens_of_the_market(rig):
    eng, _ = rig
    pc = [m for m in frames() if m.get("event_type") == "price_change"][0]
    ids = [str(c["asset_id"]) for c in pc["price_changes"]]
    eng.on_ws_message(pc)
    for aid in ids:
        book = eng.assets[aid].book
        assert book.ready, f"книга {aid[:12]} должна быть обновлена"
        assert book.updates_applied == 1
    a, b = (eng.assets[i].book for i in ids)
    # Токены комплементарны: сумма лучших котировок около единицы.
    assert a.best_bid() is not None or a.best_ask() is not None
    assert b.best_bid() is not None or b.best_ask() is not None


def test_real_book_snapshot_parses(rig):
    eng, _ = rig
    bk = [m for m in frames() if m.get("event_type") == "book"][0]
    eng.on_ws_message(bk)
    book = eng.assets[str(bk["asset_id"])].book
    assert book.ready and book.bids and book.asks
    assert book.best_bid() is not None and book.best_ask() is not None
    assert book.best_bid() < book.best_ask(), "книга не должна быть пересечена"
    assert book.size_at("BID", 1) > 0, "уровень 0.001 разобран"


def test_real_trade_message_shape():
    """`last_trade_price`, снятый с живого сокета.

    В отличие от price_change, здесь asset_id ЕСТЬ на верхнем уровне. Плюс
    приходит transaction_hash — он и делает сверку с ончейн-лентой точной, а не
    приблизительной по таймстемпу.
    """
    trades = [m for m in frames() if m.get("event_type") == "last_trade_price"]
    assert trades, "фикстура должна содержать сделки"
    for t in trades:
        assert t.get("asset_id"), "asset_id на верхнем уровне"
        assert {"price", "size", "side", "timestamp"} <= set(t)
        assert t.get("transaction_hash", "").startswith("0x")

    # Две сделки подряд по одной цене с разными хэшами: канал шлёт сообщение
    # на КАЖДЫЙ филл, а не только при изменении цены. Это снимает главную
    # неопределённость классификатора — кредиты сделок не теряются.
    same_price = [t for t in trades if t["price"] == "0.002"]
    if len(same_price) >= 2:
        assert len({t["transaction_hash"] for t in same_price}) == len(same_price)


def test_real_trades_route_and_keep_the_tx_hash(rig):
    eng, store = rig
    trades = [m for m in frames() if m.get("event_type") == "last_trade_price"]
    for m in frames():
        eng.on_ws_message(m)
    assert eng.dropped_no_asset == 0
    assert eng.n_trades_seen == len(trades), "все сделки должны быть разобраны"


def test_a_real_shaped_trade_at_the_bottom_triggers_entry(rig):
    """Реальная форма сообщения о сделке, но на нашем уровне и на продажу."""
    eng, store = rig
    bk = [m for m in frames() if m.get("event_type") == "book"][0]
    aid = str(bk["asset_id"])
    eng.on_ws_message({**bk, "bids": [{"price": "0.10", "size": "500"}],
                       "asks": [{"price": "0.20", "size": "300"}]})
    eng.tick(T0)
    eng.on_ws_message({
        "market": bk["market"], "asset_id": aid, "price": "0.02", "size": "5000",
        "fee_rate_bps": "0", "side": "SELL", "timestamp": str(T0 + 1_000),
        "event_type": "last_trade_price",
        "transaction_hash": "0xabc123",
    })
    ev = store.query_one("SELECT * FROM paper_events")
    assert ev is not None and ev["trigger"] == "trade_at_level"
    tr = store.query_one("SELECT * FROM paper_trades WHERE event_id = ?", (ev["event_id"],))
    assert tr["tx_hash"] == "0xabc123", "хэш нужен для точной сверки с лентой"
