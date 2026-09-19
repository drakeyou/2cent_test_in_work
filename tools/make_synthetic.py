#!/usr/bin/env python3
"""Генератор синтетического датасета для проверки анализатора.

Воспроизводит наблюдённую структуру: пусто на 2 центах примерно в половине
случаев, 74% позиций в ноль, прибыль в хвосте. Нужен, чтобы отчёт можно было
проверить, не дожидаясь суток живого сбора.

    python tools/make_synthetic.py --db /tmp/demo.db --events 120
"""
from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ptsim.config import load  # noqa: E402
from ptsim.discovery import MarketRec, MarketRegistry  # noqa: E402
from ptsim.engine import Engine  # noqa: E402
from ptsim.storage import SqliteStore  # noqa: E402
from ptsim.util import ms_to_iso  # noqa: E402

T0 = 1_760_000_000_000


def book(asset, bids, asks, ts):
    return {"event_type": "book", "asset_id": asset, "timestamp": str(ts),
            "buys": [{"price": p, "size": str(s)} for p, s in bids],
            "sells": [{"price": p, "size": str(s)} for p, s in asks]}


def chg(asset, changes, ts):
    return {"event_type": "price_change", "asset_id": asset, "timestamp": str(ts),
            "changes": [{"price": p, "side": sd, "size": str(s)} for p, sd, s in changes]}


def trd(asset, price, size, side, ts):
    return {"event_type": "last_trade_price", "asset_id": asset, "timestamp": str(ts),
            "price": price, "size": str(size), "side": side}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default="state/demo.db")
    ap.add_argument("--events", type=int, default=120)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    Path(a.db).parent.mkdir(parents=True, exist_ok=True)
    Path(a.db).unlink(missing_ok=True)

    rnd = random.Random(a.seed)
    store = SqliteStore(a.db, commit_interval_s=999)
    store.open()
    cfg = load("config.yaml")
    reg = MarketRegistry(cfg, store)
    eng = Engine(cfg, store, reg)

    clock = T0
    for i in range(a.events):
        cid, A, B = f"cond{i}", f"A{i}", f"B{i}"
        sport = "dota2" if i % 2 else "cs2"
        level, kind = ("map", "map_winner") if i % 3 else ("series", "winner")
        rec = MarketRec(
            condition_id=cid, asset_id_a=A, asset_id_b=B,
            question=f"Market {i}", slug=f"{sport}-t1-vs-t2-{i}",
            sport=sport, market_level=level, kind=kind, segment_no=1 if level == "map" else None,
            volume=rnd.uniform(500, 9000), liquidity=rnd.uniform(50, 800),
            game_start_ms=clock - 600_000, game_start_source="gamma",
            end_date_ms=clock + 7_200_000,
            first_seen_ms=clock - 900_000, last_seen_ms=clock,
        )
        reg.markets[cid] = rec
        store.upsert("markets", rec.to_row(), ["condition_id"])
        store.update("markets", {"condition_id": cid}, {"observed_during_game": 1})
        eng.sync_assets(reg.desired_assets(clock))

        # Пол по близнецу: чем ниже аск близнеца, тем выше пол нашего токена.
        paired_ask = rnd.choice([0.99, 0.985, 0.97, 0.95, 0.90, 0.80, 0.70])
        eng.on_ws_message(book(A, [("0.12", 600), ("0.08", 400)], [("0.20", 300)], clock - 30_000))
        eng.on_ws_message(book(B, [(f"{paired_ask - 0.02:.3f}", 900)],
                               [(f"{paired_ask:.3f}", rnd.choice([200, 1200]))], clock - 30_000))
        for k in range(16):
            ts = clock - 30_000 + k * 2_000
            # Книга близнеца живёт: иначе paired_stale_seconds всегда упирается
            # в возраст начального снапшота и критерий не проверяется.
            eng.on_ws_message(chg(B, [(f"{paired_ask:.3f}", "SELL",
                                       rnd.choice([200, 600, 1200]))], ts))
            eng.tick(ts)

        # В половине случаев на 2 центах кто-то уже стоит.
        prior = rnd.choice([0, 0, 0, 800, 2500, 4000])
        if prior:
            eng.on_ws_message(chg(A, [("0.02", "BUY", prior)], clock - 5_000))
            eng.tick(clock - 4_000)

        eng.on_ws_message(trd(A, "0.02", prior + rnd.uniform(200, 6000), "SELL", clock))
        eng.on_ws_message(chg(A, [("0.12", "BUY", 0), ("0.08", "BUY", 0),
                                  ("0.02", "BUY", 0)], clock + 30))

        # Восстановление аска: в хвосте событий он уходит высоко.
        recovers = rnd.random() < 0.26
        ask_px = rnd.choice(["0.09", "0.12", "0.30", "0.60"]) if recovers else "0.004"
        eng.on_ws_message(chg(A, [("0.20", "SELL", 0), (ask_px, "SELL", 500)], clock + 4_000))
        for k in range(0, 130, 2):
            eng.tick(clock + k * 1_000)
        if recovers:
            eng.on_ws_message(trd(A, ask_px, 1500, "BUY", clock + rnd.randint(20, 200) * 1_000))

        won = recovers and rnd.random() < 0.55
        store.upsert("resolutions", {
            "condition_id": cid, "closed": 1, "token_a": A, "token_b": B,
            "winner_a": int(won), "winner_b": int(not won),
            "price_a": 1.0 if won else 0.0, "price_b": 0.0 if won else 1.0,
            "fetched_at": ms_to_iso(clock),
        }, ["condition_id"])
        eng.on_resolution(cid, {A: won, B: not won})

        # Сверка с лентой: имитируем 96% сходимости.
        for ev in store.query("SELECT event_id FROM paper_events WHERE reconcile_verdict IS NULL"):
            v = "confirmed" if rnd.random() < 0.96 else "phantom_trade"
            store.insert("reconcile_log", {
                "event_id": ev["event_id"], "checked_at": ms_to_iso(clock),
                "verdict": v, "n_chain_trades": 1, "detail": "synthetic"})
            store.update("paper_events", {"event_id": ev["event_id"]},
                         {"reconcile_verdict": v, "trigger_confirmed": int(v == "confirmed")})

        if rnd.random() < 0.3:
            store.insert("target_activity", {
                "address": "0xe0f6ee3a23385afdf446c324f9fb69364a272ee6",
                "condition_id": cid, "asset_id": A, "ts_ms": clock,
                "side": "BUY", "price": 0.02, "size": 900.0, "tx_hash": f"0x{i:064x}"})

        eng.flush_counters(clock + 140_000)
        clock += 900_000

    store.flush()
    n = store.query_one("SELECT COUNT(*) c FROM paper_events")["c"]
    print(f"сгенерировано событий: {n}, база: {a.db}")
    if store._conn:
        store._conn.close()


if __name__ == "__main__":
    main()
