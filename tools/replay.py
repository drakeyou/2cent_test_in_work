#!/usr/bin/env python3
"""Прогон записанных WS-фреймов через весь конвейер офлайн.

Существует ради цикла отладки. Без реплея проверка изменения в классификаторе
стоит суток ожидания живого матча; с ним — секунды, и на одних и тех же данных.

    python tools/replay.py --frames captures/x.jsonl --markets markets.json --db /tmp/r.db
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ptsim import config as cfgmod  # noqa: E402
from ptsim import logging_setup  # noqa: E402
from ptsim.discovery import MarketRec, MarketRegistry  # noqa: E402
from ptsim.engine import Engine  # noqa: E402
from ptsim.storage import SqliteStore  # noqa: E402
from ptsim.util import ms_from_any  # noqa: E402


def load_frames(path: Path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            raw = rec.get("raw", rec)
            payload = json.loads(raw) if isinstance(raw, str) else raw
            for item in (payload if isinstance(payload, list) else [payload]):
                if isinstance(item, dict) and (item.get("event_type") or item.get("type")):
                    yield rec.get("recv_ms"), item


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--frames", type=Path, required=True)
    ap.add_argument("--markets", type=Path, help="JSON-список рынков из Gamma")
    ap.add_argument("--db", default="state/replay.db")
    ap.add_argument("--config", default="config.yaml")
    a = ap.parse_args()

    cfg = cfgmod.load(a.config)
    logging_setup.setup(cfg.log_level)
    store = SqliteStore(a.db, commit_interval_s=999)
    store.open()
    reg = MarketRegistry(cfg, store)

    if a.markets and a.markets.exists():
        reg.ingest(json.loads(a.markets.read_text()))
    eng = Engine(cfg, store, reg)

    frames = list(load_frames(a.frames))
    if not frames:
        print("фреймов не найдено")
        return

    # Рынки без метаданных: собираем заглушки по встреченным asset_id, иначе
    # реплей молча проглотил бы все сообщения.
    known = set()
    for rec in reg.markets.values():
        known.update(rec.assets)
    unknown = sorted({m.get("asset_id") for _, m in frames if m.get("asset_id")} - known)
    for i in range(0, len(unknown), 2):
        pair = unknown[i:i + 2]
        if len(pair) < 2:
            pair.append(pair[0] + "-synthetic-pair")
        cid = f"replay-{i // 2}"
        first_ms = ms_from_any(frames[0][1].get("timestamp"), frames[0][0])
        reg.markets[cid] = MarketRec(
            condition_id=cid, asset_id_a=pair[0], asset_id_b=pair[1],
            question="(replay)", slug="replay", sport=cfg.disciplines[0],
            market_level="unknown", kind="unknown", segment_no=None,
            volume=0.0, liquidity=0.0, game_start_ms=None, game_start_source="none",
            end_date_ms=None, first_seen_ms=first_ms, last_seen_ms=first_ms,
        )
    eng.sync_assets(reg.desired_assets())

    last_tick = 0
    for recv_ms, msg in frames:
        ts = ms_from_any(msg.get("timestamp"), recv_ms)
        eng.on_ws_message(msg)
        if ts - last_tick >= 500:
            eng.tick(ts)
            last_tick = ts
    eng.tick(last_tick + 200_000)
    eng.flush_counters(last_tick + 200_000)
    store.flush()

    ev = store.query_one("SELECT COUNT(*) c FROM paper_events")["c"]
    sup = store.query_one("SELECT COUNT(*) c FROM paper_events WHERE is_suppressed=1")["c"]
    van = store.query_one("SELECT COUNT(*) c FROM book_vanished")["c"]
    print(f"фреймов: {len(frames)}  событий: {ev} (теневых {sup})  отмен на дне: {van}")
    print(f"сделок: {eng.n_trades_seen}  unknown-классификаций: {eng.unknown_classified:.0f}")
    print(f"база: {a.db}")
    if store._conn:
        store._conn.close()


if __name__ == "__main__":
    main()
