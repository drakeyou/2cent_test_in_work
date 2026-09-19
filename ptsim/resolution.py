"""Добор исходов по condition_id.

Без резолюции нельзя отличить «сгорело в ноль» от «не успели выйти в окне» и
нельзя посчитать политику «держать до конца». Закрытые рынки кэшируются и
больше не запрашиваются.
"""
from __future__ import annotations

import logging

from .util import ms_to_iso, now_ms

log = logging.getLogger("resolution")


class ResolutionFetcher:
    def __init__(self, cfg, store, http, registry, engine) -> None:
        self.cfg = cfg
        self.store = store
        self.http = http
        self.registry = registry
        self.engine = engine

    def cached(self, condition_id: str) -> bool:
        row = self.store.query_one(
            "SELECT closed FROM resolutions WHERE condition_id = ?", (condition_id,)
        )
        return bool(row and row["closed"])

    async def fetch_one(self, condition_id: str) -> dict | None:
        if self.cached(condition_id):
            return None
        url = f"{self.cfg.resolution.url.rstrip('/')}/{condition_id}"
        data = await self.http.get_json(url)
        if not isinstance(data, dict):
            return None
        tokens = data.get("tokens") or []
        if len(tokens) < 2:
            return None
        a, b = tokens[0], tokens[1]
        closed = bool(data.get("closed"))
        row = {
            "condition_id": condition_id,
            "question": data.get("question"),
            "market_slug": data.get("market_slug") or data.get("marketSlug"),
            "game_start_time": data.get("game_start_time") or data.get("gameStartTime"),
            "end_date_iso": data.get("end_date_iso") or data.get("endDateIso"),
            "closed": int(closed),
            "token_a": str(a.get("token_id") or ""),
            "token_b": str(b.get("token_id") or ""),
            "winner_a": int(bool(a.get("winner"))),
            "winner_b": int(bool(b.get("winner"))),
            "price_a": float(a.get("price") or 0.0),
            "price_b": float(b.get("price") or 0.0),
            "fetched_at": ms_to_iso(now_ms()),
        }
        self.store.upsert("resolutions", row, ["condition_id"])
        if closed and (row["winner_a"] or row["winner_b"]):
            self.engine.on_resolution(condition_id, {
                row["token_a"]: bool(row["winner_a"]),
                row["token_b"]: bool(row["winner_b"]),
            })
            self.registry.release(condition_id, "resolved")
            log.info("резолюция %s: победил %s", condition_id[:12],
                     "A" if row["winner_a"] else "B")
        return row

    async def run_once(self, limit: int = 200) -> int:
        pending = [
            m.condition_id for m in self.registry.markets.values()
            if not m.resolved and not self.cached(m.condition_id)
        ]
        # Позиции без известного исхода — тоже кандидаты: рынок мог быть
        # освобождён по потолку подписки раньше, чем закрылся.
        for row in self.store.query(
            "SELECT DISTINCT condition_id FROM paper_positions "
            "WHERE resolution_payout IS NULL AND condition_id IS NOT NULL"
        ):
            if row["condition_id"] not in pending and not self.cached(row["condition_id"]):
                pending.append(row["condition_id"])
        n = 0
        for cid in pending[:limit]:
            if await self.fetch_one(cid):
                n += 1
        return n
