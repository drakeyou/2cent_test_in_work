"""Сверка книги, которую мы ведём по WS, с REST-книгой.

Секвенс-номеров на канале нет, только hash, алгоритм которого воспроизводить
незачем. Рассинхрон тихо отравляет prior_size_at_002, а это прямой множитель в
модели очереди — поэтому проверка периодическая и расхождения пишутся.
"""
from __future__ import annotations

import logging

from .util import fsize, ms_to_iso, now_ms, to_tick

log = logging.getLogger("integrity")


class IntegrityChecker:
    def __init__(self, cfg, store, http, engine) -> None:
        self.cfg = cfg
        self.store = store
        self.http = http
        self.engine = engine
        self._cursor = 0
        self.checks = 0
        self.desyncs = 0

    async def run_once(self) -> int:
        if not self.cfg.integrity.enabled:
            return 0
        assets = sorted(self.engine.assets)
        if not assets:
            return 0
        budget = self.cfg.integrity.max_checks_per_minute
        checked = 0
        for _ in range(min(budget, len(assets))):
            aid = assets[self._cursor % len(assets)]
            self._cursor += 1
            st = self.engine.assets.get(aid)
            if st is None or not st.book.ready:
                continue
            data = await self.http.get_json(self.cfg.integrity.rest_book_url,
                                            {"token_id": aid})
            if not isinstance(data, dict):
                continue
            checked += 1
            self.checks += 1
            rest_bids = {}
            for lvl in data.get("bids") or []:
                tk = to_tick(lvl.get("price"))
                if tk is not None:
                    rest_bids[tk] = fsize(lvl.get("size"))
            rest_asks = {}
            for lvl in data.get("asks") or []:
                tk = to_tick(lvl.get("price"))
                if tk is not None:
                    rest_asks[tk] = fsize(lvl.get("size"))
            ws_bb = st.book.best_bid_tick()
            rest_bb = max(rest_bids) if rest_bids else None
            ws_ba = st.book.best_ask_tick()
            rest_ba = min(rest_asks) if rest_asks else None
            diff = max(
                (abs(st.book.bids.get(k, 0.0) - v) for k, v in rest_bids.items()),
                default=0.0,
            )
            severity = "ok"
            if ws_bb != rest_bb or ws_ba != rest_ba:
                severity = "touch_mismatch"
            elif diff > 1.0:
                severity = "size_drift"
            if severity != "ok":
                self.desyncs += 1
                self.store.insert("book_desync", {
                    "ts": ms_to_iso(now_ms()), "asset_id": aid,
                    "ws_best_bid": st.book.best_bid(),
                    "rest_best_bid": rest_bb / 1000.0 if rest_bb else None,
                    "ws_best_ask": st.book.best_ask(),
                    "rest_best_ask": rest_ba / 1000.0 if rest_ba else None,
                    "ws_levels": len(st.book.bids), "rest_levels": len(rest_bids),
                    "max_level_diff": diff, "severity": severity,
                })
                log.warning("рассинхрон книги %s: %s", aid[:10], severity)
        return checked
