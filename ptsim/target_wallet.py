"""Активность объекта исследования по четырём адресам.

Осторожно с интерпретацией. Третья группа анализа — «он не зашёл, мы бы да» —
это то число, которое хочется видеть большим. Оно целиком держится на ПОЛНОТЕ
этой ленты: любой пропуск его сделки перекладывает событие из группы «он
зашёл» в группу «он не зашёл», то есть ошибка сбора работает в пользу
гипотезы. Поэтому пропуски опроса учитываются явно и анализатор их печатает.
"""
from __future__ import annotations

import logging

from .util import fsize, ms_from_any, now_ms

log = logging.getLogger("target")


class TargetWalletTracker:
    def __init__(self, cfg, store, http) -> None:
        self.cfg = cfg
        self.store = store
        self.http = http
        self.last_poll_ms: int = 0
        self.gap_seconds: float = 0.0

    async def poll_address(self, addr: str) -> int:
        # Потолок пагинации /activity около 5 500 записей: берём свежий срез
        # часто, а не пытаемся добрать историю вглубь одним проходом.
        data = await self.http.get_json(self.cfg.target_wallet.activity_url,
                                        {"user": addr, "limit": 500})
        if not isinstance(data, list):
            return 0
        n = 0
        for a in data:
            if not isinstance(a, dict):
                continue
            cid = a.get("conditionId") or a.get("condition_id")
            if not cid:
                continue
            self.store.insert("target_activity", {
                "address": addr.lower(), "condition_id": str(cid),
                "asset_id": str(a.get("asset") or a.get("assetId") or ""),
                "ts_ms": ms_from_any(a.get("timestamp"), 0),
                "side": str(a.get("side") or a.get("type") or ""),
                "price": fsize(a.get("price")), "size": fsize(a.get("size")),
                "tx_hash": str(a.get("transactionHash") or a.get("hash") or ""),
            })
            n += 1
        return n

    async def run_once(self) -> int:
        n = now_ms()
        if self.last_poll_ms:
            expected = self.cfg.target_wallet.poll_s * 1000
            overdue = (n - self.last_poll_ms) - expected
            if overdue > expected:
                self.gap_seconds += overdue / 1000.0
                self.store.set_kv("target_activity_gap_seconds", self.gap_seconds)
        self.last_poll_ms = n
        total = 0
        for addr in self.cfg.target_wallet.addresses:
            total += await self.poll_address(addr)
        self._backfill_flags()
        return total

    def _backfill_flags(self) -> None:
        """Поздняя простановка флага: объект мог зайти в рынок ПОСЛЕ нашего
        события. Ещё один случай, который CSV не умеет."""
        self.store.execute_raw(
            "UPDATE paper_events SET target_wallet_traded_here = 1 "
            "WHERE target_wallet_traded_here = 0 AND condition_id IN "
            "(SELECT DISTINCT condition_id FROM target_activity)"
        )
