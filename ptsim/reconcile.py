"""Сверка WS-классификации с ончейн-лентой data-api.

Это то, что делает датасет проверяемым. WS даёт провизорную классификацию
(«сделка» или «отмена»), лента даёт истину. Разность двух — матрица ошибок
классификатора, и она печатается анализатором заголовочной метрикой: если
сходимость 97%, данным можно верить; если 70%, проект надо строить на ленте, и
до этого числа PnL из симулятора ничего не значит.

Ловушки эндпоинта, учтённые здесь:
  * takerOnly=false отдаёт сделку с ОБЕИХ сторон — нужна дедупликация;
  * таймстемпы блочные, секундной гранулярности, отсюда допуск +-3 c;
  * порядок сделок внутри блока не восстанавливается.
"""
from __future__ import annotations

import logging

from .util import fsize, ms_from_any, ms_to_iso, now_ms, to_tick

log = logging.getLogger("reconcile")


class Reconciler:
    def __init__(self, cfg, store, http) -> None:
        self.cfg = cfg
        self.store = store
        self.http = http
        self.stats = {"confirmed": 0, "phantom_trade": 0, "missed_trade": 0, "no_data": 0}

    async def fetch_trades(self, condition_id: str) -> list[dict] | None:
        data = await self.http.get_json(self.cfg.reconcile.trades_url, {
            "market": condition_id, "limit": 500, "takerOnly": "false",
        })
        if not isinstance(data, list):
            return None
        seen: set[tuple] = set()
        out: list[dict] = []
        for t in data:
            if not isinstance(t, dict):
                continue
            tick = to_tick(t.get("price"))
            size = fsize(t.get("size"))
            ts = ms_from_any(t.get("timestamp"), 0)
            if tick is None or size <= 0 or not ts:
                continue
            key = (
                t.get("transactionHash") or t.get("transaction_hash") or "",
                str(t.get("asset") or t.get("assetId") or ""), tick, round(size, 6), ts,
            )
            if key in seen:
                continue  # та же сделка с обратной стороны
            seen.add(key)
            out.append({
                "asset": str(t.get("asset") or t.get("assetId") or ""),
                "tick": tick, "size": size, "ts_ms": ts,
                "side": str(t.get("side") or ""),
                "tx_hash": str(t.get("transactionHash")
                               or t.get("transaction_hash") or "").lower(),
            })
        return out

    async def reconcile_event(self, ev) -> str:
        trades = await self.fetch_trades(ev["condition_id"])
        if trades is None:
            return "no_data"
        tol = self.cfg.reconcile.match_tolerance_s * 1000
        entry_tick = self.cfg.entry.price_tick

        # Точная сверка по хэшу транзакции: last_trade_price его отдаёт, и мы
        # сохранили его в ленте окна. Допуск по времени нужен только там, где
        # хэша нет — таймстемпы ленты блочные, секундной гранулярности.
        ws_hashes = {
            r["tx_hash"].lower() for r in self.store.query(
                "SELECT tx_hash FROM paper_trades WHERE event_id = ? AND tx_hash <> ''",
                (ev["event_id"],),
            )
        }
        matched = [
            t for t in trades
            if t["asset"] == ev["asset_id"] and t["tick"] <= entry_tick
            and (t["tx_hash"] in ws_hashes if ws_hashes and t["tx_hash"]
                 else abs(t["ts_ms"] - ev["ts_ms"]) <= tol)
        ]
        chain_size = sum(t["size"] for t in matched)
        verdict = "confirmed" if matched else "phantom_trade"
        match_mode = "tx_hash" if ws_hashes else "timestamp"
        self.stats[verdict] += 1
        self.store.insert("reconcile_log", {
            "event_id": ev["event_id"], "checked_at": ms_to_iso(now_ms()),
            "verdict": verdict, "ws_size": ev["level_traded_size"],
            "chain_size": chain_size, "n_chain_trades": len(matched),
            "detail": f"mode={match_mode} tol={tol}ms entry_tick={entry_tick}",
        })
        self.store.update("paper_events", {"event_id": ev["event_id"]}, {
            "trigger_confirmed": int(verdict == "confirmed"),
            "confirmed_level_traded_size": chain_size,
            "reconcile_verdict": verdict,
            "reconciled_at": ms_to_iso(now_ms()),
        })
        return verdict

    async def check_missed(self, condition_id: str, asset_id: str,
                           since_ms: int, until_ms: int) -> int:
        """Ложноотрицательные: реальная сделка на дне была, а мы её не увидели.

        Считается по рынкам, где событий не зафиксировано. Это прямой счётчик
        того, сколько входов классификатор пропустил.
        """
        trades = await self.fetch_trades(condition_id)
        if not trades:
            return 0
        entry_tick = self.cfg.entry.price_tick
        cands = [
            t for t in trades
            if t["asset"] == asset_id and t["tick"] <= entry_tick
            and since_ms <= t["ts_ms"] <= until_ms
        ]
        if not cands:
            return 0
        rows = self.store.query(
            "SELECT ts_ms FROM paper_events WHERE asset_id = ? AND ts_ms BETWEEN ? AND ?",
            (asset_id, since_ms, until_ms),
        )
        known = [r["ts_ms"] for r in rows]
        tol = self.cfg.reconcile.match_tolerance_s * 1000
        missed = [c for c in cands if not any(abs(c["ts_ms"] - k) <= tol for k in known)]
        for c in missed:
            self.stats["missed_trade"] += 1
            self.store.insert("reconcile_log", {
                "event_id": None, "checked_at": ms_to_iso(now_ms()),
                "verdict": "missed_trade", "ws_size": None, "chain_size": c["size"],
                "n_chain_trades": 1,
                "detail": f"asset={asset_id} tick={c['tick']} ts={c['ts_ms']}",
            })
        return len(missed)

    async def run_once(self, limit: int = 50) -> int:
        delay_ms = self.cfg.reconcile.delayed_pass_s * 1000
        cutoff = now_ms() - delay_ms
        rows = self.store.query(
            "SELECT event_id, condition_id, asset_id, ts_ms, level_traded_size "
            "FROM paper_events WHERE reconcile_verdict IS NULL AND ts_ms <= ? "
            "ORDER BY ts_ms LIMIT ?", (cutoff, limit),
        )
        n = 0
        for row in rows:
            await self.reconcile_event(dict(row))
            n += 1
        n += await self.sweep_missed()
        return n

    async def sweep_missed(self, limit: int = 5, window_min: int = 30) -> int:
        """Поиск входов, которые классификатор ПРОПУСТИЛ.

        Без этого прохода матрица ошибок односторонняя: мы бы видели только
        фантомы (WS показал сделку, цепь нет) и не видели бы обратного — что
        сделка на дне была, а мы её не заметили. Оценивать качество по одной
        половине матрицы бессмысленно.

        Выборка рынков случайная и малая: эндпоинт ленты не рассчитан на
        сплошной опрос, а для оценки доли пропусков хватает выборки.
        """
        until = now_ms()
        since = until - window_min * 60_000
        rows = self.store.query(
            "SELECT condition_id, asset_id_a, asset_id_b FROM markets "
            "WHERE subscribed_at IS NOT NULL AND resolved = 0 "
            "AND condition_id NOT IN "
            "  (SELECT DISTINCT condition_id FROM paper_events WHERE ts_ms >= ?) "
            "ORDER BY RANDOM() LIMIT ?", (since, limit),
        )
        total = 0
        for row in rows:
            for aid in (row["asset_id_a"], row["asset_id_b"]):
                if aid:
                    total += await self.check_missed(
                        row["condition_id"], aid, since, until)
        if total:
            log.warning("пропущенных входов за %d мин: %d", window_min, total)
        return total
