"""Движок: маршрутизация WS-сообщений, окна событий, позиции, знаменатели.

Все производные поля считаются В МОМЕНТ ЗАПИСИ и хранятся, а не
восстанавливаются при экспорте. Единственное исключение — `bid_after` и
зависящие от него `book_sum` и `internal_dislocation`: на момент филла книга
после сделки ещё не пришла, поэтому они дописываются отложенно, когда проход
завершился. Это тоже вычисление при записи, просто запись происходит позже.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from .book import Book, BookSnapshot, RingBuffer
from .discovery import MarketRec, MarketRegistry
from .exit_policy import UndercutExit
from .tape import Classification, SideCalibrator, TapeClassifier, Trade
from .util import (
    SIZE_EPS, fsize, ms_from_any, ms_to_iso, now_ms, safe_div, to_price, to_tick,
)
from .virtual_orders import AssetOrderManager, FillEvent

log = logging.getLogger("engine")


def _px(value: float | None, ready: bool) -> float | None:
    """Пустая сторона книги — это 0.0, а не NULL.

    Различие существенное: NULL значит «мы не знаем», 0.0 значит «бидов не
    осталось». Второе — не пропуск данных, а самое содержательное наблюдение
    во всём событии, и в разрезах его надо видеть отдельно от пропусков.
    """
    if value is not None:
        return value
    return 0.0 if ready else None


@dataclass
class AssetState:
    asset_id: str
    market: MarketRec
    book: Book
    tape: TapeClassifier
    orders: AssetOrderManager
    ring: RingBuffer
    last_snapshot_ms: int = 0


@dataclass
class EventCtx:
    """Открытое событие: окно наблюдения, позиция, политика выхода."""

    event_id: str
    asset_id: str
    condition_id: str
    sport: str
    fill_ms: int
    entry_price: float
    entry_size: float = 0.0
    is_suppressed: bool = False
    exit: UndercutExit | None = None
    grid_idx: int = 0
    checkpoints_done: set[int] = field(default_factory=set)
    dense_rows: int = 0
    deferred_done: bool = False
    window_end_captured: bool = False
    closed: bool = False
    truncated_reason: str | None = None
    n_entry_fills: int = 0


class Engine:
    def __init__(self, cfg, store, registry: MarketRegistry) -> None:
        self.cfg = cfg
        self.store = store
        self.registry = registry
        self.assets: dict[str, AssetState] = {}
        self.events: dict[str, EventCtx] = {}
        self.calibrator = SideCalibrator(cfg.classify.side_calibration_trades)
        saved = store.get_kv("side_calibration")
        if saved:
            self.calibrator.load(saved)
            log.info("калибровка стороны восстановлена: %s", saved)
        self.grid = cfg.window.grid
        self.n_trades_seen = 0
        self.n_events = 0
        self.n_suppressed = 0
        self.unknown_classified = 0
        self._hour_counters: dict[tuple[str, str], dict[str, float]] = {}
        # Заполняется менеджером WS: нужно, чтобы отнести разрыв шарда к тем
        # дисциплинам, которые он реально ослепил, а не ко всем сразу.
        self.shard_of: dict[str, int] = {}

    # ------------------------------------------------------------- подписка

    def sync_assets(self, desired: dict[str, MarketRec]) -> None:
        for aid, market in desired.items():
            if aid in self.assets:
                self.assets[aid].market = market
                continue
            book = Book(aid)
            tape = TapeClassifier(
                aid,
                window_ms=self.cfg.classify.window_ms,
                credit_ttl_ms=self.cfg.classify.credit_ttl_ms,
                sweep_inference=self.cfg.classify.sweep_inference,
                calibrator=self.calibrator,
                book=book,
            )
            orders = AssetOrderManager(
                aid, book,
                entry_tick=self.cfg.entry.price_tick,
                entry_size=self.cfg.entry.size_shares,
                min_best_bid_tick=self.cfg.entry.min_best_bid_tick,
                cooldown_s=self.cfg.entry.cooldown_s,
                cancel_on_break=self.cfg.entry.cancel_when_precondition_breaks,
                sweep_window_ms=self.cfg.classify.window_ms,
                max_events_per_hour=self.cfg.entry.max_events_per_asset_per_hour,
                log_suppressed=self.cfg.entry.log_suppressed,
            )
            self.assets[aid] = AssetState(
                asset_id=aid, market=market, book=book, tape=tape, orders=orders,
                ring=RingBuffer(self.cfg.window.ring_buffer_s,
                                self.cfg.window.snapshot_interval_s),
            )
        for aid in [a for a in self.assets if a not in desired]:
            if not any(e.asset_id == aid and not e.closed for e in self.events.values()):
                del self.assets[aid]

    def paired_state(self, st: AssetState) -> AssetState | None:
        pid = st.market.paired(st.asset_id)
        return self.assets.get(pid) if pid else None

    # ------------------------------------------------------ WS-сообщения

    def on_ws_message(self, msg: dict) -> None:
        aid = msg.get("asset_id") or msg.get("assetId")
        st = self.assets.get(str(aid)) if aid else None
        if st is None:
            return
        et = msg.get("event_type") or msg.get("type")
        ts = ms_from_any(msg.get("timestamp"))
        if et == "book":
            st.book.apply_snapshot(msg.get("buys") or msg.get("bids") or [],
                                   msg.get("sells") or msg.get("asks") or [], ts)
            for c in st.tape.on_snapshot_reset(ts):
                self._on_classification(st, c)
            st.orders.on_book_update(ts)
        elif et == "price_change":
            changes = msg.get("changes") or msg.get("price_changes") or []
            if isinstance(changes, dict):
                changes = [changes]
            deltas = st.book.apply_price_change(changes, ts)
            if deltas:
                for c in st.tape.on_deltas(deltas):
                    self._on_classification(st, c)
                st.orders.on_book_update(ts)
        elif et == "last_trade_price":
            self._on_trade_msg(st, msg, ts)
        self._mark_observed(st, ts)

    def _on_trade_msg(self, st: AssetState, msg: dict, ts: int) -> None:
        tick = to_tick(msg.get("price"))
        size = fsize(msg.get("size"))
        if tick is None or size <= SIZE_EPS:
            return
        reported = str(msg.get("side") or "")
        trade = Trade(
            tick=tick, size=size, ts_ms=ts,
            side_hit=self.calibrator.side_hit(reported), reported_side=reported,
        )
        self.n_trades_seen += 1
        for c in st.tape.on_trade(trade):
            self._on_classification(st, c)
        self._record_event_trade(st, trade)
        for ev in st.orders.on_trade(trade):
            self._on_fill(st, ev)
        # Та же сделка может исполнить наш аск по открытой позиции: тейкер,
        # покупающий вверх, снимает аски, а не биды.
        self.on_exit_trade(st, trade)

    def _on_classification(self, st: AssetState, c: Classification) -> None:
        st.orders.on_classification(c)
        if c.unknown > SIZE_EPS:
            self.unknown_classified += c.unknown
            self._bump(st, "classify_unknown", c.unknown, c.ts_ms)
        # book_vanished пишется ТОЛЬКО пока у нас лежала заявка: отмены на 0.02
        # идут непрерывно, и без этого гейта отладочная таблица была бы на
        # порядки больше основной.
        if (
            self.cfg.classify.log_book_vanished
            and c.side == "BID"
            and c.tick <= self.cfg.entry.price_tick
            and c.cancelled > SIZE_EPS
            and c.traded <= SIZE_EPS
            and st.orders.order is not None
        ):
            self.store.insert("book_vanished", {
                "ts_ms": c.ts_ms, "asset_id": st.asset_id,
                "condition_id": st.market.condition_id, "tick": c.tick,
                "reduced": c.reduced, "traded": c.traded, "cancelled": c.cancelled,
                "unknown": c.unknown, "evidence": c.evidence,
                "bid_before": st.book.best_bid(),
                "opposite_side_changed_ms": c.opposite_side_changed_ms,
            })

    # ------------------------------------------------------------- события

    def _on_fill(self, st: AssetState, ev: FillEvent) -> None:
        if ev.is_new_event:
            self._open_event(st, ev)
        else:
            self._extend_event(st, ev)

    def _open_event(self, st: AssetState, ev: FillEvent) -> None:
        paired = self.paired_state(st)
        pb = paired.book if paired else None
        state = ev.state_before
        paired_bid = pb.best_bid() if pb else None
        paired_ask = pb.best_ask() if pb else None
        paired_ask_size = pb.size_at("ASK", pb.best_ask_tick()) if pb and pb.best_ask_tick() else None
        paired_stale = (
            (ev.ts_ms - pb.last_real_change_ms) / 1000.0
            if pb and pb.last_real_change_ms else None
        )
        # 1 - paired_ask: исполнимая цена хеджа, а не абстрактный «пол».
        # Купив наш токен и близнеца, получаем комплект, который на резолюции
        # стоит ровно $1 при любом исходе.
        fair_lower = (1.0 - paired_ask) if paired_ask is not None else None
        fair_upper = (1.0 - paired_bid) if paired_bid is not None else None

        m = st.market
        mins = (
            (ev.ts_ms - m.game_start_ms) / 60000.0 if m.game_start_ms is not None else None
        )
        suppressed = ev.is_shadow
        reason = st.orders.suppression_reason(ev.ts_ms) if suppressed else None

        row = {
            "event_id": ev.event_id, "ts": ms_to_iso(ev.ts_ms), "ts_ms": ev.ts_ms,
            "condition_id": m.condition_id, "asset_id": st.asset_id, "sport": m.sport,
            "market_level": m.market_level, "kind": m.kind, "trigger": ev.trigger,
            "bid_before": _px(state.best_bid, True) if state else None,
            "best_ask_before": _px(state.best_ask, True) if state else None,
            "level_traded_size": ev.level_traded_size,
            "prior_size_at_002": ev.prior_size_at_002,
            "our_fill": ev.our_fill_cum, "our_fill_cum": ev.our_fill_cum,
            "our_entry_price": ev.our_entry_price,
            "prior_size_at_001": state.size_at_001 if state else None,
            "prior_size_at_003": state.size_at_003 if state else None,
            "prior_size_at_005": state.size_at_005 if state else None,
            "depth_before": state.depth_bid_total if state else None,
            "n_bid_levels_before": state.n_bid_levels if state else None,
            "paired_bid": paired_bid, "paired_ask": paired_ask,
            "paired_ask_size": paired_ask_size, "paired_stale_seconds": paired_stale,
            "fair_lower_bound": fair_lower, "fair_upper_bound": fair_upper,
            "market_volume": m.volume, "market_liquidity": m.liquidity,
            "minutes_from_game_start": mins,
            "target_wallet_traded_here": self._target_seen(m.condition_id),
            "bid_notional_above_002": state.bid_notional_above_002 if state else None,
            "bid_shares_above_002": state.bid_shares_above_002 if state else None,
            "queue_ahead_at_placement": ev.queue_ahead_at_placement,
            "queue_ahead_est": ev.queue_ahead_est,
            "prior_size_staleness_ms": ev.prior_size_staleness_ms,
            "precondition_held_at_fill": int(ev.precondition_held_at_fill),
            # делим на нашу цену входа: платим мы 0.02, а не bid_after
            "dislocation_vs_entry": safe_div(fair_lower, ev.our_entry_price),
            "calibration_pending": int(self.calibrator.pending),
            "n_partial": ev.n_partial, "market_slug": m.slug,
            "game_start_source": m.game_start_source,
            "is_suppressed": int(suppressed), "suppressed_reason": reason,
            "dense_rows": 0, "window_complete": 0,
        }
        self.store.insert("paper_events", row)

        ctx = EventCtx(
            event_id=ev.event_id, asset_id=st.asset_id, condition_id=m.condition_id,
            sport=m.sport, fill_ms=ev.ts_ms, entry_price=ev.our_entry_price,
            entry_size=ev.our_fill_cum, is_suppressed=suppressed, n_entry_fills=1,
        )
        if not suppressed:
            ctx.exit = UndercutExit(
                ev.event_id, st.book, position_size=ev.our_fill_cum, fill_ms=ev.ts_ms,
                tick_floor=self.cfg.exit.floor_tick, follow_up=self.cfg.exit.follow_up,
                max_quote_moves=self.cfg.exit.max_quote_moves,
                min_clip_shares=self.cfg.exit.min_clip_shares,
                max_participation=self.cfg.exit.max_participation,
                sweep_window_ms=self.cfg.classify.window_ms,
            )
        self.events[ev.event_id] = ctx
        self._drain_ring(st, ctx)

        self.n_events += 1
        if suppressed:
            self.n_suppressed += 1
        else:
            self._bump(st, "n_events", 1, ev.ts_ms)
        log.info(
            "ВХОД%s %s %s fill=%.0f/%.0f prior=%.0f bid_before=%s fair_lb=%s",
            " (теневой)" if suppressed else "", m.sport, st.asset_id[:10],
            ev.our_fill_cum, self.cfg.entry.size_shares, ev.prior_size_at_002,
            state.best_bid if state else None, fair_lower,
        )

    def _extend_event(self, st: AssetState, ev: FillEvent) -> None:
        ctx = self.events.get(ev.event_id)
        if ctx is None:
            return
        ctx.entry_size = ev.our_fill_cum
        ctx.n_entry_fills += 1
        if ctx.exit is not None:
            ctx.exit.position_size = ev.our_fill_cum
        self.store.update("paper_events", {"event_id": ev.event_id}, {
            "our_fill": ev.our_fill_cum, "our_fill_cum": ev.our_fill_cum,
            "level_traded_size": ev.level_traded_size, "n_partial": ev.n_partial,
        })

    def _drain_ring(self, st: AssetState, ctx: EventCtx) -> None:
        """Предсобытийная часть окна из кольцевого буфера.

        Буфер тикает по часам процесса, а сетка отсчитывается от филла, поэтому
        узлы заполняются ближайшим снапшотом в пределах половины шага.
        """
        paired = self.paired_state(st)
        tol = self.cfg.window.snapshot_interval_s * 500
        for i, t in enumerate(self.grid):
            if t >= 0:
                break
            # grid_idx — индекс УЗЛА СЕТКИ, а не счётчик написанных строк.
            # Пропущенный снапшот не должен сдвигать все последующие узлы.
            ctx.grid_idx = i + 1
            snap = st.ring.nearest(ctx.fill_ms + t * 1000, tol)
            if snap is not None:
                self._write_book_row(ctx, t, snap, paired, is_checkpoint=0)

    def _write_book_row(
        self, ctx: EventCtx, seconds: float, snap: BookSnapshot,
        paired: AssetState | None, is_checkpoint: int,
    ) -> None:
        pb = paired.book if paired else None
        self.store.insert("paper_book", {
            "event_id": ctx.event_id, "seconds_from_fill": seconds, "ts_ms": snap.ts_ms,
            "best_bid": snap.best_bid, "best_ask": snap.best_ask, "mid": snap.mid,
            "size_at_001": snap.size_at_001, "size_at_002": snap.size_at_002,
            "size_at_003": snap.size_at_003, "size_at_005": snap.size_at_005,
            "depth_bid_total": snap.depth_bid_total, "n_bid_levels": snap.n_bid_levels,
            "paired_bid": pb.best_bid() if pb else None,
            "paired_ask": pb.best_ask() if pb else None,
            "is_checkpoint": is_checkpoint,
        })
        if not is_checkpoint:
            ctx.dense_rows += 1

    def _record_event_trade(self, st: AssetState, trade: Trade) -> None:
        """Лента внутри окна события: без неё альтернативные политики выхода
        офлайн не посчитать."""
        for ctx in self.events.values():
            if ctx.closed or ctx.asset_id != st.asset_id:
                continue
            self.store.insert("paper_trades", {
                "event_id": ctx.event_id, "ts_ms": trade.ts_ms,
                "seconds_from_fill": round((trade.ts_ms - ctx.fill_ms) / 1000.0, 3),
                "asset_id": st.asset_id, "price": to_price(trade.tick),
                "size": trade.size, "side_hit": trade.side_hit, "source": trade.source,
            })

    def _target_seen(self, condition_id: str) -> int:
        row = self.store.query_one(
            "SELECT 1 FROM target_activity WHERE condition_id = ? LIMIT 1", (condition_id,)
        )
        return 1 if row else 0

    # ------------------------------------------------------- тик по времени

    def tick(self, now: int | None = None) -> None:
        n = now or now_ms()
        for st in self.assets.values():
            for c in st.tape.tick(n):
                self._on_classification(st, c)
            if n - st.last_snapshot_ms >= self.cfg.window.snapshot_interval_s * 1000:
                st.last_snapshot_ms = n
                if st.book.ready:
                    st.ring.maybe_push(st.book.snapshot(n, self.cfg.entry.price_tick))
        for ctx in list(self.events.values()):
            self._tick_event(ctx, n)

    def _tick_event(self, ctx: EventCtx, n: int) -> None:
        st = self.assets.get(ctx.asset_id)
        if st is None:
            if not ctx.closed:
                self._close_event(ctx, n, "unsubscribed")
            return
        paired = self.paired_state(st)
        elapsed = (n - ctx.fill_ms) / 1000.0

        while ctx.grid_idx < len(self.grid) and self.grid[ctx.grid_idx] <= elapsed:
            t = self.grid[ctx.grid_idx]
            if t >= 0 and st.book.ready:
                self._write_book_row(ctx, t, st.book.snapshot(n, self.cfg.entry.price_tick),
                                     paired, is_checkpoint=0)
            ctx.grid_idx += 1

        if not ctx.deferred_done and elapsed * 1000 >= self.cfg.classify.window_ms:
            self._write_deferred(ctx, st, paired)

        for minutes in self.cfg.window.checkpoints_min:
            if minutes not in ctx.checkpoints_done and elapsed >= minutes * 60:
                ctx.checkpoints_done.add(minutes)
                if st.book.ready:
                    self._write_book_row(ctx, minutes * 60,
                                         st.book.snapshot(n, self.cfg.entry.price_tick),
                                         paired, is_checkpoint=1)

        if ctx.exit is not None:
            for mv in ctx.exit.on_book_update(n):
                self._write_quote(mv)

        if not ctx.window_end_captured and elapsed >= self.cfg.window.dense_after_s:
            ctx.window_end_captured = True
            self.store.update("paper_events", {"event_id": ctx.event_id}, {
                "dense_rows": ctx.dense_rows,
                "window_complete": int(ctx.dense_rows >= len(self.grid)),
                "window_truncated_reason": ctx.truncated_reason,
            })
            if ctx.exit is not None:
                self.store.upsert("paper_positions", {
                    "event_id": ctx.event_id,
                    "exit_vwap_at_window_end": ctx.exit.exit_vwap,
                    "filled_size_at_window_end": ctx.exit.filled,
                }, ["event_id"])
            if self.cfg.exit.simulate_until == "window_end":
                self._close_event(ctx, n, "window_end")

    def _write_deferred(self, ctx: EventCtx, st: AssetState, paired: AssetState | None) -> None:
        """bid_after и зависящие от него поля: на момент филла книга после
        сделки ещё не пришла."""
        ctx.deferred_done = True
        bid_after = _px(st.book.best_bid(), st.book.ready)
        pb = paired.book if paired else None
        paired_bid = _px(pb.best_bid(), pb.ready) if pb else None
        row = self.store.query_one(
            "SELECT fair_lower_bound FROM paper_events WHERE event_id = ?", (ctx.event_id,)
        )
        fair_lower = row["fair_lower_bound"] if row else None
        self.store.update("paper_events", {"event_id": ctx.event_id}, {
            "bid_after": bid_after,
            "book_sum": (bid_after + paired_bid)
            if bid_after is not None and paired_bid is not None else None,
            # safe_div: bid_after равен нулю ровно в самых глубоких обвалах,
            # то есть в самых интересных событиях. NULL честнее, чем inf.
            "internal_dislocation": safe_div(fair_lower, bid_after),
        })

    def _write_quote(self, mv) -> None:
        self.store.insert("paper_quotes", {
            "event_id": mv.event_id, "ts": ms_to_iso(mv.ts_ms), "ts_ms": mv.ts_ms,
            "seconds_from_fill": mv.seconds_from_fill, "action": mv.action,
            "our_ask": mv.our_ask, "best_ask_at_moment": mv.best_ask_at_moment,
            "prior_size_at_our_ask": mv.prior_size_at_our_ask,
            "filled_size": mv.filled_size, "reason": mv.reason,
        })

    def on_exit_trade(self, st: AssetState, trade: Trade) -> None:
        for ctx in self.events.values():
            if ctx.closed or ctx.asset_id != st.asset_id or ctx.exit is None:
                continue
            fills, moves = ctx.exit.on_trade(trade)
            for mv in moves:
                self._write_quote(mv)
            if fills:
                self._write_position(ctx)

    def _close_event(self, ctx: EventCtx, n: int, reason: str) -> None:
        if ctx.closed:
            return
        ctx.closed = True
        ctx.truncated_reason = ctx.truncated_reason or (
            reason if reason not in ("window_end", "ask_filled") else None
        )
        if ctx.exit is not None:
            for mv in ctx.exit.close(n, reason):
                self._write_quote(mv)
        self._write_position(ctx, closed_by=reason)
        self.store.update("paper_events", {"event_id": ctx.event_id}, {
            "dense_rows": ctx.dense_rows,
            "window_complete": int(ctx.dense_rows >= len(self.grid)),
            "window_truncated_reason": ctx.truncated_reason,
        })
        self.events.pop(ctx.event_id, None)

    def _write_position(self, ctx: EventCtx, closed_by: str | None = None) -> None:
        ex = ctx.exit
        entry_cost = ctx.entry_price * ctx.entry_size
        proceeds = ex.proceeds if ex else 0.0
        exit_size = ex.filled if ex else 0.0
        hold = None
        if ex and ex.last_fill_ms:
            hold = round((ex.last_fill_ms - ctx.fill_ms) / 1000.0, 3)
        self.store.upsert("paper_positions", {
            "event_id": ctx.event_id, "asset_id": ctx.asset_id,
            "condition_id": ctx.condition_id, "entry_price": ctx.entry_price,
            "entry_size": ctx.entry_size, "exit_vwap": ex.exit_vwap if ex else None,
            "exit_size": exit_size, "hold_seconds": hold,
            "n_partial_fills": ex.n_fills if ex else 0,
            "pnl": proceeds - entry_cost if exit_size else None,
            "multiple": safe_div(proceeds, entry_cost) if exit_size else None,
            "closed_by": closed_by or (ex.closed_by if ex else None),
            "is_suppressed": int(ctx.is_suppressed),
            "updated_at": ms_to_iso(now_ms()),
        }, ["event_id"])

    def on_resolution(self, condition_id: str, winner_by_asset: dict[str, bool]) -> None:
        """Резолюция закрывает открытые события и досчитывает остаток позиции.

        Без неё нельзя отличить «сгорело в ноль» от «не успели выйти в окне» и
        нельзя посчитать политику «держать до конца».
        """
        n = now_ms()
        for ctx in list(self.events.values()):
            if ctx.condition_id != condition_id:
                continue
            st = self.assets.get(ctx.asset_id)
            if st and st.book.ready:
                self._write_book_row(ctx, round((n - ctx.fill_ms) / 1000.0, 1),
                                     st.book.snapshot(n, self.cfg.entry.price_tick),
                                     self.paired_state(st), is_checkpoint=1)
            self._close_event(ctx, n, "resolution")
        for aid, won in winner_by_asset.items():
            payout = 1.0 if won else 0.0
            for row in self.store.query(
                "SELECT event_id, entry_price, entry_size, exit_size, exit_vwap "
                "FROM paper_positions WHERE asset_id = ?", (aid,)
            ):
                entry_cost = (row["entry_price"] or 0) * (row["entry_size"] or 0)
                proceeds = (row["exit_vwap"] or 0) * (row["exit_size"] or 0)
                residual = max(0.0, (row["entry_size"] or 0) - (row["exit_size"] or 0)) * payout
                self.store.update("paper_positions", {"event_id": row["event_id"]}, {
                    "resolution_winner": int(won), "resolution_payout": payout,
                    "pnl": proceeds + residual - entry_cost,
                    "multiple": safe_div(proceeds + residual, entry_cost),
                    "updated_at": ms_to_iso(n),
                })

    # --------------------------------------------------------- знаменатели

    def _mark_observed(self, st: AssetState, ts: int) -> None:
        m = st.market
        if m.game_start_ms is not None and not m.observed_during_game:
            if m.game_start_ms <= ts <= (m.end_date_ms or ts + 1):
                self.registry.mark_observed_during_game(m.condition_id)
        elif m.game_start_source == "none" and not m.observed_during_game:
            # Время старта неизвестно. Отметка ставится, но источник виден в
            # данных, и критерий можно посчитать в обоих прочтениях.
            self.registry.mark_observed_during_game(m.condition_id)

    def _bump(self, st: AssetState, field_name: str, value: float, ts: int | None = None) -> None:
        key = (ms_to_iso(ts or now_ms())[:13], st.market.sport or "unknown")
        self._hour_counters.setdefault(key, {})
        self._hour_counters[key][field_name] = (
            self._hour_counters[key].get(field_name, 0.0) + value
        )

    def flush_counters(self, now: int | None = None) -> None:
        n = now or now_ms()
        hour = ms_to_iso(n)[:13]
        per_sport: dict[str, dict[str, float]] = {}
        seen_markets: dict[str, set[str]] = {}
        for st in self.assets.values():
            obs, elig, rest = st.orders.drain_counters(n)
            sp = st.market.sport or "unknown"
            d = per_sport.setdefault(sp, {})
            # Ассет-секунды делим пополам: подписка идёт на оба токена рынка,
            # а единица измерения частоты — рынко-час.
            d["observed_seconds"] = d.get("observed_seconds", 0.0) + obs / 2000.0
            d["eligible_seconds"] = d.get("eligible_seconds", 0.0) + elig / 2000.0
            d["resting_seconds"] = d.get("resting_seconds", 0.0) + rest / 2000.0
            seen_markets.setdefault(sp, set()).add(st.market.condition_id)
            d["n_assets"] = d.get("n_assets", 0.0) + 1
        for (h, sp), extra in list(self._hour_counters.items()):
            per_sport.setdefault(sp, {})
            for k, v in extra.items():
                per_sport[sp][k] = per_sport[sp].get(k, 0.0) + v
        self._hour_counters.clear()
        for sp, d in per_sport.items():
            adds = {k: v for k, v in d.items() if k != "n_assets"}
            if not adds:
                continue
            self.store.accumulate("coverage", {"hour": hour, "sport": sp}, adds)
            self.store.update("coverage", {"hour": hour, "sport": sp}, {
                "n_assets": int(d.get("n_assets", 0)),
                "n_markets": len(seen_markets.get(sp, ())),
                "dropped_markets": self.registry.dropped_this_poll,
                "sampling_rate": self.registry.sampling_rate,
            })
        self.store.set_kv("side_calibration", self.calibrator.to_dict())

    def add_gap_seconds(self, gap: dict) -> None:
        """Разрыв -> секунды слепоты в coverage, по дисциплинам.

        Без этого знаменатель частоты завышен на всё время, которое мы не
        видели книгу.
        """
        per_sport: dict[str, float] = {}
        seconds = gap["duration_ms"] / 1000.0
        for aid, st in self.assets.items():
            if self._asset_in_shard(aid, gap.get("shard")):
                sp = st.market.sport or "unknown"
                per_sport[sp] = per_sport.get(sp, 0.0) + seconds / 2.0
        hour = ms_to_iso(gap["ended_ms"])[:13]
        for sp, secs in per_sport.items():
            self.store.accumulate("coverage", {"hour": hour, "sport": sp},
                                  {"gap_seconds": secs})

    def _asset_in_shard(self, asset_id: str, shard: int | None) -> bool:
        if shard is None or not self.shard_of:
            return True
        return self.shard_of.get(asset_id) == shard
