"""Виртуальные заявки: предусловие, модель очереди, филлы.

Заявки существуют только в памяти процесса. Ничего никуда не отправляется.

Три вещи, которые здесь легко сделать неправильно и которые решают результат:

1. `prior_size_at_002` и объём свипа обязаны браться ОТ ОДНОГО МОМЕНТА
   ОТСЧЁТА. Формула ТЗ верна (проверено в т.ч. на несмежном свипе), но
   реализация, которая берёт prior_size из середины прохода, а объём — за весь
   проход, завысит филл в разы: 3200 вместо 200, то есть упор в наш размер.
   Поэтому prior_size фиксируется РОВНО ОДИН РАЗ при открытии свипа.

2. Событие — это жизнь одной заявки, а не сделка. При обвале по уровню
   прилетает пачка сделок; считать их отдельными событиями значит завысить
   «срабатываний на рынко-час» на порядок.

3. Кулдаун — произвольная константа, поэтому параллельно ведётся теневая
   заявка без кулдауна. Её филлы без первичного филла пишутся как
   suppressed-события, и частота пересчитывается при любом кулдауне, включая
   нулевой, без пересбора данных.
"""
from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field

from .book import Book, BookSnapshot
from .tape import Classification, Trade
from .util import SIZE_EPS, new_event_id, to_price

log = logging.getLogger("vorders")


@dataclass(slots=True)
class VirtualOrder:
    tick: int
    size: float
    placed_ms: int
    queue_ahead_at_placement: float
    queue_ahead_est: float
    filled: float = 0.0
    precondition_held: bool = True
    is_shadow: bool = False

    @property
    def remaining(self) -> float:
        return max(0.0, self.size - self.filled)


@dataclass(slots=True)
class Sweep:
    """Один проход тейкера по уровню <= нашей цены."""

    started_ms: int
    last_trade_ms: int
    prior_size_at_entry: float
    state_before: BookSnapshot | None
    volume_at_or_below: float = 0.0
    volume_at_entry: float = 0.0
    volume_below_entry: float = 0.0
    n_trades: int = 0
    lowest_tick: int = 999


@dataclass
class FillEvent:
    """Срабатывание входа либо дофилл того же события."""

    event_id: str
    asset_id: str
    ts_ms: int
    is_new_event: bool
    is_shadow: bool
    trigger: str
    our_fill: float
    our_fill_cum: float
    our_entry_price: float
    level_traded_size: float
    prior_size_at_002: float
    queue_ahead_at_placement: float
    queue_ahead_est: float
    prior_size_staleness_ms: int
    precondition_held_at_fill: bool
    state_before: BookSnapshot | None
    n_partial: int


class AssetOrderManager:
    """Виртуальные заявки одного токена."""

    def __init__(
        self,
        asset_id: str,
        book: Book,
        *,
        entry_tick: int,
        entry_size: float,
        min_best_bid_tick: int,
        cooldown_s: int,
        cancel_on_break: bool,
        sweep_window_ms: int,
        max_events_per_hour: int,
        log_suppressed: bool = True,
    ) -> None:
        self.asset_id = asset_id
        self.book = book
        self.entry_tick = entry_tick
        self.entry_price = to_price(entry_tick) or 0.0
        self.entry_size = entry_size
        self.min_best_bid_tick = min_best_bid_tick
        self.cooldown_ms = cooldown_s * 1000
        self.cancel_on_break = cancel_on_break
        self.sweep_window_ms = sweep_window_ms
        self.max_events_per_hour = max_events_per_hour
        self.log_suppressed = log_suppressed

        self.order: VirtualOrder | None = None
        self.shadow: VirtualOrder | None = None
        self.cooldown_until_ms: int = 0
        self.event_id: str | None = None
        self.shadow_event_id: str | None = None
        self.n_partial: int = 0
        self.shadow_n_partial: int = 0
        self.sweep: Sweep | None = None
        self.shadow_sweep: Sweep | None = None
        self._event_times: deque[int] = deque()

        # накопители для coverage: три разных знаменателя частоты
        self.eligible_ms: int = 0
        self.resting_ms: int = 0
        self.observed_ms: int = 0
        self._last_acct_ms: int = 0
        self._was_eligible: bool = False
        self._was_resting: bool = False

    # ------------------------------------------------------- предусловие

    def is_eligible(self) -> bool:
        bb = self.book.best_bid_tick()
        return bb is not None and bb >= self.min_best_bid_tick

    def on_book_update(self, ts_ms: int) -> None:
        self._account(ts_ms)
        eligible = self.is_eligible()

        if self.order is not None:
            self.order.precondition_held = eligible
            if not eligible and self.cancel_on_break:
                self.order = None
                self.event_id = None
        if self.shadow is not None:
            self.shadow.precondition_held = eligible

        if eligible:
            if self.shadow is None:
                self.shadow = self._make_order(ts_ms, shadow=True)
            if self.order is None and ts_ms >= self.cooldown_until_ms and self._rate_ok(ts_ms):
                self.order = self._make_order(ts_ms, shadow=False)

    def _make_order(self, ts_ms: int, *, shadow: bool) -> VirtualOrder:
        ahead = self.book.size_at("BID", self.entry_tick)
        return VirtualOrder(
            tick=self.entry_tick,
            size=self.entry_size,
            placed_ms=ts_ms,
            queue_ahead_at_placement=ahead,
            queue_ahead_est=ahead,
            is_shadow=shadow,
        )

    def _rate_ok(self, ts_ms: int) -> bool:
        cutoff = ts_ms - 3_600_000
        while self._event_times and self._event_times[0] < cutoff:
            self._event_times.popleft()
        return len(self._event_times) < self.max_events_per_hour

    def on_classification(self, c: Classification) -> None:
        """Отменённые доли на нашем уровне уменьшают очередь впереди нас.

        Какие именно заявки сняли — те, что стояли впереди нас, или те, что
        позади, — из данных не видно. queue_ahead_est держит оптимистичную
        границу (сняли впереди), prior_size_at_002 — консервативную. Обе
        пишутся, модель выбирается офлайн.
        """
        if c.side != "BID" or c.tick != self.entry_tick or c.cancelled <= SIZE_EPS:
            return
        for o in (self.order, self.shadow):
            if o is not None:
                o.queue_ahead_est = max(0.0, o.queue_ahead_est - c.cancelled)

    # ------------------------------------------------------------- филлы

    def on_trade(self, trade: Trade) -> list[FillEvent]:
        """Сделка по цене <= нашей. Вход срабатывает здесь и только здесь.

        Триггер — ФАКТИЧЕСКАЯ СДЕЛКА, не исчезновение книги. Исчезновение
        объединяет «съели» и «сняли»; второе нас бы не залило.
        """
        if trade.side_hit != "BID" or trade.tick > self.entry_tick:
            return []
        out: list[FillEvent] = []
        for shadow in (False, True):
            ev = self._apply_trade(trade, shadow=shadow)
            if ev is not None:
                out.append(ev)
        return out

    def _apply_trade(self, trade: Trade, *, shadow: bool) -> FillEvent | None:
        order = self.shadow if shadow else self.order
        if order is None or order.remaining <= SIZE_EPS:
            return None

        sweep = self.shadow_sweep if shadow else self.sweep
        is_new = False
        if sweep is None or trade.ts_ms - sweep.last_trade_ms > self.sweep_window_ms:
            state = self.book.state_at(trade.ts_ms - 1)
            sweep = Sweep(
                started_ms=trade.ts_ms,
                last_trade_ms=trade.ts_ms,
                prior_size_at_entry=state.size_at_002 if state else 0.0,
                state_before=state,
            )
            is_new = True
        sweep.last_trade_ms = trade.ts_ms
        sweep.n_trades += 1
        sweep.volume_at_or_below += trade.size
        sweep.lowest_tick = min(sweep.lowest_tick, trade.tick)
        if trade.tick == self.entry_tick:
            sweep.volume_at_entry += trade.size
        else:
            sweep.volume_below_entry += trade.size

        # Формула ТЗ, накопительно. prior_size зафиксирован при открытии свипа
        # и здесь не пересчитывается — в этом весь смысл.
        cum = max(0.0, sweep.volume_at_or_below - sweep.prior_size_at_entry)
        cum = min(cum, order.size)
        new_fill = cum - order.filled
        if new_fill <= SIZE_EPS:
            if shadow:
                self.shadow_sweep = sweep
            else:
                self.sweep = sweep
            return None
        order.filled = cum

        # Событие открывается на ПЕРВОМ ФИЛЛЕ, а не на первой сделке свипа:
        # первые сделки прохода могут целиком уходить в очередь впереди нас и
        # не давать нам ничего. Окно наблюдения надо привязывать к филлу.
        if shadow:
            self.shadow_sweep = sweep
            first_fill = self.shadow_event_id is None
            if first_fill:
                self.shadow_event_id = new_event_id()
                self.shadow_n_partial = 0
            self.shadow_n_partial += 1
            event_id, n_partial = self.shadow_event_id, self.shadow_n_partial
        else:
            self.sweep = sweep
            first_fill = self.event_id is None
            if first_fill:
                self.event_id = new_event_id()
                self.n_partial = 0
                self._event_times.append(trade.ts_ms)
            self.n_partial += 1
            event_id, n_partial = self.event_id, self.n_partial

        state = sweep.state_before
        staleness = (trade.ts_ms - state.ts_ms) if state else -1

        ev = FillEvent(
            event_id=event_id,
            asset_id=self.asset_id,
            ts_ms=trade.ts_ms,
            is_new_event=first_fill,
            is_shadow=shadow,
            trigger="trade_at_level",
            our_fill=new_fill,
            our_fill_cum=cum,
            our_entry_price=self.entry_price,
            level_traded_size=sweep.volume_at_or_below,
            prior_size_at_002=sweep.prior_size_at_entry,
            queue_ahead_at_placement=order.queue_ahead_at_placement,
            queue_ahead_est=order.queue_ahead_est,
            prior_size_staleness_ms=staleness,
            precondition_held_at_fill=order.precondition_held,
            state_before=state,
            n_partial=n_partial,
        )

        if order.remaining <= SIZE_EPS:
            if shadow:
                self.shadow = None
                self.shadow_event_id = None
                self.shadow_sweep = None
            else:
                self.order = None
                self.event_id = None
                self.cooldown_until_ms = trade.ts_ms + self.cooldown_ms
        return ev

    def suppression_reason(self, ts_ms: int) -> str | None:
        """Почему первичной заявки нет, хотя теневая сработала."""
        if self.order is not None:
            return None
        if ts_ms < self.cooldown_until_ms:
            return "cooldown"
        if not self._rate_ok(ts_ms):
            return "rate_limit"
        if not self.is_eligible():
            return "precondition"
        return "open_position"

    # ------------------------------------------------------- знаменатели

    def _account(self, ts_ms: int) -> None:
        if self._last_acct_ms:
            dt = max(0, ts_ms - self._last_acct_ms)
            self.observed_ms += dt
            if self._was_eligible:
                self.eligible_ms += dt
            if self._was_resting:
                self.resting_ms += dt
        self._last_acct_ms = ts_ms
        self._was_eligible = self.is_eligible()
        self._was_resting = self.order is not None

    def drain_counters(self, ts_ms: int) -> tuple[int, int, int]:
        self._account(ts_ms)
        out = (self.observed_ms, self.eligible_ms, self.resting_ms)
        self.observed_ms = self.eligible_ms = self.resting_ms = 0
        return out
