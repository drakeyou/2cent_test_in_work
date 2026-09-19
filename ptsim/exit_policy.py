"""Политика выхода: виртуальный аск и его перестановки.

Симуляция выхода оптимистична по построению, и это надо знать при чтении
результата. Модель очереди покрывает «кто перед нами по нашей цене», но не
покрывает того, что наше присутствие меняет чужие котировки: мейкер, чей аск мы
подрезали, в реальности подрежет нас в ответ. Поэтому:

  * результат `undercut` — ВЕРХНЯЯ ГРАНИЦА;
  * результат статического аска на 4x от реакции контрагента почти не зависит
    и является более доверенной оценкой.

Анализатор печатает это прямо в блоке сравнения политик, а не сноской.

`exit.max_participation` ограничивает долю каждой встречной сделки, которую мы
можем забрать: наши 1000 долей в опустевшей после обвала книге не бесплатны.
Полностью проблема без реальных заявок не решается.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from .book import Book, BookSnapshot
from .tape import Trade
from .util import MAX_TICK, MIN_TICK, SIZE_EPS, to_price

log = logging.getLogger("exit")


@dataclass(slots=True)
class QuoteMove:
    event_id: str
    ts_ms: int
    seconds_from_fill: float
    action: str  # place | move | fill | cancel
    our_ask: float | None
    best_ask_at_moment: float | None
    prior_size_at_our_ask: float
    filled_size: float
    reason: str


@dataclass(slots=True)
class ExitFill:
    event_id: str
    ts_ms: int
    seconds_from_fill: float
    price: float
    size: float
    prior_size_at_our_ask: float
    level_traded_size: float


@dataclass(slots=True)
class _ExitSweep:
    started_ms: int
    last_trade_ms: int
    ask_tick: int
    prior_size_at_ask: float
    volume_at_or_above: float = 0.0
    filled_from_sweep: float = 0.0


class UndercutExit:
    """Аск на тик ниже лучшего; при спреде в один тик — В лучший аск.

    Подрезать при спреде в один тик значит выставить продажу по цене лучшего
    бида, то есть немедленно исполниться по биду. Это другая политика, и ТЗ её
    не просит.
    """

    def __init__(
        self,
        event_id: str,
        book: Book,
        *,
        position_size: float,
        fill_ms: int,
        tick_floor: int = MIN_TICK,
        follow_up: bool = False,
        max_quote_moves: int = 200,
        min_clip_shares: float = 5.0,
        max_participation: float = 1.0,
        sweep_window_ms: int = 1500,
    ) -> None:
        self.event_id = event_id
        self.book = book
        self.position_size = position_size
        self.fill_ms = fill_ms
        self.tick_floor = max(MIN_TICK, tick_floor)
        self.follow_up = follow_up
        self.max_quote_moves = max_quote_moves
        self.min_clip_shares = min_clip_shares
        self.max_participation = max_participation
        self.sweep_window_ms = sweep_window_ms

        self.our_ask_tick: int | None = None
        self.filled: float = 0.0
        self.proceeds: float = 0.0
        self.n_fills: int = 0
        self.n_moves: int = 0
        self.moves_capped: bool = False
        self.closed_by: str | None = None
        self.last_fill_ms: int | None = None
        self._sweep: _ExitSweep | None = None
        self._queue_at_ask: float = 0.0

    # ---------------------------------------------------------------- общее

    @property
    def remaining(self) -> float:
        return max(0.0, self.position_size - self.filled)

    @property
    def sellable(self) -> bool:
        return self.remaining >= self.min_clip_shares and self.closed_by is None

    @property
    def exit_vwap(self) -> float | None:
        return (self.proceeds / self.filled) if self.filled > SIZE_EPS else None

    def _secs(self, ts_ms: int) -> float:
        return round((ts_ms - self.fill_ms) / 1000.0, 3)

    # ---------------------------------------------------------- котирование

    def desired_ask_tick(self) -> int | None:
        ba = self.book.best_ask_tick()
        if ba is None:
            return None
        bb = self.book.best_bid_tick()
        cand = max(ba - 1, self.tick_floor)
        if cand > MAX_TICK:
            cand = MAX_TICK
        if bb is not None and cand <= bb:
            return ba  # спред в один тик -> встаём В лучший аск
        if cand >= ba:
            return ba
        return cand

    def on_book_update(self, ts_ms: int) -> list[QuoteMove]:
        if not self.sellable:
            return []
        want = self.desired_ask_tick()
        if want is None:
            return []
        cur = self.our_ask_tick
        if cur == want:
            return []
        if cur is None:
            reason = "initial"
        elif want < cur:
            reason = "follow_down"
        elif self.follow_up:
            reason = "follow_up"
        else:
            return []  # аск ушёл вверх, по ТЗ за ним не идём

        if self.n_moves >= self.max_quote_moves:
            if not self.moves_capped:
                self.moves_capped = True
                log.debug("event %s: лимит перестановок аска", self.event_id)
            return []

        self.our_ask_tick = want
        # Перестановка сбрасывает позицию в очереди: мы снова последние.
        self._queue_at_ask = self.book.size_at("ASK", want)
        self._sweep = None
        self.n_moves += 1
        return [
            QuoteMove(
                event_id=self.event_id,
                ts_ms=ts_ms,
                seconds_from_fill=self._secs(ts_ms),
                action="place" if reason == "initial" else "move",
                our_ask=to_price(want),
                best_ask_at_moment=self.book.best_ask(),
                prior_size_at_our_ask=self._queue_at_ask,
                filled_size=0.0,
                reason=reason,
            )
        ]

    # --------------------------------------------------------------- филлы

    def on_trade(self, trade: Trade) -> tuple[list[ExitFill], list[QuoteMove]]:
        """Наша продажа исполняется, когда пришла сделка по цене >= нашего аска.

        То же вычитание очереди, что на входе, с тем же требованием: prior_size
        фиксируется один раз при открытии прохода и не перечитывается.
        """
        if self.our_ask_tick is None or not self.sellable:
            return [], []
        if trade.side_hit != "ASK" or trade.tick < self.our_ask_tick:
            return [], []

        sweep = self._sweep
        if (
            sweep is None
            or sweep.ask_tick != self.our_ask_tick
            or trade.ts_ms - sweep.last_trade_ms > self.sweep_window_ms
        ):
            # Книга СТРОГО ДО сделки: текущая книга могла быть уменьшена той
            # же самой сделкой, и тогда очередь впереди нас обнулится задним
            # числом, а филл завысится.
            state = self.book.state_at(trade.ts_ms - 1)
            prior = (
                state.size_at("ASK", self.our_ask_tick)
                if state is not None
                else self._queue_at_ask
            )
            sweep = _ExitSweep(
                started_ms=trade.ts_ms,
                last_trade_ms=trade.ts_ms,
                ask_tick=self.our_ask_tick,
                prior_size_at_ask=prior,
            )
        sweep.last_trade_ms = trade.ts_ms
        sweep.volume_at_or_above += trade.size
        self._sweep = sweep

        cum = max(0.0, sweep.volume_at_or_above - sweep.prior_size_at_ask)
        cum = min(cum, self.max_participation * sweep.volume_at_or_above)
        cum = min(cum, self.remaining + sweep.filled_from_sweep)
        new_fill = cum - sweep.filled_from_sweep
        if new_fill <= SIZE_EPS:
            return [], []
        sweep.filled_from_sweep = cum

        price = to_price(self.our_ask_tick) or 0.0
        self.filled += new_fill
        self.proceeds += price * new_fill
        self.n_fills += 1
        self.last_fill_ms = trade.ts_ms

        fill = ExitFill(
            event_id=self.event_id,
            ts_ms=trade.ts_ms,
            seconds_from_fill=self._secs(trade.ts_ms),
            price=price,
            size=new_fill,
            prior_size_at_our_ask=sweep.prior_size_at_ask,
            level_traded_size=sweep.volume_at_or_above,
        )
        move = QuoteMove(
            event_id=self.event_id,
            ts_ms=trade.ts_ms,
            seconds_from_fill=self._secs(trade.ts_ms),
            action="fill",
            our_ask=price,
            best_ask_at_moment=self.book.best_ask(),
            prior_size_at_our_ask=sweep.prior_size_at_ask,
            filled_size=new_fill,
            reason="trade_at_or_above_ask",
        )
        if not self.sellable:
            self.closed_by = "ask_filled" if self.remaining <= SIZE_EPS else "dust"
            self.our_ask_tick = None
        return [fill], [move]

    def close(self, ts_ms: int, reason: str) -> list[QuoteMove]:
        if self.closed_by is not None:
            return []
        self.closed_by = reason
        had = self.our_ask_tick
        self.our_ask_tick = None
        if had is None:
            return []
        return [
            QuoteMove(
                event_id=self.event_id,
                ts_ms=ts_ms,
                seconds_from_fill=self._secs(ts_ms),
                action="cancel",
                our_ask=to_price(had),
                best_ask_at_moment=self.book.best_ask(),
                prior_size_at_our_ask=self._queue_at_ask,
                filled_size=0.0,
                reason=reason,
            )
        ]
