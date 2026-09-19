"""Состояние книги L2 и кольцевой буфер снапшотов.

Два решения, которые стоит держать в голове при чтении:

1. `price_change` несёт НОВЫЙ АБСОЛЮТНЫЙ размер уровня, а не дельту. Дельта
   вычисляется нами; на этом держится весь классификатор trade-vs-cancel.

2. `last_real_change_ms` двигается только когда размер уровня реально
   изменился. Повторная рассылка идентичного состояния и хартбиты не считаются
   изменением — иначе `paired_stale_seconds` всегда будет около нуля и поле,
   ради которого оно заведено (актуален ли пол по близнецу), станет ложью.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Iterable, Literal

from .util import SIZE_EPS, fsize, to_price, to_tick

Side = Literal["BID", "ASK"]

_SIDE_MAP = {
    "BUY": "BID",
    "BID": "BID",
    "B": "BID",
    "SELL": "ASK",
    "ASK": "ASK",
    "S": "ASK",
}


def norm_side(raw: str | None) -> Side | None:
    if raw is None:
        return None
    return _SIDE_MAP.get(str(raw).strip().upper())  # type: ignore[return-value]


@dataclass(slots=True)
class LevelDelta:
    """Изменение одного уровня. delta < 0 — сокращение (съели или сняли)."""

    side: Side
    tick: int
    old_size: float
    new_size: float
    ts_ms: int
    batch_id: int

    @property
    def delta(self) -> float:
        return self.new_size - self.old_size

    @property
    def reduced(self) -> float:
        return max(0.0, self.old_size - self.new_size)


@dataclass(slots=True)
class BookSnapshot:
    """Ровно те поля, которые идут в paper-book.csv, плюс служебные."""

    ts_ms: int
    best_bid: float | None
    best_ask: float | None
    mid: float | None
    size_at_001: float
    size_at_002: float
    size_at_003: float
    size_at_005: float
    depth_bid_total: float
    n_bid_levels: int
    bid_notional_above_002: float
    bid_shares_above_002: float
    last_real_change_ms: int


class Book:
    """L2-книга одного токена, ведомая пособытийно."""

    __slots__ = (
        "asset_id",
        "bids",
        "asks",
        "last_update_ms",
        "last_real_change_ms",
        "last_snapshot_ms",
        "_batch_id",
        "ready",
        "updates_applied",
        "snapshots_applied",
    )

    def __init__(self, asset_id: str) -> None:
        self.asset_id = asset_id
        self.bids: dict[int, float] = {}
        self.asks: dict[int, float] = {}
        self.last_update_ms: int = 0
        self.last_real_change_ms: int = 0
        self.last_snapshot_ms: int = 0
        self._batch_id: int = 0
        self.ready: bool = False
        self.updates_applied: int = 0
        self.snapshots_applied: int = 0

    # ---------------------------------------------------------------- приём

    def apply_snapshot(
        self, buys: Iterable[dict], sells: Iterable[dict], ts_ms: int
    ) -> bool:
        """Полный снапшот (`book`). Возвращает True, если состояние изменилось.

        После снапшота классифицировать висящие сокращения нельзя — вызывающая
        сторона обязана закрыть их как `unknown`.
        """
        new_bids: dict[int, float] = {}
        new_asks: dict[int, float] = {}
        for lvl in buys or ():
            t = to_tick(lvl.get("price"))
            s = fsize(lvl.get("size"))
            if t is not None and s > SIZE_EPS:
                new_bids[t] = new_bids.get(t, 0.0) + s
        for lvl in sells or ():
            t = to_tick(lvl.get("price"))
            s = fsize(lvl.get("size"))
            if t is not None and s > SIZE_EPS:
                new_asks[t] = new_asks.get(t, 0.0) + s

        changed = new_bids != self.bids or new_asks != self.asks
        self.bids = new_bids
        self.asks = new_asks
        self.last_update_ms = ts_ms
        if changed:
            self.last_real_change_ms = ts_ms
        self.last_snapshot_ms = ts_ms
        self.snapshots_applied += 1
        self.ready = True
        return changed

    def apply_price_change(
        self, changes: Iterable[dict], ts_ms: int
    ) -> list[LevelDelta]:
        """Инкрементальное обновление. `size` в changes — новый размер уровня."""
        self._batch_id += 1
        batch = self._batch_id
        deltas: list[LevelDelta] = []
        for ch in changes or ():
            side = norm_side(ch.get("side"))
            tick = to_tick(ch.get("price"))
            if side is None or tick is None:
                continue
            new_size = fsize(ch.get("size"))
            side_map = self.bids if side == "BID" else self.asks
            old_size = side_map.get(tick, 0.0)
            if abs(new_size - old_size) <= SIZE_EPS:
                continue  # повтор того же состояния — не изменение
            if new_size <= SIZE_EPS:
                side_map.pop(tick, None)
            else:
                side_map[tick] = new_size
            deltas.append(
                LevelDelta(
                    side=side,
                    tick=tick,
                    old_size=old_size,
                    new_size=new_size,
                    ts_ms=ts_ms,
                    batch_id=batch,
                )
            )
        self.last_update_ms = ts_ms
        if deltas:
            self.last_real_change_ms = ts_ms
            self.updates_applied += 1
            self.ready = True
        return deltas

    # ---------------------------------------------------------------- чтение

    def best_bid_tick(self) -> int | None:
        return max(self.bids) if self.bids else None

    def best_ask_tick(self) -> int | None:
        return min(self.asks) if self.asks else None

    def best_bid(self) -> float | None:
        return to_price(self.best_bid_tick())

    def best_ask(self) -> float | None:
        return to_price(self.best_ask_tick())

    def size_at(self, side: Side, tick: int) -> float:
        return (self.bids if side == "BID" else self.asks).get(tick, 0.0)

    def depth_total(self, side: Side) -> float:
        return sum((self.bids if side == "BID" else self.asks).values())

    def n_levels(self, side: Side) -> int:
        return len(self.bids if side == "BID" else self.asks)

    def notional_above(self, tick: int) -> tuple[float, float]:
        """(нотионал в долларах, размер в долях) по бидам СТРОГО выше tick.

        Нужно для второй трактовки entry.min_bid_above: ТЗ формулирует порог
        как «биды суммарно хотя бы на 0.08», а скобкой уточняет до «лучший бид
        >= 0.08». Гейтим по цене, но пишем обе величины, чтобы нотиональная
        трактовка проверялась офлайн без пересбора.
        """
        notional = 0.0
        shares = 0.0
        for t, s in self.bids.items():
            if t > tick:
                notional += (t / 1000.0) * s
                shares += s
        return notional, shares

    def contiguous_bid_band(self, top_tick: int, bottom_tick: int) -> bool:
        """Все ли тики между bottom и top (включительно) пусты после апдейта.

        Признак того, что бид-сторона выметена полосой, а не точечно.
        """
        return all(self.bids.get(t, 0.0) <= SIZE_EPS for t in range(bottom_tick, top_tick + 1))

    def snapshot(self, ts_ms: int, entry_tick: int = 20) -> BookSnapshot:
        bb = self.best_bid_tick()
        ba = self.best_ask_tick()
        notional, shares = self.notional_above(entry_tick)
        return BookSnapshot(
            ts_ms=ts_ms,
            best_bid=to_price(bb),
            best_ask=to_price(ba),
            mid=round((bb + ba) / 2000.0, 6) if bb is not None and ba is not None else None,
            size_at_001=self.bids.get(1, 0.0),
            size_at_002=self.bids.get(20, 0.0),
            size_at_003=self.bids.get(30, 0.0),
            size_at_005=self.bids.get(50, 0.0),
            depth_bid_total=self.depth_total("BID"),
            n_bid_levels=len(self.bids),
            bid_notional_above_002=notional,
            bid_shares_above_002=shares,
            last_real_change_ms=self.last_real_change_ms,
        )

    def clear(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.ready = False


class RingBuffer:
    """Снапшоты за последние `seconds` секунд с шагом `interval_s`.

    Нужен только для предсобытийной части окна (-30..0). Держать больше
    незачем: непрерывный лог книги по всем рынкам — это ровно тот путь, который
    в прошлый раз дал 48 млн снапшотов.
    """

    __slots__ = ("interval_ms", "buf", "_last_ms")

    def __init__(self, seconds: int, interval_s: int) -> None:
        self.interval_ms = interval_s * 1000
        self.buf: deque[BookSnapshot] = deque(maxlen=max(1, seconds // max(1, interval_s) + 2))
        self._last_ms = 0

    def maybe_push(self, snap: BookSnapshot) -> bool:
        if snap.ts_ms - self._last_ms < self.interval_ms:
            return False
        self.buf.append(snap)
        self._last_ms = snap.ts_ms
        return True

    def nearest(self, target_ms: int, tolerance_ms: int) -> BookSnapshot | None:
        """Ближайший снапшот к целевому моменту сетки.

        Кольцевой буфер тикает по часам процесса, а сетка окна отсчитывается от
        филла — совпадения моментов нет, поэтому узлы сетки заполняются
        ближайшим снапшотом в пределах допуска, а не подряд идущими записями.
        """
        best: BookSnapshot | None = None
        best_d = tolerance_ms + 1
        for s in self.buf:
            d = abs(s.ts_ms - target_ms)
            if d < best_d:
                best, best_d = s, d
        return best
