"""Классификатор: сделка на уровне или отмена заявок.

Исходная проблема. `price_change` сообщает только новый размер уровня. Уровень
0.02, ушедший с 3000 в 0, — это либо съели рыночной продажей (нас бы залило),
либо мейкер снял заявки (наша осталась бы лежать). Из самого сообщения эти два
случая неразличимы, а путать их нельзя: половина записей была бы о филлах,
которых не было.

Различаем корреляцией с независимым свидетельством сделки:

  Уровень A (здесь)  — `last_trade_price` как «кредит» на уровень, гасящий
                       сокращения книги в окне +-classify.window_ms.
                       Порядок сообщений не гарантирован, поэтому окно
                       двустороннее, а несопоставленные сокращения
                       откладываются до дедлайна, а не решаются сразу.
  Уровень B (здесь)  — эвристика свипа: тейкер идёт по книге сверху вниз,
                       а `last_trade_price` может отдать одну цену на весь
                       проход. Помечается отдельно (`sweep_inferred`) и
                       никогда не подменяет `trigger`.
  Уровень C (reconcile.py) — сверка с ончейн-лентой data-api. Она даёт
                       истину и матрицу ошибок этого классификатора.
                       Без уровня C числам отсюда верить нельзя.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable, Literal

from .book import Book, LevelDelta, Side
from .util import SIZE_EPS

log = logging.getLogger("tape")

Evidence = Literal["ws_trade", "sweep_inferred", "none", "snapshot_reset"]


@dataclass(slots=True)
class Trade:
    tick: int
    size: float
    ts_ms: int
    side_hit: Side
    reported_side: str
    source: str = "ws"


@dataclass(slots=True)
class Classification:
    """Итог по одному сокращению уровня."""

    asset_id: str
    side: Side
    tick: int
    reduced: float
    traded: float
    cancelled: float
    unknown: float
    evidence: Evidence
    ts_ms: int
    settled_ms: int
    batch_id: int
    opposite_side_changed_ms: int | None


@dataclass(slots=True)
class _Credit:
    tick: int
    size: float
    ts_ms: int
    consumed: float = 0.0

    @property
    def left(self) -> float:
        return max(0.0, self.size - self.consumed)


@dataclass(slots=True)
class _Pending:
    side: Side
    tick: int
    remaining: float
    original: float
    new_size: float
    ts_ms: int
    deadline_ms: int
    batch_id: int
    traded: float = 0.0
    evidence: Evidence = "none"
    opposite_side_changed_ms: int | None = None


@dataclass
class _CalibVote:
    reported_side: str
    deadline_ms: int
    bid_reduced: float = 0.0
    ask_reduced: float = 0.0


class SideCalibrator:
    """Что означает `side` в `last_trade_price` — сторону тейкера или мейкера.

    По памяти это утверждать нельзя, а ошибка переворачивает вообще всё: вход
    срабатывает на противоположных событиях. Поэтому первые N сделок процесс
    смотрит, какая сторона книги реально сократилась после сделки с
    `side=SELL`, и фиксирует вывод в state/calibration.json.

    До набора выборки работает гипотеза «side = сторона тейкера»
    (SELL -> выбиты биды), а события помечаются calibration_pending=1.
    """

    def __init__(self, target: int = 200) -> None:
        self.target = target
        self.taker_votes = 0
        self.maker_votes = 0
        self.locked: bool = False
        self.taker_semantics: bool = True

    @property
    def pending(self) -> bool:
        return not self.locked

    @property
    def n_votes(self) -> int:
        return self.taker_votes + self.maker_votes

    def side_hit(self, reported_side: str) -> Side:
        sell = str(reported_side).strip().upper() in ("SELL", "S", "ASK")
        if self.taker_semantics:
            return "BID" if sell else "ASK"
        return "ASK" if sell else "BID"

    def vote(self, reported_side: str, bid_reduced: float, ask_reduced: float) -> None:
        if self.locked:
            return
        if abs(bid_reduced - ask_reduced) <= SIZE_EPS:
            return  # неинформативно
        sell = str(reported_side).strip().upper() in ("SELL", "S", "ASK")
        bid_shrank_more = bid_reduced > ask_reduced
        if sell == bid_shrank_more:
            self.taker_votes += 1
        else:
            self.maker_votes += 1
        if self.n_votes >= self.target:
            self.taker_semantics = self.taker_votes >= self.maker_votes
            self.locked = True
            log.warning(
                "калибровка стороны завершена: taker_semantics=%s (%d/%d голосов)",
                self.taker_semantics,
                self.taker_votes,
                self.n_votes,
            )

    def to_dict(self) -> dict:
        return {
            "locked": self.locked,
            "taker_semantics": self.taker_semantics,
            "taker_votes": self.taker_votes,
            "maker_votes": self.maker_votes,
            "target": self.target,
        }

    def load(self, data: dict) -> None:
        self.locked = bool(data.get("locked", False))
        self.taker_semantics = bool(data.get("taker_semantics", True))
        self.taker_votes = int(data.get("taker_votes", 0))
        self.maker_votes = int(data.get("maker_votes", 0))


class TapeClassifier:
    """Пособытийная классификация сокращений книги одного токена."""

    def __init__(
        self,
        asset_id: str,
        *,
        window_ms: int = 1500,
        credit_ttl_ms: int = 3000,
        sweep_inference: bool = True,
        calibrator: SideCalibrator | None = None,
        on_classified: Callable[[Classification], None] | None = None,
        book: "Book | None" = None,
    ) -> None:
        self.asset_id = asset_id
        self.book = book
        self.window_ms = window_ms
        self.credit_ttl_ms = credit_ttl_ms
        self.sweep_inference = sweep_inference
        self.calibrator = calibrator or SideCalibrator()
        self.on_classified = on_classified

        self._credits: dict[tuple[Side, int], list[_Credit]] = {}
        self._pending: list[_Pending] = []
        # Батчи, в которых хоть один уровень был погашен настоящим кредитом
        # сделки. Уровень с кредитом гасится немедленно и до вывода о свипе не
        # доживает, поэтому доказательство нужно хранить отдельно от очереди.
        self._batch_traded: dict[int, float] = {}
        self._batch_seen_ms: dict[int, int] = {}
        self._calib: list[_CalibVote] = []
        self._last_change_ms: dict[Side, int] = {"BID": 0, "ASK": 0}

        self.stat_traded = 0.0
        self.stat_cancelled = 0.0
        self.stat_unknown = 0.0

    # ------------------------------------------------------------------ вход

    def on_trade(self, trade: Trade) -> list[Classification]:
        """Сделка: кладём кредит и сразу гасим уже висящие сокращения."""
        key = (trade.side_hit, trade.tick)
        self._credits.setdefault(key, []).append(
            _Credit(tick=trade.tick, size=trade.size, ts_ms=trade.ts_ms)
        )
        self._calib.append(
            _CalibVote(
                reported_side=trade.reported_side,
                deadline_ms=trade.ts_ms + self.window_ms,
            )
        )
        return self._settle_matching(trade.side_hit, trade.tick, trade.ts_ms)

    def on_deltas(self, deltas: list[LevelDelta]) -> list[Classification]:
        """Обновления книги: сокращения — в отложенную очередь."""
        out: list[Classification] = []
        for d in deltas:
            self._last_change_ms[d.side] = max(self._last_change_ms[d.side], d.ts_ms)
        for d in deltas:
            if d.reduced <= SIZE_EPS:
                continue  # добавление: очередь ПОЗАДИ нас, для классификации не нужно
            for v in self._calib:
                if d.ts_ms <= v.deadline_ms:
                    if d.side == "BID":
                        v.bid_reduced += d.reduced
                    else:
                        v.ask_reduced += d.reduced
            opposite = self._last_change_ms["ASK" if d.side == "BID" else "BID"]
            p = _Pending(
                side=d.side,
                tick=d.tick,
                remaining=d.reduced,
                original=d.reduced,
                new_size=d.new_size,
                ts_ms=d.ts_ms,
                deadline_ms=d.ts_ms + self.window_ms,
                batch_id=d.batch_id,
                opposite_side_changed_ms=(d.ts_ms - opposite) if opposite else None,
            )
            self._consume_credits(p)
            if p.remaining <= SIZE_EPS:
                out.append(self._emit(p, d.ts_ms))
            else:
                self._pending.append(p)
        return out

    def on_snapshot_reset(self, ts_ms: int) -> list[Classification]:
        """Полный снапшот обнулил состояние — классифицировать через него нельзя.

        Всё висящее закрывается как `unknown`. Это не мусор, а метрика качества:
        доля unknown идёт в coverage и прямо говорит, насколько можно верить
        остальным классификациям.
        """
        out = [self._emit(p, ts_ms, force_evidence="snapshot_reset") for p in self._pending]
        self._pending.clear()
        self._credits.clear()
        return out

    def tick(self, now_ms: int) -> list[Classification]:
        """Истёкшие дедлайны -> окончательная классификация."""
        out: list[Classification] = []
        if self._pending:
            expired = [p for p in self._pending if p.deadline_ms <= now_ms]
            if expired:
                self._pending = [p for p in self._pending if p.deadline_ms > now_ms]
                if self.sweep_inference:
                    self._infer_sweeps(expired)
                out.extend(self._emit(p, now_ms) for p in expired)
        self._expire_credits(now_ms)
        self._flush_calibration(now_ms)
        return out

    # -------------------------------------------------------------- механика

    def _consume_credits(self, p: _Pending) -> None:
        bucket = self._credits.get((p.side, p.tick))
        if not bucket:
            return
        for c in bucket:
            if p.remaining <= SIZE_EPS:
                break
            if abs(c.ts_ms - p.ts_ms) > self.window_ms:
                continue
            take = min(c.left, p.remaining)
            if take <= SIZE_EPS:
                continue
            c.consumed += take
            p.remaining -= take
            p.traded += take
            p.evidence = "ws_trade"
            self._batch_traded[p.batch_id] = self._batch_traded.get(p.batch_id, 0.0) + take
            self._batch_seen_ms[p.batch_id] = max(self._batch_seen_ms.get(p.batch_id, 0), p.ts_ms)
        self._credits[(p.side, p.tick)] = [c for c in bucket if c.left > SIZE_EPS]

    def _settle_matching(self, side: Side, tick: int, now_ms: int) -> list[Classification]:
        out: list[Classification] = []
        still: list[_Pending] = []
        for p in self._pending:
            if p.side == side and p.tick == tick:
                self._consume_credits(p)
                if p.remaining <= SIZE_EPS:
                    out.append(self._emit(p, now_ms))
                    continue
            still.append(p)
        self._pending = still
        return out

    def _infer_sweeps(self, expired: list[_Pending]) -> None:
        """Полоса сокращений сверху вниз в одном сообщении = свип.

        Тейкер, проходящий книгу, порождает несколько филлов по ценам мейкеров,
        а `last_trade_price` может отдать одно сообщение с одной ценой. Тогда
        уровни ниже остались бы без кредита и были бы ложно записаны в отмены.

        Условия, все обязательны:
          * сторона BID, в батче минимум два затронутых уровня;
          * все уровни выше нижнего выметены в ноль (частичным может быть
            только самый нижний) — так выглядит проход, а не точечная отмена;
          * в батче есть настоящий кредит сделки (возможно, на уровне, который
            уже был погашен и до сюда не дожил — отсюда журнал _batch_traded);
          * между затронутыми уровнями не осталось нетронутых бидов с
            размером, и верх полосы был лучшим бидом. Свип начинается сверху и
            не перепрыгивает уровни; отмена середины книги — перепрыгивает.

        Это догадка, а не истина, поэтому она живёт в отдельном поле evidence
        и проверяется сверкой с ончейн-лентой.
        """
        by_batch: dict[int, list[_Pending]] = {}
        for p in expired:
            if p.side == "BID":
                by_batch.setdefault(p.batch_id, []).append(p)
        for batch_id, batch in by_batch.items():
            unresolved = [p for p in batch if p.remaining > SIZE_EPS]
            if not unresolved:
                continue
            proven = self._batch_traded.get(batch_id, 0.0) > SIZE_EPS or any(
                p.traded > SIZE_EPS for p in batch
            )
            if not proven:
                continue
            batch.sort(key=lambda x: x.tick, reverse=True)
            if len(batch) < 2 and self._batch_traded.get(batch_id, 0.0) <= SIZE_EPS:
                continue
            if any(p.new_size > SIZE_EPS for p in batch[:-1]):
                continue
            if self.book is not None:
                top, bottom = batch[0].tick, batch[-1].tick
                if bottom + 1 <= top and not self.book.contiguous_bid_band(top, bottom + 1):
                    continue  # внутри полосы остались биды — это не проход
                bb = self.book.best_bid_tick()
                if bb is not None and bb > top:
                    continue  # выше полосы живые биды — свип начался бы с них
            for p in unresolved:
                p.traded += p.remaining
                p.remaining = 0.0
                p.evidence = "sweep_inferred"

    def _emit(
        self, p: _Pending, settled_ms: int, force_evidence: Evidence | None = None
    ) -> Classification:
        evidence = force_evidence or (p.evidence if p.traded > SIZE_EPS else "none")
        unknown = p.remaining if force_evidence == "snapshot_reset" else 0.0
        cancelled = 0.0 if force_evidence == "snapshot_reset" else p.remaining
        self.stat_traded += p.traded
        self.stat_cancelled += cancelled
        self.stat_unknown += unknown
        c = Classification(
            asset_id=self.asset_id,
            side=p.side,
            tick=p.tick,
            reduced=p.original,
            traded=p.traded,
            cancelled=cancelled,
            unknown=unknown,
            evidence=evidence,
            ts_ms=p.ts_ms,
            settled_ms=settled_ms,
            batch_id=p.batch_id,
            opposite_side_changed_ms=p.opposite_side_changed_ms,
        )
        if self.on_classified:
            self.on_classified(c)
        return c

    def _expire_credits(self, now_ms: int) -> None:
        if not self._credits:
            return
        cutoff = now_ms - self.credit_ttl_ms
        empty = []
        for key, bucket in self._credits.items():
            kept = [c for c in bucket if c.ts_ms >= cutoff and c.left > SIZE_EPS]
            if kept:
                self._credits[key] = kept
            else:
                empty.append(key)
        for key in empty:
            del self._credits[key]
        stale = [b for b, ts in self._batch_seen_ms.items() if ts < cutoff]
        for b in stale:
            self._batch_seen_ms.pop(b, None)
            self._batch_traded.pop(b, None)

    def _flush_calibration(self, now_ms: int) -> None:
        if not self._calib:
            return
        due = [v for v in self._calib if v.deadline_ms <= now_ms]
        if not due:
            return
        self._calib = [v for v in self._calib if v.deadline_ms > now_ms]
        for v in due:
            self.calibrator.vote(v.reported_side, v.bid_reduced, v.ask_reduced)
