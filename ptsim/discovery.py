"""Обнаружение рынков через Gamma и управление окном подписки.

Две вещи, на которых ломается сбор:

1. Момент подписки. В прошлой версии логгера подписка шла по появлению рынка в
   API, и матчи не наблюдались вовсе. Здесь окно считается от gameStartTime.
   Но у многих киберспортивных подрынков gameStartTime ПУСТ — тогда подписка
   идёт сразу при обнаружении, а поле game_start_source честно говорит, что
   время старта выведено или неизвестно. Без этого критерий
   observed_during_game >= 80% непроверяем.

2. Потолок подписок. Матч даёт десятки подрынков x 2 токена; при переполнении
   выбор ДОЛЖЕН быть случайным, а не по ликвидности: эффект на 2 центах живёт
   в тонких подрынках, и отбор по ликвидности выкинул бы именно ту популяцию,
   ради которой всё считается. Каждый отброшенный рынок пишется в coverage:
   молчаливый дроп сжимает знаменатель, не трогая числитель, и завышает
   частоту.
"""
from __future__ import annotations

import hashlib
import json
import logging
import random
from dataclasses import dataclass, field
from typing import Any, Iterable

from .classify import classify
from .util import iso_to_ms, ms_to_iso, now_ms

log = logging.getLogger("discovery")


@dataclass
class MarketRec:
    condition_id: str
    asset_id_a: str
    asset_id_b: str
    question: str
    slug: str
    sport: str
    market_level: str
    kind: str
    segment_no: int | None
    volume: float
    liquidity: float
    game_start_ms: int | None
    game_start_source: str
    end_date_ms: int | None
    game_start_time: str | None = None
    end_date: str | None = None
    first_seen_ms: int = 0
    last_seen_ms: int = 0
    subscribed_at_ms: int | None = None
    released_at_ms: int | None = None
    observed_during_game: int = 0
    release_reason: str | None = None
    resolved: int = 0
    dropped_by_overflow: int = 0

    @property
    def assets(self) -> tuple[str, str]:
        return (self.asset_id_a, self.asset_id_b)

    def paired(self, asset_id: str) -> str | None:
        if asset_id == self.asset_id_a:
            return self.asset_id_b
        if asset_id == self.asset_id_b:
            return self.asset_id_a
        return None

    def to_row(self) -> dict:
        return {
            "condition_id": self.condition_id,
            "asset_id_a": self.asset_id_a,
            "asset_id_b": self.asset_id_b,
            "question": self.question,
            "slug": self.slug,
            "sport": self.sport,
            "market_level": self.market_level,
            "kind": self.kind,
            "segment_no": self.segment_no,
            "volume": self.volume,
            "liquidity": self.liquidity,
            "game_start_time": self.game_start_time,
            "game_start_ms": self.game_start_ms,
            "game_start_source": self.game_start_source,
            "end_date": self.end_date,
            "end_date_ms": self.end_date_ms,
            "first_seen": ms_to_iso(self.first_seen_ms),
            "last_seen": ms_to_iso(self.last_seen_ms),
            "subscribed_at": ms_to_iso(self.subscribed_at_ms),
            "released_at": ms_to_iso(self.released_at_ms),
            "observed_during_game": self.observed_during_game,
            "release_reason": self.release_reason,
            "dropped_by_overflow": self.dropped_by_overflow,
            "resolved": self.resolved,
        }


def parse_token_ids(raw: Any) -> tuple[str, str] | None:
    """clobTokenIds приходит JSON-строкой, иногда уже списком."""
    if raw is None:
        return None
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError:
            return None
    if isinstance(raw, (list, tuple)) and len(raw) >= 2:
        a, b = str(raw[0]), str(raw[1])
        return (a, b) if a and b and a != b else None
    return None


def parse_market(raw: dict, disciplines: Iterable[str], ts_ms: int) -> MarketRec | None:
    cid = raw.get("conditionId") or raw.get("condition_id")
    tokens = parse_token_ids(raw.get("clobTokenIds") or raw.get("clob_token_ids"))
    if not cid or not tokens:
        return None
    slug = raw.get("slug") or ""
    question = raw.get("question") or ""
    c = classify(slug, question)
    if c["sport"] not in set(disciplines):
        return None

    gs_raw = raw.get("gameStartTime") or raw.get("game_start_time")
    gs_ms = iso_to_ms(gs_raw)
    if gs_ms is not None:
        source = "gamma"
    else:
        # Fallback: startDate. Если и его нет — время старта неизвестно, и это
        # ДОЛЖНО быть видно в данных, а не замаскировано подстановкой.
        gs_ms = iso_to_ms(raw.get("startDate") or raw.get("start_date"))
        source = "inferred" if gs_ms is not None else "none"

    return MarketRec(
        condition_id=str(cid),
        asset_id_a=tokens[0],
        asset_id_b=tokens[1],
        question=question,
        slug=slug,
        sport=c["sport"],
        market_level=c["market_level"],
        kind=c["kind"],
        segment_no=c["segment_no"],
        volume=float(raw.get("volumeNum") or raw.get("volume") or 0.0),
        liquidity=float(raw.get("liquidityNum") or raw.get("liquidity") or 0.0),
        game_start_ms=gs_ms,
        game_start_source=source,
        game_start_time=gs_raw or raw.get("startDate"),
        end_date_ms=iso_to_ms(raw.get("endDate") or raw.get("end_date")),
        end_date=raw.get("endDate") or raw.get("end_date"),
        first_seen_ms=ts_ms,
        last_seen_ms=ts_ms,
    )


def stable_sample(cids: list[str], k: int, seed: str) -> set[str]:
    """Устойчивая случайная выборка.

    Обычный random.sample на каждом опросе давал бы новый состав, то есть
    постоянные переподписки и дыры в наблюдении на ровном месте. Здесь порядок
    детерминирован хэшом от (seed, condition_id): состав меняется только когда
    меняется множество кандидатов.
    """
    if k >= len(cids):
        return set(cids)
    scored = sorted(cids, key=lambda c: hashlib.md5(f"{seed}:{c}".encode()).hexdigest())
    return set(scored[:k])


class MarketRegistry:
    """Реестр наблюдаемых рынков. Переживает рестарт: состояние в SQLite."""

    def __init__(self, cfg, store, seed: str | None = None) -> None:
        self.cfg = cfg
        self.store = store
        self.markets: dict[str, MarketRec] = {}
        self.seed = seed or f"{random.getrandbits(48):x}"
        self.dropped_this_poll: int = 0
        self.sampling_rate: float = 1.0

    def load_from_db(self) -> None:
        for row in self.store.query("SELECT * FROM markets WHERE resolved = 0"):
            rec = MarketRec(
                condition_id=row["condition_id"],
                asset_id_a=row["asset_id_a"] or "",
                asset_id_b=row["asset_id_b"] or "",
                question=row["question"] or "",
                slug=row["slug"] or "",
                sport=row["sport"] or "",
                market_level=row["market_level"] or "",
                kind=row["kind"] or "",
                segment_no=row["segment_no"],
                volume=row["volume"] or 0.0,
                liquidity=row["liquidity"] or 0.0,
                game_start_ms=row["game_start_ms"],
                game_start_source=row["game_start_source"] or "none",
                end_date_ms=row["end_date_ms"],
                game_start_time=row["game_start_time"],
                end_date=row["end_date"],
                first_seen_ms=iso_to_ms(row["first_seen"], 0) or 0,
                last_seen_ms=iso_to_ms(row["last_seen"], 0) or 0,
                subscribed_at_ms=iso_to_ms(row["subscribed_at"]),
                observed_during_game=row["observed_during_game"] or 0,
                resolved=row["resolved"] or 0,
            )
            if rec.asset_id_a:
                self.markets[rec.condition_id] = rec
        if self.markets:
            log.info("реестр восстановлен из базы: %d рынков", len(self.markets))

    def ingest(self, raws: list[dict], ts_ms: int | None = None) -> int:
        ts = ts_ms or now_ms()
        added = 0
        for raw in raws:
            rec = parse_market(raw, self.cfg.disciplines, ts)
            if rec is None:
                continue
            old = self.markets.get(rec.condition_id)
            if old is None:
                self.markets[rec.condition_id] = rec
                added += 1
            else:
                old.last_seen_ms = ts
                old.volume = rec.volume
                old.liquidity = rec.liquidity
                if old.game_start_ms is None and rec.game_start_ms is not None:
                    old.game_start_ms = rec.game_start_ms
                    old.game_start_source = rec.game_start_source
                    old.game_start_time = rec.game_start_time
        for rec in self.markets.values():
            self.store.upsert("markets", rec.to_row(), ["condition_id"])
        return added

    def wants_subscription(self, rec: MarketRec, now: int) -> bool:
        if rec.resolved:
            return False
        cap_ms = self.cfg.market_discovery.max_subscription_hours * 3_600_000
        if rec.subscribed_at_ms and now - rec.subscribed_at_ms > cap_ms:
            return False  # потолок против зависших рынков
        if rec.game_start_ms is not None:
            start = rec.game_start_ms - self.cfg.market_discovery.subscribe_before_game_s * 1000
            if now < start:
                return False
            # Релиз ТОЛЬКО по резолюции: endDate + 30 мин оставлен как нижняя
            # граница удержания, а не как правило отписки.
            return True
        return bool(self.cfg.market_discovery.subscribe_when_start_unknown)

    def active_markets(self, now: int | None = None) -> list[MarketRec]:
        n = now or now_ms()
        return [m for m in self.markets.values() if self.wants_subscription(m, n)]

    def desired_assets(self, now: int | None = None) -> dict[str, MarketRec]:
        """Множество asset_id для подписки, с учётом потолка.

        Подписка идёт на ОБА токена рынка: второй нужен для fair_lower_bound,
        то есть для пола цены по обратной стороне.
        """
        n = now or now_ms()
        active = self.active_markets(n)
        cap = self.cfg.ws.max_total_assets
        max_markets = max(1, cap // 2)

        self.dropped_this_poll = 0
        self.sampling_rate = 1.0
        if len(active) > max_markets:
            policy = self.cfg.ws.overflow_policy
            cids = [m.condition_id for m in active]
            if policy == "random_sample":
                keep = stable_sample(cids, max_markets, self.seed)
            elif policy == "by_liquidity":
                keep = {m.condition_id for m in sorted(
                    active, key=lambda m: m.liquidity, reverse=True)[:max_markets]}
            else:
                keep = {m.condition_id for m in sorted(
                    active, key=lambda m: m.subscribed_at_ms or m.first_seen_ms)[:max_markets]}
            self.dropped_this_poll = len(active) - len(keep)
            self.sampling_rate = len(keep) / len(active)
            log.warning(
                "потолок подписок: держим %d из %d рынков (policy=%s, rate=%.3f)",
                len(keep), len(active), policy, self.sampling_rate,
            )
            for m in active:
                m.dropped_by_overflow = 0 if m.condition_id in keep else 1
                if m.dropped_by_overflow:
                    self.store.update("markets", {"condition_id": m.condition_id},
                                      {"dropped_by_overflow": 1})
            active = [m for m in active if m.condition_id in keep]

        out: dict[str, MarketRec] = {}
        for m in active:
            if m.subscribed_at_ms is None:
                m.subscribed_at_ms = n
                self.store.update("markets", {"condition_id": m.condition_id},
                                  {"subscribed_at": ms_to_iso(n)})
            out[m.asset_id_a] = m
            out[m.asset_id_b] = m
        return out

    def mark_observed_during_game(self, condition_id: str) -> None:
        rec = self.markets.get(condition_id)
        if rec is None or rec.observed_during_game:
            return
        rec.observed_during_game = 1
        self.store.update("markets", {"condition_id": condition_id},
                          {"observed_during_game": 1})

    def release(self, condition_id: str, reason: str, now: int | None = None) -> None:
        rec = self.markets.get(condition_id)
        if rec is None:
            return
        rec.resolved = 1
        rec.release_reason = reason
        rec.released_at_ms = now or now_ms()
        self.store.update(
            "markets", {"condition_id": condition_id},
            {"resolved": 1, "release_reason": reason,
             "released_at": ms_to_iso(rec.released_at_ms)},
        )
