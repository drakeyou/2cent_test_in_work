"""CLOB WebSocket: шардинг, переподключение, учёт разрывов.

Разрыв ОБЯЗАН попадать в журнал с длительностью. Незалогированный разрыв
завышает частоту события напрямую: знаменатель (наблюдённое время) сжимается,
а числитель нет. То же относится к переподписке — это тоже слепота, просто
добровольная, и она тоже пишется.

Назначение ассетов по шардам липкое. Наивное «отсортировать и нарезать» при
добавлении одного ассета сдвигает состав всех соединений и рвёт наблюдение по
всему списку сразу.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
from dataclasses import dataclass, field
from typing import Awaitable, Callable

import websockets

from .util import ms_to_iso, now_ms

log = logging.getLogger("ws")

MessageHandler = Callable[[dict], None]
GapHandler = Callable[[dict], None]


@dataclass
class _Shard:
    idx: int
    assets: set[str] = field(default_factory=set)
    task: asyncio.Task | None = None
    generation: int = 0
    connected: bool = False
    gap_started_ms: int | None = None
    gap_reason: str = "startup"


class WsManager:
    def __init__(
        self,
        cfg,
        on_message: MessageHandler,
        on_gap: GapHandler,
    ) -> None:
        self.cfg = cfg
        self.on_message = on_message
        self.on_gap = on_gap
        self.shards: dict[int, _Shard] = {}
        self.assignment: dict[str, int] = {}
        self._stopping = False
        self.messages_seen = 0
        self.unknown_types: dict[str, int] = {}

    # ------------------------------------------------------------- шардинг

    def _assign(self, assets: set[str]) -> None:
        cap = self.cfg.ws.max_assets_per_conn
        for a in list(self.assignment):
            if a not in assets:
                idx = self.assignment.pop(a)
                self.shards[idx].assets.discard(a)
        for a in sorted(assets):
            if a in self.assignment:
                continue
            target = None
            for idx in sorted(self.shards):
                if len(self.shards[idx].assets) < cap:
                    target = idx
                    break
            if target is None:
                target = max(self.shards) + 1 if self.shards else 0
                self.shards[target] = _Shard(idx=target)
            self.shards[target].assets.add(a)
            self.assignment[a] = target

    async def set_assets(self, assets: set[str]) -> None:
        before = {i: set(s.assets) for i, s in self.shards.items()}
        self._assign(assets)
        for idx, shard in list(self.shards.items()):
            if shard.assets == before.get(idx, set()) and shard.task and not shard.task.done():
                continue
            if shard.task and not shard.task.done():
                shard.generation += 1
                shard.task.cancel()
            if not shard.assets:
                shard.task = None
                continue
            shard.task = asyncio.create_task(
                self._run_shard(shard, shard.generation), name=f"ws-shard-{idx}"
            )

    async def stop(self) -> None:
        self._stopping = True
        for shard in self.shards.values():
            if shard.task:
                shard.task.cancel()
        await asyncio.gather(
            *[s.task for s in self.shards.values() if s.task], return_exceptions=True
        )

    # ------------------------------------------------------------ соединение

    async def _run_shard(self, shard: _Shard, generation: int) -> None:
        backoff = self.cfg.ws.backoff_initial_s
        if shard.gap_started_ms is None:
            shard.gap_started_ms = now_ms()
            shard.gap_reason = "resubscribe" if shard.connected else "startup"
        while not self._stopping and shard.generation == generation:
            assets = sorted(shard.assets)
            if not assets:
                return
            try:
                async with websockets.connect(
                    self.cfg.ws.url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_size=8 * 1024 * 1024,
                ) as conn:
                    await conn.send(json.dumps({"assets_ids": assets, "type": "market"}))
                    self._close_gap(shard, len(assets))
                    backoff = self.cfg.ws.backoff_initial_s
                    pinger = asyncio.create_task(self._ping(conn))
                    try:
                        async for raw in conn:
                            self._dispatch(raw)
                    finally:
                        pinger.cancel()
            except asyncio.CancelledError:
                self._open_gap(shard, "resubscribe")
                raise
            except Exception as exc:  # noqa: BLE001
                self._open_gap(shard, type(exc).__name__)
                log.warning("шард %d: разрыв (%s), переподключение через %.1fs",
                            shard.idx, exc, backoff)
            else:
                self._open_gap(shard, "closed")
            if self._stopping or shard.generation != generation:
                return
            await asyncio.sleep(backoff + random.uniform(0, 0.5))
            backoff = min(backoff * 2, self.cfg.ws.backoff_max_s)

    async def _ping(self, conn) -> None:
        try:
            while True:
                await asyncio.sleep(self.cfg.ws.ping_interval_s)
                await conn.send("PING")
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            return

    def _dispatch(self, raw) -> None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", "replace")
        text = raw.strip()
        if not text or text.upper() in ("PONG", "PING"):
            return
        try:
            payload = json.loads(text)
        except ValueError:
            return
        items = payload if isinstance(payload, list) else [payload]
        for item in items:
            if not isinstance(item, dict):
                continue
            self.messages_seen += 1
            et = item.get("event_type") or item.get("type")
            if et not in ("book", "price_change", "last_trade_price", "tick_size_change"):
                # Неизвестный тип НЕ выбрасывается молча: счётчик попадёт в лог,
                # иначе изменение протокола обнаружится по пустым данным.
                self.unknown_types[str(et)] = self.unknown_types.get(str(et), 0) + 1
                continue
            try:
                self.on_message(item)
            except Exception:  # noqa: BLE001
                log.exception("обработчик сообщения упал: %s", et)

    # -------------------------------------------------------------- разрывы

    def _open_gap(self, shard: _Shard, reason: str) -> None:
        shard.connected = False
        if shard.gap_started_ms is None:
            shard.gap_started_ms = now_ms()
            shard.gap_reason = reason

    def _close_gap(self, shard: _Shard, n_assets: int) -> None:
        shard.connected = True
        if shard.gap_started_ms is None:
            return
        started, ended = shard.gap_started_ms, now_ms()
        shard.gap_started_ms = None
        self.on_gap({
            "started_at": ms_to_iso(started),
            "ended_at": ms_to_iso(ended),
            "started_ms": started,
            "ended_ms": ended,
            "duration_ms": ended - started,
            "reason": shard.gap_reason,
            "assets_resubscribed": n_assets,
            "shard": shard.idx,
        })
        log.info("шард %d подписан: %d ассетов (простой %.1f c, %s)",
                 shard.idx, n_assets, (ended - started) / 1000.0, shard.gap_reason)

    @property
    def connected_assets(self) -> int:
        return sum(len(s.assets) for s in self.shards.values() if s.connected)
