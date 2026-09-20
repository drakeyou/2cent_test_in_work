#!/usr/bin/env python3
"""Сборщик данных paper-trade симулятора Polymarket.

ТОЛЬКО ЧТЕНИЕ. Приватных ключей нет, подписи ордеров нет, обращений к
эндпоинтам размещения нет. Виртуальные заявки существуют исключительно в
памяти процесса.

    python run_collector.py --config config.yaml
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal

from ptsim import config as cfgmod
from ptsim import logging_setup
from ptsim.discovery import GammaFetcher, MarketRegistry
from ptsim.engine import Engine
from ptsim.http import Http
from ptsim.integrity import IntegrityChecker
from ptsim.reconcile import Reconciler
from ptsim.resolution import ResolutionFetcher
from ptsim.storage import SqliteStore
from ptsim.target_wallet import TargetWalletTracker
from ptsim.util import ms_to_iso, now_ms
from ptsim.ws_client import WsManager

log = logging.getLogger("collector")


class Collector:
    def __init__(self, cfg) -> None:
        self.cfg = cfg
        self.store = SqliteStore(cfg.storage.db_path, cfg.storage.commit_interval_s)
        self.http = Http(cfg.http.timeout_s, cfg.http.max_retries)
        self.registry = MarketRegistry(cfg, self.store)
        self.gamma = GammaFetcher(cfg, self.http)
        self.engine = Engine(cfg, self.store, self.registry)
        self.ws = WsManager(cfg, self.engine.on_ws_message, self._on_gap)
        self.resolution = ResolutionFetcher(cfg, self.store, self.http,
                                            self.registry, self.engine)
        self.reconciler = Reconciler(cfg, self.store, self.http)
        self.target = TargetWalletTracker(cfg, self.store, self.http)
        self.integrity = IntegrityChecker(cfg, self.store, self.http, self.engine)
        self._stop = asyncio.Event()

    # ------------------------------------------------------------ служебное

    def _on_gap(self, gap: dict) -> None:
        self.store.insert("gaps", gap)
        self.engine.add_gap_seconds(gap)

    def _note_restart_gap(self) -> None:
        """Простой процесса — такой же разрыв наблюдения, как обрыв сокета.

        Незалогированный, он завысит частоту: знаменатель сожмётся, числитель
        нет.
        """
        last = self.store.get_kv("last_heartbeat_ms")
        n = now_ms()
        if last and n - last > 5_000:
            self.store.insert("gaps", {
                "started_at": ms_to_iso(last), "ended_at": ms_to_iso(n),
                "started_ms": last, "ended_ms": n, "duration_ms": n - last,
                "reason": "process_restart", "assets_resubscribed": 0, "shard": None,
            })
            log.warning("простой процесса: %.1f c", (n - last) / 1000.0)

    # -------------------------------------------------------------- циклы

    async def _discovery_loop(self) -> None:
        while not self._stop.is_set():
            try:
                batches = await self.gamma.fetch()
                seen = sum(len(rows) for rows, _ in batches)
                self.registry.skipped_horizon = 0
                if batches:
                    added = 0
                    for rows, hint in batches:
                        added += self.registry.ingest(rows, sport_hint=hint)
                    desired = self.registry.desired_assets()
                    self.engine.sync_assets(desired)
                    await self.ws.set_assets(set(desired))
                    self.engine.shard_of = dict(self.ws.assignment)
                    log.info(
                        "обнаружение: %d рынков в ответе, +%d новых, подписка на %d "
                        "ассетов (%d рынков)%s",
                        seen, added, len(desired), len(desired) // 2,
                        (f", отброшено потолком {self.registry.dropped_this_poll}"
                         if self.registry.dropped_this_poll else "")
                        + (f", вне горизонта {self.registry.skipped_horizon}"
                           if self.registry.skipped_horizon else ""),
                    )
            except Exception:
                log.exception("цикл обнаружения упал")
            await self._sleep(self.cfg.market_discovery.poll_s)

    async def _tick_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.engine.tick()
            except Exception:
                log.exception("тик движка упал")
            await self._sleep(0.5)

    async def _coverage_loop(self) -> None:
        while not self._stop.is_set():
            await self._sleep(60)
            try:
                self.engine.flush_counters()
                self.store.set_kv("last_heartbeat_ms", now_ms())
                log.info(
                    "живы: ассетов=%d подключено=%d событий=%d (теневых=%d) "
                    "сделок=%d сообщений=%d",
                    len(self.engine.assets), self.ws.connected_assets,
                    self.engine.n_events, self.engine.n_suppressed,
                    self.engine.n_trades_seen, self.ws.messages_seen,
                )
                if self.ws.unknown_types:
                    log.warning("неизвестные типы сообщений: %s", self.ws.unknown_types)
                if self.engine.dropped_no_asset or self.engine.dropped_unknown_asset:
                    log.warning(
                        "сообщений не разложено: без asset_id=%d, ассет не подписан=%d "
                        "(если первое растёт — формат канала изменился)",
                        self.engine.dropped_no_asset, self.engine.dropped_unknown_asset,
                    )
            except Exception:
                log.exception("цикл coverage упал")

    async def _periodic(self, coro_factory, period_s: int, name: str) -> None:
        while not self._stop.is_set():
            await self._sleep(period_s)
            try:
                await coro_factory()
            except Exception:
                log.exception("цикл %s упал", name)

    async def _sleep(self, seconds: float) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)

    # --------------------------------------------------------------- запуск

    async def run(self) -> None:
        await self.store.start()
        await self.http.start()
        self.engine.load_state()
        self.registry.load_from_db()
        self._note_restart_gap()

        tasks = [
            asyncio.create_task(self._discovery_loop(), name="discovery"),
            asyncio.create_task(self._tick_loop(), name="tick"),
            asyncio.create_task(self._coverage_loop(), name="coverage"),
            asyncio.create_task(
                self._periodic(self.resolution.run_once,
                               self.cfg.resolution.poll_s, "resolution"), name="resolution"),
            asyncio.create_task(
                self._periodic(self.reconciler.run_once,
                               self.cfg.reconcile.poll_s, "reconcile"), name="reconcile"),
            asyncio.create_task(
                self._periodic(self.target.run_once,
                               self.cfg.target_wallet.poll_s, "target"), name="target"),
            asyncio.create_task(
                self._periodic(self.integrity.run_once, 60, "integrity"), name="integrity"),
        ]
        log.info("коллектор запущен (только чтение, ключей нет)")
        await self._stop.wait()
        log.info("остановка...")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.ws.stop()
        self.engine.flush_counters()
        self.store.set_kv("last_heartbeat_ms", now_ms())
        await self.http.close()
        await self.store.close()
        log.info("остановлен чисто")

    def stop(self) -> None:
        self._stop.set()


async def main_async(args) -> None:
    cfg = cfgmod.load(args.config)
    logging_setup.setup(cfg.log_level)
    c = Collector(cfg)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, c.stop)
    await c.run()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
