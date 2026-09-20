"""Переподключение WS и учёт разрывов.

Процесс должен пережить многодневный прогон. Обрыв сокета — не исключение, а
норма, и каждый обрыв ОБЯЗАН попасть в журнал с длительностью: незалогированный
разрыв завышает частоту события напрямую, потому что знаменатель (наблюдённое
время) сжимается, а числитель нет.
"""
from __future__ import annotations

import asyncio
import json

import pytest

from ptsim.config import load
from ptsim import ws_client


class FakeConn:
    """Сокет, который отдаёт заданные кадры и затем падает или закрывается."""

    def __init__(self, frames, fail_after=None):
        self.frames = list(frames)
        self.fail_after = fail_after
        self.sent: list[str] = []

    async def send(self, payload):
        self.sent.append(payload)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.frames:
            return self.frames.pop(0)
        if self.fail_after:
            raise ConnectionError(self.fail_after)
        raise StopAsyncIteration


def make(monkeypatch, conns):
    """Подменяет websockets.connect последовательностью соединений."""
    seq = list(conns)
    opened: list[FakeConn] = []

    def fake_connect(*a, **kw):
        c = seq.pop(0) if seq else FakeConn([], fail_after="нет соединений")
        opened.append(c)
        return c

    monkeypatch.setattr(ws_client.websockets, "connect", fake_connect)
    return opened


@pytest.mark.asyncio
async def test_reconnects_after_a_drop_and_logs_both_gaps(monkeypatch):
    cfg = load("config.yaml")
    cfg.ws.backoff_initial_s = 0.01
    cfg.ws.backoff_max_s = 0.02
    msgs, gaps = [], []
    opened = make(monkeypatch, [
        FakeConn([json.dumps({"event_type": "book", "asset_id": "a"})], fail_after="обрыв"),
        FakeConn([json.dumps({"event_type": "book", "asset_id": "a"})], fail_after="обрыв"),
    ])
    m = ws_client.WsManager(cfg, msgs.append, gaps.append)
    await m.set_assets({"a", "b"})
    await asyncio.sleep(0.25)
    await m.stop()

    assert len(opened) >= 2, "после обрыва должно быть переподключение"
    assert len(msgs) >= 2, "кадры должны доходить после переподключения"
    assert len(gaps) >= 2, "каждый разрыв — строка в журнале"
    for g in gaps:
        assert g["duration_ms"] >= 0 and g["assets_resubscribed"] == 2
        assert g["reason"] and g["started_at"] and g["ended_at"]
    assert gaps[0]["reason"] == "startup"
    assert gaps[1]["reason"] == "ConnectionError", "причина обрыва должна сохраняться"


@pytest.mark.asyncio
async def test_subscribe_message_lists_every_asset_of_the_shard(monkeypatch):
    cfg = load("config.yaml")
    cfg.ws.backoff_initial_s = 0.01
    opened = make(monkeypatch, [FakeConn([], fail_after="конец")])
    m = ws_client.WsManager(cfg, lambda x: None, lambda g: None)
    await m.set_assets({"a1", "a2", "a3"})
    await asyncio.sleep(0.1)
    await m.stop()
    payload = json.loads(opened[0].sent[0])
    assert payload["type"] == "market"
    assert sorted(payload["assets_ids"]) == ["a1", "a2", "a3"]


@pytest.mark.asyncio
async def test_unknown_message_types_are_counted_not_swallowed(monkeypatch):
    """Изменение протокола должно быть видно в логе, а не по пустым данным."""
    cfg = load("config.yaml")
    cfg.ws.backoff_initial_s = 0.01
    make(monkeypatch, [FakeConn([
        json.dumps({"event_type": "nonsense_v2", "asset_id": "a"}),
        json.dumps({"event_type": "book", "asset_id": "a"}),
    ], fail_after="конец")])
    got = []
    m = ws_client.WsManager(cfg, got.append, lambda g: None)
    await m.set_assets({"a"})
    await asyncio.sleep(0.1)
    await m.stop()
    assert len(got) == 1, "неизвестный тип не должен доходить до движка"
    assert m.unknown_types.get("nonsense_v2") == 1, "но обязан попасть в счётчик"
