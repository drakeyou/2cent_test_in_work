"""Назначение ассетов по WS-шардам.

Наивное «отсортировать и нарезать» при добавлении одного ассета сдвигает
состав всех соединений: каждое появление нового рынка рвало бы наблюдение по
всему списку сразу, и знаменатель частоты уезжал бы вниз на ровном месте.
Назначение должно быть ЛИПКИМ.
"""
from __future__ import annotations

from ptsim.config import load
from ptsim.ws_client import WsManager


def mgr(per_conn: int = 3):
    cfg = load("config.yaml")
    cfg.ws.max_assets_per_conn = per_conn
    return WsManager(cfg, lambda m: None, lambda g: None)


def test_assets_fill_shards_to_capacity():
    m = mgr(3)
    m._assign({f"a{i}" for i in range(7)})
    sizes = sorted(len(s.assets) for s in m.shards.values())
    assert sizes == [1, 3, 3]
    assert sum(sizes) == 7
    assert len(m.assignment) == 7


def test_adding_an_asset_does_not_move_existing_ones():
    m = mgr(3)
    m._assign({f"a{i}" for i in range(6)})
    before = dict(m.assignment)
    m._assign({f"a{i}" for i in range(7)})
    for asset, shard in before.items():
        assert m.assignment[asset] == shard, f"{asset} переехал, это лишний разрыв"
    assert m.assignment["a6"] is not None


def test_removing_assets_frees_capacity_for_new_ones():
    m = mgr(3)
    m._assign({f"a{i}" for i in range(6)})
    freed_shard = m.assignment["a0"]
    m._assign({f"a{i}" for i in range(1, 6)})
    assert "a0" not in m.assignment
    m._assign({f"a{i}" for i in range(1, 6)} | {"newbie"})
    assert m.assignment["newbie"] == freed_shard, "освободившийся слот должен переиспользоваться"


def test_no_new_shard_while_an_existing_one_has_room():
    m = mgr(5)
    m._assign({"a", "b"})
    assert len(m.shards) == 1
    m._assign({"a", "b", "c", "d", "e"})
    assert len(m.shards) == 1, "пять ассетов при ёмкости пять — одно соединение"
    m._assign({"a", "b", "c", "d", "e", "f"})
    assert len(m.shards) == 2


def test_full_churn_keeps_assignment_consistent():
    m = mgr(4)
    m._assign({f"a{i}" for i in range(10)})
    m._assign({f"b{i}" for i in range(10)})
    assert not any(k.startswith("a") for k in m.assignment)
    assert len(m.assignment) == 10
    total = sum(len(s.assets) for s in m.shards.values())
    assert total == 10, "в шардах не должно остаться следов старых ассетов"
    for asset, idx in m.assignment.items():
        assert asset in m.shards[idx].assets, "assignment и состав шарда разошлись"
