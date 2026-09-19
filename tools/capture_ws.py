#!/usr/bin/env python3
"""Запись сырых WS-фреймов в JSONL для фикстур и реплея.

    python tools/capture_ws.py --assets <id1> <id2> --seconds 300 --out captures/x.jsonl
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import websockets  # noqa: E402

from ptsim.util import now_ms  # noqa: E402

URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


async def capture(assets: list[str], seconds: int, out: Path) -> int:
    opener = gzip.open if out.suffix == ".gz" else open
    n = 0
    deadline = time.time() + seconds
    with opener(out, "wt") as fh:
        async with websockets.connect(URL, ping_interval=20, max_size=8 * 1024 * 1024) as c:
            await c.send(json.dumps({"assets_ids": assets, "type": "market"}))
            while time.time() < deadline:
                try:
                    raw = await asyncio.wait_for(c.recv(), timeout=max(1, deadline - time.time()))
                except asyncio.TimeoutError:
                    break
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", "replace")
                if raw.strip().upper() in ("PONG", "PING", ""):
                    continue
                fh.write(json.dumps({"recv_ms": now_ms(), "raw": raw}) + "\n")
                n += 1
    return n


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--assets", nargs="+", required=True)
    ap.add_argument("--seconds", type=int, default=300)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    a.out.parent.mkdir(parents=True, exist_ok=True)
    n = asyncio.run(capture(a.assets, a.seconds, a.out))
    print(f"записано фреймов: {n} -> {a.out}")


if __name__ == "__main__":
    main()
