"""HTTP с ретраями. Только GET, только чтение."""
from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

import httpx

log = logging.getLogger("http")


class Http:
    def __init__(self, timeout_s: float = 20.0, max_retries: int = 4) -> None:
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(
            timeout=self.timeout_s,
            headers={"User-Agent": "polymarket-paper-trade/0.1 (read-only)"},
            follow_redirects=True,
        )

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def get_json(self, url: str, params: dict | None = None) -> Any | None:
        assert self._client is not None, "Http.start() не вызван"
        delay = 1.0
        for attempt in range(self.max_retries + 1):
            try:
                r = await self._client.get(url, params=params)
                if r.status_code == 429 or 500 <= r.status_code < 600:
                    raise httpx.HTTPStatusError(f"HTTP {r.status_code}", request=r.request, response=r)
                if r.status_code >= 400:
                    log.warning("GET %s -> %s", url, r.status_code)
                    return None
                return r.json()
            except Exception as exc:  # noqa: BLE001
                if attempt >= self.max_retries:
                    log.warning("GET %s не удался после %d попыток: %s", url, attempt + 1, exc)
                    return None
                await asyncio.sleep(delay + random.uniform(0, 0.3))
                delay = min(delay * 2, 30.0)
        return None
