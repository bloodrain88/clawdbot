from __future__ import annotations

import asyncio
import json
import random
import time
import urllib.parse
from collections.abc import Awaitable

import aiohttp


class HttpService:
    """Centralized HTTP layer with host pacing, retry/backoff and stale cache fallback."""

    def __init__(
        self,
        *,
        conn_limit: int,
        conn_per_host: int,
        dns_ttl_sec: int,
        keepalive_sec: float,
        min_gap_ms: float,
        retries_429: int,
        retries_5xx: int,
        default_cache_ttl: float,
        default_stale_ttl: float,
        should_log,
        error_tick,
        log_warn,
    ):
        self._conn_limit = max(1, int(conn_limit))
        self._conn_per_host = max(1, int(conn_per_host))
        self._dns_ttl_sec = max(0, int(dns_ttl_sec))
        self._keepalive_sec = max(5.0, float(keepalive_sec))
        self._min_gap_s = max(0.0, float(min_gap_ms) / 1000.0)
        self._retries_429 = max(0, int(retries_429))
        self._retries_5xx = max(0, int(retries_5xx))
        self._cache_ttl = max(0.0, float(default_cache_ttl))
        self._stale_ttl = max(1.0, float(default_stale_ttl))

        self._should_log = should_log
        self._error_tick = error_tick
        self._log_warn = log_warn

        self._session: aiohttp.ClientSession | None = None
        self._host_backoff: dict[str, float] = {}
        self._cache: dict[str, dict] = {}
        self._host_last_ts: dict[str, float] = {}
        self._host_locks: dict[str, asyncio.Lock] = {}

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()

    async def _ensure_session(self) -> None:
        if self._session is not None and not self._session.closed:
            return
        connector = aiohttp.TCPConnector(
            limit=self._conn_limit,
            limit_per_host=self._conn_per_host,
            ttl_dns_cache=self._dns_ttl_sec,
            enable_cleanup_closed=True,
            keepalive_timeout=self._keepalive_sec,
        )
        self._session = aiohttp.ClientSession(
            connector=connector,
            headers={"User-Agent": "clawdbot-live/1.0"},
        )

    async def get_json(
        self,
        url: str,
        *,
        params: dict | None = None,
        timeout: float = 8.0,
        cache_ttl: float | None = None,
        stale_ttl: float | None = None,
    ):
        cache_ttl = self._cache_ttl if cache_ttl is None else max(0.0, float(cache_ttl))
        stale_ttl = self._stale_ttl if stale_ttl is None else max(1.0, float(stale_ttl))

        now = time.time()
        host = urllib.parse.urlparse(url).netloc
        pk = json.dumps(params or {}, sort_keys=True, separators=(",", ":"))
        ck = f"{url}?{pk}"
        cached = self._cache.get(ck)
        if cached is not None and (now - float(cached.get("ts", 0.0) or 0.0)) <= cache_ttl:
            return cached.get("data")

        await self._ensure_session()
        assert self._session is not None

        lock = self._host_locks.get(host)
        if lock is None:
            lock = asyncio.Lock()
            self._host_locks[host] = lock

        async with lock:
            now = time.time()
            last_ts = float(self._host_last_ts.get(host, 0.0) or 0.0)
            if last_ts > 0 and (now - last_ts) < self._min_gap_s:
                await asyncio.sleep(self._min_gap_s - (now - last_ts))
            self._host_last_ts[host] = time.time()

            bt = float(self._host_backoff.get(host, 0.0) or 0.0)
            if bt > time.time():
                if cached is not None and (time.time() - float(cached.get("ts", 0.0) or 0.0)) <= stale_ttl:
                    return cached.get("data")
                raise RuntimeError(f"http 429 backoff active for {host} ({bt - time.time():.0f}s left)")

            last_err = None
            attempts = max(1, self._retries_429 + 1)
            for i in range(attempts):
                try:
                    async with self._session.get(
                        url,
                        params=params,
                        timeout=aiohttp.ClientTimeout(total=timeout),
                    ) as r:
                        if r.status == 429:
                            retry_after = float(r.headers.get("Retry-After", "2") or 2.0)
                            retry_after = max(1.0, retry_after)
                            backoff_s = min(90.0, retry_after + (0.35 * i) + random.uniform(0.05, 0.35))
                            self._host_backoff[host] = max(
                                float(self._host_backoff.get(host, 0.0) or 0.0),
                                time.time() + backoff_s,
                            )
                            if i < (attempts - 1):
                                await asyncio.sleep(backoff_s)
                                continue
                            if cached is not None and (time.time() - float(cached.get("ts", 0.0) or 0.0)) <= stale_ttl:
                                if self._should_log(f"429-stale:{host}", 45):
                                    self._log_warn(f"[HTTP] 429 {host} -> using stale cache")
                                return cached.get("data")
                            raise RuntimeError(f"http 429 {url}")

                        if r.status >= 500 and i < self._retries_5xx:
                            await asyncio.sleep(0.25 + (0.25 * i))
                            continue

                        if r.status >= 400:
                            if cached is not None and (time.time() - float(cached.get("ts", 0.0) or 0.0)) <= stale_ttl:
                                return cached.get("data")
                            raise RuntimeError(f"http {r.status} {url}")

                        payload = await r.json()
                        self._cache[ck] = {"ts": time.time(), "data": payload}
                        return payload
                except RuntimeError as e:
                    last_err = e
                except Exception as e:
                    last_err = e
                    if i < (attempts - 1):
                        await asyncio.sleep(0.20 + (0.15 * i))
                        continue

            if cached is not None and (time.time() - float(cached.get("ts", 0.0) or 0.0)) <= stale_ttl:
                return cached.get("data")

            self._error_tick("http_get_json", self._log_warn, err=last_err, every=20)
            raise RuntimeError(f"http get failed: {url} err={last_err}")

    async def gather_bounded(self, coros: list[Awaitable], limit: int):
        sem = asyncio.Semaphore(max(1, int(limit)))

        async def _run(coro):
            async with sem:
                return await coro

        return await asyncio.gather(*[_run(c) for c in coros], return_exceptions=True)
