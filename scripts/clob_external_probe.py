#!/usr/bin/env python3
import argparse
import asyncio
import json
import statistics
import sys
import time
import urllib.parse
import urllib.request

import websockets


GAMMA = "https://gamma-api.polymarket.com/markets"
CLOB_BOOK = "https://clob.polymarket.com/book"
CLOB_MID = "https://clob.polymarket.com/midpoint"
CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def _http_json(url: str, timeout: float = 6.0):
    req = urllib.request.Request(url, headers={"User-Agent": "clob-external-probe/1.0"})
    t0 = time.perf_counter()
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
        code = int(resp.getcode() or 0)
    dt = (time.perf_counter() - t0) * 1000.0
    return code, dt, json.loads(body.decode("utf-8"))


def _latest_tokens(limit: int = 20):
    qs = urllib.parse.urlencode(
        {
            "active": "true",
            "closed": "false",
            "archived": "false",
            "limit": str(limit),
            "order": "id",
            "ascending": "false",
        }
    )
    url = f"{GAMMA}?{qs}"
    code, dt, rows = _http_json(url, timeout=8.0)
    if code != 200 or not isinstance(rows, list) or not rows:
        raise RuntimeError(f"gamma fetch failed code={code}")
    for m in rows:
        raw = m.get("clobTokenIds")
        if not raw:
            continue
        try:
            toks = json.loads(raw) if isinstance(raw, str) else list(raw)
        except Exception:
            continue
        toks = [str(x) for x in toks if str(x)]
        if len(toks) >= 2:
            return toks[:2], dt
    raise RuntimeError("no live token ids found")


def _probe_rest(token_id: str, tries: int):
    book_lat = []
    mid_lat = []
    for _ in range(tries):
        c1, t1, j1 = _http_json(f"{CLOB_BOOK}?token_id={token_id}", timeout=6.0)
        c2, t2, j2 = _http_json(f"{CLOB_MID}?token_id={token_id}", timeout=6.0)
        if c1 != 200:
            raise RuntimeError(f"book http={c1}")
        if c2 != 200:
            raise RuntimeError(f"midpoint http={c2}")
        if not isinstance(j1, dict) or not j1.get("asks"):
            raise RuntimeError("book payload invalid/empty asks")
        if isinstance(j2, dict):
            mid = float(j2.get("mid", 0.0) or 0.0)
        else:
            mid = float(j2 or 0.0)
        if mid <= 0:
            raise RuntimeError("midpoint payload invalid")
        book_lat.append(t1)
        mid_lat.append(t2)
    return book_lat, mid_lat


async def _probe_ws(tokens: list[str], timeout_sec: float):
    seen = {t: 0 for t in tokens}
    t0 = time.perf_counter()
    async with websockets.connect(
        CLOB_WS,
        ping_interval=20,
        ping_timeout=20,
        max_size=2**22,
        compression=None,
    ) as ws:
        await ws.send(
            json.dumps(
                {"assets_ids": tokens, "type": "market", "custom_feature_enabled": True}
            )
        )
        await ws.send("PING")
        while (time.perf_counter() - t0) < timeout_sec:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
            except asyncio.TimeoutError:
                continue
            if str(raw).strip().upper() == "PONG":
                continue
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            rows = msg if isinstance(msg, list) else [msg]
            for row in rows:
                if not isinstance(row, dict):
                    continue
                # Native market WS payload is already an event row:
                # {"asset_id": "...", "event_type":"book", ...}
                aid = str(row.get("asset_id") or row.get("assetId") or "")
                if aid in seen:
                    seen[aid] += 1
                    continue
                # Backward-compatible wrapper handling.
                data = row.get("data")
                if isinstance(data, dict):
                    data = [data]
                if not isinstance(data, list):
                    continue
                for ev in data:
                    if not isinstance(ev, dict):
                        continue
                    aid2 = str(ev.get("asset_id") or ev.get("assetId") or "")
                    if aid2 in seen:
                        seen[aid2] += 1
            if all(v > 0 for v in seen.values()):
                break
    return seen


def _p95(xs):
    if not xs:
        return 0.0
    if len(xs) == 1:
        return xs[0]
    return statistics.quantiles(xs, n=20)[18]


def main():
    ap = argparse.ArgumentParser(description="External Polymarket CLOB health probe")
    ap.add_argument("--tries", type=int, default=6)
    ap.add_argument("--ws-timeout", type=float, default=15.0)
    args = ap.parse_args()

    try:
        tokens, gamma_ms = _latest_tokens(limit=30)
        t0 = tokens[0]
        b_lat, m_lat = _probe_rest(t0, tries=max(2, int(args.tries)))
        seen = asyncio.run(_probe_ws(tokens, timeout_sec=max(6.0, float(args.ws_timeout))))
        ok_ws = all(v > 0 for v in seen.values())

        print(f"gamma_ms={gamma_ms:.1f} tokens={tokens}")
        print(
            "rest_book_ms avg={:.1f} p95={:.1f} | rest_mid_ms avg={:.1f} p95={:.1f}".format(
                statistics.mean(b_lat), _p95(b_lat), statistics.mean(m_lat), _p95(m_lat)
            )
        )
        print(f"ws_seen={seen}")

        if not ok_ws:
            print("FAIL: ws stream missing updates for one or more tokens")
            sys.exit(2)
        if _p95(b_lat) > 800 or _p95(m_lat) > 800:
            print("FAIL: rest latency p95 too high")
            sys.exit(3)
        print("OK: external CLOB REST/WS probe passed")
    except Exception as e:
        print(f"FAIL: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
