"""
ClawdBot Paper Trading v4 — REAL Polymarket Markets
===================================================
- REAL markets: btc/eth/sol/xrp Up or Down 5-min & 15-min series
- Price: Polymarket RTDS (Binance feed, proxy for Chainlink resolution)
- Model: P(Up) = N( log(P_now/P_open) / (vol * sqrt(T)) ) — Black-Scholes
- Fees: ~1.56% at 50% → MIN_EDGE = 0.06 (6%) to clear fees
- $1000 bankroll, 1% per trade, hard stop -5%
"""

import asyncio
import aiohttp
import websockets
import json
import math
import csv
import os
from datetime import datetime, timezone
from scipy.stats import norm

# ── CONFIG ────────────────────────────────────────────────────────────────────
BANKROLL       = 1000.0
BET_SIZE_PCT   = 0.01     # 1% per trade = ~$10
MIN_EDGE       = 0.06     # 6% min edge (covers 1.56% fee + margin)
MAX_DAILY_LOSS = 0.05     # 5% = $50 hard stop
SCAN_INTERVAL  = 10       # seconds between market scans
PING_INTERVAL  = 5        # RTDS keep-alive
STATUS_INTERVAL= 10       # seconds between status prints
LOG_FILE       = os.path.expanduser("~/clawdbot_paper_trades.csv")
STATS_FILE     = os.path.expanduser("~/clawdbot_paper_stats.txt")

# ── Polymarket Series IDs (confirmed) ─────────────────────────────────────────
SERIES = {
    "btc-up-or-down-5m":  {"asset":"BTC", "duration":5,  "id":"10684"},
    "btc-up-or-down-15m": {"asset":"BTC", "duration":15, "id":"10192"},
    "eth-up-or-down-15m": {"asset":"ETH", "duration":15, "id":"10191"},
    "sol-up-or-down-15m": {"asset":"SOL", "duration":15, "id":"10423"},
    "xrp-up-or-down-15m": {"asset":"XRP", "duration":15, "id":"10422"},
}

GAMMA = "https://gamma-api.polymarket.com"
RTDS  = "wss://ws-live-data.polymarket.com"

G="\033[92m"; R="\033[91m"; Y="\033[93m"; B="\033[94m"; W="\033[97m"; RS="\033[0m"

# ─────────────────────────────────────────────────────────────────────────────

class PaperTrader:
    def __init__(self):
        self.prices     = {}            # asset → current price (RTDS)
        self.vols       = {"BTC":0.65,"ETH":0.80,"SOL":1.20,"XRP":0.90}
        self.open_prices= {}            # conditionId → price at market startTime
        self.active_mkts= {}            # conditionId → market dict
        self.pending    = {}            # conditionId → trade dict
        self.seen       = set()         # conditionIds already evaluated
        self.bankroll   = BANKROLL
        self.start_bank = BANKROLL
        self.daily_pnl  = 0.0
        self.total      = 0
        self.wins       = 0
        self.start_time = datetime.now(timezone.utc)
        self.rtds_ok    = False
        self._init_log()

    # ── LOG ───────────────────────────────────────────────────────────────────
    def _init_log(self):
        with open(LOG_FILE, "w", newline="") as f:
            csv.writer(f).writerow([
                "time","market","asset","duration","side",
                "bet","entry","open_price","current_price","true_prob",
                "mkt_price","edge","mins_left","result","pnl","bankroll"
            ])
        print(f"{B}[LOG] {LOG_FILE}{RS}")

    def _log(self, m, t, result="PENDING", pnl=0):
        with open(LOG_FILE, "a", newline="") as f:
            csv.writer(f).writerow([
                datetime.now(timezone.utc).strftime("%H:%M:%S"),
                m.get("question","")[:50], t.get("asset"),
                f"{t.get('duration')}min", t["side"],
                f"{t['size']:.2f}", f"{t['entry']:.3f}",
                f"{t.get('open_price',0):.2f}", f"{t.get('current_price',0):.2f}",
                f"{t.get('true_prob',0):.3f}", f"{t.get('mkt_price',0):.3f}",
                f"{t.get('edge',0):+.3f}", f"{t.get('mins_left',0):.1f}",
                result, f"{pnl:+.2f}", f"{self.bankroll:.2f}",
            ])

    # ── STATUS ────────────────────────────────────────────────────────────────
    def status(self):
        el   = datetime.now(timezone.utc) - self.start_time
        h, m = int(el.total_seconds()//3600), int(el.total_seconds()%3600//60)
        wr   = f"{self.wins/self.total*100:.1f}%" if self.total else "–"
        roi  = (self.bankroll - self.start_bank) / self.start_bank * 100
        pc   = G if self.daily_pnl >= 0 else R
        rs   = G if self.rtds_ok else R
        price_str = "  ".join(
            f"{B}{a}:{RS} ${p:,.2f}" for a,p in self.prices.items() if p>0
        )
        print(
            f"\n{W}{'─'*66}{RS}\n"
            f"  {B}Time:{RS} {h}h{m}m  {rs}RTDS{'✓' if self.rtds_ok else '✗'}{RS}  "
            f"{B}Trades:{RS} {self.total}  {B}Win:{RS} {wr}  "
            f"{B}ROI:{RS} {pc}{roi:+.1f}%{RS}\n"
            f"  {B}Bankroll:{RS} ${self.bankroll:.2f}  "
            f"{B}P&L:{RS} {pc}${self.daily_pnl:+.2f}{RS}  "
            f"{B}Active mkts:{RS} {len(self.active_mkts)}  "
            f"{B}Open bets:{RS} {len(self.pending)}\n"
            f"  {price_str}\n"
            f"{W}{'─'*66}{RS}"
        )

    # ── RTDS STREAM ───────────────────────────────────────────────────────────
    async def stream_rtds(self):
        while True:
            try:
                async with websockets.connect(
                    RTDS, additional_headers={"Origin":"https://polymarket.com"}
                ) as ws:
                    await ws.send(json.dumps({
                        "action":"subscribe",
                        "subscriptions":[{"topic":"crypto_prices","type":"update"}]
                    }))
                    self.rtds_ok = True
                    print(f"{G}[RTDS] Live — streaming BTC/ETH/SOL/XRP{RS}")

                    async def pinger():
                        while True:
                            await asyncio.sleep(PING_INTERVAL)
                            try: await ws.send(json.dumps({"action":"ping"}))
                            except: break

                    asyncio.create_task(pinger())

                    async for raw in ws:
                        if not raw: continue
                        try: msg = json.loads(raw)
                        except: continue
                        if msg.get("topic") != "crypto_prices": continue
                        p   = msg.get("payload",{})
                        sym = p.get("symbol","").lower()
                        val = float(p.get("value",0) or 0)
                        if val == 0: continue
                        MAP = {"btcusdt":"BTC","ethusdt":"ETH","solusdt":"SOL","xrpusdt":"XRP"}
                        asset = MAP.get(sym)
                        if asset:
                            self.prices[asset] = val
                            now = datetime.now(timezone.utc).timestamp()
                            # Capture open price for markets just starting
                            for cid, m in self.active_mkts.items():
                                if m.get("asset") == asset:
                                    st = m.get("start_ts", 0)
                                    if abs(now - st) < 30:
                                        self.open_prices[cid] = val
                            # Emit live unrealized P&L for open bets on this asset
                            self._print_live_pnl(asset, val)

            except Exception as e:
                self.rtds_ok = False
                print(f"{R}[RTDS] Reconnect: {e}{RS}")
                await asyncio.sleep(3)

    # ── LIVE UNREALIZED P&L (throttled: max 1 update/sec per asset) ──────────
    _last_live_print: dict = {}

    def _print_live_pnl(self, asset: str, price: float):
        import time
        now_ts = time.monotonic()
        # Throttle: max 1 print per asset per second
        if now_ts - self._last_live_print.get(asset, 0) < 1.0:
            return
        self._last_live_print[asset] = now_ts

        now = datetime.now(timezone.utc).timestamp()

        # Compute combined unrealized P&L across ALL open positions
        combined_unreal = 0.0
        lines = []
        for cid, (m, trade) in self.pending.items():
            open_p    = trade["open_price"]
            cur_price = self.prices.get(trade["asset"], open_p)
            mins_left = max((m["end_ts"] - now) / 60, 0)
            up_now    = cur_price >= open_p
            won_now   = (trade["side"]=="Up" and up_now) or (trade["side"]=="Down" and not up_now)
            unreal    = trade["size"] * (1/trade["entry"]-1) if won_now else -trade["size"]
            combined_unreal += unreal
            arrow = "↑" if cur_price >= open_p else "↓"
            c = G if unreal >= 0 else R
            lines.append(
                f"  {B}{trade['asset']}{RS} ${cur_price:,.2f}{arrow}${open_p:,.2f} "
                f"{trade['side']} {trade['duration']}m {c}${unreal:+.2f}{RS} "
                f"({mins_left:.1f}m left)"
            )

        if not lines:
            return

        total = self.daily_pnl + combined_unreal
        tc = G if total >= 0 else R
        uc = G if combined_unreal >= 0 else R
        print(
            f"\n{B}[LIVE]{RS} {datetime.now(timezone.utc).strftime('%H:%M:%S')} | "
            f"Unreal {uc}${combined_unreal:+.2f}{RS} | "
            f"Total P&L {tc}${total:+.2f}{RS} | "
            f"{len(self.pending)} positions\n" +
            "\n".join(lines),
            flush=True
        )

    # ── REALIZED VOL ──────────────────────────────────────────────────────────
    async def vol_loop(self):
        while True:
            for sym, asset in [("BTCUSDT","BTC"),("ETHUSDT","ETH"),("SOLUSDT","SOL")]:
                try:
                    async with aiohttp.ClientSession() as s:
                        async with s.get(
                            "https://api.binance.com/api/v3/klines",
                            params={"symbol":sym,"interval":"1m","limit":"30"}
                        ) as r:
                            candles = await r.json()
                    closes = [float(c[4]) for c in candles]
                    rets   = [math.log(closes[i]/closes[i-1]) for i in range(1,len(closes))]
                    std    = (sum(x**2 for x in rets)/len(rets))**0.5
                    self.vols[asset] = std * math.sqrt(252*24*60)
                except: pass
            await asyncio.sleep(600)

    # ── MARKET FETCHER ────────────────────────────────────────────────────────
    async def fetch_markets(self):
        """Fetch currently active 5-min and 15-min Up/Down markets from all series."""
        now = datetime.now(timezone.utc).timestamp()
        found = {}

        for slug, info in SERIES.items():
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(
                        f"{GAMMA}/events",
                        params={
                            "series_id":    info["id"],
                            "active":       "true",
                            "closed":       "false",
                            "order":        "startDate",
                            "ascending":    "true",
                            "limit":        "20",
                        },
                        timeout=aiohttp.ClientTimeout(total=8)
                    ) as r:
                        data = await r.json()

                events = data if isinstance(data, list) else data.get("data",[])
                for ev in events:
                    # Key timing fields from event
                    # startTime = when market interval BEGINS (reference price set here)
                    # endDate   = when market interval ENDS (resolution)
                    interval_start_str = ev.get("startTime","")
                    end_str            = ev.get("endDate","")
                    q                  = ev.get("title","") or ev.get("question","")

                    # Get market-level data (conditionId, prices)
                    mkts   = ev.get("markets", [])
                    m_data = mkts[0] if mkts else ev
                    cid    = m_data.get("conditionId","") or ev.get("conditionId","")
                    prices = m_data.get("outcomePrices") or ev.get("outcomePrices")

                    if not cid or not end_str or not interval_start_str:
                        continue
                    try:
                        end_ts   = datetime.fromisoformat(end_str.replace("Z","+00:00")).timestamp()
                        start_ts = datetime.fromisoformat(interval_start_str.replace("Z","+00:00")).timestamp()
                    except:
                        continue

                    # Only include markets whose interval is active or starting very soon
                    if end_ts <= now:
                        continue   # already over
                    if start_ts > now + 60:
                        continue   # more than 60s in the future — skip for now

                    mins_left = (end_ts - now) / 60

                    try:
                        if isinstance(prices, str): prices = json.loads(prices)
                        up_price = float(prices[0]) if prices else 0.5
                    except:
                        up_price = 0.5

                    found[cid] = {
                        "conditionId": cid,
                        "question":    q,
                        "asset":       info["asset"],
                        "duration":    info["duration"],
                        "end_ts":      end_ts,
                        "start_ts":    start_ts,
                        "up_price":    up_price,
                        "mins_left":   mins_left,
                    }
            except Exception as e:
                print(f"{R}[FETCH] {slug}: {e}{RS}")

        self.active_mkts = found
        return found

    # ── EVALUATE ──────────────────────────────────────────────────────────────
    async def evaluate(self, m: dict):
        cid       = m["conditionId"]
        asset     = m["asset"]
        duration  = m["duration"]
        mins_left = m["mins_left"]
        up_price  = m["up_price"]   # market's implied P(Up)
        now       = datetime.now(timezone.utc).timestamp()

        # Skip if hard stop hit
        if self.daily_pnl <= -(BANKROLL * MAX_DAILY_LOSS):
            return

        # Need open price — use RTDS price at start, or current if market just started
        current_price = self.prices.get(asset, 0)
        if current_price == 0:
            return

        # Estimate open price
        open_price = self.open_prices.get(cid)
        if open_price is None:
            # Market is pre-open (startTime in future) — record current as proxy
            # or if market is live and we missed the open, use current ± drift
            open_price = current_price
            self.open_prices[cid] = open_price

        vol      = self.vols.get(asset, 0.70)
        true_prob = self._prob_up(current_price, open_price, mins_left, vol)

        # Edge vs market price (Up side)
        edge_up   = true_prob - up_price
        edge_down = (1 - true_prob) - (1 - up_price)  # = -edge_up

        if abs(edge_up) < MIN_EDGE:
            return

        side  = "Up" if edge_up > 0 else "Down"
        edge  = edge_up if side=="Up" else -edge_up
        entry = up_price if side=="Up" else (1 - up_price)
        size  = min(self.bankroll * BET_SIZE_PCT, self.bankroll * 0.03)

        trade = {
            "side":          side,
            "size":          size,
            "entry":         entry,
            "open_price":    open_price,
            "current_price": current_price,
            "true_prob":     true_prob,
            "mkt_price":     up_price,
            "edge":          round(edge, 4),
            "mins_left":     mins_left,
            "end_ts":        m["end_ts"],
            "asset":         asset,
            "duration":      duration,
        }

        self.seen.add(cid)
        self.pending[cid] = (m, trade)
        self._log(m, trade)

        ec = G if edge > 0 else R
        print(
            f"{Y}[BET]{RS} {side} {ec}{edge:+.1%}{RS} | "
            f"{asset} open=${open_price:,.1f} now=${current_price:,.1f} | "
            f"{duration}min ({mins_left:.1f}min left) | "
            f"mkt={up_price:.2f} true={true_prob:.2f} | ${size:.1f}"
        )

    # ── MAIN LOOP ─────────────────────────────────────────────────────────────
    async def scan_loop(self):
        await asyncio.sleep(6)
        while True:
            markets = await self.fetch_markets()
            now     = datetime.now(timezone.utc).timestamp()

            if not markets:
                # No live markets — find next open time
                async with aiohttp.ClientSession() as s:
                    async with s.get(
                        f"{GAMMA}/events",
                        params={"series_id":"10192","active":"true","closed":"false",
                                "order":"startDate","ascending":"true","limit":"5"},
                        timeout=aiohttp.ClientTimeout(total=8)
                    ) as r:
                        nxt = await r.json()
                nxt_evs = nxt if isinstance(nxt,list) else []
                for ev in nxt_evs:
                    st = ev.get("startTime","")
                    if st:
                        try:
                            st_ts = datetime.fromisoformat(st.replace("Z","+00:00")).timestamp()
                            wait_min = (st_ts - now) / 60
                            wait_h   = int(wait_min // 60)
                            wait_m   = int(wait_min % 60)
                            print(f"{Y}[WAIT] Markets closed. Next opens in {wait_h}h{wait_m}m "
                                  f"({ev.get('title','')[:40]}){RS}")
                            break
                        except: pass
            else:
                print(f"{B}[SCAN] Live markets: {len(markets)} | "
                      f"Pending bets: {len(self.pending)}{RS}")

            for cid, m in markets.items():
                # Only trade markets that have started and have >1 min left
                if m["start_ts"] > now:
                    continue
                mins_left = (m["end_ts"] - now) / 60
                if mins_left < 1:
                    continue
                m["mins_left"] = mins_left
                if cid not in self.seen:
                    await self.evaluate(m)

            await self._resolve()
            await asyncio.sleep(SCAN_INTERVAL)

    # ── RESOLVE ───────────────────────────────────────────────────────────────
    async def _resolve_loop(self):
        while True:
            await self._resolve()
            await asyncio.sleep(2)

    async def _resolve(self):
        now     = datetime.now(timezone.utc).timestamp()
        expired = [k for k,(m,t) in self.pending.items() if m["end_ts"] <= now]

        for k in expired:
            m, trade = self.pending.pop(k)
            asset  = trade["asset"]
            price  = self.prices.get(asset, trade["open_price"])
            up_won = price >= trade["open_price"]
            won    = (trade["side"]=="Up" and up_won) or (trade["side"]=="Down" and not up_won)
            pnl    = trade["size"] * (1/trade["entry"] - 1) if won else -trade["size"]
            # Apply fee (1.56% max at 50%)
            fee = trade["size"] * 0.0156 * (1 - abs(trade["mkt_price"] - 0.5) * 2)
            pnl = pnl - fee if won else pnl

            self.daily_pnl    += pnl
            self.bankroll     += pnl
            self.total        += 1
            if won: self.wins += 1
            self._log(m, trade, "WIN" if won else "LOSS", pnl)

            c  = G if won else R
            wr = f"{self.wins/self.total*100:.0f}%" if self.total else "–"
            print(
                f"{c}[{'WIN' if won else 'LOSS'}]{RS} "
                f"{asset} open=${trade['open_price']:,.1f} final=${price:,.1f} | "
                f"{trade['duration']}min | {c}${pnl:+.2f}{RS} | "
                f"Bank ${self.bankroll:.2f} | WR {wr}"
            )

    # ── MATH ──────────────────────────────────────────────────────────────────
    def _prob_up(self, current, open_price, mins_left, vol):
        """P(price at end ≥ open_price) given current price and time left"""
        if open_price <= 0 or vol <= 0 or mins_left <= 0:
            return 0.5
        T = max(mins_left, 0.1) / (252 * 24 * 60)
        # If current > open → price already up, more likely to stay/end up
        # d = log(current/open) / (vol * sqrt(T))
        # But this ignores remaining time randomness correctly:
        # P(end ≥ open | current, T_left) = N(log(current/open) / (vol*sqrt(T_left)))
        d = math.log(current / open_price) / (vol * math.sqrt(T))
        return float(norm.cdf(d))

    # ── STATS ─────────────────────────────────────────────────────────────────
    async def stats_loop(self):
        while True:
            await asyncio.sleep(1800)  # every 30 min
            wr  = f"{self.wins/self.total*100:.1f}%" if self.total else "0%"
            roi = (self.bankroll - self.start_bank) / self.start_bank * 100
            ev  = self.daily_pnl / self.total if self.total else 0
            rep = (
                f"\n=== {datetime.now().strftime('%H:%M')} Report ===\n"
                f"Bankroll:  ${self.bankroll:.2f}\n"
                f"P&L:       ${self.daily_pnl:+.2f} ({roi:+.2f}% ROI)\n"
                f"Trades:    {self.total}  Wins: {self.wins}  WR: {wr}\n"
                f"Avg EV:    ${ev:+.3f}/trade\n"
                f"Prices:    " + " ".join(f"{a}=${p:,.2f}" for a,p in self.prices.items() if p) + "\n"
            )
            print(f"{B}{rep}{RS}")
            with open(STATS_FILE,"a") as f: f.write(rep+"\n")

    async def _status_loop(self):
        while True:
            await asyncio.sleep(STATUS_INTERVAL)
            print()  # newline after \r live line
            self.status()

    # ── MAIN ──────────────────────────────────────────────────────────────────
    async def run(self):
        print(f"""
{B}╔═══════════════════════════════════════════════════════════╗
║       ClawdBot Paper Trading v4 — REAL Polymarket        ║
║  $1,000 bankroll | 5-min & 15-min Up/Down only          ║
║  Series: BTC/ETH/SOL 5m & 15m | Edge ≥ 6% | Stop -5%  ║
║  Price: RTDS (Binance) | Resolution: Chainlink BTC/USD  ║
║  No LLM. Black-Scholes P(Up). Real market prices.       ║
╚═══════════════════════════════════════════════════════════╝{RS}
""")
        await asyncio.gather(
            self.stream_rtds(),
            self.vol_loop(),
            self.scan_loop(),
            self._resolve_loop(),   # fast resolution check every 2s
            self._status_loop(),
            self.stats_loop(),
        )


if __name__ == "__main__":
    try:
        asyncio.run(PaperTrader().run())
    except KeyboardInterrupt:
        print(f"\n{Y}[STOP] Results: {LOG_FILE}{RS}")
