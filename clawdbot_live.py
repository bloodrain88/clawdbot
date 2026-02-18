"""
ClawdBot Live Trading v1 — Polymarket CLOB
==========================================
- Real orders via py-clob-client on Polygon mainnet
- Same Black-Scholes edge logic as v4 paper bot
- Loads wallet from ~/.clawdbot.env
- Set POLY_NETWORK=polygon for mainnet, amoy for testnet (no Polymarket on testnet)

SETUP:
  1. Fund wallet with USDC on Polygon (bridge from ETH or buy on exchange)
  2. Fund wallet with ~$1 MATIC for gas
  3. First run will approve Polymarket CTF contracts (one-time tx)
  4. Set BANKROLL= actual USDC balance in .env
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
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/.clawdbot.env"))

from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON, AMOY
from py_clob_client.clob_types import OrderArgs, OrderType, MarketOrderArgs, AssetType, BalanceAllowanceParams

# ── CONFIG ────────────────────────────────────────────────────────────────────
PRIVATE_KEY    = os.environ["POLY_PRIVATE_KEY"]
ADDRESS        = os.environ["POLY_ADDRESS"]
NETWORK        = os.environ.get("POLY_NETWORK", "polygon")  # polygon | amoy
BANKROLL       = float(os.environ.get("BANKROLL", "100.0"))
MIN_EDGE       = 0.06     # 6% min edge
MIN_BET        = 5.0      # $5 min per trade
MAX_BET        = 10.0     # $10 max per trade
MAX_DAILY_LOSS = 0.05     # 5% hard stop
SCAN_INTERVAL  = 10
PING_INTERVAL  = 5
STATUS_INTERVAL= 30
LOG_FILE       = os.path.expanduser("~/clawdbot_live_trades.csv")

DRY_RUN   = os.environ.get("DRY_RUN", "true").lower() == "true"
CHAIN_ID  = POLYGON  # CLOB API esiste solo su mainnet
CLOB_HOST = "https://clob.polymarket.com"

SERIES = {
    "btc-up-or-down-5m":  {"asset": "BTC", "duration": 5,  "id": "10684"},
    "btc-up-or-down-15m": {"asset": "BTC", "duration": 15, "id": "10192"},
    "eth-up-or-down-15m": {"asset": "ETH", "duration": 15, "id": "10191"},
    "sol-up-or-down-15m": {"asset": "SOL", "duration": 15, "id": "10423"},
    "xrp-up-or-down-15m": {"asset": "XRP", "duration": 15, "id": "10422"},
}

GAMMA = "https://gamma-api.polymarket.com"
RTDS  = "wss://ws-live-data.polymarket.com"

G="\033[92m"; R="\033[91m"; Y="\033[93m"; B="\033[94m"; W="\033[97m"; RS="\033[0m"

# ─────────────────────────────────────────────────────────────────────────────

class LiveTrader:
    def __init__(self):
        self.prices      = {}
        self.vols        = {"BTC": 0.65, "ETH": 0.80, "SOL": 1.20, "XRP": 0.90}
        self.open_prices = {}
        self.active_mkts = {}
        self.pending     = {}   # cid → {order_id, token_id, side, size, entry, open_price, ...}
        self.seen        = set()
        self.bankroll    = BANKROLL
        self.start_bank  = BANKROLL
        self.daily_pnl   = 0.0
        self.total       = 0
        self.wins        = 0
        self.start_time  = datetime.now(timezone.utc)
        self.rtds_ok     = False
        self.clob        = None
        self._init_log()

    # ── CLOB INIT ─────────────────────────────────────────────────────────────
    def init_clob(self):
        print(f"{B}[CLOB] Connecting to Polymarket CLOB ({NETWORK})...{RS}")
        self.clob = ClobClient(
            host=CLOB_HOST,
            key=PRIVATE_KEY,
            chain_id=CHAIN_ID,
        )
        # Derive/create API credentials (signed from private key — no external account needed)
        try:
            creds = self.clob.create_or_derive_api_creds()
            self.clob.set_api_creds(creds)
            print(f"{G}[CLOB] API creds OK: {creds.api_key[:8]}...{RS}")
        except Exception as e:
            print(f"{R}[CLOB] Creds error: {e}{RS}")
            raise

        # Check USDC balance
        try:
            bal = self.clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
            usdc = float(bal.get("balance", 0))
            print(f"{G}[CLOB] USDC balance: ${usdc:.2f}{RS}")
            if usdc < 10 and not DRY_RUN:
                print(f"{R}[WARN] Saldo basso! Fondi il wallet prima di fare trading live.{RS}")
        except Exception as e:
            print(f"{Y}[CLOB] Balance: {e}{RS}")

    # ── LOG ───────────────────────────────────────────────────────────────────
    def _init_log(self):
        if not os.path.exists(LOG_FILE):
            with open(LOG_FILE, "w", newline="") as f:
                csv.writer(f).writerow([
                    "time", "market", "asset", "duration", "side",
                    "bet_usdc", "entry", "open_price", "current_price",
                    "true_prob", "mkt_price", "edge", "mins_left",
                    "order_id", "token_id", "result", "pnl", "bankroll"
                ])
        print(f"{B}[LOG] {LOG_FILE}{RS}")

    def _log(self, m, t, result="PENDING", pnl=0):
        with open(LOG_FILE, "a", newline="") as f:
            csv.writer(f).writerow([
                datetime.now(timezone.utc).strftime("%H:%M:%S"),
                m.get("question", "")[:50], t.get("asset"),
                f"{t.get('duration')}min", t["side"],
                f"{t['size']:.2f}", f"{t['entry']:.3f}",
                f"{t.get('open_price', 0):.2f}", f"{t.get('current_price', 0):.2f}",
                f"{t.get('true_prob', 0):.3f}", f"{t.get('mkt_price', 0):.3f}",
                f"{t.get('edge', 0):+.3f}", f"{t.get('mins_left', 0):.1f}",
                t.get("order_id", ""), t.get("token_id", ""),
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
            f"{B}{a}:{RS} ${p:,.2f}" for a, p in self.prices.items() if p > 0
        )
        print(
            f"\n{W}{'─'*66}{RS}\n"
            f"  {B}Time:{RS} {h}h{m}m  {rs}RTDS{'✓' if self.rtds_ok else '✗'}{RS}  "
            f"{B}Trades:{RS} {self.total}  {B}Win:{RS} {wr}  "
            f"{B}ROI:{RS} {pc}{roi:+.1f}%{RS}\n"
            f"  {B}Bankroll:{RS} ${self.bankroll:.2f}  "
            f"{B}P&L:{RS} {pc}${self.daily_pnl:+.2f}{RS}  "
            f"{B}Network:{RS} {NETWORK}  "
            f"{B}Open:{RS} {len(self.pending)}\n"
            f"  {price_str}\n"
            f"{W}{'─'*66}{RS}"
        )

    # ── RTDS ──────────────────────────────────────────────────────────────────
    async def stream_rtds(self):
        while True:
            try:
                async with websockets.connect(
                    RTDS, additional_headers={"Origin": "https://polymarket.com"}
                ) as ws:
                    await ws.send(json.dumps({
                        "action": "subscribe",
                        "subscriptions": [{"topic": "crypto_prices", "type": "update"}]
                    }))
                    self.rtds_ok = True
                    print(f"{G}[RTDS] Live — streaming BTC/ETH/SOL/XRP{RS}")

                    async def pinger():
                        while True:
                            await asyncio.sleep(PING_INTERVAL)
                            try: await ws.send(json.dumps({"action": "ping"}))
                            except: break
                    asyncio.create_task(pinger())

                    async for raw in ws:
                        if not raw: continue
                        try: msg = json.loads(raw)
                        except: continue
                        if msg.get("topic") != "crypto_prices": continue
                        p   = msg.get("payload", {})
                        sym = p.get("symbol", "").lower()
                        val = float(p.get("value", 0) or 0)
                        if val == 0: continue
                        MAP = {"btcusdt":"BTC","ethusdt":"ETH","solusdt":"SOL","xrpusdt":"XRP"}
                        asset = MAP.get(sym)
                        if asset:
                            self.prices[asset] = val
                            now = datetime.now(timezone.utc).timestamp()
                            for cid, m in self.active_mkts.items():
                                if m.get("asset") == asset:
                                    if abs(now - m.get("start_ts", 0)) < 30:
                                        self.open_prices[cid] = val
            except Exception as e:
                self.rtds_ok = False
                print(f"{R}[RTDS] Reconnect: {e}{RS}")
                await asyncio.sleep(3)

    # ── VOL LOOP ──────────────────────────────────────────────────────────────
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
        now   = datetime.now(timezone.utc).timestamp()
        found = {}
        for slug, info in SERIES.items():
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(
                        f"{GAMMA}/events",
                        params={
                            "series_id":  info["id"],
                            "active":     "true",
                            "closed":     "false",
                            "order":      "startDate",
                            "ascending":  "true",
                            "limit":      "20",
                        },
                        timeout=aiohttp.ClientTimeout(total=8)
                    ) as r:
                        data = await r.json()
                events = data if isinstance(data, list) else data.get("data", [])
                for ev in events:
                    start_str = ev.get("startTime", "")
                    end_str   = ev.get("endDate", "")
                    q         = ev.get("title", "") or ev.get("question", "")
                    mkts      = ev.get("markets", [])
                    m_data    = mkts[0] if mkts else ev
                    cid       = m_data.get("conditionId", "") or ev.get("conditionId", "")
                    prices    = m_data.get("outcomePrices") or ev.get("outcomePrices")
                    # token IDs for Up (index 0) and Down (index 1) outcomes
                    tokens    = m_data.get("clobTokenIds") or ev.get("clobTokenIds") or []
                    if isinstance(tokens, str):
                        try: tokens = json.loads(tokens)
                        except: tokens = []

                    if not cid or not end_str or not start_str:
                        continue
                    try:
                        end_ts   = datetime.fromisoformat(end_str.replace("Z","+00:00")).timestamp()
                        start_ts = datetime.fromisoformat(start_str.replace("Z","+00:00")).timestamp()
                    except:
                        continue
                    if end_ts <= now or start_ts > now + 60:
                        continue
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
                        "mins_left":   (end_ts - now) / 60,
                        "token_up":    tokens[0] if len(tokens) > 0 else "",
                        "token_down":  tokens[1] if len(tokens) > 1 else "",
                    }
            except Exception as e:
                print(f"{R}[FETCH] {slug}: {e}{RS}")
        self.active_mkts = found
        return found

    # ── EVALUATE + PLACE ORDER ────────────────────────────────────────────────
    async def evaluate(self, m: dict):
        cid       = m["conditionId"]
        asset     = m["asset"]
        duration  = m["duration"]
        mins_left = m["mins_left"]
        up_price  = m["up_price"]

        if self.daily_pnl <= -(self.bankroll * MAX_DAILY_LOSS):
            return

        current = self.prices.get(asset, 0)
        if current == 0:
            return

        open_price = self.open_prices.get(cid)
        if open_price is None:
            open_price = current
            self.open_prices[cid] = open_price

        vol       = self.vols.get(asset, 0.70)
        true_prob = self._prob_up(current, open_price, mins_left, vol)
        edge_up   = true_prob - up_price

        if abs(edge_up) < MIN_EDGE:
            return

        side       = "Up" if edge_up > 0 else "Down"
        edge       = abs(edge_up)
        entry      = up_price if side == "Up" else (1 - up_price)
        size       = round(max(MIN_BET, min(MAX_BET, self.bankroll * 0.10)), 2)
        token_id   = m["token_up"] if side == "Up" else m["token_down"]

        if not token_id:
            print(f"{Y}[SKIP] No token_id for {asset} {side} — market data incomplete{RS}")
            self.seen.add(cid)
            return

        # ── Place real order ──────────────────────────────────────────────────
        order_id = await self._place_order(token_id, side, entry, size, asset, duration, mins_left)

        trade = {
            "side":          side,
            "size":          size,
            "entry":         entry,
            "open_price":    open_price,
            "current_price": current,
            "true_prob":     true_prob,
            "mkt_price":     up_price,
            "edge":          round(edge, 4),
            "mins_left":     mins_left,
            "end_ts":        m["end_ts"],
            "asset":         asset,
            "duration":      duration,
            "token_id":      token_id,
            "order_id":      order_id or "",
        }

        self.seen.add(cid)
        if order_id:
            self.pending[cid] = (m, trade)
            self._log(m, trade)

    async def _place_order(self, token_id, side, price, size_usdc, asset, duration, mins_left):
        """Place a market buy order on Polymarket CLOB. Returns order_id or None."""
        if DRY_RUN:
            fake_id = f"DRY-{asset[:3]}-{int(datetime.now(timezone.utc).timestamp())}"
            print(
                f"{Y}[DRY-RUN]{RS} {side} {asset} {duration}m | "
                f"${size_usdc:.2f} USDC @ {price:.3f} | id={fake_id}"
            )
            return fake_id

        try:
            loop = asyncio.get_event_loop()
            order_args = MarketOrderArgs(
                token_id=token_id,
                amount=size_usdc,
            )
            signed = await loop.run_in_executor(
                None, lambda: self.clob.create_market_order(order_args)
            )
            resp = await loop.run_in_executor(
                None, lambda: self.clob.post_order(signed, OrderType.FOK)
            )
            order_id = resp.get("orderID") or resp.get("id") or str(resp)
            print(
                f"{Y}[ORDER]{RS} {side} {asset} {duration}m | "
                f"${size_usdc:.2f} USDC @ {price:.3f} | order={order_id[:16]}..."
            )
            return order_id
        except Exception as e:
            print(f"{R}[ORDER FAILED] {asset} {side}: {e}{RS}")
            return None

    # ── RESOLVE ───────────────────────────────────────────────────────────────
    async def _resolve(self):
        now     = datetime.now(timezone.utc).timestamp()
        expired = [k for k, (m, t) in self.pending.items() if m["end_ts"] <= now]

        for k in expired:
            m, trade = self.pending.pop(k)
            asset  = trade["asset"]
            price  = self.prices.get(asset, trade["open_price"])
            up_won = price >= trade["open_price"]
            won    = (trade["side"] == "Up" and up_won) or (trade["side"] == "Down" and not up_won)

            # P&L: if won, receive 1 USDC per token bought at `entry` price
            # tokens_bought = size_usdc / entry
            # payout = tokens_bought * 1.0 = size_usdc / entry
            # net pnl = payout - size_usdc = size_usdc * (1/entry - 1)
            pnl = trade["size"] * (1/trade["entry"] - 1) if won else -trade["size"]
            fee = trade["size"] * 0.0156 * (1 - abs(trade["mkt_price"] - 0.5) * 2)
            pnl = pnl - fee if won else pnl

            self.daily_pnl += pnl
            self.bankroll  += pnl
            self.total     += 1
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

            # Redeem winning tokens → USDC back to wallet
            if won and not DRY_RUN:
                asyncio.create_task(self._redeem(trade))

    # ── REDEEM WINNING TOKENS → USDC ──────────────────────────────────────────
    async def _redeem(self, trade: dict):
        """After market resolves, redeem winning CTF tokens back to USDC."""
        try:
            loop = asyncio.get_event_loop()
            token_id = trade["token_id"]
            # Wait 60s for Polymarket to process resolution on-chain
            await asyncio.sleep(60)
            resp = await loop.run_in_executor(
                None, lambda: self.clob.redeem(token_id=token_id)
            )
            print(f"{G}[REDEEM]{RS} {trade['asset']} {trade['side']} | USDC → wallet | {resp}")
        except Exception as e:
            print(f"{Y}[REDEEM] {trade['asset']}: {e} (may need manual redeem on polymarket.com){RS}")

    # ── MATH ──────────────────────────────────────────────────────────────────
    def _prob_up(self, current, open_price, mins_left, vol):
        if open_price <= 0 or vol <= 0 or mins_left <= 0:
            return 0.5
        T = max(mins_left, 0.1) / (252 * 24 * 60)
        d = math.log(current / open_price) / (vol * math.sqrt(T))
        return float(norm.cdf(d))

    # ── SCAN LOOP ─────────────────────────────────────────────────────────────
    async def scan_loop(self):
        await asyncio.sleep(6)
        while True:
            markets = await self.fetch_markets()
            now     = datetime.now(timezone.utc).timestamp()
            print(f"{B}[SCAN] Live markets: {len(markets)} | Open: {len(self.pending)}{RS}")

            for cid, m in markets.items():
                if m["start_ts"] > now: continue
                if (m["end_ts"] - now) / 60 < 1: continue
                m["mins_left"] = (m["end_ts"] - now) / 60
                if cid not in self.seen:
                    await self.evaluate(m)

            await self._resolve()
            await asyncio.sleep(SCAN_INTERVAL)

    async def _status_loop(self):
        while True:
            await asyncio.sleep(STATUS_INTERVAL)
            self.status()

    # ── MAIN ──────────────────────────────────────────────────────────────────
    async def run(self):
        print(f"""
{B}╔══════════════════════════════════════════════════════════════╗
║       ClawdBot LIVE Trading v1 — Polymarket CLOB            ║
║  Network: {NETWORK:<10}  Wallet: {ADDRESS[:10]}...          ║
║  Bankroll: ${BANKROLL:<8.2f}  Edge ≥ 6%  Stop -5%           ║
║  {'DRY-RUN (simulated, no real orders)' if DRY_RUN else 'LIVE — ordini reali su Polymarket':<44}║
╚══════════════════════════════════════════════════════════════╝{RS}
""")
        self.init_clob()
        await asyncio.gather(
            self.stream_rtds(),
            self.vol_loop(),
            self.scan_loop(),
            self._status_loop(),
        )


if __name__ == "__main__":
    try:
        asyncio.run(LiveTrader().run())
    except KeyboardInterrupt:
        print(f"\n{Y}[STOP] Log: {LOG_FILE}{RS}")
