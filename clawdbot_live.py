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
import time as _time
from collections import deque
from datetime import datetime, timezone
from scipy.stats import norm
from dotenv import load_dotenv
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from eth_account import Account

load_dotenv(os.path.expanduser("~/.clawdbot.env"))

from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON, AMOY
from py_clob_client.clob_types import OrderArgs, OrderType, MarketOrderArgs, AssetType, BalanceAllowanceParams
from py_clob_client.config import get_contract_config

POLYGON_RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon-mainnet.public.blastapi.io",
    "https://polygon.drpc.org",
    "https://rpc.ankr.com/polygon",
]
CTF_ABI = [
    {"inputs":[{"name":"collateralToken","type":"address"},
               {"name":"parentCollectionId","type":"bytes32"},
               {"name":"conditionId","type":"bytes32"},
               {"name":"indexSets","type":"uint256[]"}],
     "name":"redeemPositions","outputs":[],"stateMutability":"nonpayable","type":"function"},
]

# ── CONFIG ────────────────────────────────────────────────────────────────────
PRIVATE_KEY    = os.environ["POLY_PRIVATE_KEY"]
ADDRESS        = os.environ["POLY_ADDRESS"]
NETWORK        = os.environ.get("POLY_NETWORK", "polygon")  # polygon | amoy
BANKROLL       = float(os.environ.get("BANKROLL", "100.0"))
MIN_EDGE       = 0.07     # 7% base min edge (auto-adapted per recent WR)
MIN_MOVE       = 0.002    # 0.2% min actual price move
MOMENTUM_WEIGHT = 0.40   # initial BS vs momentum blend (0=pure BS, 1=pure momentum)
MIN_BET        = 5.0      # $5 floor
MAX_BET        = 25.0     # $25 ceiling (Kelly can go higher on strong edges)
KELLY_FRAC     = 0.30     # 30% Kelly — still conservative, more capital on strong edge
MAX_DAILY_LOSS = 0.05     # 5% hard stop
MAX_OPEN       = 8        # max simultaneous open positions

# Chainlink oracle feeds on Polygon (same source Polymarket uses for resolution)
CHAINLINK_FEEDS = {
    "BTC": "0xc907E116054Ad103354f2D350FD2514433D57F6f",
    "ETH": "0xF9680D99D6C9589e2a93a78A04A279e509205945",
    "SOL": "0x10C8264C0935b3B9870013e057f330Ff3e9C56dC",
    "XRP": "0x785ba89291f676b5386652eB12b30cF361020694",
}
CHAINLINK_ABI = [
    {"inputs":[],"name":"latestRoundData","outputs":[
        {"name":"roundId","type":"uint80"},{"name":"answer","type":"int256"},
        {"name":"startedAt","type":"uint256"},{"name":"updatedAt","type":"uint256"},
        {"name":"answeredInRound","type":"uint80"}],
     "stateMutability":"view","type":"function"},
]
SCAN_INTERVAL  = 5   # market slot discovery only — real-time eval via RTDS ticks
PING_INTERVAL  = 5
STATUS_INTERVAL= 30
_DATA_DIR      = os.environ.get("DATA_DIR", os.path.expanduser("~"))
LOG_FILE       = os.path.join(_DATA_DIR, "clawdbot_live_trades.csv")
PENDING_FILE   = os.path.join(_DATA_DIR, "clawdbot_pending.json")
SEEN_FILE      = os.path.join(_DATA_DIR, "clawdbot_seen.json")
STATS_FILE     = os.path.join(_DATA_DIR, "clawdbot_stats.json")

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
        self.pending         = {}   # cid → (m, trade)
        self.pending_redeem  = {}   # cid → (side, asset)  — waiting on-chain resolution
        self.seen            = set()
        self._session        = None   # persistent aiohttp session
        self.cl_prices       = {}    # Chainlink oracle prices (resolution source)
        self.cl_updated      = {}    # Chainlink last update timestamp per asset
        self.bankroll        = BANKROLL
        self.start_bank      = BANKROLL
        self.daily_pnl       = 0.0
        self.total           = 0
        self.wins            = 0
        self.start_time      = datetime.now(timezone.utc)
        self.rtds_ok         = False
        self.clob            = None
        self.w3              = None   # shared Polygon RPC connection
        # ── Adaptive strategy state ──────────────────────────────────────────
        self.price_history   = {a: deque(maxlen=300) for a in ["BTC","ETH","SOL","XRP"]}
        self.stats           = {}    # {asset: {side: {wins, total}}} — persisted
        self.recent_trades   = deque(maxlen=30)   # rolling window for WR adaptation
        self._last_eval_time    = {}              # cid → last RTDS-triggered evaluate() timestamp
        self.structural_trades  = deque(maxlen=10)  # track structural bet outcomes
        self.peak_bankroll      = BANKROLL           # track peak for drawdown guard
        self._init_log()
        self._load_pending()
        self._load_stats()
        self._init_w3()

    def _init_w3(self):
        for rpc in POLYGON_RPCS:
            try:
                _w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
                _w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
                _w3.eth.block_number
                self.w3 = _w3
                print(f"{G}[RPC] Connected: {rpc}{RS}")
                return
            except Exception:
                continue
        print(f"{Y}[RPC] No working Polygon RPC — on-chain redemption disabled{RS}")

    def _save_pending(self):
        try:
            with open(PENDING_FILE, "w") as f:
                json.dump({k: [m, t] for k, (m, t) in self.pending.items()}, f)
        except Exception:
            pass

    def _save_seen(self):
        try:
            with open(SEEN_FILE, "w") as f:
                json.dump(list(self.seen), f)
        except Exception:
            pass

    def _load_pending(self):
        # Load seen from disk (survive restarts → no duplicate bets)
        if os.path.exists(SEEN_FILE):
            try:
                with open(SEEN_FILE) as f:
                    self.seen = set(json.load(f))
                print(f"{Y}[RESUME] Loaded {len(self.seen)} seen markets from disk{RS}")
            except Exception:
                pass
        # Load also from Polymarket open positions (belt-and-suspenders)
        try:
            import requests as _req
            positions = _req.get(
                "https://data-api.polymarket.com/positions",
                params={"user": ADDRESS, "sizeThreshold": "0.01"},
                timeout=8
            ).json()
            now_ts = datetime.now(timezone.utc).timestamp()
            restored = 0
            for p in positions:
                cid        = p.get("conditionId", "")
                redeemable = p.get("redeemable", False)
                outcome    = p.get("outcome", "")
                val        = float(p.get("currentValue", 0))
                size       = float(p.get("size", 0))
                title      = p.get("title", "")
                self.seen.add(cid)
                # Restore active (unresolved) positions to pending so Open count is correct
                if not redeemable and outcome and cid and cid not in self.pending:
                    asset = ("BTC" if "Bitcoin" in title else "ETH" if "Ethereum" in title
                             else "SOL" if "Solana" in title else "XRP" if "XRP" in title else "?")
                    end_ts = now_ts + 2 * 3600  # conservative 2h; _redeemable_scan handles real resolution
                    m_r = {"conditionId": cid, "question": title, "asset": asset,
                           "duration": 15, "end_ts": end_ts, "start_ts": now_ts - 60,
                           "up_price": 0.5, "mins_left": 120, "token_up": "", "token_down": ""}
                    t_r = {"side": outcome, "size": val, "entry": 0.5,
                           "open_price": 0, "current_price": 0, "true_prob": 0.5,
                           "mkt_price": 0.5, "edge": 0, "mins_left": 120,
                           "end_ts": end_ts, "asset": asset, "duration": 15,
                           "token_id": "", "order_id": "SYNCED"}
                    self.pending[cid] = (m_r, t_r)
                    restored += 1
                    print(f"{Y}[RESUME] Active: {title[:45]} {outcome} ~${val:.2f}{RS}")
            print(f"{Y}[RESUME] Synced {len(self.seen)} seen, {restored} active restored{RS}")
        except Exception:
            pass
        # Load pending trades
        if not os.path.exists(PENDING_FILE):
            return
        try:
            with open(PENDING_FILE) as f:
                data = json.load(f)
            self.pending = {k: (m, t) for k, (m, t) in data.items()}
            self.seen.update(self.pending.keys())
            if self.pending:
                print(f"{Y}[RESUME] Loaded {len(self.pending)} pending trades from previous run{RS}")
        except Exception as e:
            print(f"{Y}[RESUME] Could not load pending: {e}{RS}")

    # ── CLOB INIT ─────────────────────────────────────────────────────────────
    def init_clob(self):
        print(f"{B}[CLOB] Connecting to Polymarket CLOB ({NETWORK})...{RS}")
        self.clob = ClobClient(
            host=CLOB_HOST,
            key=PRIVATE_KEY,
            chain_id=CHAIN_ID,
            signature_type=0,   # EOA (direct wallet, not proxy)
            funder=ADDRESS,     # address holding the USDC
        )
        # Derive/create API credentials (signed from private key — no external account needed)
        try:
            creds = self.clob.create_or_derive_api_creds()
            self.clob.set_api_creds(creds)
            print(f"{G}[CLOB] API creds OK: {creds.api_key[:8]}...{RS}")
        except Exception as e:
            print(f"{R}[CLOB] Creds error: {e}{RS}")
            raise

        # Sync USDC (COLLATERAL) allowance with Polymarket backend
        # CONDITIONAL not needed — bot only places BUY orders (USDC→tokens)
        if not DRY_RUN:
            try:
                resp = self.clob.update_balance_allowance(
                    BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                )
                print(f"{G}[CLOB] Allowance synced (COLLATERAL): {resp or 'OK'}{RS}")
            except Exception as e:
                print(f"{Y}[CLOB] Allowance sync: {e}{RS}")

        # Sync real USDC balance → override bankroll with actual on-chain value
        try:
            bal  = self.clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
            usdc = float(bal.get("balance", 0)) / 1e6
            allowances = bal.get("allowances", {})
            allow = max((float(v) for v in allowances.values()), default=0) / 1e6
            if usdc > 0:
                self.bankroll   = usdc
                self.start_bank = usdc
            print(f"{G}[CLOB] USDC balance: ${usdc:.2f}  allowance: ${allow:.2f}  → bankroll set to ${self.bankroll:.2f}{RS}")
            if usdc < 10 and not DRY_RUN:
                print(f"{R}[WARN] Saldo basso! Fondi il wallet prima di fare trading live.{RS}")
        except Exception as e:
            print(f"{Y}[CLOB] Balance check: {e}{RS}")

        # Cancel any stale GTC orders (from previous run) before syncing positions
        if not DRY_RUN:
            self._cancel_open_orders()

        # Sync open positions from Polymarket → rebuild pending for active markets
        if not DRY_RUN:
            self._sync_open_positions()

    def _cancel_open_orders(self):
        """Cancel all open GTC orders from previous runs to prevent duplicate fills."""
        try:
            resp = self.clob.cancel_all()
            print(f"{Y}[CLOB] Cancelled stale open orders: {resp or 'OK'}{RS}")
        except Exception as e:
            print(f"{Y}[CLOB] Cancel orders: {e}{RS}")

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
        # Show each open position with current win/loss status
        now_ts = _time.time()
        for cid, (m, t) in list(self.pending.items()):
            asset      = t.get("asset", "?")
            side       = t.get("side", "?")
            size       = t.get("size", 0)
            open_p     = t.get("open_price", 0)
            # Use Chainlink (resolution source) for win/loss; fall back to RTDS if unavailable
            cl_p       = self.cl_prices.get(asset, 0)
            cur_p      = cl_p if cl_p > 0 else self.prices.get(asset, 0)
            end_ts     = t.get("end_ts", 0)
            mins_left  = max(0, (end_ts - now_ts) / 60)
            title      = m.get("question", "")[:38]
            src        = "CL" if cl_p > 0 else "RTDS"
            winning    = (side == "Up" and cur_p > open_p) or (side == "Down" and cur_p < open_p)
            move_pct   = ((cur_p - open_p) / open_p * 100) if open_p > 0 else 0
            c          = G if winning else R
            status_str = "WIN" if winning else "LOSS"
            payout_est = size / t.get("entry", 0.5) if winning else 0
            print(f"  {c}[{status_str}]{RS} {asset} {side} | {title} | "
                  f"open={open_p:.2f} {src}={cur_p:.2f} ({move_pct:+.2f}%) | "
                  f"bet=${size:.2f} est=${payout_est:.2f} | {mins_left:.1f}min left")

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
                            self.price_history[asset].append((_time.time(), val))
                            # Event-driven: evaluate unseen markets immediately on price tick
                            now_t = _time.time()
                            for cid, m in list(self.active_mkts.items()):
                                if m.get("asset") != asset: continue
                                if cid in self.seen: continue
                                if cid not in self.open_prices: continue
                                if now_t - self._last_eval_time.get(cid, 0) < 1.0: continue
                                mins = (m["end_ts"] - now_t) / 60
                                if mins < 1: continue
                                self._last_eval_time[cid] = now_t
                                m_rt = dict(m); m_rt["mins_left"] = mins
                                asyncio.create_task(self.evaluate(m_rt))
            except Exception as e:
                self.rtds_ok = False
                print(f"{R}[RTDS] Reconnect: {e}{RS}")
                await asyncio.sleep(min(15, 2 * 2 ** getattr(self, "_rtds_fails", 0)))
                self._rtds_fails = getattr(self, "_rtds_fails", 0) + 1
            else:
                self._rtds_fails = 0

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

    # ── CHAINLINK ORACLE LOOP ─────────────────────────────────────────────────
    async def chainlink_loop(self):
        """Poll Chainlink feeds every 5s — same source Polymarket uses for resolution."""
        if self.w3 is None:
            print(f"{Y}[CL] No RPC — Chainlink disabled, using RTDS fallback{RS}")
            return
        contracts = {}
        for asset, addr in CHAINLINK_FEEDS.items():
            try:
                contracts[asset] = self.w3.eth.contract(
                    address=Web3.to_checksum_address(addr), abi=CHAINLINK_ABI
                )
            except Exception as e:
                print(f"{Y}[CL] {asset} contract error: {e}{RS}")
        if not contracts:
            return
        loop = asyncio.get_event_loop()
        ok_assets = list(contracts.keys())
        print(f"{G}[CL] Chainlink feeds: {', '.join(ok_assets)}{RS}")
        while True:
            for asset, contract in contracts.items():
                try:
                    data = await loop.run_in_executor(
                        None, contract.functions.latestRoundData().call
                    )
                    price   = data[1] / 1e8
                    updated = data[3]
                    age     = _time.time() - updated
                    if age < 60:   # only use if fresh (<60s)
                        self.cl_prices[asset]  = price
                        self.cl_updated[asset] = updated
                except Exception:
                    pass
            await asyncio.sleep(5)

    def _current_price(self, asset: str) -> float:
        """For direction decisions: RTDS (real-time) is current price.
        Chainlink is used only as open_price baseline (set once at market start).
        Fallback to Chainlink if RTDS not yet streaming."""
        rtds = self.prices.get(asset, 0)
        if rtds > 0:
            return rtds
        return self.cl_prices.get(asset, 0)

    def _kelly_size(self, true_prob: float, entry: float, edge: float = 0.0) -> float:
        """Kelly bet size with dynamic ceiling scaled by edge strength.
        Floor=$5. Ceiling: edge<15%→$25, edge 15-30%→$50, edge>30%→$80 (or 35% bankroll)."""
        if entry <= 0 or entry >= 1:
            return MIN_BET
        b = (1 / entry) - 1
        q = 1 - true_prob
        kelly_f = (true_prob * b - q) / b
        kelly_f = max(0.0, kelly_f)
        size = self.bankroll * kelly_f * KELLY_FRAC
        if edge >= 0.30:
            max_bet = min(80.0, self.bankroll * 0.35)
        elif edge >= 0.15:
            max_bet = min(50.0, self.bankroll * 0.30)
        else:
            max_bet = MAX_BET
        return round(max(MIN_BET, min(max_bet, size)), 2)

    # ── MARKET FETCHER ────────────────────────────────────────────────────────
    async def _fetch_series(self, slug: str, info: dict, now: float) -> dict:
        """Fetch one series — called in parallel for all series."""
        result = {}
        try:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession()
            async with self._session.get(
                f"{GAMMA}/events",
                params={
                    "series_id":  info["id"],
                    "active":     "true",
                    "closed":     "false",
                    "order":      "startDate",
                    "ascending":  "true",
                    "limit":      "20",
                },
                timeout=aiohttp.ClientTimeout(total=5)
            ) as r:
                if r.status == 429:
                    retry = int(r.headers.get("Retry-After", 30))
                    print(f"{Y}[FETCH] Rate limited — waiting {retry}s{RS}")
                    await asyncio.sleep(retry)
                    return result
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
                result[cid] = {
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
        return result

    async def fetch_markets(self):
        now = datetime.now(timezone.utc).timestamp()
        # Fetch all 5 series IN PARALLEL — ~27ms instead of 5×27ms+2.5s stagger
        results = await asyncio.gather(
            *[self._fetch_series(slug, info, now) for slug, info in SERIES.items()]
        )
        found = {}
        for r in results:
            found.update(r)
        self.active_mkts = found
        return found

    # ── EVALUATE + PLACE ORDER ────────────────────────────────────────────────
    async def evaluate(self, m: dict):
        cid       = m["conditionId"]
        if cid in self.seen:
            return
        asset     = m["asset"]
        duration  = m["duration"]
        mins_left = m["mins_left"]
        up_price  = m["up_price"]

        if self.daily_pnl <= -(self.bankroll * MAX_DAILY_LOSS):
            return

        # Max open positions guard
        if len(self.pending) >= MAX_OPEN:
            return

        # Use RTDS (Binance) as current price for direction analysis — real-time, sub-second.
        # open_price is Chainlink (resolution source), current is RTDS (fastest signal).
        # Momentum and BS use RTDS to detect where price is heading right now.
        current = self.prices.get(asset, 0) or self.cl_prices.get(asset, 0)
        if current == 0:
            return

        open_price = self.open_prices.get(cid)
        if not open_price:
            return  # wait until scan_loop sets Chainlink-based open_price

        # Entry window — per-duration minimum life remaining:
        # 5m markets: 20% (enter within first 4 min), 15m markets: 35% (enter within first ~10 min)
        total_life    = m["end_ts"] - m["start_ts"]
        pct_remaining = (mins_left * 60) / total_life if total_life > 0 else 0
        pct_min       = 0.20 if duration <= 5 else 0.35
        if pct_remaining < pct_min:
            return

        # Per-duration MIN_MOVE: 5m markets need less move (BTC barely moves 0.2% in 5min)
        min_move_dur = 0.0007 if duration <= 5 else MIN_MOVE   # 0.07% for 5m, 0.2% for 15m

        # Check directional move
        move_pct      = abs(current - open_price) / open_price if open_price > 0 else 0
        directional   = move_pct >= min_move_dur

        # Check RTDS vs Chainlink direction agreement
        cl_now   = self.cl_prices.get(asset, 0)
        cl_agree = True
        if cl_now > 0 and open_price > 0:
            rtds_up = current > open_price
            cl_up   = cl_now  > open_price
            if rtds_up != cl_up:
                cl_agree = False

        # Structural bet: no directional move but check for CLOB structural mispricing.
        # Only allowed when safety guard is green (no excessive losses).
        structural = False
        if not directional:
            if self._structural_ok():
                structural = True   # pass to _place_order which requires 12% CLOB edge
            else:
                return  # safety guard disabled structural bets

        # ── Combined probability: Black-Scholes + Momentum + Direction bias ──
        vol      = self.vols.get(asset, 0.70)
        bs_prob  = self._prob_up(current, open_price, mins_left, vol)
        mom_prob = self._momentum_prob(asset, seconds=60)
        mw       = self._adaptive_momentum_weight()
        combined = (1 - mw) * bs_prob + mw * mom_prob

        # Direction bias from historical WR for this asset+side
        bias_up   = self._direction_bias(asset, "Up")
        bias_down = self._direction_bias(asset, "Down")
        prob_up   = max(0.05, min(0.95, combined + bias_up))
        prob_down = max(0.05, min(0.95, (1 - combined) + bias_down))

        edge_up   = prob_up   - up_price
        edge_down = prob_down - (1 - up_price)

        # AMM-based pre-filter — loose (real edge checked vs CLOB ask in _place_order).
        # Use low threshold (0.02) here: CLOB asks can be far cheaper than AMM mid,
        # so we don't want to block opportunities where AMM edge is small but CLOB edge is large.
        fee_est      = 0.025 * (1 - abs(up_price - 0.5) * 2)
        min_edge     = self._adaptive_min_edge()
        pre_filter   = max(0.02, min_edge * 0.30)   # loose pre-filter vs AMM

        if edge_up >= edge_down and edge_up >= pre_filter:
            side, edge, true_prob = "Up", edge_up, prob_up
        elif edge_down >= pre_filter:
            side, edge, true_prob = "Down", edge_down, prob_down
        else:
            return

        entry = up_price if side == "Up" else (1 - up_price)
        size  = self._kelly_size(true_prob, entry, edge)
        token_id = m["token_up"] if side == "Up" else m["token_down"]

        if not token_id:
            print(f"{Y}[SKIP] No token_id for {asset} {side} — market data incomplete{RS}")
            self.seen.add(cid)
            return

        print(f"{B}[EDGE] {asset} {side} | move={((current-open_price)/open_price):+.3%} "
              f"bs={bs_prob:.3f} mom={mom_prob:.3f} combined={true_prob:.3f} "
              f"mkt={up_price:.3f} edge={edge:.3f} fee~{fee_est:.3f} {mins_left:.1f}min left{RS}")

        # Mark seen before placing — prevents double-bet if evaluate called concurrently
        self.seen.add(cid)
        self._save_seen()

        # ── Place real order ──────────────────────────────────────────────────
        order_id = await self._place_order(token_id, side, entry, size, asset, duration, mins_left, true_prob, cl_agree, structural)

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
            "structural":    structural,
        }

        if order_id:
            self.pending[cid] = (m, trade)
            self._save_pending()
            self._log(m, trade)

    async def _place_order(self, token_id, side, price, size_usdc, asset, duration, mins_left, true_prob=0.5, cl_agree=True, structural=False):
        """Maker-first order strategy:
        1. Post bid at mid-price (best_bid+best_ask)/2 — collect the spread
        2. Wait up to 45s for fill (other market evals run in parallel via asyncio)
        3. If unfilled, cancel and fall back to taker at best_ask+tick
        4. If taker unfilled after 3s, cancel and return None.
        Returns order_id or None."""
        if DRY_RUN:
            fake_id = f"DRY-{asset[:3]}-{int(datetime.now(timezone.utc).timestamp())}"
            # Simulate at AMM price (approximates maker fill quality)
            print(f"{Y}[DRY-RUN]{RS} {side} {asset} {duration}m | ${size_usdc:.2f} @ {price:.3f} | id={fake_id}")
            return fake_id

        for attempt in range(2):
            try:
                loop = asyncio.get_event_loop()

                # Fetch live order book — always fresh, ~36ms, worth it for accurate edge
                book     = await loop.run_in_executor(None, lambda: self.clob.get_order_book(token_id))
                tick     = float(book.tick_size or "0.01")
                asks     = sorted(book.asks, key=lambda x: float(x.price)) if book.asks else []
                bids     = sorted(book.bids, key=lambda x: float(x.price), reverse=True) if book.bids else []

                if not asks:
                    print(f"{Y}[SKIP] {asset} {side}: empty order book{RS}")
                    return None

                best_ask = float(asks[0].price)
                best_bid = float(bids[0].price) if bids else best_ask - 0.10
                spread   = best_ask - best_bid

                # Edge requirement varies by trade type:
                # structural (no move): needs 12% CLOB edge — higher bar, no directional signal
                # oracles disagree:    needs 12% CLOB edge — higher bar, conflicting signals
                # normal directional:  needs 7% CLOB edge
                if structural:
                    edge_req = MIN_EDGE + 0.05   # 12%
                    # Structural bets use half size for safety
                    size_usdc = round(size_usdc * 0.5, 2)
                elif not cl_agree:
                    edge_req = MIN_EDGE + 0.05   # 12%
                else:
                    edge_req = MIN_EDGE          # 7%

                taker_edge = true_prob - best_ask
                if taker_edge < edge_req:
                    kind = "structural" if structural else ("disagree" if not cl_agree else "directional")
                    print(f"{Y}[SKIP] {asset} {side} [{kind}]: taker_edge={taker_edge:.3f} < {edge_req:.2f} "
                          f"(ask={best_ask:.3f} model={true_prob:.3f}){RS}")
                    return None
                kind = "structural" if structural else ("cautious" if not cl_agree else "directional")
                print(f"{B}[{kind.upper()}]{RS} {asset} {side} edge={taker_edge:.3f} req={edge_req:.2f}")

                # ── PHASE 1: Maker bid at mid ──────────────────────────────
                mid          = (best_bid + best_ask) / 2
                maker_price  = round(min(mid + tick, best_ask - tick), 4)
                maker_price  = max(maker_price, tick)
                maker_edge   = true_prob - maker_price
                size_tok_m   = round(size_usdc / maker_price, 2)

                print(f"{G}[MAKER] {asset} {side}: bid={maker_price:.3f} "
                      f"ask={best_ask:.3f} spread={spread:.3f} "
                      f"maker_edge={maker_edge:.3f} taker_edge={taker_edge:.3f}{RS}")

                order_args = OrderArgs(token_id=token_id, price=maker_price,
                                       size=size_tok_m, side="BUY")
                signed  = await loop.run_in_executor(None, lambda: self.clob.create_order(order_args))
                resp    = await loop.run_in_executor(None, lambda: self.clob.post_order(signed, OrderType.GTC))
                order_id = resp.get("orderID") or resp.get("id", "")
                status   = resp.get("status", "")

                if not order_id:
                    print(f"{Y}[MAKER] No order_id — skipping{RS}")
                    return None

                if status == "matched":
                    self.bankroll -= size_usdc
                    payout = size_usdc / maker_price
                    print(f"{G}[MAKER FILL]{RS} {side} {asset} {duration}m | "
                          f"${size_usdc:.2f} @ {maker_price:.3f} | payout=${payout:.2f} | "
                          f"Bank ${self.bankroll:.2f}")
                    return order_id

                # Poll up to 20s for maker fill — shorter wait so taker fallback is timely
                max_wait  = min(20, int(mins_left * 60 * 0.20))
                polls     = max(1, max_wait // 10)
                print(f"{G}[MAKER] posted {asset} {side} @ {maker_price:.3f} — "
                      f"waiting up to {polls*10}s for fill...{RS}")

                filled = False
                for _ in range(polls):
                    await asyncio.sleep(10)
                    try:
                        info = await loop.run_in_executor(None, lambda: self.clob.get_order(order_id))
                        if isinstance(info, dict) and info.get("status") in ("matched", "filled"):
                            filled = True
                            break
                    except Exception:
                        pass

                if filled:
                    self.bankroll -= size_usdc
                    payout = size_usdc / maker_price
                    print(f"{G}[MAKER FILL]{RS} {side} {asset} {duration}m | "
                          f"${size_usdc:.2f} @ {maker_price:.3f} | payout=${payout:.2f} | "
                          f"Bank ${self.bankroll:.2f}")
                    return order_id

                # Cancel maker, fall back to taker
                try:
                    await loop.run_in_executor(None, lambda: self.clob.cancel(order_id))
                except Exception:
                    pass
                print(f"{Y}[MAKER] unfilled — falling back to taker @ {best_ask+tick:.3f}{RS}")

                # ── PHASE 2: Taker fallback ────────────────────────────────
                taker_price = round(min(best_ask + tick, 0.97), 4)
                size_tok_t  = round(size_usdc / taker_price, 2)

                order_args = OrderArgs(token_id=token_id, price=taker_price,
                                       size=size_tok_t, side="BUY")
                signed  = await loop.run_in_executor(None, lambda: self.clob.create_order(order_args))
                resp    = await loop.run_in_executor(None, lambda: self.clob.post_order(signed, OrderType.GTC))
                order_id = resp.get("orderID") or resp.get("id", "")
                status   = resp.get("status", "")

                if not order_id:
                    return None

                if status == "matched":
                    self.bankroll -= size_usdc
                    print(f"{Y}[TAKER FILL]{RS} {side} {asset} {duration}m | "
                          f"${size_usdc:.2f} @ {taker_price:.3f} | Bank ${self.bankroll:.2f}")
                    return order_id

                await asyncio.sleep(3)
                try:
                    info = await loop.run_in_executor(None, lambda: self.clob.get_order(order_id))
                    if isinstance(info, dict) and info.get("status") in ("matched", "filled"):
                        self.bankroll -= size_usdc
                        print(f"{Y}[TAKER FILL]{RS} {side} {asset} {duration}m | "
                              f"${size_usdc:.2f} @ {taker_price:.3f} | Bank ${self.bankroll:.2f}")
                        return order_id
                except Exception:
                    pass

                try:
                    await loop.run_in_executor(None, lambda: self.clob.cancel(order_id))
                except Exception:
                    pass
                # Final check: cancel might have raced with a fill — verify before giving up
                try:
                    info = await loop.run_in_executor(None, lambda: self.clob.get_order(order_id))
                    if isinstance(info, dict) and info.get("status") in ("matched", "filled"):
                        self.bankroll -= size_usdc
                        print(f"{Y}[TAKER FILL (late)]{RS} {side} {asset} {duration}m | "
                              f"${size_usdc:.2f} @ {taker_price:.3f} | Bank ${self.bankroll:.2f}")
                        return order_id
                except Exception:
                    pass
                print(f"{Y}[ORDER] Both maker and taker unfilled — cancelled{RS}")
                return None

            except Exception as e:
                err = str(e)
                if "429" in err or "rate limit" in err.lower():
                    await asyncio.sleep(10 * (attempt + 1))
                    continue
                print(f"{R}[ORDER FAILED] {asset} {side}: {e}{RS}")
                return None
        return None

    # ── RESOLVE ───────────────────────────────────────────────────────────────
    async def _resolve(self):
        """Queue all expired positions for on-chain resolution.
        Never trust local price comparison — Polymarket resolves on Chainlink
        at the exact expiry timestamp, which may differ from current price."""
        now     = datetime.now(timezone.utc).timestamp()
        expired = [k for k, (m, t) in self.pending.items() if m["end_ts"] <= now]

        for k in expired:
            m, trade = self.pending.pop(k)
            self._save_pending()
            asset = trade["asset"]

            if DRY_RUN:
                # Simulation only: use current price as approximation
                price  = self._current_price(asset) or self.prices.get(asset, trade["open_price"])
                up_won = price >= trade["open_price"]
                won    = (trade["side"] == "Up" and up_won) or (trade["side"] == "Down" and not up_won)
                pnl    = trade["size"] * (1/trade["entry"] - 1) if won else -trade["size"]
                self.daily_pnl += pnl
                if won:
                    self.bankroll += trade["size"] / trade["entry"]
                self.total += 1
                if won: self.wins += 1
                self._record_result(asset, trade["side"], won, trade.get("structural", False))
                self._log(m, trade, "WIN" if won else "LOSS", pnl)
                c  = G if won else R
                wr = f"{self.wins/self.total*100:.0f}%" if self.total else "–"
                print(f"{c}[{'WIN' if won else 'LOSS'}]{RS} {asset} {trade['side']} "
                      f"{trade['duration']}m | {c}${pnl:+.2f}{RS} | Bank ${self.bankroll:.2f} | WR {wr}")
            else:
                # Live: queue for on-chain check — result determined by payoutNumerators
                self.pending_redeem[k] = (m, trade)
                print(f"{B}[RESOLVE] {asset} {trade['side']} {trade['duration']}m → on-chain queue{RS}")

    # ── REDEEM LOOP — polls every 30s, determines win/loss on-chain ───────────
    async def _redeem_loop(self):
        """Authoritative win/loss determination via payoutNumerators on-chain.
        Updates bankroll/P&L only after confirmed on-chain result."""
        if DRY_RUN or self.w3 is None:
            return
        cfg      = get_contract_config(CHAIN_ID, neg_risk=False)
        ctf_addr = Web3.to_checksum_address(cfg.conditional_tokens)
        collat   = Web3.to_checksum_address(cfg.collateral)
        acct     = Account.from_key(PRIVATE_KEY)
        CTF_ABI_FULL = CTF_ABI + [
            {"inputs":[{"name":"conditionId","type":"bytes32"}],
             "name":"payoutDenominator","outputs":[{"name":"","type":"uint256"}],
             "stateMutability":"view","type":"function"},
            {"inputs":[{"name":"conditionId","type":"bytes32"},{"name":"index","type":"uint256"}],
             "name":"payoutNumerators","outputs":[{"name":"","type":"uint256"}],
             "stateMutability":"view","type":"function"},
        ]
        ctf  = self.w3.eth.contract(address=ctf_addr, abi=CTF_ABI_FULL)
        loop = asyncio.get_event_loop()

        while True:
            await asyncio.sleep(30)
            if not self.pending_redeem:
                continue
            done = []
            for cid, val in list(self.pending_redeem.items()):
                # Support both (m, trade) from _resolve and legacy (side, asset) from _sync_redeemable
                if isinstance(val[0], dict):
                    m, trade = val
                    side  = trade["side"]
                    asset = trade["asset"]
                else:
                    side, asset = val
                    m     = {"conditionId": cid, "question": ""}
                    trade = {"side": side, "asset": asset, "size": 0, "entry": 0.5,
                             "duration": 0, "mkt_price": 0.5, "mins_left": 0,
                             "open_price": 0, "token_id": "", "order_id": ""}
                try:
                    cid_bytes = bytes.fromhex(cid.lstrip("0x").zfill(64))
                    denom = await loop.run_in_executor(
                        None, lambda b=cid_bytes: ctf.functions.payoutDenominator(b).call()
                    )
                    if denom == 0:
                        continue   # not yet resolved on-chain

                    # Determine actual winner from on-chain oracle result
                    n0 = await loop.run_in_executor(
                        None, lambda b=cid_bytes: ctf.functions.payoutNumerators(b, 0).call()
                    )
                    n1 = await loop.run_in_executor(
                        None, lambda b=cid_bytes: ctf.functions.payoutNumerators(b, 1).call()
                    )
                    winner = "Up" if n0 > 0 else "Down"
                    won    = (winner == side)
                    size   = trade.get("size", 0)
                    entry  = trade.get("entry", 0.5)

                    if won and size > 0:
                        # Redeem winning CTF tokens → USDC
                        index_set = 1 if side == "Up" else 2
                        nonce  = await loop.run_in_executor(
                            None, lambda: self.w3.eth.get_transaction_count(acct.address)
                        )
                        # EIP-1559 gas pricing — required by Polygon mainnet RPCs
                        latest   = await loop.run_in_executor(None, lambda: self.w3.eth.get_block("latest"))
                        base_fee = latest["baseFeePerGas"]
                        pri_fee  = self.w3.to_wei(40, "gwei")
                        max_fee  = base_fee * 2 + pri_fee
                        tx = ctf.functions.redeemPositions(
                            collat, b'\x00'*32, cid_bytes, [index_set]
                        ).build_transaction({
                            "from": acct.address, "nonce": nonce,
                            "gas": 200_000,
                            "maxFeePerGas": max_fee,
                            "maxPriorityFeePerGas": pri_fee,
                            "chainId": 137,
                        })
                        signed  = acct.sign_transaction(tx)
                        tx_hash = await loop.run_in_executor(
                            None, lambda: self.w3.eth.send_raw_transaction(signed.raw_transaction)
                        )
                        receipt = await loop.run_in_executor(
                            None, lambda h=tx_hash: self.w3.eth.wait_for_transaction_receipt(h, timeout=60)
                        )
                        if receipt.status == 1:
                            fee    = size * 0.0156 * (1 - abs(trade.get("mkt_price", 0.5) - 0.5) * 2)
                            payout = size / entry - fee
                            pnl    = payout - size
                            self.bankroll  += payout
                            self.daily_pnl += pnl
                            self.total += 1; self.wins += 1
                            self._record_result(asset, side, True, trade.get("structural", False))
                            self._log(m, trade, "WIN", pnl)
                            wr = f"{self.wins/self.total*100:.0f}%" if self.total else "–"
                            print(f"{G}[WIN]{RS} {asset} {side} {trade.get('duration',0)}m | "
                                  f"{G}${pnl:+.2f}{RS} | Bank ${self.bankroll:.2f} | WR {wr} | "
                                  f"tx={tx_hash.hex()[:16]}")
                            done.append(cid)
                        else:
                            print(f"{Y}[REDEEM] {asset} {side} tx failed, retrying...{RS}")
                    else:
                        # Lost on-chain
                        if size > 0:
                            pnl = -size
                            self.daily_pnl += pnl
                            self.total += 1
                            self._record_result(asset, side, False, trade.get("structural", False))
                            self._log(m, trade, "LOSS", pnl)
                            wr = f"{self.wins/self.total*100:.0f}%" if self.total else "–"
                            print(f"{R}[LOSS]{RS} {asset} {side} {trade.get('duration',0)}m | "
                                  f"{R}${pnl:+.2f}{RS} | Bank ${self.bankroll:.2f} | WR {wr}")
                        done.append(cid)
                except Exception as e:
                    print(f"{Y}[REDEEM] {asset}: {e}{RS}")
            for cid in done:
                self.pending_redeem.pop(cid, None)

    # ── STARTUP OPEN POSITIONS SYNC ───────────────────────────────────────────
    def _sync_open_positions(self):
        """Rebuild self.pending from Polymarket API for any active (non-resolved) positions."""
        try:
            import requests as _req
            positions = _req.get(
                "https://data-api.polymarket.com/positions",
                params={"user": ADDRESS, "sizeThreshold": "0.01"},
                timeout=10
            ).json()
        except Exception as e:
            print(f"{Y}[SYNC] Could not fetch positions: {e}{RS}")
            return

        now     = datetime.now(timezone.utc).timestamp()
        synced  = 0
        for pos in positions:
            cid        = pos.get("conditionId", "")
            redeemable = pos.get("redeemable", False)
            outcome    = pos.get("outcome", "")   # "Up" or "Down"
            val        = float(pos.get("currentValue", 0))
            size_tok   = float(pos.get("size", 0))
            title      = pos.get("title", "")

            # Only active (not yet resolved) positions not already tracked
            if redeemable or not outcome or not cid:
                continue
            if cid in self.pending:
                continue

            self.seen.add(cid)

            # Defaults
            asset    = ("BTC" if "Bitcoin" in title else "ETH" if "Ethereum" in title
                        else "SOL" if "Solana" in title else "XRP" if "XRP" in title else "?")
            end_ts   = now + 30 * 60
            start_ts = now - 60
            duration = 15
            token_up = token_down = ""

            # Fetch real market data from Gamma API using conditionId
            try:
                import requests as _req
                mkt_data = _req.get(
                    f"{GAMMA}/markets",
                    params={"conditionId": cid},
                    timeout=8
                ).json()
                mkt = mkt_data[0] if isinstance(mkt_data, list) and mkt_data else (
                      mkt_data if isinstance(mkt_data, dict) else {})
                end_str   = mkt.get("endDate") or mkt.get("end_date", "")
                start_str = mkt.get("startDate") or mkt.get("start_date", "")
                if end_str:
                    end_ts = datetime.fromisoformat(end_str.replace("Z", "+00:00")).timestamp()
                if start_str:
                    start_ts = datetime.fromisoformat(start_str.replace("Z", "+00:00")).timestamp()
                slug = mkt.get("seriesSlug") or mkt.get("series_slug", "")
                if slug in SERIES:
                    duration = SERIES[slug]["duration"]
                    asset    = SERIES[slug]["asset"]
                tokens = mkt.get("clobTokenIds") or []
                if isinstance(tokens, str):
                    try: tokens = json.loads(tokens)
                    except: tokens = []
                token_up   = tokens[0] if len(tokens) > 0 else ""
                token_down = tokens[1] if len(tokens) > 1 else ""
            except Exception as e:
                print(f"{Y}[SYNC] Gamma lookup failed for {cid[:10]}: {e}{RS}")

            # Skip if already expired
            if end_ts <= now:
                continue

            entry     = (val / size_tok) if size_tok > 0 else 0.5
            mins_left = (end_ts - now) / 60

            m = {"conditionId": cid, "question": title, "asset": asset,
                 "duration": duration, "end_ts": end_ts, "start_ts": start_ts,
                 "up_price": entry if outcome == "Up" else 1 - entry,
                 "mins_left": mins_left, "token_up": token_up, "token_down": token_down}
            trade = {"side": outcome, "size": val, "entry": entry,
                     "open_price": 0, "current_price": 0, "true_prob": 0.5,
                     "mkt_price": entry, "edge": 0, "mins_left": mins_left,
                     "end_ts": end_ts, "asset": asset, "duration": duration,
                     "token_id": token_up if outcome == "Up" else token_down,
                     "order_id": "SYNCED"}

            self.pending[cid] = (m, trade)
            synced += 1
            print(f"{Y}[SYNC] Restored: {title[:45]} {outcome} ~${val:.2f} | {duration}m ends in {mins_left:.1f}min{RS}")

        if synced:
            self._save_pending()
            self._save_seen()
            print(f"{Y}[SYNC] {synced} open position(s) restored to pending{RS}")

    # ── STARTUP REDEEM SYNC ───────────────────────────────────────────────────
    def _sync_redeemable(self):
        """At startup, find any winning positions not yet redeemed and queue them."""
        if DRY_RUN or self.w3 is None:
            return
        try:
            import requests as _req
            r = _req.get(
                "https://data-api.polymarket.com/positions",
                params={"user": ADDRESS, "sizeThreshold": "0.01"},
                timeout=10
            )
            positions = r.json()
        except Exception as e:
            print(f"{Y}[SYNC] Could not fetch positions: {e}{RS}")
            return

        cfg      = get_contract_config(CHAIN_ID, neg_risk=False)
        ctf_addr = Web3.to_checksum_address(cfg.conditional_tokens)
        ctf      = self.w3.eth.contract(address=ctf_addr, abi=[
            {"inputs":[{"name":"conditionId","type":"bytes32"}],
             "name":"payoutDenominator","outputs":[{"name":"","type":"uint256"}],
             "stateMutability":"view","type":"function"},
            {"inputs":[{"name":"conditionId","type":"bytes32"},{"name":"index","type":"uint256"}],
             "name":"payoutNumerators","outputs":[{"name":"","type":"uint256"}],
             "stateMutability":"view","type":"function"},
        ])

        queued = 0
        for pos in positions:
            cid       = pos.get("conditionId", "")
            redeemable= pos.get("redeemable", False)
            val       = float(pos.get("currentValue", 0))
            outcome   = pos.get("outcome", "")   # "Up" or "Down" — what we bet on
            size      = float(pos.get("size", 0))
            title     = pos.get("title", "")[:40]

            if not redeemable or val < 0.01 or not outcome or not cid:
                continue
            if cid in self.pending_redeem:
                continue

            try:
                b     = bytes.fromhex(cid[2:])
                denom = ctf.functions.payoutDenominator(b).call()
                if denom == 0:
                    continue
                n0 = ctf.functions.payoutNumerators(b, 0).call()
                n1 = ctf.functions.payoutNumerators(b, 1).call()
                winner = "Up" if n0 > 0 else "Down"
                if winner == outcome:
                    asset = "BTC" if "Bitcoin" in title else "ETH" if "Ethereum" in title \
                        else "SOL" if "Solana" in title else "XRP" if "XRP" in title else "?"
                    # Use (m, trade) format so _redeem_loop can update P&L correctly
                    m_s = {"conditionId": cid, "question": title}
                    t_s = {"side": outcome, "asset": asset, "size": val, "entry": 0.5,
                           "duration": 0, "mkt_price": 0.5, "mins_left": 0,
                           "open_price": 0, "token_id": "", "order_id": "SYNC"}
                    self.pending_redeem[cid] = (m_s, t_s)
                    queued += 1
                    print(f"{G}[SYNC] Queued for redeem: {title} {outcome} ~${val:.2f}{RS}")
            except Exception:
                continue

        if queued:
            print(f"{G}[SYNC] {queued} winning position(s) queued for redemption{RS}")
        else:
            print(f"{B}[SYNC] No unredeemed wins found{RS}")

    # ── MATH ──────────────────────────────────────────────────────────────────
    def _prob_up(self, current, open_price, mins_left, vol):
        if open_price <= 0 or vol <= 0 or mins_left <= 0:
            return 0.5
        T = max(mins_left, 0.1) / (252 * 24 * 60)
        d = math.log(current / open_price) / (vol * math.sqrt(T))
        return float(norm.cdf(d))

    def _momentum_prob(self, asset: str, seconds: int = 60) -> float:
        """Momentum-based P(Up) from last `seconds` of RTDS price history."""
        hist = self.price_history.get(asset)
        if not hist or len(hist) < 2:
            return 0.5
        now_ts  = _time.time()
        cutoff  = now_ts - seconds
        past    = [(ts, p) for ts, p in hist if ts <= cutoff]
        if not past:
            return 0.5
        past_price  = past[-1][1]
        current     = list(hist)[-1][1]
        if past_price <= 0:
            return 0.5
        move = (current - past_price) / past_price
        vol  = self.vols.get(asset, 0.70)
        vol_t = vol * math.sqrt(seconds / (252 * 24 * 3600))
        if vol_t == 0:
            return 0.5
        return float(norm.cdf(move / vol_t))

    def _direction_bias(self, asset: str, side: str) -> float:
        """Additive bias from historical win rate for this asset+direction.
        Returns 0 until 5 trades, then scales from -0.10 to +0.10."""
        s = self.stats.get(asset, {}).get(side, {})
        total = s.get("total", 0)
        if total < 5:
            return 0.0
        wr = s["wins"] / total
        return (wr - 0.5) * 0.25   # max ±0.125

    def _adaptive_min_edge(self) -> float:
        """Tighten MIN_EDGE when losing, relax slightly when winning."""
        if len(self.recent_trades) < 10:
            return MIN_EDGE
        recent_wr = sum(self.recent_trades) / len(self.recent_trades)
        if recent_wr > 0.65:
            return max(0.06, MIN_EDGE - 0.01)
        elif recent_wr < 0.50:
            return min(0.13, MIN_EDGE + 0.02)
        return MIN_EDGE

    def _adaptive_momentum_weight(self) -> float:
        """Shift toward momentum when recent WR is poor."""
        if len(self.recent_trades) < 10:
            return MOMENTUM_WEIGHT
        recent_wr = sum(self.recent_trades) / len(self.recent_trades)
        if recent_wr < 0.50:
            return min(0.65, MOMENTUM_WEIGHT + 0.15)
        return MOMENTUM_WEIGHT

    def _load_stats(self):
        try:
            with open(STATS_FILE) as f:
                data = json.load(f)
            self.stats        = data.get("stats", {})
            self.recent_trades = deque(data.get("recent", []), maxlen=30)
            total = sum(s.get("total",0) for a in self.stats.values() for s in a.values())
            wins  = sum(s.get("wins",0)  for a in self.stats.values() for s in a.values())
            if total:
                print(f"{G}[STATS] Loaded {total} trades, WR {wins/total*100:.1f}%{RS}")
        except Exception:
            self.stats        = {}
            self.recent_trades = deque(maxlen=30)

    def _save_stats(self):
        try:
            with open(STATS_FILE, "w") as f:
                json.dump({"stats": self.stats, "recent": list(self.recent_trades)}, f)
        except Exception:
            pass

    def _structural_ok(self) -> bool:
        """Safety guard: disable structural bets if they're underperforming or drawdown is excessive."""
        # Drawdown guard: if bankroll dropped >8% from peak, pause structural bets
        if self.bankroll < self.peak_bankroll * 0.92:
            print(f"{Y}[GUARD] Structural bets paused — drawdown {(1-self.bankroll/self.peak_bankroll):.1%} from peak ${self.peak_bankroll:.2f}{RS}")
            return False
        # Win rate guard: after 5 structural bets, require ≥40% WR
        if len(self.structural_trades) >= 5:
            wr = sum(self.structural_trades) / len(self.structural_trades)
            if wr < 0.40:
                print(f"{Y}[GUARD] Structural bets paused — WR={wr:.0%} < 40% on last {len(self.structural_trades)} structural bets{RS}")
                return False
        return True

    def _record_result(self, asset: str, side: str, won: bool, structural: bool = False):
        if asset not in self.stats:
            self.stats[asset] = {}
        if side not in self.stats[asset]:
            self.stats[asset][side] = {"wins": 0, "total": 0}
        self.stats[asset][side]["total"] += 1
        if won:
            self.stats[asset][side]["wins"] += 1
        self.recent_trades.append(1 if won else 0)
        if structural:
            self.structural_trades.append(1 if won else 0)
        # Update peak bankroll
        if self.bankroll > self.peak_bankroll:
            self.peak_bankroll = self.bankroll
        self._save_stats()
        # Print adaptive state for visibility
        mw  = self._adaptive_momentum_weight()
        me  = self._adaptive_min_edge()
        rwr = sum(self.recent_trades) / len(self.recent_trades) if self.recent_trades else 0
        print(f"{B}[ADAPT] Recent WR={rwr:.0%}  MinEdge={me:.2f}  MomWeight={mw:.2f}{RS}")

    # ── SCAN LOOP ─────────────────────────────────────────────────────────────
    async def scan_loop(self):
        await asyncio.sleep(6)
        while True:
            markets = await self.fetch_markets()
            now     = datetime.now(timezone.utc).timestamp()
            print(f"{B}[SCAN] Live markets: {len(markets)} | Open: {len(self.pending)}{RS}")

            # Evaluate ALL eligible markets in parallel — no more sequential blocking
            candidates = []
            for cid, m in markets.items():
                if m["start_ts"] > now: continue
                if (m["end_ts"] - now) / 60 < 1: continue
                m["mins_left"] = (m["end_ts"] - now) / 60
                # Set open_price from Chainlink the first time we see this market.
                # Chainlink is the SAME source Polymarket uses for resolution.
                # Must happen before evaluate() so direction comparison is correct.
                if cid not in self.open_prices:
                    asset = m.get("asset")
                    ref = self.cl_prices.get(asset, 0) or self.prices.get(asset, 0)
                    if ref > 0:
                        self.open_prices[cid] = ref
                        print(f"{B}[OPEN] {asset} ref={ref:.4f} (Chainlink){RS}")
                if cid not in self.seen:
                    candidates.append(m)
            if candidates:
                await asyncio.gather(*[self.evaluate(m) for m in candidates])

            await self._resolve()
            await asyncio.sleep(SCAN_INTERVAL)

    async def _refresh_balance(self):
        """Sync bankroll from real CLOB USDC balance every STATUS_INTERVAL.
        Skips when positions are active — mid-trade CLOB balance is unreliable
        (bet USDC is locked in CTF, not reflected until redeemed)."""
        while True:
            await asyncio.sleep(STATUS_INTERVAL)
            if DRY_RUN or self.clob is None:
                continue
            if self.pending or self.pending_redeem:
                continue   # don't override local tracking mid-trade
            try:
                loop = asyncio.get_event_loop()
                bal  = await loop.run_in_executor(
                    None, lambda: self.clob.get_balance_allowance(
                        BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                    )
                )
                usdc = float(bal.get("balance", 0)) / 1e6
                if usdc > 0 and usdc != self.bankroll:
                    print(f"{B}[BANK] Synced from CLOB: ${usdc:.2f} (was ${self.bankroll:.2f}){RS}")
                    self.bankroll = usdc
            except Exception:
                pass

    async def _redeemable_scan(self):
        """Every 60s, re-scan Polymarket API for redeemable positions not yet queued.
        Catches winners that weren't on-chain resolved when _sync_redeemable ran at startup."""
        if DRY_RUN or self.w3 is None:
            return
        while True:
            await asyncio.sleep(60)
            try:
                loop = asyncio.get_event_loop()
                import requests as _req
                positions = await loop.run_in_executor(
                    None, lambda: _req.get(
                        "https://data-api.polymarket.com/positions",
                        params={"user": ADDRESS, "sizeThreshold": "0.01"},
                        timeout=10
                    ).json()
                )
                for pos in positions:
                    cid        = pos.get("conditionId", "")
                    redeemable = pos.get("redeemable", False)
                    val        = float(pos.get("currentValue", 0))
                    outcome    = pos.get("outcome", "")
                    title      = pos.get("title", "")[:45]
                    if not redeemable or val < 0.01 or not outcome or not cid:
                        continue
                    if cid in self.pending_redeem:
                        continue
                    asset = ("BTC" if "Bitcoin" in title else "ETH" if "Ethereum" in title
                             else "SOL" if "Solana" in title else "XRP" if "XRP" in title else "?")
                    m_s = {"conditionId": cid, "question": title}
                    t_s = {"side": outcome, "asset": asset, "size": val, "entry": 0.5,
                           "duration": 0, "mkt_price": 0.5, "mins_left": 0,
                           "open_price": 0, "token_id": "", "order_id": "SCAN"}
                    self.pending_redeem[cid] = (m_s, t_s)
                    print(f"{G}[SCAN-REDEEM] Queued: {title} {outcome} ~${val:.2f}{RS}")
            except Exception as e:
                print(f"{Y}[SCAN-REDEEM] Error: {e}{RS}")

    async def _position_sync_loop(self):
        """Every 5 min: sync on-chain positions to pending — catches any fills the bot missed."""
        if DRY_RUN:
            return
        while True:
            await asyncio.sleep(300)
            try:
                import requests as _req
                loop = asyncio.get_event_loop()
                positions = await loop.run_in_executor(None, lambda: _req.get(
                    "https://data-api.polymarket.com/positions",
                    params={"user": ADDRESS, "sizeThreshold": "0.01"}, timeout=10
                ).json())
                now = datetime.now(timezone.utc).timestamp()
                added = 0
                for pos in positions:
                    cid  = pos.get("conditionId", "")
                    rdm  = pos.get("redeemable", False)
                    val  = float(pos.get("currentValue", 0))
                    side = pos.get("outcome", "")
                    if rdm or val < 0.01 or not side or not cid:
                        continue
                    if cid in self.pending or cid in self.pending_redeem:
                        continue
                    title = pos.get("title", "")
                    asset = ("BTC" if "Bitcoin" in title else "ETH" if "Ethereum" in title
                             else "SOL" if "Solana" in title else "XRP" if "XRP" in title else "?")
                    end_ts = now + 30 * 60
                    m_r = {"conditionId": cid, "question": title, "asset": asset,
                           "duration": 15, "end_ts": end_ts, "start_ts": now - 60,
                           "up_price": 0.5, "mins_left": 30, "token_up": "", "token_down": ""}
                    t_r = {"side": side, "size": val, "entry": 0.5,
                           "open_price": 0, "current_price": 0, "true_prob": 0.5,
                           "mkt_price": 0.5, "edge": 0, "mins_left": 30,
                           "end_ts": end_ts, "asset": asset, "duration": 15,
                           "token_id": "", "order_id": "SYNC-PERIODIC"}
                    self.pending[cid] = (m_r, t_r)
                    self.seen.add(cid)
                    added += 1
                    print(f"{Y}[SYNC] Recovered missed position: {title[:45]} {side} ~${val:.2f}{RS}")
                if added:
                    self._save_pending()
                    self._save_seen()
            except Exception as e:
                print(f"{Y}[SYNC] position_sync_loop error: {e}{RS}")

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
        self._sync_redeemable()   # redeem any wins from previous runs
        await asyncio.gather(
            self.stream_rtds(),
            self.vol_loop(),
            self.scan_loop(),
            self._status_loop(),
            self._refresh_balance(),
            self._redeem_loop(),
            self.chainlink_loop(),
            self._redeemable_scan(),
            self._position_sync_loop(),
        )


if __name__ == "__main__":
    try:
        asyncio.run(LiveTrader().run())
    except KeyboardInterrupt:
        print(f"\n{Y}[STOP] Log: {LOG_FILE}{RS}")
