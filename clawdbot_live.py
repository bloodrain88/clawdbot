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
MIN_EDGE       = 0.08     # 8% base min edge (auto-adapted per recent WR)
MIN_MOVE       = 0.0003   # 0.03% below this = truly flat — use momentum to determine direction
MOMENTUM_WEIGHT = 0.40   # initial BS vs momentum blend (0=pure BS, 1=pure momentum)
DUST_BET       = 5.0      # $5 absolute floor (Polymarket minimum order size)
MAX_ABS_BET    = 20.0     # $20 hard ceiling regardless of bankroll/WR/Kelly — prevents runaway sizing
MAX_BANKROLL_PCT = 0.35   # never risk more than 35% of bankroll on a single bet
MAX_OPEN       = 2        # max 2 simultaneous positions — prevent 3 correlated losses
MAX_SAME_DIR   = 1        # max 1 position per direction (no 2x Up or 2x Down)

USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"   # USDC.e on Polygon

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
    {"inputs":[{"name":"_roundId","type":"uint80"}],"name":"getRoundData","outputs":[
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
    # 5-min markets EXCLUDED: negative EV (36% WR vs 38% breakeven, payout ~1.6x)
    # AMM on 5-min adjusts too fast — no edge window by the time bot detects a move
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
        self.open_prices        = {}   # cid → float price
        self.open_prices_source = {}   # cid → "CL-exact" | "CL-fallback"
        self._mkt_log_ts        = {}   # cid → last [MKT] log time
        self.asset_cur_open     = {}   # asset → current market open price (for inter-market continuity)
        self.asset_prev_open    = {}   # asset → previous market open price
        self.active_mkts = {}
        self.pending         = {}   # cid → (m, trade)
        self.pending_redeem  = {}   # cid → (side, asset)  — waiting on-chain resolution
        self._redeem_queued_ts = {}  # cid → timestamp when queued for redeem
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
        self.peak_bankroll      = BANKROLL           # track peak for drawdown guard
        self.consec_losses      = 0                  # consecutive resolved losses counter
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
                    loaded = json.load(f)
                # Keep only last 200 entries — older conditionIds are from expired markets
                self.seen = set(loaded[-200:] if len(loaded) > 200 else loaded)
                print(f"{Y}[RESUME] Loaded {len(self.seen)} seen markets from disk{RS}")
            except Exception:
                pass
        # Load seen cids from Polymarket positions — suppress duplicate bets on restart.
        # DO NOT add to self.pending here; _sync_open_positions (called in init_clob)
        # fetches real end_ts from Gamma API and adds them properly.
        try:
            import requests as _req
            positions = _req.get(
                "https://data-api.polymarket.com/positions",
                params={"user": ADDRESS, "sizeThreshold": "0.01"},
                timeout=8
            ).json()
            for p in positions:
                cid        = p.get("conditionId", "")
                redeemable = p.get("redeemable", False)
                outcome    = p.get("outcome", "")
                val        = float(p.get("currentValue", 0))
                title      = p.get("title", "")
                if cid:
                    self.seen.add(cid)
                if not redeemable and outcome and cid:
                    print(f"{Y}[RESUME] Position: {title[:45]} {outcome} ~${val:.2f}{RS}")
            print(f"{Y}[RESUME] Seen {len(self.seen)} markets from API{RS}")
        except Exception:
            pass
        # Load pending trades
        if not os.path.exists(PENDING_FILE):
            return
        try:
            with open(PENDING_FILE) as f:
                data = json.load(f)
            now_ts = datetime.now(timezone.utc).timestamp()
            loaded = 0
            for k, (m, t) in data.items():
                end_ts = m.get("end_ts", 0)
                # Drop entries that already expired — _sync_open_positions handles resolution
                if end_ts > 0 and end_ts < now_ts - 300:   # expired >5min ago
                    print(f"{Y}[RESUME] Dropped expired: {m.get('question','')[:40]}{RS}")
                    continue
                self.pending[k] = (m, t)
                self.seen.add(k)
                loaded += 1
            if loaded:
                print(f"{Y}[RESUME] Loaded {loaded} pending trades from previous run{RS}")
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
        pnl  = self.bankroll - self.start_bank
        roi  = pnl / self.start_bank * 100 if self.start_bank > 0 else 0
        pc   = G if pnl >= 0 else R
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
            f"{B}P&L:{RS} {pc}${pnl:+.2f}{RS}  "
            f"{B}Network:{RS} {NETWORK}  "
            f"{B}Open:{RS} {len(self.pending)}  {Y}Settling:{RS} {len(self.pending_redeem)}\n"
            f"  {price_str}\n"
            f"{W}{'─'*66}{RS}"
        )
        # Show each open position with current win/loss status
        now_ts = _time.time()
        for cid, (m, t) in list(self.pending.items()):
            asset      = t.get("asset", "?")
            side       = t.get("side", "?")
            size       = t.get("size", 0)
            # Use market reference price (Chainlink at market open = Polymarket "price to beat")
            # NOT the bot's trade entry price — market determines outcome from its own start
            open_p     = self.open_prices.get(cid) or t.get("open_price", 0)
            # Use Chainlink (resolution source) for win/loss; fall back to RTDS if unavailable
            cl_p       = self.cl_prices.get(asset, 0)
            cur_p      = cl_p if cl_p > 0 else self.prices.get(asset, 0)
            end_ts     = t.get("end_ts", 0)
            mins_left  = max(0, (end_ts - now_ts) / 60)
            title      = m.get("question", "")[:38]
            src        = "CL" if cl_p > 0 else "RTDS"
            if open_p > 0 and cur_p > 0:
                winning    = (side == "Up" and cur_p > open_p) or (side == "Down" and cur_p < open_p)
                move_pct   = (cur_p - open_p) / open_p * 100
                c          = G if winning else R
                status_str = "WIN" if winning else "LOSS"
                payout_est = size / t.get("entry", 0.5) if winning else 0
                move_str   = f"({move_pct:+.2f}%)"
            else:
                c          = Y
                status_str = "?"
                payout_est = 0
                move_str   = "(no ref)"
            print(f"  {c}[{status_str}]{RS} {asset} {side} | {title} | "
                  f"open={open_p:.2f} {src}={cur_p:.2f} {move_str} | "
                  f"bet=${size:.2f} est=${payout_est:.2f} | {mins_left:.1f}min left")
        # Show settling (pending_redeem) positions
        for cid, val in list(self.pending_redeem.items()):
            if isinstance(val[0], dict):
                m_r, t_r = val
                asset_r = t_r.get("asset", "?")
                side_r  = t_r.get("side", "?")
                size_r  = t_r.get("size", 0)
                title_r = m_r.get("question", "")[:38]
            else:
                side_r, asset_r = val
                size_r = 0; title_r = ""
            elapsed_r = (_time.time() - self._redeem_queued_ts.get(cid, _time.time())) / 60
            print(f"  {Y}[SETTLING]{RS} {asset_r} {side_r} | {title_r} | bet=${size_r:.2f} | waiting {elapsed_r:.0f}min")

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

    def _kelly_size(self, true_prob: float, entry: float, kelly_frac: float) -> float:
        """Fully dynamic Kelly bet — no hardcoded dollar caps.
        kelly_frac: conviction-based fraction (0.12–0.55), driven by score.
        Absolute cap: MAX_BANKROLL_PCT of current bankroll.
        Floor: max($2, 2% of bankroll)."""
        if entry <= 0 or entry >= 1:
            return max(DUST_BET, self.bankroll * 0.02)
        b = (1 / entry) - 1
        q = 1 - true_prob
        kelly_f = max(0.0, (true_prob * b - q) / b)
        size  = self.bankroll * kelly_f * kelly_frac * self._kelly_drawdown_scale()
        floor = max(DUST_BET, self.bankroll * 0.02)
        cap   = self.bankroll * MAX_BANKROLL_PCT
        return round(max(floor, min(cap, size)), 2)

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
                end_str   = ev.get("endDate", "")
                q         = ev.get("title", "") or ev.get("question", "")
                mkts      = ev.get("markets", [])
                m_data    = mkts[0] if mkts else ev
                # eventStartTime = exact window open (e.g. 5:45PM for 5:45-5:50 market)
                # This is when Chainlink locks the "price to beat" — must match exactly
                start_str = (m_data.get("eventStartTime") or ev.get("startTime", ""))
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

    # ── SCORE + EXECUTE ───────────────────────────────────────────────────────
    async def _score_market(self, m: dict) -> dict | None:
        """Score a market opportunity. Returns signal dict or None if hard-blocked.
        Pure analysis — no side effects, no order placement."""
        cid       = m["conditionId"]
        if cid in self.seen:
            return None
        asset     = m["asset"]
        duration  = m["duration"]
        mins_left = m["mins_left"]
        up_price  = m["up_price"]
        label     = f"{asset} {duration}m | {m.get('question','')[:45]}"

        current = self.prices.get(asset, 0) or self.cl_prices.get(asset, 0)
        if current == 0:
            return None

        open_price = self.open_prices.get(cid)
        if not open_price:
            print(f"{Y}[WAIT] {label} → waiting for first CL round{RS}")
            return None

        src = self.open_prices_source.get(cid, "?")
        if src == "CL-fallback":
            return None

        src_tag = f"[{src}]"

        # Timing gate: hard block only in last 6 min (need ≥40% remaining).
        # 40-60% remaining: allowed but earns no timing bonus — only high-edge markets pass.
        # >60% remaining: timing bonus scored (+1 or +2 pts) — preferred entry window.
        total_life    = m["end_ts"] - m["start_ts"]
        pct_remaining = (mins_left * 60) / total_life if total_life > 0 else 0
        if pct_remaining < 0.40:
            return None   # last 6 min — AMM fully priced, no edge left

        move_pct = abs(current - open_price) / open_price if open_price > 0 else 0
        move_str = f"{(current-open_price)/open_price:+.3%}"

        # ── Multi-signal Score ────────────────────────────────────────────────
        # Resolution rule: ANY price above open = Up wins (even $0.001).
        # Direction from price if moved; from momentum consensus if flat.
        # Max possible: 2+3+4+1+3+3+2+1 = 19 pts
        score = 0

        # Compute momentum first — needed to determine direction when price is flat
        mom_30s  = self._momentum_prob(asset, seconds=30)
        mom_90s  = self._momentum_prob(asset, seconds=90)
        mom_180s = self._momentum_prob(asset, seconds=180)
        th_up, th_dn = 0.53, 0.47
        tf_up_votes = sum([mom_30s > th_up, mom_90s > th_up, mom_180s > th_up])
        tf_dn_votes = sum([mom_30s < th_dn, mom_90s < th_dn, mom_180s < th_dn])

        # Chainlink current — the resolution oracle
        cl_now = self.cl_prices.get(asset, 0)
        cl_move_pct = abs(cl_now - open_price) / open_price if cl_now > 0 and open_price > 0 else 0
        cl_direction = ("Up" if cl_now > open_price else "Down") if cl_move_pct >= 0.0002 else None

        # Direction: Chainlink is authoritative (it resolves the market)
        # Use Binance RTDS only when it's clearly moving; fall back to CL then momentum
        if move_pct >= 0.0003:
            direction = "Up" if current > open_price else "Down"
            # Binance small move + Chainlink clearly opposite → skip (conflicting oracles)
            if cl_direction and cl_direction != direction and move_pct < 0.0010:
                return None
        elif cl_direction:
            direction = cl_direction   # Binance flat → trust Chainlink direction
        elif tf_up_votes > tf_dn_votes:
            direction = "Up"
        elif tf_dn_votes > tf_up_votes:
            direction = "Down"
        else:
            return None   # price flat AND momentum flat — no signal, skip

        is_up    = (direction == "Up")
        tf_votes = tf_up_votes if is_up else tf_dn_votes
        very_strong_mom = (tf_votes == 3)

        # Require ≥2 momentum TFs when price/CL are flat — 1/3 is a coin flip
        if move_pct < 0.0003 and cl_move_pct < 0.0002 and tf_votes < 2:
            return None

        # Entry timing (0-2 pts) — earlier = AMM hasn't repriced yet = better odds
        if   pct_remaining >= 0.85: score += 2   # first 2.25 min of 15-min market
        elif pct_remaining >= 0.70: score += 1   # first 4.5 min

        # Move size (0-3 pts) — price confirmation bonus; not a gate
        if   move_pct >= 0.0020: score += 3
        elif move_pct >= 0.0012: score += 2
        elif move_pct >= 0.0005: score += 1
        # flat/tiny move → +0 pts (still tradeable if momentum/taker confirm)

        # Multi-timeframe momentum (0-4 pts)
        if   tf_votes == 3: score += 4
        elif tf_votes == 2: score += 2

        # Chainlink direction agreement (+1 agree / −3 disagree)
        # CL is the resolution oracle — disagreement is a major red flag
        cl_agree = True
        if cl_now > 0 and open_price > 0:
            if (is_up) != (cl_now > open_price):
                cl_agree = False
        if cl_agree:  score += 1
        else:         score -= 3

        # ── Fetch all Binance signals IN PARALLEL (~30-80ms total) ──────────────
        (ob_imbalance,
         (taker_ratio, vol_ratio),
         (perp_basis, funding_rate),
         (vwap_dev, vol_mult)) = await asyncio.gather(
            self._binance_imbalance(asset),
            self._binance_taker_flow(asset),
            self._binance_perp_signals(asset),
            self._binance_window_stats(asset, m["start_ts"]),
        )

        # Order book imbalance (−1 to +3; >40% contra = hard block)
        if (is_up and ob_imbalance < -0.40) or (not is_up and ob_imbalance > 0.40):
            return None   # extreme OB contra — hard block
        if   (is_up and ob_imbalance > 0.25) or (not is_up and ob_imbalance < -0.25): score += 3
        elif (is_up and ob_imbalance > 0.10) or (not is_up and ob_imbalance < -0.10): score += 2
        elif abs(ob_imbalance) <= 0.10: score += 1
        else:                           score -= 1
        imbalance_confirms = (is_up and ob_imbalance > 0.10) or (not is_up and ob_imbalance < -0.10)

        # Taker buy/sell flow + volume vs 30-min avg (−1 to +5 pts)
        if   (is_up and taker_ratio > 0.62) or (not is_up and taker_ratio < 0.38): score += 3
        elif (is_up and taker_ratio > 0.55) or (not is_up and taker_ratio < 0.45): score += 2
        elif abs(taker_ratio - 0.50) < 0.05: score += 1
        else:                                score -= 1
        if   vol_ratio > 2.0: score += 2
        elif vol_ratio > 1.3: score += 1

        # Perp futures basis: premium = leveraged longs crowding in = bullish (−1 to +2 pts)
        perp_confirms = (is_up and perp_basis > 0.0002) or (not is_up and perp_basis < -0.0002)
        perp_strong   = (is_up and perp_basis > 0.0005) or (not is_up and perp_basis < -0.0005)
        perp_contra   = (is_up and perp_basis < -0.0002) or (not is_up and perp_basis > 0.0002)
        if   perp_strong:   score += 2
        elif perp_confirms: score += 1
        elif perp_contra:   score -= 1

        # Funding rate: extreme = crowded = contrarian (−1 to +1 pts)
        # High positive funding + Down bet = contrarian confirms shorts
        # Very high positive funding + Up bet = overcrowded longs = risky
        if   (not is_up and funding_rate >  0.0005): score += 1  # crowded longs → contrarian short
        elif (is_up     and funding_rate < -0.0002): score += 1  # crowded shorts → contrarian long
        elif (is_up     and funding_rate >  0.0010): score -= 1  # extremely crowded long + bet Up
        elif (not is_up and funding_rate < -0.0005): score -= 1  # crowded shorts + bet Down risky

        # VWAP: momentum continuation signal (−2 to +2 pts)
        # Data: 87.7% of 15-min markets CONTINUE the mid-window direction — NOT mean reversion
        # Price ABOVE window VWAP in our direction = momentum confirms = good
        # Price BELOW window VWAP in our direction = momentum weak = bad
        vwap_net = vwap_dev if is_up else -vwap_dev   # positive = price above VWAP in bet direction
        if   vwap_net >  0.0015: score += 2   # strongly above VWAP → momentum confirms bet
        elif vwap_net >  0.0008: score += 1
        elif vwap_net < -0.0015: score -= 2   # strongly below VWAP → momentum against bet
        elif vwap_net < -0.0008: score -= 1

        # Vol-normalized displacement: continuation signal (−2 to +2 pts)
        # 87.7% continuation — extended moves tend to KEEP going, not revert
        sigma_15m = self.vols.get(asset, 0.70) * (15 / (252 * 390)) ** 0.5
        open_price_disp = self.open_prices.get(asset)
        if open_price_disp and sigma_15m > 0:
            net_disp = (current - open_price_disp) / open_price_disp * (1 if is_up else -1)
            if   net_disp > sigma_15m * 1.0: score += 2   # extended in direction = strong momentum
            elif net_disp > sigma_15m * 0.5: score += 1
            elif net_disp < -sigma_15m * 1.0: score -= 2  # price moved opposite to our bet
            elif net_disp < -sigma_15m * 0.5: score -= 1

        # Cross-asset confirmation: how many other assets trending same direction? (0-2 pts)
        cross_count = self._cross_asset_direction(asset, direction)
        if   cross_count == 3: score += 2   # all other assets confirm → strong macro signal
        elif cross_count >= 2: score += 1   # majority confirm

        # Inter-market continuity: previous 15-min market direction (+1 pt)
        prev_open = self.asset_prev_open.get(asset, 0)
        if prev_open > 0 and open_price > 0:
            prev_was_up = open_price > prev_open
            if (is_up and prev_was_up) or (not is_up and not prev_was_up):
                score += 1

        # AMM edge check — still required even at score ≥ 4
        vol      = self.vols.get(asset, 0.70)
        bs_prob  = self._prob_up(current, open_price, mins_left, vol)
        mom_prob = mom_90s
        mw       = self._adaptive_momentum_weight()
        combined = (1 - mw) * bs_prob + mw * mom_prob
        bias_up   = self._direction_bias(asset, "Up")
        bias_down = self._direction_bias(asset, "Down")
        prob_up   = max(0.05, min(0.95, combined + bias_up))
        prob_down = max(0.05, min(0.95, (1 - combined) + bias_down))
        edge_up   = prob_up   - up_price
        edge_down = prob_down - (1 - up_price)
        min_edge   = self._adaptive_min_edge()
        pre_filter = max(0.02, min_edge * 0.25)   # loose pre-filter vs AMM

        # Lock side to price direction when move is clear — 87.7% continuation on 15-min markets
        # AMM edge comparison only used when price is flat (no clear directional signal)
        if move_pct >= 0.0005:
            forced_side = "Up" if current > open_price else "Down"
            forced_edge = edge_up if forced_side == "Up" else edge_down
            forced_prob = prob_up if forced_side == "Up" else prob_down
            if forced_edge >= 0:
                side, edge, true_prob = forced_side, forced_edge, forced_prob
            elif forced_edge < -0.15:
                return None   # AMM has massively overpriced this direction — no edge at all
            else:
                side, edge, true_prob = forced_side, max(0.01, forced_edge), forced_prob
        else:
            # Flat price: side MUST match direction (CL/momentum consensus).
            # AMM edge only used to reject extreme mispricing (< -15%).
            # 86% continuation means momentum direction >> AMM edge comparison.
            dir_edge = edge_up if direction == "Up" else edge_down
            dir_prob = prob_up if direction == "Up" else prob_down
            if dir_edge < -0.15:
                return None   # AMM massively overpriced our direction — no edge
            side, edge, true_prob = direction, max(0.01, dir_edge), dir_prob

        # Kelly fraction scales with conviction score (replaces hardcoded MIN/MAX_BET)
        # Higher score → larger fraction of Kelly → bigger bet automatically
        if   score >= 12: kelly_frac = 0.55
        elif score >= 10: kelly_frac = 0.45
        elif score >=  8: kelly_frac = 0.35
        elif score >=  6: kelly_frac = 0.22
        else:             kelly_frac = 0.12   # score 4-5: probe entry only

        entry    = up_price if side == "Up" else (1 - up_price)
        wr_scale = self._wr_bet_scale()
        raw_size = self._kelly_size(true_prob, entry, kelly_frac)
        size     = round(min(MAX_ABS_BET,
                             self.bankroll * MAX_BANKROLL_PCT,
                             raw_size * vol_mult * wr_scale), 2)
        token_id = m["token_up"] if side == "Up" else m["token_down"]
        if not token_id:
            return None

        force_taker = score >= 12 and very_strong_mom and imbalance_confirms and move_pct > 0.0015

        return {
            "cid": cid, "m": m, "score": score,
            "side": side, "entry": entry, "size": size, "token_id": token_id,
            "true_prob": true_prob, "cl_agree": cl_agree, "min_edge": min_edge,
            "force_taker": force_taker, "edge": edge,
            "label": label, "asset": asset, "duration": duration,
            "open_price": open_price, "current": current, "move_str": move_str,
            "src_tag": src_tag, "bs_prob": bs_prob, "mom_prob": mom_prob,
            "up_price": up_price, "ob_imbalance": ob_imbalance,
            "imbalance_confirms": imbalance_confirms, "tf_votes": tf_votes,
            "very_strong_mom": very_strong_mom, "taker_ratio": taker_ratio,
            "vol_ratio": vol_ratio, "pct_remaining": pct_remaining, "mins_left": mins_left,
            "perp_basis": perp_basis, "funding_rate": funding_rate,
            "vwap_dev": vwap_dev, "vol_mult": vol_mult, "cross_count": cross_count,
        }

    async def _execute_trade(self, sig: dict):
        """Execute a pre-scored signal: log, mark seen, place order, update state."""
        cid = sig["cid"]
        if cid in self.seen or len(self.pending) >= MAX_OPEN:
            return
        score       = sig["score"]
        score_stars = f"{G}★★★{RS}" if score >= 12 else (f"{G}★★{RS}" if score >= 9 else "★")
        agree_str   = "" if sig["cl_agree"] else f" {Y}[CL!]{RS}"
        ob_str      = f" ob={sig['ob_imbalance']:+.2f}" + ("✓" if sig["imbalance_confirms"] else "")
        tf_str      = f" TF={sig['tf_votes']}/3" + ("★" if sig["very_strong_mom"] else "")
        prev_open   = self.asset_prev_open.get(sig["asset"], 0)
        prev_str    = f" prev={((sig['open_price']-prev_open)/prev_open*100):+.2f}%" if prev_open > 0 else ""
        perp_str    = f" perp={sig.get('perp_basis',0)*100:+.3f}%"
        vwap_str    = f" vwap={sig.get('vwap_dev',0)*100:+.3f}%"
        cross_str   = f" cross={sig.get('cross_count',0)}/3"
        print(f"{G}[EDGE] {sig['label']} → {sig['side']} | score={score} {score_stars} | "
              f"beat=${sig['open_price']:,.2f} {sig['src_tag']} now=${sig['current']:,.2f} "
              f"move={sig['move_str']}{prev_str} pct={sig['pct_remaining']:.0%} | "
              f"bs={sig['bs_prob']:.3f} mom={sig['mom_prob']:.3f} prob={sig['true_prob']:.3f} "
              f"mkt={sig['up_price']:.3f} edge={sig['edge']:.3f} ${sig['size']:.2f}"
              f"{agree_str}{ob_str}{tf_str} tk={sig['taker_ratio']:.2f} vol={sig['vol_ratio']:.1f}x"
              f"{perp_str}{vwap_str}{cross_str}{RS}")

        self.seen.add(cid)
        self._save_seen()
        order_id = await self._place_order(
            sig["token_id"], sig["side"], sig["entry"], sig["size"],
            sig["asset"], sig["duration"], sig["mins_left"],
            sig["true_prob"], sig["cl_agree"],
            min_edge_req=sig["min_edge"], force_taker=sig["force_taker"],
            score=sig["score"]
        )
        m = sig["m"]
        trade = {
            "side": sig["side"], "size": sig["size"], "entry": sig["entry"],
            "open_price": sig["open_price"], "current_price": sig["current"],
            "true_prob": sig["true_prob"], "mkt_price": sig["up_price"],
            "edge": round(sig["edge"], 4), "mins_left": sig["mins_left"],
            "end_ts": m["end_ts"], "asset": sig["asset"], "duration": sig["duration"],
            "token_id": sig["token_id"], "order_id": order_id or "",
        }
        if order_id:
            self.pending[cid] = (m, trade)
            self._save_pending()
            self._log(m, trade)

    async def evaluate(self, m: dict):
        """RTDS fast-path: score a single market and execute if score ≥ 4."""
        sig = await self._score_market(m)
        if sig and sig["score"] >= 4:
            await self._execute_trade(sig)

    async def _place_order(self, token_id, side, price, size_usdc, asset, duration, mins_left, true_prob=0.5, cl_agree=True, min_edge_req=None, force_taker=False, score=0):
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

                taker_edge = true_prob - best_ask
                best_bid_p = float(bids[0].price) if bids else best_ask - 0.10
                mid_est    = (best_bid_p + best_ask) / 2
                maker_edge_est = true_prob - mid_est

                if score >= 10:
                    # High conviction: gate on maker edge (mid price), not taker (ask)
                    # Maker posts at mid → much better fill than ask even with wide spread
                    if maker_edge_est < 0:
                        print(f"{Y}[SKIP] {asset} {side} [high-conv]: maker_edge={maker_edge_est:.3f} < 0 "
                              f"(mid={mid_est:.3f} model={true_prob:.3f}){RS}")
                        return None
                    print(f"{B}[FILL]{RS} {asset} {side} score={score} maker_edge={maker_edge_est:.3f} taker_edge={taker_edge:.3f}")
                else:
                    # Normal conviction: taker edge gate applies
                    edge_floor = min_edge_req if min_edge_req is not None else 0.04
                    if not cl_agree: edge_floor += 0.02
                    if taker_edge < edge_floor:
                        kind = "disagree" if not cl_agree else "directional"
                        print(f"{Y}[SKIP] {asset} {side} [{kind}]: taker_edge={taker_edge:.3f} < {edge_floor:.2f} "
                              f"(ask={best_ask:.3f} model={true_prob:.3f}){RS}")
                        return None
                    print(f"{B}[FILL]{RS} {asset} {side} edge={taker_edge:.3f} floor={edge_floor:.2f}")

                # High conviction: skip maker, go straight to taker for instant fill
                if force_taker:
                    taker_price = round(min(best_ask + tick, 0.97), 4)
                    size_tok_t  = round(size_usdc / taker_price, 2)
                    print(f"{G}[FAST-TAKER]{RS} {asset} {side} HIGH-CONV @ {taker_price:.3f} | ${size_usdc:.2f}")
                    order_args = OrderArgs(token_id=token_id, price=taker_price, size=size_tok_t, side="BUY")
                    signed  = await loop.run_in_executor(None, lambda: self.clob.create_order(order_args))
                    resp    = await loop.run_in_executor(None, lambda: self.clob.post_order(signed, OrderType.GTC))
                    order_id = resp.get("orderID") or resp.get("id", "")
                    if order_id and resp.get("status") in ("matched", "filled", ""):
                        self.bankroll -= size_usdc
                        print(f"{G}[FAST-FILL]{RS} {side} {asset} {duration}m | ${size_usdc:.2f} @ {taker_price:.3f} | Bank ${self.bankroll:.2f}")
                        return order_id
                    # If not filled immediately, fall through to normal maker/taker flow
                    force_taker = False  # reset so we don't loop

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

                # 5m markets: 5s maker wait (can't waste 1/3 of market life)
                # 15m markets: 15s maker wait
                poll_interval = 5 if duration <= 5 else 10
                max_wait  = min(15 if duration <= 5 else 20, int(mins_left * 60 * 0.15))
                polls     = max(1, max_wait // poll_interval)
                print(f"{G}[MAKER] posted {asset} {side} @ {maker_price:.3f} — "
                      f"waiting up to {polls*poll_interval}s for fill...{RS}")

                filled = False
                for _ in range(polls):
                    await asyncio.sleep(poll_interval)
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

                # Cancel maker, fall back to taker with fresh book
                try:
                    await loop.run_in_executor(None, lambda: self.clob.cancel(order_id))
                except Exception:
                    pass

                # ── PHASE 2: Taker fallback — re-fetch book for fresh ask ──
                try:
                    fresh    = await loop.run_in_executor(None, lambda: self.clob.get_order_book(token_id))
                    f_asks   = sorted(fresh.asks, key=lambda x: float(x.price)) if fresh.asks else asks
                    f_tick   = float(fresh.tick_size or tick)
                    fresh_ask = float(f_asks[0].price) if f_asks else best_ask
                    fresh_ep  = true_prob - fresh_ask
                    if fresh_ep < edge_floor:
                        print(f"{Y}[SKIP] {asset} {side} taker: fresh ask={fresh_ask:.3f} edge={fresh_ep:.3f} < {edge_floor:.2f} — price moved against us{RS}")
                        return None
                    taker_price = round(min(fresh_ask + f_tick, 0.97), 4)
                except Exception:
                    taker_price = round(min(best_ask + tick, 0.97), 4)
                    fresh_ask   = best_ask
                print(f"{Y}[MAKER] unfilled — taker @ {taker_price:.3f} (fresh ask={fresh_ask:.3f}){RS}")
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
        expired = [k for k, (m, t) in self.pending.items() if m.get("end_ts", 0) > 0 and m["end_ts"] <= now]

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
                self._redeem_queued_ts[k] = _time.time()
                print(f"{B}[RESOLVE] {asset} {trade['side']} {trade['duration']}m → on-chain queue (checking in 5s){RS}")

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
            {"inputs":[{"name":"owner","type":"address"},{"name":"id","type":"uint256"}],
             "name":"balanceOf","outputs":[{"name":"","type":"uint256"}],
             "stateMutability":"view","type":"function"},
        ]
        ctf  = self.w3.eth.contract(address=ctf_addr, abi=CTF_ABI_FULL)
        loop = asyncio.get_event_loop()
        # USDC contract for immediate bankroll refresh after wins
        _usdc = self.w3.eth.contract(
            address=Web3.to_checksum_address(USDC_E),
            abi=[{"inputs":[{"name":"account","type":"address"}],"name":"balanceOf",
                  "outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]
        )
        _addr_cs = Web3.to_checksum_address(ADDRESS)

        _wait_log_ts = {}   # cid → last time we printed [WAIT] for it
        while True:
            await asyncio.sleep(5)
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
                        # Log once per minute so user can see we're waiting
                        now_ts = _time.time()
                        if now_ts - _wait_log_ts.get(cid, 0) >= 60:
                            _wait_log_ts[cid] = now_ts
                            elapsed = (now_ts - self._redeem_queued_ts.get(cid, now_ts)) / 60
                            size = trade.get("size", 0)
                            print(f"{Y}[WAIT] {asset} {side} ~${size:.2f} — awaiting oracle ({elapsed:.0f}min){RS}")
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

                    # For CLOB positions: CTF tokens are held by the exchange contract,
                    # not the user wallet. Always try redeemPositions — if Polymarket
                    # already auto-redeemed, the tx reverts harmlessly; if not, we collect.
                    if won and size > 0:
                        fee    = size * 0.0156 * (1 - abs(trade.get("mkt_price", 0.5) - 0.5) * 2)
                        payout = size / entry - fee
                        pnl    = payout - size

                        index_set = 1 if side == "Up" else 2
                        nonce  = await loop.run_in_executor(
                            None, lambda: self.w3.eth.get_transaction_count(acct.address)
                        )
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
                        try:
                            tx_hash = await loop.run_in_executor(
                                None, lambda: self.w3.eth.send_raw_transaction(signed.raw_transaction)
                            )
                            receipt = await loop.run_in_executor(
                                None, lambda h=tx_hash: self.w3.eth.wait_for_transaction_receipt(h, timeout=60)
                            )
                            if receipt.status != 1:
                                suffix = f"auto-redeemed (reverted)"
                            else:
                                suffix = f"tx={tx_hash.hex()[:16]}"
                        except Exception as _redeem_err:
                            # Tx reverted = already auto-redeemed by Polymarket (USDC already in wallet)
                            suffix = f"auto-redeemed (err={str(_redeem_err)[:40]})"

                        # Immediately refresh bankroll from on-chain USDC so daily loss check
                        # in evaluate() uses the correct value right away (not 30s stale)
                        try:
                            _raw = await loop.run_in_executor(
                                None, lambda: _usdc.functions.balanceOf(_addr_cs).call()
                            )
                            if _raw > 0:
                                self.bankroll = _raw / 1e6
                        except Exception:
                            pass
                        self.daily_pnl += pnl
                        self.total += 1; self.wins += 1
                        self._record_result(asset, side, True, trade.get("structural", False))
                        self._log(m, trade, "WIN", pnl)
                        wr = f"{self.wins/self.total*100:.0f}%" if self.total else "–"
                        print(f"{G}[WIN]{RS} {asset} {side} {trade.get('duration',0)}m | "
                              f"{G}${pnl:+.2f}{RS} | Bank ${self.bankroll:.2f} | WR {wr} | {suffix}")
                        done.append(cid)
                    else:
                        # Lost on-chain (on-chain is authoritative)
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
            changed_pending = False
            for cid in done:
                self.pending_redeem.pop(cid, None)
                if self.pending.pop(cid, None) is not None:
                    changed_pending = True
            if changed_pending:
                self._save_pending()

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
        api_cids = {p.get("conditionId","") for p in positions}

        # Remove from pending any position the API now shows as resolved/redeemable
        for cid in list(self.pending.keys()):
            pos_data = next((p for p in positions if p.get("conditionId") == cid), None)
            if pos_data is None:
                continue  # not in API at all — leave it, _resolve() handles expiry
            if pos_data.get("redeemable"):
                val = float(pos_data.get("currentValue", 0))
                m_p, t_p = self.pending.pop(cid)
                title_p = m_p.get("question", "")[:40]
                side_p  = t_p.get("side", "")
                asset_p = t_p.get("asset", "")
                if val >= 0.01 and cid not in self.pending_redeem:
                    self.pending_redeem[cid] = (m_p, t_p)
                    print(f"{G}[SYNC] Resolved→redeem: {title_p} {side_p} ~${val:.2f}{RS}")
                else:
                    print(f"{Y}[SYNC] Resolved→loss: {title_p} {side_p} (${val:.2f}){RS}")
                    # Record the loss
                    size_p = t_p.get("size", 0)
                    if size_p > 0:
                        self.total += 1
                        self._record_result(asset_p, side_p, False, t_p.get("structural", False))
                self._save_pending()

        for pos in positions:
            cid        = pos.get("conditionId", "")
            redeemable = pos.get("redeemable", False)
            outcome    = pos.get("outcome", "")   # "Up" or "Down"
            val        = float(pos.get("currentValue", 0))
            size_tok   = float(pos.get("size", 0))
            title      = pos.get("title", "")

            # Skip resolved/redeemable or incomplete
            if redeemable or not outcome or not cid:
                continue

            self.seen.add(cid)

            # Skip dust positions — too small to be a real bot bet
            if val < DUST_BET:
                continue

            # Fetch real market data from Gamma API — always, even if already in pending
            # (to fix wrong end_ts set by _load_pending or _position_sync_loop)
            asset    = ("BTC" if "Bitcoin" in title else "ETH" if "Ethereum" in title
                        else "SOL" if "Solana" in title else "XRP" if "XRP" in title else "?")
            end_ts   = 0
            start_ts = now - 60
            duration = 15
            token_up = token_down = ""
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
                start_str = mkt.get("eventStartTime") or mkt.get("startDate") or mkt.get("start_date", "")
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

            # Gamma /markets sometimes returns date-only endDate (e.g. "2026-02-20") which
            # parses to midnight UTC — falsely appears expired.
            # Authoritative truth: Polymarket positions API said redeemable=False → still open.
            # Fix: trust stored end_ts if valid; else estimate from duration; never expire
            # a position the API confirms is still open.
            if end_ts > 0 and end_ts <= now:
                stored_end = self.pending.get(cid, ({},))[0].get("end_ts", 0)
                if stored_end > now:
                    end_ts = stored_end  # trust stored value
                    print(f"{Y}[SYNC] {title[:40]} — Gamma date-only, using stored end_ts ({(end_ts-now)/60:.1f}min){RS}")
                else:
                    # No reliable end_ts — API says open, so use duration estimate
                    end_ts = now + duration * 60
                    print(f"{Y}[SYNC] {title[:40]} — Gamma date-only, estimating {duration}min remaining{RS}")

            if end_ts == 0:
                # No end_ts at all — estimate from duration
                end_ts = now + duration * 60
                print(f"{Y}[SYNC] {title[:40]} — no end_ts, estimating {duration}min{RS}")
            entry     = (val / size_tok) if size_tok > 0 else 0.5
            mins_left = (end_ts - now) / 60

            m = {"conditionId": cid, "question": title, "asset": asset,
                 "duration": duration, "end_ts": end_ts, "start_ts": start_ts,
                 "up_price": entry if outcome == "Up" else 1 - entry,
                 "mins_left": mins_left, "token_up": token_up, "token_down": token_down}

            if cid in self.pending_redeem:
                # Already queued for on-chain redeem — don't move it back to pending
                print(f"{Y}[SYNC] Skip re-add (in redeem queue): {title[:40]} {outcome}{RS}")
                continue
            elif cid in self.pending:
                # Update end_ts and market data on existing pending entry
                old_m, old_t = self.pending[cid]
                old_m.update({"end_ts": end_ts, "start_ts": start_ts, "duration": duration,
                               "asset": asset, "token_up": token_up, "token_down": token_down})
                old_t.update({"end_ts": end_ts, "asset": asset, "duration": duration,
                               "token_id": token_up if outcome == "Up" else token_down})
                print(f"{Y}[SYNC] Updated: {title[:40]} {outcome} | {duration}m ends in {mins_left:.1f}min{RS}")
            else:
                trade = {"side": outcome, "size": val, "entry": entry,
                         "open_price": 0, "current_price": 0, "true_prob": 0.5,
                         "mkt_price": entry, "edge": 0, "mins_left": mins_left,
                         "end_ts": end_ts, "asset": asset, "duration": duration,
                         "token_id": token_up if outcome == "Up" else token_down,
                         "order_id": "SYNCED"}
                self.pending[cid] = (m, trade)
                synced += 1
                print(f"{Y}[SYNC] Restored: {title[:40]} {outcome} ~${val:.2f} | {duration}m ends in {mins_left:.1f}min{RS}")

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

        # No on-chain checks at startup — _redeem_loop handles payoutDenominator/winner
        # determination. Just queue all API-confirmed redeemable positions immediately.
        queued = 0
        for pos in positions:
            cid        = pos.get("conditionId", "")
            redeemable = pos.get("redeemable", False)
            val        = float(pos.get("currentValue", 0))
            outcome    = pos.get("outcome", "")
            title      = pos.get("title", "")[:40]

            if not redeemable or val < 0.01 or not outcome or not cid:
                continue
            if cid in self.pending_redeem:
                continue

            asset = ("BTC" if "Bitcoin" in title else "ETH" if "Ethereum" in title
                     else "SOL" if "Solana" in title else "XRP" if "XRP" in title else "?")
            m_s = {"conditionId": cid, "question": title}
            t_s = {"side": outcome, "asset": asset, "size": val, "entry": 0.5,
                   "duration": 0, "mkt_price": 0.5, "mins_left": 0,
                   "open_price": 0, "token_id": "", "order_id": "SYNC"}
            self.pending.pop(cid, None)   # remove from pending — _redeem_loop now owns it
            self.pending_redeem[cid] = (m_s, t_s)
            queued += 1
            print(f"{G}[SYNC] Queued for redeem: {title} {outcome} ~${val:.2f}{RS}")

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

    async def _binance_imbalance(self, asset: str) -> float:
        """Fetch Binance top-10 order book imbalance for asset.
        Returns +1.0 (all bids, buying pressure) to -1.0 (all asks, selling pressure).
        Fast ~30ms call — use as non-price directional confirmation."""
        try:
            import requests as _req
            sym  = asset + "USDT"
            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(None, lambda: _req.get(
                "https://api.binance.com/api/v3/depth",
                params={"symbol": sym, "limit": 10}, timeout=2
            ).json())
            bid_vol = sum(float(b[1]) for b in data.get("bids", []))
            ask_vol = sum(float(a[1]) for a in data.get("asks", []))
            if bid_vol + ask_vol == 0:
                return 0.0
            return (bid_vol - ask_vol) / (bid_vol + ask_vol)
        except Exception:
            return 0.0  # neutral on error — don't block trade

    async def _binance_taker_flow(self, asset: str) -> tuple:
        """Fetch last 3 Binance 1m candles → (taker_buy_ratio, vol_vs_30min_avg).
        taker_buy_ratio: fraction of volume from aggressive market-buy orders (0-1).
        vol_ratio: recent 3-candle avg vol vs prior 30-candle avg (1.0 = normal).
        Returns (0.5, 1.0) on error (neutral, don't block)."""
        try:
            import requests as _req
            sym  = asset + "USDT"
            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(None, lambda: _req.get(
                "https://api.binance.com/api/v3/klines",
                params={"symbol": sym, "interval": "1m", "limit": 33},
                timeout=3
            ).json())
            if not isinstance(data, list) or len(data) < 4:
                return 0.5, 1.0
            recent = data[-3:]           # last 3 complete 1m candles
            hist   = data[:-3]           # prior 30 candles for baseline avg
            rec_vol   = sum(float(k[5]) for k in recent)
            rec_taker = sum(float(k[9]) for k in recent)
            hist_avg  = (sum(float(k[5]) for k in hist) / len(hist)) if hist else rec_vol
            taker_ratio = rec_taker / rec_vol if rec_vol > 0 else 0.5
            vol_ratio   = (rec_vol / 3) / hist_avg if hist_avg > 0 else 1.0
            return round(taker_ratio, 3), round(vol_ratio, 2)
        except Exception:
            return 0.5, 1.0   # neutral on error

    async def _binance_perp_signals(self, asset: str) -> tuple:
        """Perp futures basis + funding rate in one API call.
        basis: (markPrice - indexPrice) / indexPrice  (+ve = perp premium = bullish)
        funding: lastFundingRate (+ve = longs crowded, -ve = shorts crowded)
        Returns (0.0, 0.0) on error."""
        try:
            import requests as _req
            sym  = asset + "USDT"
            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(None, lambda: _req.get(
                "https://fapi.binance.com/fapi/v1/premiumIndex",
                params={"symbol": sym}, timeout=3
            ).json())
            mark    = float(data.get("markPrice", 0))
            index   = float(data.get("indexPrice", 0))
            funding = float(data.get("lastFundingRate", 0))
            basis   = (mark - index) / index if index > 0 else 0.0
            return round(basis, 7), round(funding, 7)
        except Exception:
            return 0.0, 0.0

    async def _binance_window_stats(self, asset: str, window_start_ts: float) -> tuple:
        """VWAP deviation and vol regime from 1-min klines.
        vwap_dev: (current - window_vwap) / vwap  (+ve = above VWAP = overbought)
        vol_mult: realized vol regime multiplier (0.7=low, 1.0=normal, 1.2=high, 1.4=spike)
        Returns (0.0, 1.0) on error."""
        try:
            import requests as _req, statistics as _stats
            sym  = asset + "USDT"
            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(None, lambda: _req.get(
                "https://api.binance.com/api/v3/klines",
                params={"symbol": sym, "interval": "1m", "limit": 20}, timeout=3
            ).json())
            if not data or len(data) < 3:
                return 0.0, 1.0
            start_ms = int(window_start_ts * 1000)
            window_k = [k for k in data if int(k[0]) >= start_ms] or data[-3:]
            # VWAP from window candles (typical price × volume)
            sum_tv = sum((float(k[2])+float(k[3])+float(k[4]))/3 * float(k[5]) for k in window_k)
            sum_v  = sum(float(k[5]) for k in window_k)
            vwap   = sum_tv / sum_v if sum_v > 0 else float(window_k[-1][4])
            cur    = float(window_k[-1][4])
            vwap_dev = (cur - vwap) / vwap if vwap > 0 else 0.0
            # Realized vol from all 20 candles
            closes = [float(k[4]) for k in data]
            pct_ch = [abs(closes[i]/closes[i-1]-1) for i in range(1, len(closes)) if closes[i-1] > 0]
            if pct_ch:
                ann = _stats.mean(pct_ch) * (252 * 24 * 60) ** 0.5
                if   ann < 0.40: vol_mult = 0.7
                elif ann < 0.80: vol_mult = 1.0
                elif ann < 1.50: vol_mult = 1.2
                else:            vol_mult = 1.4
            else:
                vol_mult = 1.0
            return round(vwap_dev, 6), vol_mult
        except Exception:
            return 0.0, 1.0

    def _cross_asset_direction(self, asset: str, direction: str) -> int:
        """Count how many OTHER assets are trending in the same direction right now.
        Uses current Binance RTDS price vs each market's Chainlink open_price.
        Returns 0-3."""
        is_up = (direction == "Up")
        count = 0
        for cid, m in self.active_mkts.items():
            a = m.get("asset", "")
            if a == asset:
                continue
            op  = self.open_prices.get(cid, 0)
            cur = self.prices.get(a, 0) or self.cl_prices.get(a, 0)
            if op <= 0 or cur <= 0:
                continue
            if (cur > op) == is_up:
                count += 1
        return count

    def _direction_bias(self, asset: str, side: str) -> float:
        """Additive bias from historical win rate for this asset+direction.
        Returns 0 until 5 trades, then scales from -0.10 to +0.10."""
        s = self.stats.get(asset, {}).get(side, {})
        total = s.get("total", 0)
        if total < 5:
            return 0.0
        wr = s["wins"] / total
        return (wr - 0.5) * 0.25   # max ±0.125

    def _kelly_drawdown_scale(self) -> float:
        """Scale Kelly fraction down when in drawdown vs session high.
        Bot keeps trading but reduces size automatically — no hard stop."""
        if self.peak_bankroll <= 0:
            return 1.0
        dd = (self.peak_bankroll - self.bankroll) / self.peak_bankroll
        if dd < 0.10:
            return 1.0       # normal
        elif dd < 0.20:
            return 0.60      # -10-20% drawdown: 60% of normal size
        elif dd < 0.30:
            return 0.35      # -20-30%: 35%
        else:
            return 0.20      # >30%: 20% (survival mode, still trading)

    def _last5_wr(self) -> float:
        """Win rate over the last 5 resolved trades (0.0–1.0). -1 if < 5 trades."""
        if len(self.recent_trades) < 5:
            return -1.0
        return sum(list(self.recent_trades)[-5:]) / 5

    def _wr_bet_scale(self) -> float:
        """Scale bet size up when win rate is consistently high (last 10 trades).
        Needs ≥10 resolved trades to activate — avoids overconfidence on small samples.
        Returns multiplier applied to raw Kelly size:
          WR ≥ 80%  → 2.0x (hot streak, push hard)
          WR ≥ 70%  → 1.5x
          WR ≥ 60%  → 1.2x
          WR < 60%  → 1.0x (base — no change)
        """
        trades = list(self.recent_trades)
        if len(trades) < 10:
            return 1.0
        wr10 = sum(trades[-10:]) / 10
        if   wr10 >= 0.80: return 2.0
        elif wr10 >= 0.70: return 1.5
        elif wr10 >= 0.60: return 1.2
        else:              return 1.0

    def _adaptive_min_edge(self) -> float:
        """CLOB edge sanity floor — keeps us from buying at obviously bad prices.
        Range: 3-8% only. Never blocks trading on its own."""
        wr5 = self._last5_wr()
        if   wr5 < 0:        return 0.04   # no history: moderate
        elif wr5 >= 0.80:    return 0.03   # hot streak: very permissive
        elif wr5 >= 0.60:    return 0.04   # normal
        elif wr5 >= 0.40:    return 0.05   # slightly cold
        else:                return 0.06   # cold: 6% — circuit breaker handles the rest

    def _adaptive_momentum_weight(self) -> float:
        """Shift toward momentum when recent WR is poor."""
        wr5 = self._last5_wr()
        if wr5 < 0:
            return MOMENTUM_WEIGHT
        if wr5 < 0.40:
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

    def _record_result(self, asset: str, side: str, won: bool, structural: bool = False):
        if asset not in self.stats:
            self.stats[asset] = {}
        if side not in self.stats[asset]:
            self.stats[asset][side] = {"wins": 0, "total": 0}
        self.stats[asset][side]["total"] += 1
        if won:
            self.stats[asset][side]["wins"] += 1
        self.recent_trades.append(1 if won else 0)
        # Track consecutive losses for adaptive signals (no pause — trade every cycle)
        if won:
            self.consec_losses = 0
        else:
            self.consec_losses += 1
        # Update peak bankroll
        if self.bankroll > self.peak_bankroll:
            self.peak_bankroll = self.bankroll
        self._save_stats()
        # Print adaptive state for visibility
        mw   = self._adaptive_momentum_weight()
        me   = self._adaptive_min_edge()
        wr5  = self._last5_wr()
        last5 = list(self.recent_trades)[-5:] if len(self.recent_trades) >= 5 else list(self.recent_trades)
        streak = "".join("W" if x else "L" for x in last5)
        label  = f"{wr5:.0%}" if wr5 >= 0 else "–"
        wr_scale = self._wr_bet_scale()
        wr_str   = f"  BetScale={wr_scale:.1f}x" if wr_scale > 1.0 else ""
        print(f"{B}[ADAPT] Last5={streak} WR={label}  MinEdge={me:.2f}  Streak={self.consec_losses}L  DrawdownScale={self._kelly_drawdown_scale():.0%}{wr_str}{RS}")

    # ── CHAINLINK HISTORICAL PRICE ────────────────────────────────────────────
    async def _get_chainlink_at(self, asset: str, start_ts: float) -> tuple:
        """Return (price, source) matching Polymarket's 'price to beat'.
        Polymarket uses the FIRST Chainlink round posted AT OR AFTER eventStartTime.
        Walk backward to find where we cross start_ts, then return the round just before that
        (i.e. the first one that's >= start_ts as seen walking forward).
        BTC/ETH update every ~27s → 60 rounds covers ~27 minutes lookback."""
        if self.w3 is None or asset not in CHAINLINK_FEEDS:
            return 0.0, "no-w3"
        loop = asyncio.get_event_loop()
        try:
            contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(CHAINLINK_FEEDS[asset]),
                abi=CHAINLINK_ABI
            )
            latest = await loop.run_in_executor(None, contract.functions.latestRoundData().call)
            round_id   = latest[0]
            updated_at = latest[3]
            price      = latest[1]

            # Latest round is before start_ts: no post-start round exists yet → wait
            if updated_at < start_ts:
                return 0.0, "not-ready"

            # Latest round is at or after start — walk backward to find the FIRST round
            # that is >= start_ts (i.e. the first Chainlink update after window opened).
            # As we walk back, track the most recent round still >= start_ts.
            first_after_price = price
            first_after_ts    = updated_at

            phase_id = round_id >> 64
            agg_id   = round_id & ((1 << 64) - 1)
            for i in range(1, 61):
                prev_agg = agg_id - i
                if prev_agg <= 0:
                    break
                prev_id = (phase_id << 64) | prev_agg
                try:
                    data = await loop.run_in_executor(
                        None, lambda rid=prev_id: contract.functions.getRoundData(rid).call()
                    )
                    prev_updated = data[3]
                    prev_price   = data[1]
                    if prev_price <= 0:
                        continue
                    if prev_updated >= start_ts:
                        # Still at or after window start — this is now the earliest candidate
                        first_after_price = prev_price
                        first_after_ts    = prev_updated
                    else:
                        # Crossed into pre-window territory — first_after is our answer
                        secs_after = first_after_ts - start_ts
                        print(f"{G}[CL] {asset} price to beat: ${first_after_price/1e8:,.2f} "
                              f"(first CL round +{secs_after:.0f}s after window open){RS}")
                        return first_after_price / 1e8, "CL-exact"
                except Exception:
                    continue

            # All 60 rounds were >= start_ts (market started very recently or long window)
            # first_after_price is the oldest available round after start
            secs_after = first_after_ts - start_ts
            print(f"{G}[CL] {asset} price to beat: ${first_after_price/1e8:,.2f} "
                  f"(oldest round in window, +{secs_after:.0f}s){RS}")
            return first_after_price / 1e8, "CL-exact"
        except Exception as e:
            print(f"{Y}[CL] _get_chainlink_at {asset}: {e}{RS}")
            return 0.0, "error"

    # ── SCAN LOOP ─────────────────────────────────────────────────────────────
    async def scan_loop(self):
        await asyncio.sleep(6)
        while True:
          try:
            markets = await self.fetch_markets()
            now     = datetime.now(timezone.utc).timestamp()
            print(f"{B}[SCAN] Live markets: {len(markets)} | Open: {len(self.pending)} | Settling: {len(self.pending_redeem)}{RS}")

            # Evaluate ALL eligible markets in parallel — no more sequential blocking
            candidates = []
            for cid, m in markets.items():
                if m["start_ts"] > now: continue
                if (m["end_ts"] - now) / 60 < 1: continue
                m["mins_left"] = (m["end_ts"] - now) / 60
                # Set open_price from Chainlink at exact market start time.
                # Must match Polymarket's resolution reference ("price to beat").
                asset    = m.get("asset")
                dur      = m.get("duration", 0)
                title_s  = m.get("question", "")[:50]
                if cid not in self.open_prices:
                    start_ts = m.get("start_ts", now)
                    ref, src = await self._get_chainlink_at(asset, start_ts)
                    if src == "not-ready":
                        # Window just opened — no CL round yet. Wait for next scan.
                        print(f"{W}[NEW MARKET] {asset} {dur}m | {title_s} | waiting for first CL round...{RS}")
                    elif ref <= 0:
                        # CL error — don't store fallback price; retry next scan cycle
                        # (evaluate() rejects CL-fallback anyway, so storing it wastes cycles)
                            print(f"{Y}[NEW MARKET] {asset} {dur}m | {title_s} | beat=${ref:,.2f} [fallback]{RS}")
                    else:
                        # Track inter-market continuity: prev market close = this market open
                        if asset in self.asset_cur_open:
                            self.asset_prev_open[asset] = self.asset_cur_open[asset]
                        self.asset_cur_open[asset]   = ref
                        self.open_prices[cid]        = ref
                        self.open_prices_source[cid] = src
                        print(f"{W}[NEW MARKET] {asset} {dur}m | {title_s} | {m['mins_left']:.1f}min left{RS}")
                else:
                    # Already known — log every 30s (not every scan) to reduce noise
                    ref = self.open_prices[cid]
                    src = self.open_prices_source.get(cid, "?")
                    cur = self.prices.get(asset, 0) or self.cl_prices.get(asset, 0)
                    now_ts = _time.time()
                    if cur > 0 and now_ts - self._mkt_log_ts.get(cid, 0) >= 30:
                        self._mkt_log_ts[cid] = now_ts
                        move = (cur - ref) / ref * 100
                        print(f"{B}[MKT] {asset} {dur}m | beat=${ref:,.2f} [{src}] | "
                              f"now=${cur:,.2f} move={move:+.3f}% | {m['mins_left']:.1f}min left{RS}")
                if cid not in self.seen:
                    candidates.append(m)
            if candidates:
                # Score all markets in parallel — pick best (best-of-round strategy)
                signals = list(await asyncio.gather(*[self._score_market(m) for m in candidates]))
                valid   = sorted([s for s in signals if s is not None], key=lambda x: -x["score"])
                if valid:
                    best = valid[0]
                    other_strs = " | others: " + ", ".join(s["asset"] + "=" + str(s["score"]) for s in valid[1:]) if len(valid) > 1 else ""
                    print(f"{B}[ROUND] Best signal: {best['asset']} {best['side']} score={best['score']}{other_strs}{RS}")
                slots = MAX_OPEN - len(self.pending)
                placed = 0
                for sig in valid:
                    if placed >= slots: break
                    # Direction cap: no 2x same-direction positions
                    pending_up = sum(1 for _, t in self.pending.values() if t.get("side") == "Up")
                    pending_dn = sum(1 for _, t in self.pending.values() if t.get("side") == "Down")
                    if sig["side"] == "Up"   and pending_up >= MAX_SAME_DIR: continue
                    if sig["side"] == "Down" and pending_dn >= MAX_SAME_DIR: continue
                    # Late entry (40-60% remaining, no timing bonus): require score ≥ 8
                    is_late = sig["pct_remaining"] < 0.60
                    min_score = 8 if is_late else 4
                    if sig["score"] < min_score: break
                    # 2nd trade: only at score ≥ 8
                    if placed >= 1 and sig["score"] < 8: break
                    await self._execute_trade(sig)
                    placed += 1

            await self._resolve()
            await asyncio.sleep(SCAN_INTERVAL)
          except Exception as e:
            print(f"{R}[SCAN] Error (continuing): {e}{RS}")
            await asyncio.sleep(SCAN_INTERVAL)

    async def _refresh_balance(self):
        """Always sync bankroll, trades and win rate from on-chain truth.
        Never skips — no local accounting assumptions."""
        USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
        ERC20_ABI = [{"inputs":[{"name":"account","type":"address"}],
                      "name":"balanceOf","outputs":[{"name":"","type":"uint256"}],
                      "stateMutability":"view","type":"function"}]
        usdc_contract = None
        if self.w3 is not None:
            try:
                usdc_contract = self.w3.eth.contract(
                    address=Web3.to_checksum_address(USDC_E), abi=ERC20_ABI
                )
            except Exception:
                pass

        tick = 0
        while True:
            await asyncio.sleep(STATUS_INTERVAL)
            if DRY_RUN or self.w3 is None:
                continue
            tick += 1
            try:
                loop = asyncio.get_event_loop()
                import requests as _req

                # 1. On-chain USDC.e balance
                usdc_raw = await loop.run_in_executor(
                    None, lambda: usdc_contract.functions.balanceOf(
                        Web3.to_checksum_address(ADDRESS)
                    ).call()
                ) if usdc_contract else 0
                usdc = usdc_raw / 1e6

                # 2. Current value of open CTF positions (locked in CTF, not in wallet)
                positions = await loop.run_in_executor(None, lambda: _req.get(
                    "https://data-api.polymarket.com/positions",
                    params={"user": ADDRESS, "sizeThreshold": "0.01"}, timeout=10
                ).json())
                api_cids = {p.get("conditionId","") for p in positions}
                open_val = sum(
                    float(p.get("currentValue", 0))
                    for p in positions
                    if not p.get("redeemable") and p.get("outcome")
                )
                # Add pending positions not yet visible in the API (fill→API lag ~10-30s)
                for cid, (_, t) in self.pending.items():
                    if cid not in api_cids:
                        open_val += t.get("size", 0)

                total = round(usdc + open_val, 2)
                print(f"{B}[BANK] on-chain USDC=${usdc:.2f}  open_positions=${open_val:.2f}  total=${total:.2f}{RS}")
                if total > 0:
                    self.bankroll = total
                    if total > self.peak_bankroll:
                        self.peak_bankroll = total
                    # Keep start_bank current so daily loss % tracks from real balance
                    # (don't let a stale startup value permanently block trading)
                    if total > self.start_bank:
                        self.start_bank = total

                # 3. Recover any on-chain positions not tracked in pending (every tick = 30s)
                now_ts = datetime.now(timezone.utc).timestamp()
                for p in positions:
                    cid  = p.get("conditionId", "")
                    rdm  = p.get("redeemable", False)
                    val  = float(p.get("currentValue", 0))
                    side = p.get("outcome", "")
                    title = p.get("title", "")
                    if rdm or not side or not cid or val < DUST_BET:
                        continue
                    if cid in self.pending or cid in self.pending_redeem:
                        continue
                    # Position exists on-chain but bot has no record — recover it
                    asset = ("BTC" if "Bitcoin" in title else "ETH" if "Ethereum" in title
                             else "SOL" if "Solana" in title else "XRP" if "XRP" in title else "?")
                    # Fetch real end_ts from Gamma API
                    end_ts = 0; start_ts = now_ts - 60; duration = 15
                    token_up = token_down = ""
                    try:
                        mkt_data = await loop.run_in_executor(None, lambda c=cid: _req.get(
                            f"{GAMMA}/markets", params={"conditionId": c}, timeout=8
                        ).json())
                        mkt = mkt_data[0] if isinstance(mkt_data, list) and mkt_data else (
                              mkt_data if isinstance(mkt_data, dict) else {})
                        es = mkt.get("endDate") or mkt.get("end_date", "")
                        ss = mkt.get("eventStartTime") or mkt.get("startDate", "")
                        if es: end_ts   = datetime.fromisoformat(es.replace("Z","+00:00")).timestamp()
                        if ss: start_ts = datetime.fromisoformat(ss.replace("Z","+00:00")).timestamp()
                        slug = mkt.get("seriesSlug") or mkt.get("series_slug", "")
                        if slug in SERIES:
                            duration = SERIES[slug]["duration"]
                            asset    = SERIES[slug]["asset"]
                        toks = mkt.get("clobTokenIds") or []
                        if isinstance(toks, str):
                            try: toks = json.loads(toks)
                            except: toks = []
                        token_up   = toks[0] if len(toks) > 0 else ""
                        token_down = toks[1] if len(toks) > 1 else ""
                    except Exception:
                        pass
                    if end_ts > 0 and end_ts <= now_ts:
                        stored_end = self.pending.get(cid, ({},))[0].get("end_ts", 0)
                        end_ts = stored_end if stored_end > now_ts else now_ts + duration * 60
                    if end_ts == 0:
                        end_ts = now_ts + duration * 60
                    mins_left = (end_ts - now_ts) / 60
                    open_p, _ = await self._get_chainlink_at(asset, start_ts) if start_ts > 0 else (0, "")
                    m_r = {"conditionId": cid, "question": title, "asset": asset,
                           "duration": duration, "end_ts": end_ts, "start_ts": start_ts,
                           "up_price": 0.5, "mins_left": mins_left,
                           "token_up": token_up, "token_down": token_down}
                    t_r = {"side": side, "size": val, "entry": 0.5,
                           "open_price": open_p, "current_price": 0, "true_prob": 0.5,
                           "mkt_price": 0.5, "edge": 0, "mins_left": mins_left,
                           "end_ts": end_ts, "asset": asset, "duration": duration,
                           "token_id": token_up if side == "Up" else token_down,
                           "order_id": "RECOVERED"}
                    self.pending[cid] = (m_r, t_r)
                    self.seen.add(cid)
                    self._save_pending()
                    print(f"{G}[RECOVER] Added to pending: {title[:40]} {side} ${val:.2f} | open={open_p:.2f} | {mins_left:.1f}min left{RS}")

                # 4. Sync trades + win rate from Polymarket activity API (every 5 ticks ~2.5min)
                if tick % 5 == 1:
                    await self._sync_stats_from_api(loop, _req)

            except Exception as e:
                print(f"{Y}[BANK] refresh error: {e}{RS}")

    async def _sync_stats_from_api(self, loop, _req):
        """Sync win/loss stats from Polymarket activity API (on-chain truth).
        BUY = bet placed, REDEEM = win. Also syncs recent_trades deque for last-5 gate."""
        try:
            activity = await loop.run_in_executor(None, lambda: _req.get(
                "https://data-api.polymarket.com/activity",
                params={"user": ADDRESS, "limit": "500"},
                timeout=10
            ).json())
            if not isinstance(activity, list):
                return

            # Group by conditionId, preserving order (API returns newest first)
            by_cid = {}
            order  = []   # conditionIds in newest-first order
            for evt in activity:
                typ = (evt.get("type") or "").upper()
                cid = evt.get("conditionId") or evt.get("market", {}).get("conditionId", "")
                if not cid:
                    continue
                if cid not in by_cid:
                    by_cid[cid] = {"buy": False, "redeem": False}
                    order.append(cid)
                if typ in ("BUY", "TRADE", "PURCHASE"):
                    by_cid[cid]["buy"] = True
                elif typ == "REDEEM":
                    by_cid[cid]["redeem"] = True

            total_bets = sum(1 for d in by_cid.values() if d["buy"])
            total_wins = sum(1 for d in by_cid.values() if d["buy"] and d["redeem"])

            # Last 5 COMPLETED bets from API (skip still-open/settling positions)
            api_last5 = []
            for cid in order:
                if len(api_last5) >= 5:
                    break
                if not by_cid[cid]["buy"]:
                    continue
                if cid in self.pending or cid in self.pending_redeem:
                    continue  # still active — not resolved yet
                api_last5.append(1 if by_cid[cid]["redeem"] else 0)

            if total_bets > 0:
                old_t, old_w = self.total, self.wins
                self.total = total_bets
                self.wins  = total_wins
                # Overwrite the tail of recent_trades with API truth (oldest→newest)
                if api_last5:
                    api_oldest_first = list(reversed(api_last5))
                    cur = list(self.recent_trades)
                    if len(cur) >= len(api_oldest_first):
                        cur[-len(api_oldest_first):] = api_oldest_first
                    else:
                        cur = api_oldest_first
                    self.recent_trades = deque(cur, maxlen=30)
                wr  = f"{total_wins/total_bets*100:.1f}%"
                wr5 = f"{sum(api_last5)/len(api_last5):.0%}" if api_last5 else "–"
                streak = "".join("W" if x else "L" for x in api_last5)
                print(f"{B}[STATS] {total_bets} bets {total_wins} wins ({wr}) | "
                      f"Last{len(api_last5)}={streak} ({wr5}) ← on-chain truth{RS}")
        except Exception as e:
            print(f"{Y}[STATS] activity sync error: {e}{RS}")

    async def _redeemable_scan(self):
        """Every 60s, re-scan Polymarket API for redeemable positions not yet queued.
        Catches winners that weren't on-chain resolved when _sync_redeemable ran at startup."""
        if DRY_RUN or self.w3 is None:
            return
        while True:
            await asyncio.sleep(20)
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
                    self.pending.pop(cid, None)   # remove from pending — _redeem_loop now owns it
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
                    if rdm or not side or not cid:
                        continue
                    self.seen.add(cid)
                    if val < DUST_BET:   # dust — mark seen only, don't track
                        continue
                    if cid in self.pending or cid in self.pending_redeem:
                        continue
                    title = pos.get("title", "")
                    asset = ("BTC" if "Bitcoin" in title else "ETH" if "Ethereum" in title
                             else "SOL" if "Solana" in title else "XRP" if "XRP" in title else "?")
                    # Fetch real end_ts from Gamma API — never use fake timestamps
                    end_ts = 0; start_ts = now - 60; duration = 15
                    token_up = token_down = ""
                    try:
                        mkt_data = await loop.run_in_executor(None, lambda c=cid: _req.get(
                            f"{GAMMA}/markets", params={"conditionId": c}, timeout=8
                        ).json())
                        mkt = mkt_data[0] if isinstance(mkt_data, list) and mkt_data else (
                              mkt_data if isinstance(mkt_data, dict) else {})
                        es = mkt.get("endDate") or mkt.get("end_date", "")
                        ss = mkt.get("eventStartTime") or mkt.get("startDate") or mkt.get("start_date", "")
                        if es: end_ts   = datetime.fromisoformat(es.replace("Z","+00:00")).timestamp()
                        if ss: start_ts = datetime.fromisoformat(ss.replace("Z","+00:00")).timestamp()
                        slug = mkt.get("seriesSlug") or mkt.get("series_slug", "")
                        if slug in SERIES:
                            duration = SERIES[slug]["duration"]
                            asset    = SERIES[slug]["asset"]
                        toks = mkt.get("clobTokenIds") or []
                        if isinstance(toks, str):
                            try: toks = json.loads(toks)
                            except: toks = []
                        token_up   = toks[0] if len(toks) > 0 else ""
                        token_down = toks[1] if len(toks) > 1 else ""
                    except Exception:
                        pass
                    # If already expired, queue for on-chain resolution.
                    # Guard: Gamma date-only endDate → midnight UTC → false expiry.
                    if end_ts > 0 and end_ts <= now:
                        stored_end = self.pending.get(cid, ({},))[0].get("end_ts", 0)
                        if stored_end > now:
                            end_ts = stored_end  # trust stored
                        else:
                            end_ts = now + duration * 60  # estimate; _resolve handles real expiry
                    if end_ts == 0:
                        print(f"{Y}[SYNC] {title[:40]} — no end_ts from Gamma, skipping{RS}")
                        continue
                    mins_left = (end_ts - now) / 60
                    m_r = {"conditionId": cid, "question": title, "asset": asset,
                           "duration": duration, "end_ts": end_ts, "start_ts": start_ts,
                           "up_price": 0.5, "mins_left": mins_left,
                           "token_up": token_up, "token_down": token_down}
                    t_r = {"side": side, "size": val, "entry": 0.5,
                           "open_price": 0, "current_price": 0, "true_prob": 0.5,
                           "mkt_price": 0.5, "edge": 0, "mins_left": mins_left,
                           "end_ts": end_ts, "asset": asset, "duration": duration,
                           "token_id": token_up if side == "Up" else token_down,
                           "order_id": "SYNC-PERIODIC"}
                    # Verify on-chain before adding — don't trust API alone
                    _token_id_str = token_up if side == "Up" else token_down
                    _onchain_bal  = 0
                    if _token_id_str and self.w3:
                        try:
                            _BABI = [{"inputs":[{"name":"owner","type":"address"},{"name":"id","type":"uint256"}],
                                      "name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]
                            _ctf_v = self.w3.eth.contract(
                                address=Web3.to_checksum_address(get_contract_config(CHAIN_ID, neg_risk=False).conditional_tokens),
                                abi=_BABI)
                            _onchain_bal = await loop.run_in_executor(
                                None, lambda ti=int(_token_id_str): _ctf_v.functions.balanceOf(
                                    Web3.to_checksum_address(ADDRESS), ti).call())
                        except Exception:
                            _onchain_bal = -1  # unknown — allow, _verify will catch it
                    if _onchain_bal == 0 and _token_id_str:
                        print(f"{Y}[SYNC] Skip recovery (bal=0 on-chain): {title[:35]} {side}{RS}")
                        continue
                    self.pending[cid] = (m_r, t_r)
                    self.seen.add(cid)
                    added += 1
                    print(f"{Y}[SYNC] Recovered: {title[:40]} {side} ~${val:.2f} | {duration}m ends in {mins_left:.1f}min{RS}")
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
        # Sync stats from API immediately so first status print shows real data
        import requests as _req
        loop = asyncio.get_event_loop()
        await self._sync_stats_from_api(loop, _req)
        async def _guard(name, method):
            """Restart a loop if it crashes — one failure can't kill all loops."""
            while True:
                try:
                    await method()
                    break  # clean exit (e.g. DRY_RUN early return)
                except Exception as e:
                    print(f"{R}[CRASH] {name}: {e} — restarting in 10s{RS}")
                    await asyncio.sleep(10)

        await asyncio.gather(
            _guard("stream_rtds",         self.stream_rtds),
            _guard("vol_loop",            self.vol_loop),
            _guard("scan_loop",           self.scan_loop),
            _guard("_status_loop",        self._status_loop),
            _guard("_refresh_balance",    self._refresh_balance),
            _guard("_redeem_loop",        self._redeem_loop),
            _guard("chainlink_loop",      self.chainlink_loop),
            _guard("_redeemable_scan",    self._redeemable_scan),
            _guard("_position_sync_loop", self._position_sync_loop),
        )


if __name__ == "__main__":
    try:
        asyncio.run(LiveTrader().run())
    except KeyboardInterrupt:
        print(f"\n{Y}[STOP] Log: {LOG_FILE}{RS}")
