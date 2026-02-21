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
DUST_BET       = 5.0      # $5 floor — at 4.55x payout: $5 → $22.75 win
MAX_ABS_BET    = 15.0     # $15 hard ceiling
MAX_BANKROLL_PCT = 0.35   # never risk more than 35% of bankroll on a single bet
MAX_OPEN       = int(os.environ.get("MAX_OPEN", "24"))
MAX_SAME_DIR   = int(os.environ.get("MAX_SAME_DIR", "24"))
TRADE_ALL_MARKETS = os.environ.get("TRADE_ALL_MARKETS", "true").lower() == "true"
MIN_SCORE_GATE = int(os.environ.get("MIN_SCORE_GATE", "0"))
MAX_ENTRY_PRICE = float(os.environ.get("MAX_ENTRY_PRICE", "0.45"))
MIN_PAYOUT_MULT = float(os.environ.get("MIN_PAYOUT_MULT", "2.2"))
MIN_EV_NET = float(os.environ.get("MIN_EV_NET", "0.04"))
FEE_RATE_EST = float(os.environ.get("FEE_RATE_EST", "0.0156"))
HC15_ENABLED = os.environ.get("HC15_ENABLED", "true").lower() == "true"
HC15_MIN_SCORE = int(os.environ.get("HC15_MIN_SCORE", "10"))
HC15_MIN_TRUE_PROB = float(os.environ.get("HC15_MIN_TRUE_PROB", "0.62"))
HC15_MIN_EDGE = float(os.environ.get("HC15_MIN_EDGE", "0.10"))
HC15_TARGET_ENTRY = float(os.environ.get("HC15_TARGET_ENTRY", "0.30"))
HC15_FALLBACK_PCT_LEFT = float(os.environ.get("HC15_FALLBACK_PCT_LEFT", "0.35"))
PULLBACK_LIMIT_ENABLED = os.environ.get("PULLBACK_LIMIT_ENABLED", "true").lower() == "true"
PULLBACK_LIMIT_MIN_PCT_LEFT = float(os.environ.get("PULLBACK_LIMIT_MIN_PCT_LEFT", "0.25"))
FAST_EXEC_ENABLED = os.environ.get("FAST_EXEC_ENABLED", "true").lower() == "true"
FAST_EXEC_SCORE = int(os.environ.get("FAST_EXEC_SCORE", "7"))
FAST_EXEC_EDGE = float(os.environ.get("FAST_EXEC_EDGE", "0.03"))

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
SCAN_INTERVAL  = int(os.environ.get("SCAN_INTERVAL", "2"))
PING_INTERVAL  = int(os.environ.get("PING_INTERVAL", "3"))
STATUS_INTERVAL= int(os.environ.get("STATUS_INTERVAL", "20"))
_DATA_DIR      = os.environ.get("DATA_DIR", os.path.expanduser("~"))
LOG_FILE       = os.path.join(_DATA_DIR, "clawdbot_live_trades.csv")
PENDING_FILE   = os.path.join(_DATA_DIR, "clawdbot_pending.json")
SEEN_FILE      = os.path.join(_DATA_DIR, "clawdbot_seen.json")
STATS_FILE     = os.path.join(_DATA_DIR, "clawdbot_stats.json")
METRICS_FILE   = os.path.join(_DATA_DIR, "clawdbot_onchain_metrics.jsonl")

DRY_RUN   = os.environ.get("DRY_RUN", "true").lower() == "true"
LOG_VERBOSE = os.environ.get("LOG_VERBOSE", "false").lower() == "true"
LOG_SCAN_EVERY_SEC = int(os.environ.get("LOG_SCAN_EVERY_SEC", "30"))
LOG_MARKET_EVERY_SEC = int(os.environ.get("LOG_MARKET_EVERY_SEC", "90"))
LOG_OPEN_WAIT_EVERY_SEC = int(os.environ.get("LOG_OPEN_WAIT_EVERY_SEC", "120"))
LOG_REDEEM_WAIT_EVERY_SEC = int(os.environ.get("LOG_REDEEM_WAIT_EVERY_SEC", "180"))
LOG_MKT_MOVE_THRESHOLD_PCT = float(os.environ.get("LOG_MKT_MOVE_THRESHOLD_PCT", "0.15"))
FORCE_REDEEM_SCAN_SEC = int(os.environ.get("FORCE_REDEEM_SCAN_SEC", "90"))
ENABLE_5M = os.environ.get("ENABLE_5M", "true").lower() == "true"
FIVE_MIN_ASSETS = {
    s.strip().upper() for s in os.environ.get("FIVE_MIN_ASSETS", "BTC,ETH").split(",") if s.strip()
}
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
if ENABLE_5M:
    _FIVE_MIN_SERIES = {
        "btc-up-or-down-5m": {"asset": "BTC", "duration": 5, "id": "10684"},
        # Keep optional assets defined but disabled by default due thinner books.
        "eth-up-or-down-5m": {"asset": "ETH", "duration": 5, "id": "10683"},
    }
    for slug, info in _FIVE_MIN_SERIES.items():
        if info["asset"] in FIVE_MIN_ASSETS:
            SERIES[slug] = info

GAMMA = "https://gamma-api.polymarket.com"
RTDS  = "wss://ws-live-data.polymarket.com"

# Binance symbols per asset (spot + futures share same symbol)
BNB_SYM = {info["asset"]: info["asset"].lower() + "usdt" for info in SERIES.values()}
# e.g. {"BTC": "btcusdt", "ETH": "ethusdt", ...}

G="\033[92m"; R="\033[91m"; Y="\033[93m"; B="\033[94m"; W="\033[97m"; RS="\033[0m"

# ─────────────────────────────────────────────────────────────────────────────

class LiveTrader:
    def __init__(self):
        self.prices      = {}
        self.vols        = {"BTC": 0.65, "ETH": 0.80, "SOL": 1.20, "XRP": 0.90}
        # Binance WS cache — populated by _stream_binance_* loops, read by _binance_* helpers
        self.binance_cache = {
            a: {"depth_bids": [], "depth_asks": [], "klines": [],
                "mark": 0.0, "index": 0.0, "funding": 0.0}
            for a in BNB_SYM
        }
        self.open_prices        = {}   # cid → float price
        self.open_prices_source = {}   # cid → "CL-exact" | "CL-fallback"
        self._mkt_log_ts        = {}   # cid → last [MKT] log time
        self._log_ts            = {}   # throttle map for repetitive logs
        self.asset_cur_open     = {}   # asset → current market open price (for inter-market continuity)
        self.asset_prev_open    = {}   # asset → previous market open price
        self.active_mkts = {}
        self.pending         = {}   # cid → (m, trade)
        self.pending_redeem  = {}   # cid → (side, asset)  — waiting on-chain resolution
        self.redeemed_cids   = set()  # cids already processed — prevents _redeemable_scan re-queueing
        self.token_prices    = {}     # token_id → real-time price from RTDS market stream
        self._rtds_ws        = None   # live WebSocket handle for dynamic subscriptions
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
        self._redeem_tx_lock    = asyncio.Lock()  # serialize redeem txs to avoid nonce clashes
        self.peak_bankroll      = BANKROLL           # track peak for drawdown guard
        self.consec_losses      = 0                  # consecutive resolved losses counter
        # ── Mathematical signal state (EMA + Kalman) ──────────────────────────
        _EMA_HLS = (5, 15, 30, 60, 120)
        self.emas    = {a: {hl: 0.0 for hl in _EMA_HLS} for a in ["BTC","ETH","SOL","XRP"]}
        self._ema_ts = {a: 0.0 for a in ["BTC","ETH","SOL","XRP"]}
        # Kalman: constant-velocity model; state = [price, velocity]; P = 2×2 cov
        self.kalman  = {a: {"pos":0.0,"vel":0.0,"p00":1.0,"p01":0.0,"p11":1.0,"rdy":False}
                        for a in ["BTC","ETH","SOL","XRP"]}
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

    def _should_log(self, key: str, every_sec: float) -> bool:
        now = _time.time()
        last = self._log_ts.get(key, 0.0)
        if now - last >= every_sec:
            self._log_ts[key] = now
            return True
        return False

    # ── Mathematical signal helpers ───────────────────────────────────────────

    def _tick_update(self, asset: str, price: float, ts: float) -> None:
        """Called on every RTDS price tick — updates time-based EMAs and Kalman filter."""
        import math as _m
        ema_dict = self.emas.get(asset)
        if ema_dict is None:
            return
        dt = ts - self._ema_ts.get(asset, ts)
        self._ema_ts[asset] = ts
        for hl, prev in list(ema_dict.items()):
            if prev == 0.0:
                ema_dict[hl] = price   # seed on first tick
            else:
                alpha = 1.0 - _m.exp(-dt / hl) if dt > 0 else 0.0
                ema_dict[hl] = prev + alpha * (price - prev)
        # Constant-velocity Kalman filter (predict + update)
        k = self.kalman.get(asset)
        if k is None:
            return
        if not k["rdy"]:
            k["pos"] = price; k["vel"] = 0.0
            k["p00"] = 1.0;   k["p01"] = 0.0; k["p11"] = 0.01
            k["rdy"] = True
            return
        # Process noise (tune to asset volatility)
        q_pos = (self.vols.get(asset, 0.7) * price / _m.sqrt(252*24*3600)) ** 2
        q_vel = q_pos * 0.01
        r_obs = (self.vols.get(asset, 0.7) * price * 0.001) ** 2
        # Predict
        pos_p = k["pos"] + k["vel"] * dt
        vel_p = k["vel"]
        p00_p = k["p00"] + dt*(k["p01"]+k["p10"] if "p10" in k else k["p01"]) + dt*dt*k["p11"] + q_pos
        p01_p = k["p01"] + dt * k["p11"]
        p11_p = k["p11"] + q_vel
        # Update
        innov  = price - pos_p
        s_inv  = 1.0 / (p00_p + r_obs)
        k0     = p00_p * s_inv
        k1     = p01_p * s_inv
        k["pos"] = pos_p + k0 * innov
        k["vel"] = vel_p + k1 * innov
        k["p00"] = (1.0 - k0) * p00_p
        k["p01"] = (1.0 - k0) * p01_p
        k["p11"] = p11_p - k1 * p01_p

    def _kalman_vel_prob(self, asset: str) -> float:
        """P(Up) from Kalman-estimated velocity; 0.5 if filter not yet ready."""
        import math as _m
        k = self.kalman.get(asset, {})
        if not k.get("rdy") or k.get("pos", 0) == 0:
            return 0.5
        price   = k["pos"]
        vel     = k["vel"]
        per_sec = max(self.vols.get(asset, 0.7) * price / _m.sqrt(252*24*3600), 1e-10)
        z       = vel / per_sec
        return float(norm.cdf(z * 10))

    def _ob_depth_weighted(self, asset: str) -> float:
        """Depth-weighted OB imbalance: 1/rank weighting on top-20 levels."""
        c    = self.binance_cache.get(asset, {})
        bids = c.get("depth_bids", [])
        asks = c.get("depth_asks", [])
        bid_w = sum(float(b[1]) / (i + 1) for i, b in enumerate(bids[:20]))
        ask_w = sum(float(a[1]) / (i + 1) for i, a in enumerate(asks[:20]))
        if bid_w + ask_w == 0:
            return 0.0
        return (bid_w - ask_w) / (bid_w + ask_w)

    def _autocorr_regime(self, asset: str, lags: int = 1) -> float:
        """Lag-1 autocorrelation of 1m returns from klines cache.
        Positive = trending; negative = mean-reverting."""
        klines = self.binance_cache.get(asset, {}).get("klines", [])
        closes = [float(k[4]) for k in klines[-32:] if float(k[4]) > 0]
        if len(closes) < 5:
            return 0.0
        rets = [closes[i] / closes[i-1] - 1 for i in range(1, len(closes))]
        if len(rets) < 4:
            return 0.0
        mu   = sum(rets) / len(rets)
        dev  = [r - mu for r in rets]
        num  = sum(dev[i] * dev[i - lags] for i in range(lags, len(dev)))
        den  = sum(d * d for d in dev)
        return num / den if den > 0 else 0.0

    def _variance_ratio(self, asset: str, q: int = 5) -> float:
        """Lo-MacKinlay variance ratio test (q=5). VR>1=trending, VR<1=mean-reverting."""
        klines = self.binance_cache.get(asset, {}).get("klines", [])
        closes = [float(k[4]) for k in klines if float(k[4]) > 0]
        n      = len(closes)
        if n < q * 4 + 2:
            return 1.0
        log_p = [math.log(closes[i] / closes[i-1]) for i in range(1, n)]
        mu    = sum(log_p) / len(log_p)
        var1  = sum((r - mu) ** 2 for r in log_p) / (len(log_p) - 1)
        if var1 == 0:
            return 1.0
        # q-period returns
        q_rets = [math.log(closes[i] / closes[i-q]) for i in range(q, n)]
        varq   = sum((r - q * mu) ** 2 for r in q_rets) / ((len(q_rets) - 1) * q)
        return varq / var1

    def _jump_detect(self, asset: str) -> tuple:
        """Z-score of last 10s move vs baseline tick vol.
        Returns (is_jump: bool, direction: str|None, z_score: float)."""
        hist = self.price_history.get(asset)
        if not hist or len(hist) < 5:
            return False, None, 0.0
        now    = _time.time()
        recent = [(ts, p) for ts, p in hist if ts >= now - 10]
        base   = [(ts, p) for ts, p in hist if now - 60 <= ts < now - 10]
        if len(recent) < 2 or len(base) < 5:
            return False, None, 0.0
        p0, p1 = recent[0][1], recent[-1][1]
        move_10s = (p1 - p0) / p0 if p0 > 0 else 0.0
        # baseline vol from tick-to-tick moves
        bt = list(base)
        tick_moves = [abs(bt[i][1]/bt[i-1][1]-1) for i in range(1, len(bt)) if bt[i-1][1] > 0]
        if not tick_moves:
            return False, None, 0.0
        sigma = (sum(m*m for m in tick_moves) / len(tick_moves)) ** 0.5
        if sigma == 0:
            return False, None, 0.0
        z = move_10s / sigma
        if abs(z) > 3.5:
            return True, "Up" if z > 0 else "Down", z
        return False, None, z

    def _btc_lead_signal(self, asset: str) -> float:
        """P(Up) for `asset` based on BTC's 30-60s lagged move.
        For BTC itself returns 0.5 (no self-lead)."""
        if asset == "BTC":
            return 0.5
        hist_btc = self.price_history.get("BTC")
        if not hist_btc or len(hist_btc) < 5:
            return 0.5
        now = _time.time()
        # BTC move from 60s ago to 30s ago (lagged window)
        p60  = [(ts, p) for ts, p in hist_btc if now - 65 <= ts <= now - 55]
        p30  = [(ts, p) for ts, p in hist_btc if now - 35 <= ts <= now - 25]
        if not p60 or not p30:
            return 0.5
        btc_lag_move = (p30[-1][1] - p60[-1][1]) / p60[-1][1] if p60[-1][1] > 0 else 0.0
        vol_btc = self.vols.get("BTC", 0.65)
        vol_t   = vol_btc * math.sqrt(30 / (252 * 24 * 3600))
        if vol_t == 0:
            return 0.5
        corr = 0.75   # empirical BTC→altcoin lag correlation
        return float(norm.cdf(btc_lag_move / vol_t * corr))

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

    def _log_onchain_event(self, event_type: str, cid: str, payload: dict):
        """Append a structured event for on-chain-first analytics/backtesting."""
        rec = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "event": event_type,
            "condition_id": cid,
            **payload,
        }
        try:
            with open(METRICS_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, separators=(",", ":")) + "\n")
        except Exception:
            pass

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
            open_p     = self.open_prices.get(cid, 0)
            src        = self.open_prices_source.get(cid, "?")
            # If no open price yet, try Polymarket API inline (best effort)
            if open_p <= 0:
                start_ts_m = m.get("start_ts", 0)
                end_ts_m   = m.get("end_ts", 0)
                if start_ts_m > 0 and end_ts_m > 0:
                    try:
                        import requests as _rq
                        from datetime import datetime as _dt, timezone as _tz
                        st = _dt.fromtimestamp(start_ts_m, tz=_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                        et = _dt.fromtimestamp(end_ts_m,   tz=_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                        _r = _rq.get("https://polymarket.com/api/crypto/crypto-price",
                                     params={"symbol": asset, "eventStartTime": st,
                                             "variant": "fifteen", "endDate": et}, timeout=3)
                        _p = _r.json().get("openPrice") or 0
                        if _p:
                            open_p = float(_p)
                            self.open_prices[cid]        = open_p
                            self.open_prices_source[cid] = "PM"
                            src = "PM"
                    except Exception:
                        pass
            # Use Chainlink (resolution source) for win/loss; fall back to RTDS if unavailable
            cl_p       = self.cl_prices.get(asset, 0)
            cur_p      = cl_p if cl_p > 0 else self.prices.get(asset, 0)
            end_ts     = t.get("end_ts", 0)
            mins_left  = max(0, (end_ts - now_ts) / 60)
            title      = m.get("question", "")[:38]
            if open_p > 0 and src == "?":
                src = "CL" if cl_p > 0 else "RTDS"
            if open_p > 0 and cur_p > 0:
                winning    = (side == "Up" and cur_p > open_p) or (side == "Down" and cur_p < open_p)
                move_pct   = (cur_p - open_p) / open_p * 100
                c          = G if winning else R
                status_str = "LEADING" if winning else "TRAILING"
                payout_est = size / t.get("entry", 0.5) if winning else 0
                move_str   = f"({move_pct:+.2f}%)"
            else:
                c          = Y
                status_str = "UNSETTLED"
                payout_est = 0
                move_str   = "(no ref)"
            tok_price = t.get("entry", 0)
            tok_str   = f"@{tok_price*100:.0f}¢→{(1/tok_price):.2f}x" if tok_price > 0 else "@?¢"
            print(f"  {c}[{status_str}]{RS} {asset} {side} | {title} | "
                  f"beat={open_p:.4f}[{src}] now={cur_p:.4f} {move_str} | "
                  f"bet=${size:.2f} {tok_str} est=${payout_est:.2f} | {mins_left:.1f}min left")
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
                    self._rtds_ws = ws   # expose for dynamic market subscriptions
                    print(f"{G}[RTDS] Live — streaming BTC/ETH/SOL/XRP{RS}")

                    # Subscribe to active market token prices for instant up_price updates
                    for cid, m in list(self.active_mkts.items()):
                        for tid in [m.get("token_up",""), m.get("token_down","")]:
                            if tid:
                                try:
                                    await ws.send(json.dumps({
                                        "action": "subscribe",
                                        "subscriptions": [{"asset_id": tid, "type": "market"}]
                                    }))
                                except Exception:
                                    pass

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

                        # ── Market token price update (instant up_price refresh) ──
                        if msg.get("event_type") == "price_change" or msg.get("topic") == "market":
                            pl = msg.get("payload", {}) or msg
                            tid   = pl.get("asset_id") or pl.get("token_id","")
                            price = float(pl.get("price") or pl.get("mid_price") or 0)
                            if tid and price > 0:
                                self.token_prices[tid] = price
                                # Update active_mkts up_price in real time
                                for cid, m in list(self.active_mkts.items()):
                                    if m.get("token_up") == tid:
                                        m["up_price"] = price
                                    elif m.get("token_down") == tid:
                                        m["up_price"] = 1 - price

                        if msg.get("topic") != "crypto_prices": continue
                        p   = msg.get("payload", {})
                        sym = p.get("symbol", "").lower()
                        val = float(p.get("value", 0) or 0)
                        if val == 0: continue
                        MAP = {"btcusdt":"BTC","ethusdt":"ETH","solusdt":"SOL","xrpusdt":"XRP"}
                        asset = MAP.get(sym)
                        if asset:
                            self.prices[asset] = val
                            _now_ts = _time.time()
                            self.price_history[asset].append((_now_ts, val))
                            self._tick_update(asset, val, _now_ts)
                            # Event-driven: evaluate unseen markets immediately on price tick
                            now_t = _time.time()
                            for cid, m in list(self.active_mkts.items()):
                                if m.get("asset") != asset: continue
                                if cid in self.seen: continue
                                if cid not in self.open_prices: continue
                                if now_t - self._last_eval_time.get(cid, 0) < 0.5: continue
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
        """Parkinson OHLC vol from WS klines cache — no HTTP, updated every 60s."""
        while True:
            for asset in list(BNB_SYM.keys()):
                klines = self.binance_cache.get(asset, {}).get("klines", [])
                if len(klines) < 5:
                    continue
                try:
                    recent = klines[-30:]
                    log_hl_sq = [
                        math.log(float(k[2]) / float(k[3])) ** 2
                        for k in recent
                        if float(k[3]) > 0 and float(k[2]) > float(k[3])
                    ]
                    if log_hl_sq:
                        park_var = sum(log_hl_sq) / (4.0 * math.log(2) * len(log_hl_sq))
                        ann_vol  = math.sqrt(park_var * 252 * 24 * 60)
                        self.vols[asset] = max(0.10, min(5.0, ann_vol))
                except Exception:
                    pass
            await asyncio.sleep(60)

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
    async def _fetch_pm_book_safe(self, token_id: str):
        """Fetch Polymarket CLOB book; return (best_bid, best_ask, tick) or None."""
        if not token_id:
            return None
        loop = asyncio.get_event_loop()
        try:
            book     = await loop.run_in_executor(None, lambda: self.clob.get_order_book(token_id))
            tick     = float(book.tick_size or "0.01")
            asks     = sorted(book.asks, key=lambda x: float(x.price)) if book.asks else []
            bids     = sorted(book.bids, key=lambda x: float(x.price), reverse=True) if book.bids else []
            if not asks:
                return None
            best_ask = float(asks[0].price)
            best_bid = float(bids[0].price) if bids else best_ask - 0.10
            return (best_bid, best_ask, tick)
        except Exception:
            return None

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
        # Pre-fetch likely token book (cheap-side = token_up iff up_price≤0.50)
        prefetch_token = m.get("token_up", "") if up_price <= 0.50 else m.get("token_down", "")

        current = self.prices.get(asset, 0) or self.cl_prices.get(asset, 0)
        if current == 0:
            return None

        open_price = self.open_prices.get(cid)
        if not open_price:
            if LOG_VERBOSE or self._should_log(f"wait-open:{cid}", LOG_OPEN_WAIT_EVERY_SEC):
                print(f"{Y}[WAIT] {label} → waiting for first CL round{RS}")
            return None

        src = self.open_prices_source.get(cid, "?")
        src_tag = f"[{src}]"

        # Timing gate: only block final 2 min (price already moved fully, no edge).
        total_life    = m["end_ts"] - m["start_ts"]
        pct_remaining = (mins_left * 60) / total_life if total_life > 0 else 0
        if pct_remaining < 0.15:
            return None   # last ~2 min — too late to fill at good price

        # Previous window direction from CL prices — the mapleghost signal.
        # At window open AMM applies mean-reversion discount (prices 8-40¢ for continuation)
        # but on-chain data shows 87.7% continuation. Exploiting this is the key edge.
        prev_open     = self.asset_prev_open.get(asset, 0)
        prev_win_move = abs((open_price - prev_open) / prev_open) if prev_open > 0 and open_price > 0 else 0.0
        prev_win_dir  = None
        if prev_open > 0 and open_price > 0:
            diff = (open_price - prev_open) / prev_open
            if   diff >  0.0002: prev_win_dir = "Up"
            elif diff < -0.0002: prev_win_dir = "Down"

        move_pct = abs(current - open_price) / open_price if open_price > 0 else 0
        move_str = f"{(current-open_price)/open_price:+.3%}"

        # ── Multi-signal Score ────────────────────────────────────────────────
        # Resolution rule: ANY price above open = Up wins (even $0.001).
        # Direction from price if moved; from momentum consensus if flat.
        # Max possible: 2+3+4+1+3+3+2+1 = 19 pts
        score = 0

        # Compute momentum — EMA-based (O(1) from cache) + Kalman velocity signal
        mom_5s   = self._momentum_prob(asset, seconds=5)
        mom_30s  = self._momentum_prob(asset, seconds=30)
        mom_180s = self._momentum_prob(asset, seconds=180)
        mom_kal  = self._kalman_vel_prob(asset)
        th_up, th_dn = 0.53, 0.47
        tf_up_votes = sum([mom_5s > th_up, mom_30s > th_up, mom_180s > th_up, mom_kal > th_up])
        tf_dn_votes = sum([mom_5s < th_dn, mom_30s < th_dn, mom_180s < th_dn, mom_kal < th_dn])

        # Chainlink current — the resolution oracle
        cl_now = self.cl_prices.get(asset, 0)
        cl_updated = self.cl_updated.get(asset, 0)
        cl_age_s = (_time.time() - cl_updated) if cl_updated else None
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
            return None   # no clear signal — don't guess

        is_up    = (direction == "Up")
        tf_votes = tf_up_votes if is_up else tf_dn_votes
        very_strong_mom = (tf_votes >= 3)

        is_early_continuation = (prev_win_dir == direction and pct_remaining > 0.85)

        # Entry timing (0-2 pts) — earlier = AMM hasn't repriced yet = better odds
        if   pct_remaining >= 0.85: score += 2   # first 2.25 min of 15-min market
        elif pct_remaining >= 0.70: score += 1   # first 4.5 min

        # Move size (0-3 pts) — price confirmation bonus; not a gate
        if   move_pct >= 0.0020: score += 3
        elif move_pct >= 0.0012: score += 2
        elif move_pct >= 0.0005: score += 1
        # flat/tiny move → +0 pts (still tradeable if momentum/taker confirm)

        # Multi-TF momentum + Kalman (0-5 pts; signals: 5s/30s/180s EMA + Kalman velocity)
        if   tf_votes == 4: score += 5
        elif tf_votes == 3: score += 4
        elif tf_votes == 2: score += 2

        # Chainlink direction agreement (+1 agree / −3 disagree)
        # CL is the resolution oracle — disagreement is a major red flag
        cl_agree = True
        if cl_now > 0 and open_price > 0:
            if (is_up) != (cl_now > open_price):
                cl_agree = False
        if cl_agree:  score += 1
        else:         score -= 3

        # On-chain-first confidence: prefer authoritative open-price source + fresh oracle.
        open_src = self.open_prices_source.get(cid, "?")
        src_conf = 1.0 if open_src == "PM" else (0.9 if open_src == "CL-exact" else 0.6)
        onchain_adj = 0
        if open_src == "PM":
            onchain_adj += 1
        elif open_src not in ("CL-exact", "PM"):
            onchain_adj -= 1
        if cl_age_s is None:
            onchain_adj -= 1
        elif cl_age_s > 90:
            return None
        elif cl_age_s > 45:
            onchain_adj -= 2
        score += onchain_adj

        # ── Binance signals from WS cache (instant) + PM book fetch (async ~36ms) ─
        ob_imbalance               = self._binance_imbalance(asset)
        (taker_ratio, vol_ratio)   = self._binance_taker_flow(asset)
        (perp_basis, funding_rate) = self._binance_perp_signals(asset)
        (vwap_dev, vol_mult)       = self._binance_window_stats(asset, m["start_ts"])
        _pm_book = await self._fetch_pm_book_safe(prefetch_token)

        # Additional instant signals from Binance cache (zero latency)
        dw_ob     = self._ob_depth_weighted(asset)
        autocorr  = self._autocorr_regime(asset)
        vr_ratio  = self._variance_ratio(asset)
        is_jump, jump_dir, jump_z = self._jump_detect(asset)
        btc_lead_p = self._btc_lead_signal(asset)

        # Jump detection: sudden move against our direction = hard abort
        if is_jump and jump_dir is not None and jump_dir != direction:
            return None
        if is_jump and jump_dir == direction:
            score += 2   # jump confirms direction — strong momentum signal

        # Order book imbalance — depth-weighted 1/rank (more reliable than flat sum)
        ob_sig = dw_ob if is_up else -dw_ob    # positive = OB confirms direction
        if ob_sig < -0.40:
            return None    # extreme contra OB — hard block
        if   ob_sig > 0.25: score += 3
        elif ob_sig > 0.10: score += 2
        elif ob_sig > -0.10: score += 1
        else:               score -= 1
        imbalance_confirms = ob_sig > 0.10

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
        open_price_disp = open_price
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

        # BTC lead signal for non-BTC assets — BTC lagged move predicts altcoins (0–2 pts)
        if asset != "BTC":
            if   (is_up and btc_lead_p > 0.60) or (not is_up and btc_lead_p < 0.40): score += 2
            elif (is_up and btc_lead_p > 0.55) or (not is_up and btc_lead_p < 0.45): score += 1
            elif (is_up and btc_lead_p < 0.40) or (not is_up and btc_lead_p > 0.60): score -= 1

        # Previous window continuation — mapleghost strategy (0–5 pts or −2 pts)
        # AMM prices continuation at 8–40¢ (mean-reversion model) but true rate is 87.7%.
        # Early entry (first 3 min of window) exploits the biggest mispricing.
        if prev_win_dir is not None:
            if prev_win_dir == direction:
                if   pct_remaining > 0.85: score += 5  # first ~90s: AMM hasn't repriced yet
                elif pct_remaining > 0.80: score += 4  # still very early
                elif pct_remaining > 0.70: score += 2  # early but some repricing happened
                else:                      score += 1  # later: normal continuation bonus
            else:
                score -= 2  # betting against continuation direction — risky

        # Autocorr + Variance Ratio regime: trending boosts momentum confidence
        if vr_ratio > 1.05 and autocorr > 0.05:
            score += 1       # trending regime — momentum more reliable
            regime_mult = 1.15
        elif vr_ratio < 0.95 and autocorr < -0.05:
            score -= 1       # mean-reverting — momentum less reliable
            regime_mult = 0.85
        else:
            regime_mult = 1.0

        # Log-likelihood true_prob — Bayesian combination of independent signals
        sigma_15m = self.vols.get(asset, 0.70) * (15 / (252 * 390)) ** 0.5
        llr = 0.0
        # 1. Price displacement z-score
        if open_price > 0 and sigma_15m > 0:
            llr += (current - open_price) / open_price / sigma_15m * 1.5
        # 2. Short vs long EMA cross
        ema5  = self.emas.get(asset, {}).get(5, current)
        ema60 = self.emas.get(asset, {}).get(60, current)
        if ema60 > 0:
            llr += (ema5 / ema60 - 1.0) * 300.0
        # 3. Kalman velocity
        k = self.kalman.get(asset, {})
        if k.get("rdy"):
            per_sec_vol = max(self.vols.get(asset, 0.7) / math.sqrt(252 * 24 * 3600), 1e-8)
            llr += k["vel"] / per_sec_vol * 0.4
        # 4. Depth-weighted OB imbalance
        llr += dw_ob * 2.5
        # 5. Taker buy/sell flow
        llr += (taker_ratio - 0.5) * 5.0
        # 6. Perp basis
        if abs(perp_basis) > 1e-7:
            llr += math.copysign(min(abs(perp_basis) * 1000.0, 1.5), perp_basis)
        # 7. Chainlink oracle agreement
        if cl_agree:  llr += 0.4
        else:         llr -= 1.0
        # 8. BTC lead for altcoins
        if asset != "BTC":
            llr += (btc_lead_p - 0.5) * 3.0
        # 9. Regime scale
        llr *= regime_mult
        # Sigmoid → prob_up
        p_up_ll   = 1.0 / (1.0 + math.exp(-max(-6.0, min(6.0, llr))))
        bias_up   = self._direction_bias(asset, "Up")
        bias_down = self._direction_bias(asset, "Down")
        prob_up   = max(0.05, min(0.95, p_up_ll + bias_up))
        prob_down = max(0.05, min(0.95, 1.0 - p_up_ll + bias_down))

        # Early continuation prior: at window open BS/momentum ≈ 0.50 (no current-window move yet).
        # Apply 87.7% continuation prior: prior scales 70–80% with prev move size.
        # This is why mapleghost can buy at 15¢ — true prob is 75%, not 50%.
        if prev_win_dir == direction and pct_remaining > 0.80:
            prior = min(0.80, 0.70 + prev_win_move * 20)   # 70–80% depending on prev move size
            if direction == "Up":
                prob_up   = max(prob_up, prior)
                prob_down = 1 - prob_up
            else:
                prob_down = max(prob_down, prior)
                prob_up   = 1 - prob_down

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

        # Bet the signal direction — win rate drives P&L, not just payout
        entry = up_price if side == "Up" else (1 - up_price)

        # ── Score gate ────────────────────────────────────────────────────────
        if score < MIN_SCORE_GATE:
            return None

        token_id = m["token_up"] if side == "Up" else m["token_down"]
        if not token_id:
            return None
        pm_book_data = _pm_book if token_id == prefetch_token else None

        # ── Live CLOB price (more accurate than Gamma up_price) ──────────────
        live_entry = entry
        if pm_book_data is not None:
            _, clob_ask, _ = pm_book_data
            live_entry = clob_ask

        # ── Entry strategy ────────────────────────────────────────────────────
        # Trade every eligible market while still preferring higher-payout entries.
        use_limit = False
        # High-conviction 15m mode: target lower cents early for better payout.
        hc15 = (
            HC15_ENABLED and duration == 15 and
            score >= HC15_MIN_SCORE and
            true_prob >= HC15_MIN_TRUE_PROB and
            edge >= HC15_MIN_EDGE
        )
        if hc15 and pct_remaining > HC15_FALLBACK_PCT_LEFT and live_entry > HC15_TARGET_ENTRY:
            use_limit = True
            entry = min(HC15_TARGET_ENTRY, MAX_ENTRY_PRICE)
        else:
            if live_entry <= MAX_ENTRY_PRICE:
                entry = live_entry
            elif PULLBACK_LIMIT_ENABLED and pct_remaining >= PULLBACK_LIMIT_MIN_PCT_LEFT:
                # Don't miss good-payout setups: park a pullback limit at max acceptable entry.
                use_limit = True
                entry = MAX_ENTRY_PRICE
            else:
                if LOG_VERBOSE:
                    print(f"{Y}[SKIP] {asset} {side} entry={live_entry:.3f} > max_entry={MAX_ENTRY_PRICE:.2f}{RS}")
                return None

        payout_mult = 1.0 / max(entry, 1e-9)
        if payout_mult < MIN_PAYOUT_MULT:
            if LOG_VERBOSE:
                print(f"{Y}[SKIP] {asset} {side} payout={payout_mult:.2f}x < min={MIN_PAYOUT_MULT:.2f}x{RS}")
            return None
        ev_net = (true_prob / max(entry, 1e-9)) - 1.0 - FEE_RATE_EST
        if ev_net < MIN_EV_NET:
            if LOG_VERBOSE:
                print(f"{Y}[SKIP] {asset} {side} ev_net={ev_net:.3f} < min={MIN_EV_NET:.3f}{RS}")
            return None

        # ── ENTRY PRICE TIERS ─────────────────────────────────────────────────
        # Higher payout (cheaper tokens) gets larger Kelly fraction.
        # Expensive tokens still traded but with smaller exposure.
        if entry <= 0.20:
            if   score >= 12: kelly_frac, bankroll_pct = 0.55, 0.30
            elif score >= 8:  kelly_frac, bankroll_pct = 0.45, 0.22
            else:             kelly_frac, bankroll_pct = 0.30, 0.15
        elif entry <= 0.30:
            if   score >= 12: kelly_frac, bankroll_pct = 0.40, 0.20
            elif score >= 8:  kelly_frac, bankroll_pct = 0.28, 0.14
            else:             kelly_frac, bankroll_pct = 0.18, 0.09
        elif entry <= 0.40:
            if   score >= 12: kelly_frac, bankroll_pct = 0.25, 0.12
            elif score >= 8:  kelly_frac, bankroll_pct = 0.15, 0.08
            else:             kelly_frac, bankroll_pct = 0.10, 0.05
        elif entry <= 0.55:
            if   score >= 12: kelly_frac, bankroll_pct = 0.08, 0.04
            elif score >= 8:  kelly_frac, bankroll_pct = 0.06, 0.03
            else:             kelly_frac, bankroll_pct = 0.05, 0.025
        else:  # 0.55–0.85
            if   score >= 12: kelly_frac, bankroll_pct = 0.05, 0.025
            elif score >= 8:  kelly_frac, bankroll_pct = 0.04, 0.02
            else:             kelly_frac, bankroll_pct = 0.03, 0.015

        wr_scale   = self._wr_bet_scale()
        raw_size   = self._kelly_size(true_prob, entry, kelly_frac)
        max_single = min(100.0, self.bankroll * bankroll_pct)
        abs_cap    = max_single * 1.5 if score >= 12 and entry <= 0.20 else max_single
        size       = max(DUST_BET, round(min(abs_cap, raw_size * vol_mult * wr_scale), 2))

        # Immediate fills: FOK on strong signal, GTC limit otherwise
        # Limit orders (use_limit=True) are always GTC — force_taker stays False
        force_taker = (not use_limit) and (
            (score >= 12 and very_strong_mom and imbalance_confirms and move_pct > 0.0015) or
            (score >= 12 and is_early_continuation)
        )
        if FAST_EXEC_ENABLED and (not use_limit):
            if score >= FAST_EXEC_SCORE and edge >= FAST_EXEC_EDGE and entry <= MAX_ENTRY_PRICE:
                force_taker = True

        return {
            "cid": cid, "m": m, "score": score,
            "side": side, "entry": entry, "size": size, "token_id": token_id,
            "true_prob": true_prob, "cl_agree": cl_agree, "min_edge": min_edge,
            "force_taker": force_taker, "edge": edge,
            "label": label, "asset": asset, "duration": duration,
            "open_price": open_price, "current": current, "move_str": move_str,
            "src_tag": src_tag, "bs_prob": true_prob, "mom_prob": true_prob,
            "up_price": up_price, "ob_imbalance": ob_imbalance,
            "imbalance_confirms": imbalance_confirms, "tf_votes": tf_votes,
            "very_strong_mom": very_strong_mom, "taker_ratio": taker_ratio,
            "vol_ratio": vol_ratio, "pct_remaining": pct_remaining, "mins_left": mins_left,
            "perp_basis": perp_basis, "funding_rate": funding_rate,
            "vwap_dev": vwap_dev, "vol_mult": vol_mult, "cross_count": cross_count,
            "prev_win_dir": prev_win_dir, "prev_win_move": prev_win_move,
            "is_early_continuation": is_early_continuation,
            "pm_book_data": pm_book_data, "use_limit": use_limit,
            "hc15_mode": hc15,
            "open_price_source": open_src, "chainlink_age_s": cl_age_s,
            "onchain_score_adj": onchain_adj, "source_confidence": src_conf,
            "oracle_gap_bps": ((self.prices.get(asset, 0) - cl_now) / cl_now * 10000.0)
                              if self.prices.get(asset, 0) > 0 and cl_now > 0 else 0.0,
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
        chain_str   = f" cl_age={sig.get('chainlink_age_s', 0) if sig.get('chainlink_age_s') is not None else -1:.1f}s src={sig.get('open_price_source','?')}"
        cont_str    = (f" {G}[CONT {sig['prev_win_dir']} {sig['prev_win_move']*100:.2f}%]{RS}"
                       if sig.get("is_early_continuation") else "")
        tag = f"{G}[CONT-ENTRY]{RS}" if sig.get("is_early_continuation") else f"{G}[EDGE]{RS}"
        if LOG_VERBOSE:
            hc_tag = f" {B}[HC15]{RS}" if sig.get("hc15_mode") else ""
            print(f"{tag} {sig['label']} → {sig['side']} | score={score} {score_stars} | "
                  f"beat=${sig['open_price']:,.2f} {sig['src_tag']} now=${sig['current']:,.2f} "
                  f"move={sig['move_str']}{prev_str} pct={sig['pct_remaining']:.0%} | "
                  f"bs={sig['bs_prob']:.3f} mom={sig['mom_prob']:.3f} prob={sig['true_prob']:.3f} "
                  f"mkt={sig['up_price']:.3f} edge={sig['edge']:.3f} "
                  f"@{sig['entry']*100:.0f}¢→{(1/sig['entry']):.2f}x ${sig['size']:.2f}"
                  f"{agree_str}{ob_str}{tf_str} tk={sig['taker_ratio']:.2f} vol={sig['vol_ratio']:.1f}x"
                  f"{perp_str}{vwap_str}{cross_str} {chain_str}{cont_str}{hc_tag}{RS}")
        else:
            hc_tag = " hc15" if sig.get("hc15_mode") else ""
            print(f"{tag} {sig['asset']} {sig['duration']}m {sig['side']} | "
                  f"score={score} edge={sig['edge']:+.3f} size=${sig['size']:.2f} "
                  f"entry={sig['entry']:.3f} src={sig.get('open_price_source','?')} "
                  f"cl_age={sig.get('chainlink_age_s', -1):.0f}s{hc_tag}{agree_str}{RS}")

        self.seen.add(cid)
        self._save_seen()
        order_id = await self._place_order(
            sig["token_id"], sig["side"], sig["entry"], sig["size"],
            sig["asset"], sig["duration"], sig["mins_left"],
            sig["true_prob"], sig["cl_agree"],
            min_edge_req=sig["min_edge"], force_taker=sig["force_taker"],
            score=sig["score"], pm_book_data=sig.get("pm_book_data"),
            use_limit=sig.get("use_limit", False)
        )
        m = sig["m"]
        trade = {
            "side": sig["side"], "size": sig["size"], "entry": sig["entry"],
            "open_price": sig["open_price"], "current_price": sig["current"],
            "true_prob": sig["true_prob"], "mkt_price": sig["up_price"],
            "edge": round(sig["edge"], 4), "mins_left": sig["mins_left"],
            "end_ts": m["end_ts"], "asset": sig["asset"], "duration": sig["duration"],
            "token_id": sig["token_id"], "order_id": order_id or "",
            "score": sig["score"], "cl_agree": sig["cl_agree"],
            "open_price_source": sig.get("open_price_source", "?"),
            "chainlink_age_s": sig.get("chainlink_age_s"),
            "onchain_score_adj": sig.get("onchain_score_adj", 0),
            "source_confidence": sig.get("source_confidence", 0.0),
            "oracle_gap_bps": sig.get("oracle_gap_bps", 0.0),
        }
        self._log_onchain_event("ENTRY", cid, {
            "asset": sig["asset"],
            "side": sig["side"],
            "score": sig["score"],
            "size_usdc": sig["size"],
            "entry_price": sig["entry"],
            "edge": round(sig["edge"], 4),
            "true_prob": round(sig["true_prob"], 4),
            "cl_agree": bool(sig["cl_agree"]),
            "open_price_source": sig.get("open_price_source", "?"),
            "chainlink_age_s": sig.get("chainlink_age_s"),
            "onchain_score_adj": sig.get("onchain_score_adj", 0),
            "source_confidence": sig.get("source_confidence", 0.0),
            "oracle_gap_bps": sig.get("oracle_gap_bps", 0.0),
            "placed": bool(order_id),
            "order_id": order_id or "",
        })
        if order_id:
            self.pending[cid] = (m, trade)
            self._save_pending()
            self._log(m, trade)

    async def evaluate(self, m: dict):
        """RTDS fast-path: score a single market and execute if score gate passes."""
        sig = await self._score_market(m)
        if sig and sig["score"] >= MIN_SCORE_GATE:
            await self._execute_trade(sig)

    async def _place_order(self, token_id, side, price, size_usdc, asset, duration, mins_left, true_prob=0.5, cl_agree=True, min_edge_req=None, force_taker=False, score=0, pm_book_data=None, use_limit=False):
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

                # Use pre-fetched book from scoring phase (free — ran in parallel with Binance signals)
                # or fetch fresh if not cached (~36ms)
                if pm_book_data is not None:
                    best_bid, best_ask, tick = pm_book_data
                    spread = best_ask - best_bid
                    pm_book_data = None   # consume once; retries fetch fresh
                else:
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

                taker_edge     = true_prob - best_ask
                mid_est        = (best_bid + best_ask) / 2
                maker_edge_est = true_prob - mid_est
                edge_floor = min_edge_req if min_edge_req is not None else 0.04
                if not cl_agree:
                    edge_floor += 0.02

                if use_limit:
                    # GTC limit at target price (price << market) — skip market-based edge gate
                    # Edge is computed vs target, not current ask
                    limit_edge = true_prob - price
                    print(f"{B}[LIMIT]{RS} {asset} {side} target={price:.3f} limit_edge={limit_edge:.3f} ask={best_ask:.3f}")
                elif score >= 10:
                    # High conviction: gate on maker edge (mid price), not taker (ask)
                    if maker_edge_est < 0:
                        print(f"{Y}[SKIP] {asset} {side} [high-conv]: maker_edge={maker_edge_est:.3f} < 0 "
                              f"(mid={mid_est:.3f} model={true_prob:.3f}){RS}")
                        return None
                    print(f"{B}[FILL]{RS} {asset} {side} score={score} maker_edge={maker_edge_est:.3f} taker_edge={taker_edge:.3f}")
                else:
                    # Normal conviction: taker edge gate applies
                    if taker_edge < edge_floor:
                        kind = "disagree" if not cl_agree else "directional"
                        print(f"{Y}[SKIP] {asset} {side} [{kind}]: taker_edge={taker_edge:.3f} < {edge_floor:.2f} "
                              f"(ask={best_ask:.3f} model={true_prob:.3f}){RS}")
                        return None
                    print(f"{B}[FILL]{RS} {asset} {side} edge={taker_edge:.3f} floor={edge_floor:.2f}")

                # High conviction: skip maker, go straight to FOK taker for instant fill
                # FOK = Fill-or-Kill: fills completely at price or cancels instantly — no waiting
                if force_taker:
                    taker_price = round(min(best_ask + tick, 0.97), 4)
                    print(f"{G}[FAST-TAKER]{RS} {asset} {side} HIGH-CONV @ {taker_price:.3f} | ${size_usdc:.2f}")
                    order_args = MarketOrderArgs(token_id=token_id, amount=round(size_usdc, 2), side="BUY")
                    signed  = await loop.run_in_executor(None, lambda: self.clob.create_market_order(order_args))
                    resp    = await loop.run_in_executor(None, lambda: self.clob.post_order(signed, OrderType.FOK))
                    order_id = resp.get("orderID") or resp.get("id", "")
                    if resp.get("status") in ("matched", "filled"):
                        self.bankroll -= size_usdc
                        print(f"{G}[FAST-FILL]{RS} {side} {asset} {duration}m | ${size_usdc:.2f} @ {taker_price:.3f} | Bank ${self.bankroll:.2f}")
                        return order_id
                    # FOK not filled (thin liquidity) — fall through to normal maker/taker flow
                    print(f"{Y}[FOK] unfilled — falling back to maker{RS}")
                    force_taker = False  # reset so we don't loop

                # ── PHASE 1: Maker bid ────────────────────────────────────
                mid          = (best_bid + best_ask) / 2
                if use_limit:
                    # True GTC limit at target price — will fill only on pullback
                    maker_price = round(max(price, tick), 4)
                else:
                    maker_price = round(min(mid + tick, best_ask - tick), 4)
                    maker_price = max(maker_price, tick)
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

                # Ultra-low-latency waits to avoid blocking other opportunities.
                poll_interval = 0.5
                max_wait = min(1 if duration <= 5 else 2, int(mins_left * 60 * 0.04))
                if use_limit:
                    # Pullback limit: do not stall the cycle waiting on far-away price.
                    max_wait = 0.8
                polls     = max(1, int(max_wait / poll_interval))
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

                # ── PHASE 2: FAK taker fallback — re-fetch book for fresh ask ──
                # FAK = Fill-and-Kill (IOC): fills what's available instantly, cancels remainder
                # No sleep needed — response is immediate
                try:
                    fresh     = await loop.run_in_executor(None, lambda: self.clob.get_order_book(token_id))
                    f_asks    = sorted(fresh.asks, key=lambda x: float(x.price)) if fresh.asks else []
                    f_tick    = float(fresh.tick_size or tick)
                    fresh_ask = float(f_asks[0].price) if f_asks else best_ask
                    if use_limit and fresh_ask > MAX_ENTRY_PRICE:
                        print(f"{Y}[SKIP] {asset} {side} pullback missed: ask={fresh_ask:.3f} > max_entry={MAX_ENTRY_PRICE:.2f}{RS}")
                        return None
                    fresh_ep  = true_prob - fresh_ask
                    if fresh_ep < edge_floor:
                        print(f"{Y}[SKIP] {asset} {side} taker: fresh ask={fresh_ask:.3f} edge={fresh_ep:.3f} < {edge_floor:.2f} — price moved against us{RS}")
                        return None
                    taker_price = round(min(fresh_ask + f_tick, 0.97), 4)
                except Exception:
                    taker_price = round(min(best_ask + tick, 0.97), 4)
                    fresh_ask   = best_ask
                print(f"{Y}[MAKER] unfilled — FAK taker @ {taker_price:.3f} (fresh ask={fresh_ask:.3f}){RS}")
                order_args = MarketOrderArgs(token_id=token_id, amount=round(size_usdc, 2), side="BUY")
                signed   = await loop.run_in_executor(None, lambda: self.clob.create_market_order(order_args))
                resp     = await loop.run_in_executor(None, lambda: self.clob.post_order(signed, OrderType.FAK))
                order_id = resp.get("orderID") or resp.get("id", "")
                status   = resp.get("status", "")

                if order_id and status in ("matched", "filled"):
                    self.bankroll -= size_usdc
                    print(f"{Y}[TAKER FILL]{RS} {side} {asset} {duration}m | "
                          f"${size_usdc:.2f} @ {taker_price:.3f} | Bank ${self.bankroll:.2f}")
                    return order_id

                # FAK may partially fill — check order state once
                if order_id:
                    try:
                        info = await loop.run_in_executor(None, lambda: self.clob.get_order(order_id))
                        if isinstance(info, dict) and info.get("status") in ("matched", "filled"):
                            self.bankroll -= size_usdc
                            print(f"{Y}[TAKER FILL]{RS} {side} {asset} {duration}m | "
                                  f"${size_usdc:.2f} @ {taker_price:.3f} | Bank ${self.bankroll:.2f}")
                            return order_id
                    except Exception:
                        pass
                print(f"{Y}[ORDER] Both maker and FAK taker unfilled — cancelled{RS}")
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
                self._log_onchain_event("QUEUE_REDEEM", k, {
                    "asset": asset,
                    "side": trade.get("side", ""),
                    "size_usdc": trade.get("size", 0),
                    "entry_price": trade.get("entry", 0),
                    "open_price_source": trade.get("open_price_source", "?"),
                    "chainlink_age_s": trade.get("chainlink_age_s"),
                })
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
                        # Throttled wait log so operator sees progress without spam
                        now_ts = _time.time()
                        if now_ts - _wait_log_ts.get(cid, 0) >= LOG_REDEEM_WAIT_EVERY_SEC:
                            _wait_log_ts[cid] = now_ts
                            elapsed = (now_ts - self._redeem_queued_ts.get(cid, now_ts)) / 60
                            size = trade.get("size", 0)
                            print(f"{Y}[WAIT] {asset} {side} ~${size:.2f} — awaiting oracle ({elapsed:.0f}min){RS}")
                        continue   # not yet resolved on-chain

                    # On-chain truth only: determine winner from payoutNumerators.
                    n0 = await loop.run_in_executor(
                        None, lambda b=cid_bytes: ctf.functions.payoutNumerators(b, 0).call()
                    )
                    n1 = await loop.run_in_executor(
                        None, lambda b=cid_bytes: ctf.functions.payoutNumerators(b, 1).call()
                    )
                    winner_source = "ONCHAIN_NUMERATOR"
                    if n0 > 0 and n1 == 0:
                        winner = "Up"
                    elif n1 > 0 and n0 == 0:
                        winner = "Down"
                    elif n0 == 0 and n1 == 0:
                        # Not finalized in a usable way yet
                        continue
                    else:
                        # Ambiguous payout state (unexpected for binary market) — skip and retry
                        print(f"{Y}[REDEEM] Ambiguous numerators for {asset} cid={cid[:10]}... n0={n0} n1={n1}{RS}")
                        continue
                    won = (winner == side)
                    size   = trade.get("size", 0)
                    entry  = trade.get("entry", 0.5)

                    # For CLOB positions: CTF tokens are held by the exchange contract,
                    # not the user wallet. Always try redeemPositions — if Polymarket
                    # already auto-redeemed, the tx reverts harmlessly; if not, we collect.
                    if won and size > 0:
                        fee    = size * 0.0156 * (1 - abs(trade.get("mkt_price", 0.5) - 0.5) * 2)
                        payout = size / entry - fee
                        pnl    = payout - size

                        # Try redeemPositions from wallet first; if unclaimable, settle as auto-redeemed.
                        suffix = "auto-redeemed"
                        try:
                            tx_hash = await self._submit_redeem_tx(
                                ctf=ctf, collat=collat, acct=acct,
                                cid_bytes=cid_bytes, index_set=(1 if side == "Up" else 2),
                                loop=loop
                            )
                            suffix = f"tx={tx_hash[:16]}"
                        except Exception:
                            # If still claimable, keep in queue and retry later (never miss redeem).
                            if await self._is_redeem_claimable(
                                ctf=ctf, collat=collat, acct_addr=acct.address,
                                cid_bytes=cid_bytes, index_set=(1 if side == "Up" else 2), loop=loop
                            ):
                                print(f"{Y}[REDEEM] claimable but tx failed; will retry {asset} {side}{RS}")
                                continue

                        # Record win regardless of TX outcome
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
                        self._log_onchain_event("RESOLVE", cid, {
                            "asset": asset,
                            "side": side,
                            "result": "WIN",
                            "winner_side": winner,
                            "winner_source": winner_source,
                            "size_usdc": size,
                            "entry_price": entry,
                            "pnl": round(pnl, 4),
                            "bankroll_after": round(self.bankroll, 4),
                            "score": trade.get("score"),
                            "cl_agree": trade.get("cl_agree"),
                            "open_price_source": trade.get("open_price_source", "?"),
                            "chainlink_age_s": trade.get("chainlink_age_s"),
                            "onchain_score_adj": trade.get("onchain_score_adj", 0),
                            "source_confidence": trade.get("source_confidence", 0.0),
                        })
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
                            self._log_onchain_event("RESOLVE", cid, {
                                "asset": asset,
                                "side": side,
                                "result": "LOSS",
                                "winner_side": winner,
                                "winner_source": winner_source,
                                "size_usdc": size,
                                "entry_price": entry,
                                "pnl": round(pnl, 4),
                                "bankroll_after": round(self.bankroll, 4),
                                "score": trade.get("score"),
                                "cl_agree": trade.get("cl_agree"),
                                "open_price_source": trade.get("open_price_source", "?"),
                                "chainlink_age_s": trade.get("chainlink_age_s"),
                                "onchain_score_adj": trade.get("onchain_score_adj", 0),
                                "source_confidence": trade.get("source_confidence", 0.0),
                            })
                            wr = f"{self.wins/self.total*100:.0f}%" if self.total else "–"
                            print(f"{R}[LOSS]{RS} {asset} {side} {trade.get('duration',0)}m | "
                                  f"{R}${pnl:+.2f}{RS} | Bank ${self.bankroll:.2f} | WR {wr}")
                        done.append(cid)
                except Exception as e:
                    print(f"{Y}[REDEEM] {asset}: {e}{RS}")
            changed_pending = False
            for cid in done:
                self.redeemed_cids.add(cid)
                self.pending_redeem.pop(cid, None)
                if self.pending.pop(cid, None) is not None:
                    changed_pending = True
            if changed_pending:
                self._save_pending()

    async def _submit_redeem_tx(self, ctf, collat, acct, cid_bytes: bytes, index_set: int, loop):
        """Submit redeem tx with serialized nonce handling."""
        async with self._redeem_tx_lock:
            last_err = None
            for _ in range(4):
                try:
                    # Use pending nonce to avoid clashes with in-flight txs.
                    nonce = await loop.run_in_executor(
                        None, lambda: self.w3.eth.get_transaction_count(acct.address, "pending")
                    )
                    latest = await loop.run_in_executor(None, lambda: self.w3.eth.get_block("latest"))
                    base_fee = latest["baseFeePerGas"]
                    pri_fee = self.w3.to_wei(40, "gwei")
                    max_fee = base_fee * 2 + pri_fee
                    tx = ctf.functions.redeemPositions(
                        collat, b'\x00' * 32, cid_bytes, [index_set]
                    ).build_transaction({
                        "from": acct.address, "nonce": nonce,
                        "gas": 200_000,
                        "maxFeePerGas": max_fee,
                        "maxPriorityFeePerGas": pri_fee,
                        "chainId": 137,
                    })
                    signed = acct.sign_transaction(tx)
                    tx_hash = await loop.run_in_executor(
                        None, lambda: self.w3.eth.send_raw_transaction(signed.raw_transaction)
                    )
                    receipt = await loop.run_in_executor(
                        None, lambda h=tx_hash: self.w3.eth.wait_for_transaction_receipt(h, timeout=60)
                    )
                    if receipt.status != 1:
                        raise RuntimeError("redeem tx reverted")
                    return tx_hash.hex()
                except Exception as e:
                    last_err = e
                    msg = str(e).lower()
                    if "nonce too low" in msg or "already known" in msg:
                        await asyncio.sleep(0.4)
                        continue
                    raise
            raise RuntimeError(f"redeem tx failed after retries: {last_err}")

    async def _is_redeem_claimable(self, ctf, collat, acct_addr: str, cid_bytes: bytes, index_set: int, loop) -> bool:
        """Best-effort eth_call preflight for redeem claimability."""
        try:
            await loop.run_in_executor(
                None, lambda: ctf.functions.redeemPositions(
                    collat, b'\x00' * 32, cid_bytes, [index_set]
                ).call({"from": acct_addr})
            )
            return True
        except Exception:
            return False

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
        """P(Up) from time-based EMA at the closest cached half-life; O(1) from cache."""
        ema_dict = self.emas.get(asset)
        if not ema_dict:
            return 0.5
        hl    = min(ema_dict.keys(), key=lambda h: abs(h - seconds))
        price = self.prices.get(asset, 0.0)
        ema   = ema_dict.get(hl, 0.0)
        if price == 0 or ema == 0:
            return 0.5
        move  = (price - ema) / ema
        vol   = self.vols.get(asset, 0.70)
        vol_t = vol * math.sqrt(hl / (252 * 24 * 3600))
        if vol_t == 0:
            return 0.5
        return float(norm.cdf(move / vol_t))

    # ── Binance helpers — read from WS cache (instant, no network) ───────────

    def _binance_imbalance(self, asset: str) -> float:
        c = self.binance_cache.get(asset, {})
        bids = c.get("depth_bids", [])
        asks = c.get("depth_asks", [])
        bid_vol = sum(float(b[1]) for b in bids[:10])
        ask_vol = sum(float(a[1]) for a in asks[:10])
        if bid_vol + ask_vol == 0:
            return 0.0
        return (bid_vol - ask_vol) / (bid_vol + ask_vol)

    def _binance_taker_flow(self, asset: str) -> tuple:
        klines = self.binance_cache.get(asset, {}).get("klines", [])
        if len(klines) < 4:
            return 0.5, 1.0
        recent = klines[-3:]
        hist   = klines[:-3]
        rec_vol   = sum(float(k[5]) for k in recent)
        rec_taker = sum(float(k[9]) for k in recent)
        hist_avg  = (sum(float(k[5]) for k in hist) / len(hist)) if hist else rec_vol
        taker_ratio = rec_taker / rec_vol if rec_vol > 0 else 0.5
        vol_ratio   = (rec_vol / 3) / hist_avg if hist_avg > 0 else 1.0
        return round(taker_ratio, 3), round(vol_ratio, 2)

    def _binance_perp_signals(self, asset: str) -> tuple:
        c = self.binance_cache.get(asset, {})
        mark    = c.get("mark", 0.0)
        index   = c.get("index", 0.0)
        funding = c.get("funding", 0.0)
        basis   = (mark - index) / index if index > 0 else 0.0
        return round(basis, 7), round(funding, 7)

    def _binance_window_stats(self, asset: str, window_start_ts: float) -> tuple:
        import statistics as _stats
        klines = self.binance_cache.get(asset, {}).get("klines", [])
        if not klines or len(klines) < 3:
            return 0.0, 1.0
        start_ms = int(window_start_ts * 1000)
        window_k = [k for k in klines if int(k[0]) >= start_ms] or klines[-3:]
        sum_tv = sum((float(k[2])+float(k[3])+float(k[4]))/3 * float(k[5]) for k in window_k)
        sum_v  = sum(float(k[5]) for k in window_k)
        vwap   = sum_tv / sum_v if sum_v > 0 else float(window_k[-1][4])
        cur    = float(klines[-1][4])
        vwap_dev = (cur - vwap) / vwap if vwap > 0 else 0.0
        closes = [float(k[4]) for k in klines]
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

    # ── Binance WebSocket streams ─────────────────────────────────────────────

    async def _seed_binance_cache(self):
        """One-time REST seed so cache is ready before WS streams connect."""
        import requests as _req
        loop = asyncio.get_event_loop()
        for asset, sym in BNB_SYM.items():
            sym_api = sym.upper()
            try:
                depth = await loop.run_in_executor(None, lambda s=sym_api: _req.get(
                    "https://api.binance.com/api/v3/depth",
                    params={"symbol": s, "limit": 20}, timeout=5).json())
                self.binance_cache[asset]["depth_bids"] = depth.get("bids", [])
                self.binance_cache[asset]["depth_asks"] = depth.get("asks", [])
            except Exception: pass
            try:
                klines = await loop.run_in_executor(None, lambda s=sym_api: _req.get(
                    "https://api.binance.com/api/v3/klines",
                    params={"symbol": s, "interval": "1m", "limit": 33}, timeout=5).json())
                if isinstance(klines, list):
                    self.binance_cache[asset]["klines"] = klines
            except Exception: pass
            try:
                mark = await loop.run_in_executor(None, lambda s=sym_api: _req.get(
                    "https://fapi.binance.com/fapi/v1/premiumIndex",
                    params={"symbol": s}, timeout=5).json())
                self.binance_cache[asset]["mark"]    = float(mark.get("markPrice", 0))
                self.binance_cache[asset]["index"]   = float(mark.get("indexPrice", 0))
                self.binance_cache[asset]["funding"] = float(mark.get("lastFundingRate", 0))
            except Exception: pass
        print(f"{G}[BNB-SEED] Binance cache seeded for {list(BNB_SYM)}{RS}")

    async def _stream_binance_spot(self):
        """Persistent WS: depth20 + kline_1m for all assets → binance_cache."""
        import websockets as _ws, json as _j
        sym_map = {v: k for k, v in BNB_SYM.items()}  # "btcusdt" → "BTC"
        streams = [f"{s}@depth20@100ms/{s}@kline_1m" for s in BNB_SYM.values()]
        url = "wss://stream.binance.com/stream?streams=" + "/".join(streams)
        delay = 5
        while True:
            try:
                async with _ws.connect(url, ping_interval=20, ping_timeout=30) as ws:
                    print(f"{G}[BNB-SPOT] WS connected{RS}")
                    delay = 5
                    async for raw in ws:
                        msg    = _j.loads(raw)
                        stream = msg.get("stream", "")
                        data   = msg.get("data", {})
                        sym    = stream.split("@")[0]
                        asset  = sym_map.get(sym)
                        if not asset:
                            continue
                        c = self.binance_cache[asset]
                        if "@depth20" in stream:
                            c["depth_bids"] = data.get("bids", [])
                            c["depth_asks"] = data.get("asks", [])
                        elif "@kline_1m" in stream:
                            k = data.get("k", {})
                            kline = [k.get("t",0), k.get("o","0"), k.get("h","0"),
                                     k.get("l","0"), k.get("c","0"), k.get("v","0"),
                                     0, 0, 0, k.get("V","0"), 0, 0]
                            klines = c["klines"]
                            if klines and klines[-1][0] == kline[0]:
                                klines[-1] = kline
                            else:
                                klines.append(kline)
                                if len(klines) > 33:
                                    klines.pop(0)
            except Exception as e:
                print(f"{Y}[BNB-SPOT] {e} — reconnect in {delay}s{RS}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)

    async def _stream_binance_futures(self):
        """Persistent WS: markPrice for all assets → binance_cache mark/index/funding."""
        import websockets as _ws, json as _j
        sym_map = {v: k for k, v in BNB_SYM.items()}
        streams = [f"{s}@markPrice" for s in BNB_SYM.values()]
        url = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)
        delay = 5
        while True:
            try:
                async with _ws.connect(url, ping_interval=20, ping_timeout=30) as ws:
                    print(f"{G}[BNB-PERP] WS connected{RS}")
                    delay = 5
                    async for raw in ws:
                        msg   = _j.loads(raw)
                        data  = msg.get("data", {})
                        s     = data.get("s", "").lower()
                        asset = sym_map.get(s)
                        if not asset:
                            continue
                        c = self.binance_cache[asset]
                        c["mark"]    = float(data.get("p", 0) or 0)
                        c["index"]   = float(data.get("i", 0) or 0)
                        c["funding"] = float(data.get("r", 0) or 0)
            except Exception as e:
                print(f"{Y}[BNB-PERP] {e} — reconnect in {delay}s{RS}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)

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
    async def _get_polymarket_open_price(self, asset: str, start_ts: float, end_ts: float) -> float:
        """Call Polymarket's own price API to get the authoritative 'price to beat'.
        Returns openPrice float or 0.0 on any error."""
        try:
            from datetime import datetime, timezone as _tz
            sym  = asset  # BTC, ETH, SOL, XRP
            st   = datetime.fromtimestamp(start_ts, tz=_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            et   = datetime.fromtimestamp(end_ts,   tz=_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            url  = "https://polymarket.com/api/crypto/crypto-price"
            loop = asyncio.get_event_loop()
            import requests as _req
            r = await loop.run_in_executor(None, lambda: _req.get(
                url, params={"symbol": sym, "eventStartTime": st, "variant": "fifteen", "endDate": et},
                timeout=6
            ))
            data = r.json()
            price = data.get("openPrice") or 0.0
            return float(price) if price else 0.0
        except Exception:
            return 0.0

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
            if LOG_VERBOSE or self._should_log("scan-summary", LOG_SCAN_EVERY_SEC):
                print(f"{B}[SCAN] Live markets: {len(markets)} | Open: {len(self.pending)} | Settling: {len(self.pending_redeem)}{RS}")

            # Subscribe new markets to RTDS token price stream
            if self._rtds_ws:
                for cid, m in markets.items():
                    if cid not in self.active_mkts:
                        for tid in [m.get("token_up",""), m.get("token_down","")]:
                            if tid:
                                try:
                                    await self._rtds_ws.send(json.dumps({
                                        "action": "subscribe",
                                        "subscriptions": [{"asset_id": tid, "type": "market"}]
                                    }))
                                except Exception:
                                    pass

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
                    end_ts_m = m.get("end_ts", now + dur * 60)
                    # Authoritative reference: Polymarket's own price API
                    ref = await self._get_polymarket_open_price(asset, start_ts, end_ts_m)
                    if ref > 0:
                        src = "PM"
                    else:
                        # Fallback to Chainlink
                        ref, src = await self._get_chainlink_at(asset, start_ts)
                    if src == "not-ready":
                        print(f"{W}[NEW MARKET] {asset} {dur}m | {title_s} | waiting for first round...{RS}")
                    elif ref <= 0:
                        print(f"{Y}[NEW MARKET] {asset} {dur}m | {title_s} | no open price yet — retry{RS}")
                    else:
                        if asset in self.asset_cur_open:
                            self.asset_prev_open[asset] = self.asset_cur_open[asset]
                        self.asset_cur_open[asset]   = ref
                        self.open_prices[cid]        = ref
                        self.open_prices_source[cid] = src
                        print(f"{W}[NEW MARKET] {asset} {dur}m | {title_s} | beat=${ref:,.4f} [{src}] | {m['mins_left']:.1f}min left{RS}")
                else:
                    # Already known — but if source isn't PM yet, retry authoritative API
                    src = self.open_prices_source.get(cid, "?")
                    if src != "PM":
                        start_ts = m.get("start_ts", now)
                        end_ts_m = m.get("end_ts", now + dur * 60)
                        pm_ref = await self._get_polymarket_open_price(asset, start_ts, end_ts_m)
                        if pm_ref > 0:
                            self.open_prices[cid]        = pm_ref
                            self.open_prices_source[cid] = "PM"
                            src = "PM"
                    ref = self.open_prices[cid]
                    cur = self.prices.get(asset, 0) or self.cl_prices.get(asset, 0)
                    now_ts = _time.time()
                    if cur > 0 and now_ts - self._mkt_log_ts.get(cid, 0) >= LOG_MARKET_EVERY_SEC:
                        self._mkt_log_ts[cid] = now_ts
                        move = (cur - ref) / ref * 100
                        if LOG_VERBOSE or abs(move) >= LOG_MKT_MOVE_THRESHOLD_PCT:
                            print(f"{B}[MKT] {asset} {dur}m | beat=${ref:,.2f} [{src}] | "
                                  f"now=${cur:,.2f} move={move:+.3f}% | {m['mins_left']:.1f}min left{RS}")
                if cid not in self.seen:
                    candidates.append(m)
            if candidates:
                # Score all markets in parallel.
                signals = list(await asyncio.gather(*[self._score_market(m) for m in candidates]))
                valid   = sorted([s for s in signals if s is not None], key=lambda x: -x["score"])
                if valid:
                    best = valid[0]
                    other_strs = " | others: " + ", ".join(s["asset"] + "=" + str(s["score"]) for s in valid[1:]) if len(valid) > 1 else ""
                    print(f"{B}[ROUND] Best signal: {best['asset']} {best['side']} score={best['score']}{other_strs}{RS}")
                active_pending = {c: (m2, t) for c, (m2, t) in self.pending.items() if m2.get("end_ts", 0) > now}
                slots = max(0, MAX_OPEN - len(active_pending))
                to_exec = []
                for sig in valid:
                    if len(to_exec) >= slots:
                        break
                    if not TRADE_ALL_MARKETS:
                        pending_up = sum(1 for _, t in active_pending.values() if t.get("side") == "Up")
                        pending_dn = sum(1 for _, t in active_pending.values() if t.get("side") == "Down")
                        if sig["side"] == "Up" and pending_up >= MAX_SAME_DIR:
                            continue
                        if sig["side"] == "Down" and pending_dn >= MAX_SAME_DIR:
                            continue
                    to_exec.append(sig)
                if to_exec:
                    await asyncio.gather(*[self._execute_trade(sig) for sig in to_exec])

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
                    if cid in self.redeemed_cids:
                        continue   # already processed — don't re-add to pending
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
                        if stored_end > now_ts:
                            end_ts = stored_end  # trust stored real end_ts
                        elif end_ts % 86400 < 3600:  # midnight UTC — Gamma sent date-only, false expiry
                            end_ts = now_ts + duration * 60
                        # else: genuinely expired — keep real end_ts; _resolve() queues immediately
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
                    if cid in self.redeemed_cids:
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

    async def _force_redeem_backfill_loop(self):
        """Periodic backfill: force redeem resolved winners from recent activity.
        Prevents missed redeems when pending tracking is incomplete."""
        if DRY_RUN or self.w3 is None:
            return
        cfg      = get_contract_config(CHAIN_ID, neg_risk=False)
        ctf_addr = Web3.to_checksum_address(cfg.conditional_tokens)
        collat   = Web3.to_checksum_address(cfg.collateral)
        acct     = Account.from_key(PRIVATE_KEY)
        ctf = self.w3.eth.contract(address=ctf_addr, abi=[
            {"inputs":[{"name":"collateralToken","type":"address"},
                       {"name":"parentCollectionId","type":"bytes32"},
                       {"name":"conditionId","type":"bytes32"},
                       {"name":"indexSets","type":"uint256[]"}],
             "name":"redeemPositions","outputs":[],"stateMutability":"nonpayable","type":"function"},
            {"inputs":[{"name":"conditionId","type":"bytes32"}],
             "name":"payoutDenominator","outputs":[{"name":"","type":"uint256"}],
             "stateMutability":"view","type":"function"},
            {"inputs":[{"name":"conditionId","type":"bytes32"},{"name":"index","type":"uint256"}],
             "name":"payoutNumerators","outputs":[{"name":"","type":"uint256"}],
             "stateMutability":"view","type":"function"},
        ])
        loop = asyncio.get_event_loop()
        while True:
            await asyncio.sleep(FORCE_REDEEM_SCAN_SEC)
            try:
                import requests as _req
                activity = await loop.run_in_executor(None, lambda: _req.get(
                    "https://data-api.polymarket.com/activity",
                    params={"user": ADDRESS, "limit": "600"},
                    timeout=12
                ).json())
                if not isinstance(activity, list):
                    continue

                by_cid = {}
                for evt in activity:
                    cid = evt.get("conditionId") or ""
                    if not cid:
                        continue
                    typ = (evt.get("type") or "").upper()
                    d = by_cid.setdefault(cid, {"redeem": False, "net": {}, "title": evt.get("title", "")})
                    if typ == "REDEEM":
                        d["redeem"] = True
                    elif typ in ("BUY", "TRADE", "PURCHASE"):
                        outcome = evt.get("outcome") or ""
                        if not outcome:
                            continue
                        side = (evt.get("side") or "BUY").upper()
                        size = float(evt.get("size") or 0.0)
                        if size <= 0:
                            continue
                        prev = d["net"].get(outcome, 0.0)
                        d["net"][outcome] = prev + (size if side == "BUY" else -size)

                attempts = 0
                for cid, d in by_cid.items():
                    if attempts >= 8:  # bound each cycle
                        break
                    if d["redeem"] or cid in self.redeemed_cids:
                        continue
                    cid_bytes = bytes.fromhex(cid.lstrip("0x").zfill(64))
                    try:
                        denom = await loop.run_in_executor(None, lambda b=cid_bytes: ctf.functions.payoutDenominator(b).call())
                        if denom == 0:
                            continue
                        n0 = await loop.run_in_executor(None, lambda b=cid_bytes: ctf.functions.payoutNumerators(b, 0).call())
                        n1 = await loop.run_in_executor(None, lambda b=cid_bytes: ctf.functions.payoutNumerators(b, 1).call())
                    except Exception:
                        continue
                    if n0 > 0 and n1 == 0:
                        winner = "Up"
                    elif n1 > 0 and n0 == 0:
                        winner = "Down"
                    else:
                        continue
                    held = d["net"].get(winner, 0.0)
                    if held <= 0.0001:
                        continue
                    idx = 1 if winner == "Up" else 2
                    if not await self._is_redeem_claimable(
                        ctf=ctf, collat=collat, acct_addr=acct.address,
                        cid_bytes=cid_bytes, index_set=idx, loop=loop
                    ):
                        continue
                    try:
                        tx_hash = await self._submit_redeem_tx(
                            ctf=ctf, collat=collat, acct=acct,
                            cid_bytes=cid_bytes, index_set=idx, loop=loop
                        )
                        attempts += 1
                        self.redeemed_cids.add(cid)
                        print(f"{G}[FORCE-REDEEM] {winner} {d['title'][:36]} | tx={tx_hash[:16]}{RS}")
                    except Exception as e:
                        print(f"{Y}[FORCE-REDEEM] retry later {cid[:10]}...: {e}{RS}")
            except Exception as e:
                print(f"{Y}[FORCE-REDEEM] Error: {e}{RS}")

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
                    if cid in self.redeemed_cids:
                        continue   # already processed — don't re-add
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
                    # If already expired: check if it's a Gamma date-only midnight UTC false alarm.
                    if end_ts > 0 and end_ts <= now:
                        stored_end = self.pending.get(cid, ({},))[0].get("end_ts", 0)
                        if stored_end > now:
                            end_ts = stored_end  # trust stored real end_ts
                        elif end_ts % 86400 < 3600:  # midnight UTC — date-only field, false expiry
                            end_ts = now + duration * 60
                        # else: genuinely expired — keep real end_ts; _resolve() queues immediately
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
        await self._seed_binance_cache()   # populate Binance cache before WS streams start
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
            _guard("stream_rtds",           self.stream_rtds),
            _guard("_stream_binance_spot",  self._stream_binance_spot),
            _guard("_stream_binance_futures", self._stream_binance_futures),
            _guard("vol_loop",              self.vol_loop),
            _guard("scan_loop",             self.scan_loop),
            _guard("_status_loop",          self._status_loop),
            _guard("_refresh_balance",      self._refresh_balance),
            _guard("_redeem_loop",          self._redeem_loop),
            _guard("_force_redeem_backfill_loop", self._force_redeem_backfill_loop),
            _guard("chainlink_loop",        self.chainlink_loop),
            _guard("_redeemable_scan",      self._redeemable_scan),
            _guard("_position_sync_loop",   self._position_sync_loop),
        )


if __name__ == "__main__":
    try:
        asyncio.run(LiveTrader().run())
    except KeyboardInterrupt:
        print(f"\n{Y}[STOP] Log: {LOG_FILE}{RS}")
