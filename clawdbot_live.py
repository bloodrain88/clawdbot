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
import re
import time as _time
from collections import deque
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from scipy.stats import norm
from dotenv import load_dotenv
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from eth_account import Account
try:
    from runtime_utils import NonceManager, ErrorTracker, BucketStats
except ModuleNotFoundError:
    from collections import defaultdict

    class NonceManager:
        def __init__(self, web3, address):
            self.w3 = web3
            self.address = address
            self._lock = asyncio.Lock()
            self._next_nonce = None

        async def next_nonce(self, loop):
            async with self._lock:
                chain_nonce = await loop.run_in_executor(
                    None, lambda: self.w3.eth.get_transaction_count(self.address, "pending")
                )
                if self._next_nonce is None or self._next_nonce < chain_nonce:
                    self._next_nonce = chain_nonce
                out = self._next_nonce
                self._next_nonce += 1
                return out

        async def reset_from_chain(self, loop):
            async with self._lock:
                self._next_nonce = await loop.run_in_executor(
                    None, lambda: self.w3.eth.get_transaction_count(self.address, "pending")
                )

    class ErrorTracker:
        def __init__(self):
            self.counts = defaultdict(int)

        def tick(self, key: str, log_fn, err=None, every: int = 25):
            self.counts[key] += 1
            n = self.counts[key]
            if n % every == 0:
                suffix = f" last={err}" if err else ""
                log_fn(f"[WARN] {key} repeated {n}x{suffix}")

    class BucketStats:
        def __init__(self):
            self.rows = defaultdict(
                lambda: {
                    "n": 0,
                    "wins": 0,
                    "losses": 0,
                    "outcomes": 0,
                    "pnl": 0.0,
                    "gross_win": 0.0,
                    "gross_loss": 0.0,
                    "slip_bps": 0.0,
                    "fills": 0,
                }
            )

        def add_fill(self, bucket: str, slip_bps: float):
            r = self.rows[bucket]
            r["n"] += 1
            r["fills"] += 1
            r["slip_bps"] += slip_bps

        def add_outcome(self, bucket: str, won: bool, pnl: float):
            r = self.rows[bucket]
            r["n"] += 1
            r["outcomes"] += 1
            if won:
                r["wins"] += 1
                r["gross_win"] += max(0.0, float(pnl))
            else:
                r["losses"] += 1
                r["gross_loss"] += max(0.0, -float(pnl))
            r["pnl"] += pnl

load_dotenv(os.path.expanduser("~/.clawdbot.env"))

from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON, AMOY
from py_clob_client.clob_types import OrderArgs, MarketOrderArgs, OrderType, AssetType, BalanceAllowanceParams, ApiCreds
from py_clob_client.config import get_contract_config

POLYGON_RPCS = [
    "https://polygon-mainnet.public.blastapi.io",
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon.drpc.org",
    "https://rpc.ankr.com/polygon",
]
USDC_E_ADDR = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_ABI = [
    {"inputs":[{"name":"collateralToken","type":"address"},
               {"name":"parentCollectionId","type":"bytes32"},
               {"name":"conditionId","type":"bytes32"},
               {"name":"indexSets","type":"uint256[]"}],
     "name":"redeemPositions","outputs":[],"stateMutability":"nonpayable","type":"function"},
]

# ── CONFIG ────────────────────────────────────────────────────────────────────
def _clean_env(v: str) -> str:
    if v is None:
        return ""
    return str(v).strip().strip('"').strip("'")


def _pick_env(*names: str) -> str:
    for n in names:
        v = _clean_env(os.environ.get(n, ""))
        if v:
            return v
    return ""


PRIVATE_KEY    = _pick_env("POLY_PRIVATE_KEY", "PRIVATE_KEY", "PK")
ADDRESS        = _pick_env("POLY_ADDRESS", "ADDRESS")
POLY_API_KEY   = _pick_env("POLY_API_KEY")
POLY_API_SECRET = _pick_env("POLY_API_SECRET")
POLY_API_PASSPHRASE = _pick_env("POLY_API_PASSPHRASE")
NETWORK        = os.environ.get("POLY_NETWORK", "polygon")  # polygon | amoy
BANKROLL       = float(os.environ.get("BANKROLL", "100.0"))
MIN_EDGE       = 0.08     # 8% base min edge (auto-adapted per realized PnL quality)
MIN_MOVE       = 0.0003   # 0.03% below this = truly flat — use momentum to determine direction
MOMENTUM_WEIGHT = 0.40   # initial BS vs momentum blend (0=pure BS, 1=pure momentum)
DUST_BET       = 5.0      # $5 floor — at 4.55x payout: $5 → $22.75 win
MIN_BET_ABS    = float(os.environ.get("MIN_BET_ABS", "2.50"))
MIN_ORDER_SIZE_SHARES = float(os.environ.get("MIN_ORDER_SIZE_SHARES", "5.0"))
ORDER_SIZE_PAD_SHARES = float(os.environ.get("ORDER_SIZE_PAD_SHARES", "0.02"))
MIN_BET_PCT    = float(os.environ.get("MIN_BET_PCT", "0.025"))
DUST_RECOVER_MIN = float(os.environ.get("DUST_RECOVER_MIN", "0.50"))
MAX_ABS_BET    = float(os.environ.get("MAX_ABS_BET", "14.0"))     # hard ceiling
MAX_BANKROLL_PCT = 0.35   # never risk more than 35% of bankroll on a single bet
MAX_OPEN       = int(os.environ.get("MAX_OPEN", "8"))
MAX_SAME_DIR   = int(os.environ.get("MAX_SAME_DIR", "8"))
MAX_CID_EXPOSURE_PCT = float(os.environ.get("MAX_CID_EXPOSURE_PCT", "0.10"))
BLOCK_OPPOSITE_SIDE_SAME_CID = os.environ.get("BLOCK_OPPOSITE_SIDE_SAME_CID", "true").lower() == "true"
BLOCK_OPPOSITE_SIDE_SAME_ROUND = os.environ.get("BLOCK_OPPOSITE_SIDE_SAME_ROUND", "true").lower() == "true"
TRADE_ALL_MARKETS = os.environ.get("TRADE_ALL_MARKETS", "true").lower() == "true"
ROUND_BEST_ONLY = os.environ.get("ROUND_BEST_ONLY", "false").lower() == "true"
MIN_SCORE_GATE = int(os.environ.get("MIN_SCORE_GATE", "0"))
MIN_SCORE_GATE_5M = int(os.environ.get("MIN_SCORE_GATE_5M", "8"))
MIN_SCORE_GATE_15M = int(os.environ.get("MIN_SCORE_GATE_15M", "6"))
MAX_ENTRY_PRICE = float(os.environ.get("MAX_ENTRY_PRICE", "0.58"))
MAX_ENTRY_TOL = float(os.environ.get("MAX_ENTRY_TOL", "0.015"))
MIN_ENTRY_PRICE_5M = float(os.environ.get("MIN_ENTRY_PRICE_5M", "0.35"))
MAX_ENTRY_PRICE_5M = float(os.environ.get("MAX_ENTRY_PRICE_5M", "0.52"))
MIN_PAYOUT_MULT = float(os.environ.get("MIN_PAYOUT_MULT", "1.85"))
MIN_EV_NET = float(os.environ.get("MIN_EV_NET", "0.018"))
FEE_RATE_EST = float(os.environ.get("FEE_RATE_EST", "0.0156"))
HC15_ENABLED = os.environ.get("HC15_ENABLED", "false").lower() == "true"
HC15_MIN_SCORE = int(os.environ.get("HC15_MIN_SCORE", "10"))
HC15_MIN_TRUE_PROB = float(os.environ.get("HC15_MIN_TRUE_PROB", "0.62"))
HC15_MIN_EDGE = float(os.environ.get("HC15_MIN_EDGE", "0.10"))
HC15_TARGET_ENTRY = float(os.environ.get("HC15_TARGET_ENTRY", "0.30"))
HC15_FALLBACK_PCT_LEFT = float(os.environ.get("HC15_FALLBACK_PCT_LEFT", "0.35"))
HC15_FALLBACK_MAX_ENTRY = float(os.environ.get("HC15_FALLBACK_MAX_ENTRY", "0.36"))
MIN_PAYOUT_MULT_5M = float(os.environ.get("MIN_PAYOUT_MULT_5M", "1.75"))
MIN_EV_NET_5M = float(os.environ.get("MIN_EV_NET_5M", "0.018"))
ENTRY_HARD_CAP_5M = float(os.environ.get("ENTRY_HARD_CAP_5M", "0.54"))
ENTRY_HARD_CAP_15M = float(os.environ.get("ENTRY_HARD_CAP_15M", "0.65"))
ENTRY_NEAR_MISS_TOL = float(os.environ.get("ENTRY_NEAR_MISS_TOL", "0.030"))
PULLBACK_LIMIT_ENABLED = os.environ.get("PULLBACK_LIMIT_ENABLED", "true").lower() == "true"
PULLBACK_LIMIT_MIN_PCT_LEFT = float(os.environ.get("PULLBACK_LIMIT_MIN_PCT_LEFT", "0.25"))
FAST_EXEC_ENABLED = os.environ.get("FAST_EXEC_ENABLED", "false").lower() == "true"
FAST_EXEC_SCORE = int(os.environ.get("FAST_EXEC_SCORE", "6"))
FAST_EXEC_EDGE = float(os.environ.get("FAST_EXEC_EDGE", "0.02"))
QUALITY_MODE = os.environ.get("QUALITY_MODE", "true").lower() == "true"
STRICT_PM_SOURCE = os.environ.get("STRICT_PM_SOURCE", "true").lower() == "true"
MAX_SIGNAL_LATENCY_MS = float(os.environ.get("MAX_SIGNAL_LATENCY_MS", "1200"))
MAX_QUOTE_STALENESS_MS = float(os.environ.get("MAX_QUOTE_STALENESS_MS", "1200"))
MIN_MINS_LEFT_5M = float(os.environ.get("MIN_MINS_LEFT_5M", "1.2"))
MIN_MINS_LEFT_15M = float(os.environ.get("MIN_MINS_LEFT_15M", "1.2"))
EXTRA_SCORE_GATE_BTC_5M = int(os.environ.get("EXTRA_SCORE_GATE_BTC_5M", "1"))
EXTRA_SCORE_GATE_XRP_15M = int(os.environ.get("EXTRA_SCORE_GATE_XRP_15M", "0"))
EXPOSURE_CAP_TOTAL_TREND = float(os.environ.get("EXPOSURE_CAP_TOTAL_TREND", "0.80"))
EXPOSURE_CAP_TOTAL_CHOP = float(os.environ.get("EXPOSURE_CAP_TOTAL_CHOP", "0.60"))
EXPOSURE_CAP_SIDE_TREND = float(os.environ.get("EXPOSURE_CAP_SIDE_TREND", "0.55"))
EXPOSURE_CAP_SIDE_CHOP = float(os.environ.get("EXPOSURE_CAP_SIDE_CHOP", "0.40"))

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
SCAN_INTERVAL  = float(os.environ.get("SCAN_INTERVAL", "0.5"))
PING_INTERVAL  = int(os.environ.get("PING_INTERVAL", "2"))
STATUS_INTERVAL= int(os.environ.get("STATUS_INTERVAL", "15"))
_DATA_DIR      = os.environ.get("DATA_DIR", os.path.expanduser("~"))
LOG_FILE       = os.path.join(_DATA_DIR, "clawdbot_live_trades.csv")
PENDING_FILE   = os.path.join(_DATA_DIR, "clawdbot_pending.json")
SEEN_FILE      = os.path.join(_DATA_DIR, "clawdbot_seen.json")
STATS_FILE     = os.path.join(_DATA_DIR, "clawdbot_stats.json")
METRICS_FILE   = os.path.join(_DATA_DIR, "clawdbot_onchain_metrics.jsonl")
PNL_BASELINE_FILE = os.path.join(_DATA_DIR, "clawdbot_pnl_baseline.json")
PNL_BASELINE_RESET_ON_BOOT = os.environ.get("PNL_BASELINE_RESET_ON_BOOT", "false").lower() == "true"

DRY_RUN   = os.environ.get("DRY_RUN", "true").lower() == "true"
LOG_VERBOSE = os.environ.get("LOG_VERBOSE", "false").lower() == "true"
LOG_SCAN_EVERY_SEC = int(os.environ.get("LOG_SCAN_EVERY_SEC", "30"))
LOG_SCAN_ON_CHANGE_ONLY = os.environ.get("LOG_SCAN_ON_CHANGE_ONLY", "true").lower() == "true"
LOG_FLOW_EVERY_SEC = int(os.environ.get("LOG_FLOW_EVERY_SEC", "120"))
LOG_ROUND_EMPTY_EVERY_SEC = int(os.environ.get("LOG_ROUND_EMPTY_EVERY_SEC", "180"))
LOG_SETTLE_FIRST_EVERY_SEC = int(os.environ.get("LOG_SETTLE_FIRST_EVERY_SEC", "20"))
LOG_BANK_EVERY_SEC = int(os.environ.get("LOG_BANK_EVERY_SEC", "45"))
LOG_BANK_MIN_DELTA = float(os.environ.get("LOG_BANK_MIN_DELTA", "0.50"))
LOG_MARKET_EVERY_SEC = int(os.environ.get("LOG_MARKET_EVERY_SEC", "90"))
LOG_OPEN_WAIT_EVERY_SEC = int(os.environ.get("LOG_OPEN_WAIT_EVERY_SEC", "120"))
LOG_REDEEM_WAIT_EVERY_SEC = int(os.environ.get("LOG_REDEEM_WAIT_EVERY_SEC", "180"))
REDEEM_POLL_SEC = float(os.environ.get("REDEEM_POLL_SEC", "2.0"))
REDEEM_REQUIRE_ONCHAIN_CONFIRM = os.environ.get("REDEEM_REQUIRE_ONCHAIN_CONFIRM", "true").lower() == "true"
LOG_MKT_MOVE_THRESHOLD_PCT = float(os.environ.get("LOG_MKT_MOVE_THRESHOLD_PCT", "0.15"))
FORCE_REDEEM_SCAN_SEC = int(os.environ.get("FORCE_REDEEM_SCAN_SEC", "5"))
REDEEMABLE_SCAN_SEC = int(os.environ.get("REDEEMABLE_SCAN_SEC", "10"))
ROUND_RETRY_COOLDOWN_SEC = float(os.environ.get("ROUND_RETRY_COOLDOWN_SEC", "12"))
RPC_OPTIMIZE_SEC = int(os.environ.get("RPC_OPTIMIZE_SEC", "45"))
RPC_PROBE_COUNT = int(os.environ.get("RPC_PROBE_COUNT", "3"))
RPC_SWITCH_MARGIN_MS = float(os.environ.get("RPC_SWITCH_MARGIN_MS", "15"))
COPYFLOW_FILE = os.environ.get("COPYFLOW_FILE", os.path.join(_DATA_DIR, "clawdbot_copyflow.json"))
COPYFLOW_RELOAD_SEC = int(os.environ.get("COPYFLOW_RELOAD_SEC", "20"))
COPYFLOW_BONUS_MAX = int(os.environ.get("COPYFLOW_BONUS_MAX", "2"))
COPYFLOW_REFRESH_ENABLED = os.environ.get("COPYFLOW_REFRESH_ENABLED", "true").lower() == "true"
COPYFLOW_REFRESH_SEC = int(os.environ.get("COPYFLOW_REFRESH_SEC", "300"))
COPYFLOW_MIN_ROI = float(os.environ.get("COPYFLOW_MIN_ROI", "-0.03"))
COPYFLOW_LIVE_ENABLED = os.environ.get("COPYFLOW_LIVE_ENABLED", "true").lower() == "true"
COPYFLOW_LIVE_REFRESH_SEC = int(os.environ.get("COPYFLOW_LIVE_REFRESH_SEC", "12"))
COPYFLOW_LIVE_TRADES_LIMIT = int(os.environ.get("COPYFLOW_LIVE_TRADES_LIMIT", "80"))
COPYFLOW_LIVE_MAX_MARKETS = int(os.environ.get("COPYFLOW_LIVE_MAX_MARKETS", "8"))
COPYFLOW_INTEL_ENABLED = os.environ.get("COPYFLOW_INTEL_ENABLED", "true").lower() == "true"
COPYFLOW_INTEL_REFRESH_SEC = int(os.environ.get("COPYFLOW_INTEL_REFRESH_SEC", "90"))
COPYFLOW_INTEL_TRADES_PER_MARKET = int(os.environ.get("COPYFLOW_INTEL_TRADES_PER_MARKET", "120"))
COPYFLOW_INTEL_MAX_WALLETS = int(os.environ.get("COPYFLOW_INTEL_MAX_WALLETS", "40"))
COPYFLOW_INTEL_ACTIVITY_LIMIT = int(os.environ.get("COPYFLOW_INTEL_ACTIVITY_LIMIT", "250"))
COPYFLOW_INTEL_MIN_SETTLED = int(os.environ.get("COPYFLOW_INTEL_MIN_SETTLED", "12"))
COPYFLOW_INTEL_MIN_ROI = float(os.environ.get("COPYFLOW_INTEL_MIN_ROI", "0.02"))
COPYFLOW_INTEL_WINDOW_SEC = int(os.environ.get("COPYFLOW_INTEL_WINDOW_SEC", "86400"))
COPYFLOW_INTEL_SETTLE_LAG_SEC = int(os.environ.get("COPYFLOW_INTEL_SETTLE_LAG_SEC", "1200"))
COPYFLOW_INTEL_MIN_SETTLED_FAMILY_24H = int(os.environ.get("COPYFLOW_INTEL_MIN_SETTLED_FAMILY_24H", "6"))
COPYFLOW_INTEL_MIN_SETTLED_FAMILY_ALL = int(os.environ.get("COPYFLOW_INTEL_MIN_SETTLED_FAMILY_ALL", "20"))
PREBID_ARM_ENABLED = os.environ.get("PREBID_ARM_ENABLED", "true").lower() == "true"
PREBID_MIN_CONF = float(os.environ.get("PREBID_MIN_CONF", "0.58"))
PREBID_ARM_WINDOW_SEC = float(os.environ.get("PREBID_ARM_WINDOW_SEC", "30"))
ENABLE_5M = os.environ.get("ENABLE_5M", "false").lower() == "true"
ORDER_FAST_MODE = os.environ.get("ORDER_FAST_MODE", "true").lower() == "true"
MAKER_POLL_5M_SEC = float(os.environ.get("MAKER_POLL_5M_SEC", "0.15"))
MAKER_POLL_15M_SEC = float(os.environ.get("MAKER_POLL_15M_SEC", "0.25"))
MAKER_WAIT_5M_SEC = float(os.environ.get("MAKER_WAIT_5M_SEC", "0.30"))
MAKER_WAIT_15M_SEC = float(os.environ.get("MAKER_WAIT_15M_SEC", "0.45"))
FAST_TAKER_NEAR_END_5M_SEC = float(os.environ.get("FAST_TAKER_NEAR_END_5M_SEC", "100"))
FAST_TAKER_NEAR_END_15M_SEC = float(os.environ.get("FAST_TAKER_NEAR_END_15M_SEC", "150"))
FAST_TAKER_SPREAD_MAX_5M = float(os.environ.get("FAST_TAKER_SPREAD_MAX_5M", "0.015"))
FAST_TAKER_SPREAD_MAX_15M = float(os.environ.get("FAST_TAKER_SPREAD_MAX_15M", "0.010"))
FAST_TAKER_SCORE_5M = int(os.environ.get("FAST_TAKER_SCORE_5M", "8"))
FAST_TAKER_SCORE_15M = int(os.environ.get("FAST_TAKER_SCORE_15M", "9"))
FAST_TAKER_EDGE_DIFF_MAX = float(os.environ.get("FAST_TAKER_EDGE_DIFF_MAX", "0.015"))
FAST_TAKER_EARLY_WINDOW_SEC_5M = float(os.environ.get("FAST_TAKER_EARLY_WINDOW_SEC_5M", "45"))
FAST_TAKER_EARLY_WINDOW_SEC_15M = float(os.environ.get("FAST_TAKER_EARLY_WINDOW_SEC_15M", "75"))
MAX_TAKER_SLIP_BPS_5M = float(os.environ.get("MAX_TAKER_SLIP_BPS_5M", "80"))
MAX_TAKER_SLIP_BPS_15M = float(os.environ.get("MAX_TAKER_SLIP_BPS_15M", "70"))
MAX_BOOK_SPREAD_5M = float(os.environ.get("MAX_BOOK_SPREAD_5M", "0.060"))
MAX_BOOK_SPREAD_15M = float(os.environ.get("MAX_BOOK_SPREAD_15M", "0.120"))
MAKER_ENTRY_TICK_TOL = int(os.environ.get("MAKER_ENTRY_TICK_TOL", "1"))
ORDER_RETRY_MAX = int(os.environ.get("ORDER_RETRY_MAX", "4"))
ORDER_RETRY_BASE_SEC = float(os.environ.get("ORDER_RETRY_BASE_SEC", "0.35"))
MAX_WIN_MODE = os.environ.get("MAX_WIN_MODE", "true").lower() == "true"
WINMODE_MIN_TRUE_PROB_5M = float(os.environ.get("WINMODE_MIN_TRUE_PROB_5M", "0.58"))
WINMODE_MIN_TRUE_PROB_15M = float(os.environ.get("WINMODE_MIN_TRUE_PROB_15M", "0.60"))
WINMODE_MIN_EDGE = float(os.environ.get("WINMODE_MIN_EDGE", "0.015"))
WINMODE_MAX_ENTRY_5M = float(os.environ.get("WINMODE_MAX_ENTRY_5M", "0.60"))
WINMODE_MAX_ENTRY_15M = float(os.environ.get("WINMODE_MAX_ENTRY_15M", "0.56"))
WINMODE_REQUIRE_CL_AGREE = os.environ.get("WINMODE_REQUIRE_CL_AGREE", "true").lower() == "true"
FIVE_MIN_ASSETS = {
    s.strip().upper() for s in os.environ.get("FIVE_MIN_ASSETS", "BTC,ETH").split(",") if s.strip()
}
CHAIN_ID  = POLYGON  # CLOB API esiste solo su mainnet
CLOB_HOST = "https://clob.polymarket.com"

# Auto-derive address if only private key is set.
if not ADDRESS and PRIVATE_KEY:
    try:
        ADDRESS = Account.from_key(PRIVATE_KEY).address
    except Exception:
        ADDRESS = ""

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
        self._scan_state_last   = None
        self._bank_state_last   = None
        self._open_ref_fetch_ts = {}
        self.asset_cur_open     = {}   # asset → current market open price (for inter-market continuity)
        self.asset_prev_open    = {}   # asset → previous market open price
        self.active_mkts = {}
        self.pending         = {}   # cid → (m, trade)
        self.pending_redeem  = {}   # cid → (side, asset)  — waiting on-chain resolution
        self.redeemed_cids   = set()  # cids already processed — prevents _redeemable_scan re-queueing
        self.token_prices    = {}     # token_id → real-time price from RTDS market stream
        self._rtds_ws        = None   # live WebSocket handle for dynamic subscriptions
        self._redeem_queued_ts = {}  # cid → timestamp when queued for redeem
        self._redeem_verify_counts = {}  # cid → non-claimable verification cycles before auto-close
        self.seen            = set()
        self._session        = None   # persistent aiohttp session
        self.cl_prices       = {}    # Chainlink oracle prices (resolution source)
        self.cl_updated      = {}    # Chainlink last update timestamp per asset
        self.bankroll        = BANKROLL
        self.start_bank      = BANKROLL
        self._pnl_baseline_locked = False
        self._pnl_baseline_ts = ""
        self.onchain_wallet_usdc = BANKROLL
        self.onchain_open_positions = 0.0
        self.onchain_open_count = 0
        self.onchain_redeemable_count = 0
        self.onchain_total_equity = BANKROLL
        self.onchain_snapshot_ts = 0.0
        self.daily_pnl       = 0.0
        self.total           = 0
        self.wins            = 0
        self.start_time      = datetime.now(timezone.utc)
        self.rtds_ok         = False
        self.clob            = None
        self.w3              = None   # shared Polygon RPC connection
        self._rpc_url        = ""
        self._rpc_epoch      = 0
        self._rpc_stats      = {}
        self._perf_stats     = {
            "score_ms_ema": 0.0, "score_n": 0,
            "order_ms_ema": 0.0, "order_n": 0,
        }
        # ── Adaptive strategy state ──────────────────────────────────────────
        self.price_history   = {a: deque(maxlen=300) for a in ["BTC","ETH","SOL","XRP"]}
        self.stats           = {}    # {asset: {side: {wins, total}}} — persisted
        self.recent_trades   = deque(maxlen=30)   # rolling window for WR adaptation
        self.recent_pnl      = deque(maxlen=40)   # rolling pnl window for profit-factor/expectancy adaptation
        self.side_perf       = {}                 # "ASSET|SIDE" -> {n, gross_win, gross_loss, pnl}
        self._last_eval_time    = {}              # cid → last RTDS-triggered evaluate() timestamp
        self._exec_lock         = asyncio.Lock()
        self._executing_cids    = set()
        self._round_side_attempt_ts = {}          # "round_key|side" → last attempt ts
        self._cid_side_attempt_ts = {}            # "cid|side" → last attempt ts
        self._round_side_block_until = {}         # "round_fingerprint|side" → ttl epoch
        self._redeem_tx_lock    = asyncio.Lock()  # serialize redeem txs to avoid nonce clashes
        self._nonce_mgr         = None
        self._errors            = ErrorTracker()
        self._bucket_stats      = BucketStats()
        self._copyflow_map      = {}
        self._copyflow_leaders  = {}
        self._copyflow_leaders_live = {}
        self._copyflow_leaders_family = {}
        self._copyflow_live     = {}
        self._cid_family_cache  = {}
        self._prebid_plan       = {}
        self._copyflow_mtime    = 0.0
        self._copyflow_last_try = 0.0
        self.peak_bankroll      = BANKROLL           # track peak for drawdown guard
        self.consec_losses      = 0                  # consecutive resolved losses counter
        self._last_round_best   = ""
        self._last_round_pick   = ""
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
        self._load_pnl_baseline()
        self._init_w3()

    def _build_w3(self, rpc: str, timeout: int = 6):
        _w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": timeout}))
        _w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        return _w3

    def _init_w3(self):
        best_rpc = ""
        best_ms = 1e18
        best_w3 = None
        for rpc in POLYGON_RPCS:
            try:
                _w3 = self._build_w3(rpc, timeout=6)
                samples = []
                for _ in range(max(1, RPC_PROBE_COUNT)):
                    t0 = _time.perf_counter()
                    _ = _w3.eth.block_number
                    samples.append((_time.perf_counter() - t0) * 1000.0)
                samples.sort()
                ms = samples[len(samples) // 2]
                self._rpc_stats[rpc] = ms
                if ms < best_ms:
                    best_ms = ms
                    best_rpc = rpc
                    best_w3 = _w3
            except Exception:
                continue
        if best_w3 is not None:
            self.w3 = best_w3
            self._rpc_url = best_rpc
            self._rpc_epoch += 1
            self._nonce_mgr = NonceManager(self.w3, ADDRESS)
            print(f"{G}[RPC] Connected: {best_rpc} ({best_ms:.0f}ms){RS}")
            return
        print(f"{Y}[RPC] No working Polygon RPC — on-chain redemption disabled{RS}")

    def _perf_update(self, key: str, ms: float):
        ema_key = f"{key}_ema"
        n_key = f"{key.replace('_ms', '')}_n"
        alpha = 0.2
        prev = self._perf_stats.get(ema_key, 0.0)
        self._perf_stats[ema_key] = ms if prev <= 0 else (alpha * ms + (1 - alpha) * prev)
        self._perf_stats[n_key] = int(self._perf_stats.get(n_key, 0)) + 1

    def _should_log(self, key: str, every_sec: float) -> bool:
        now = _time.time()
        last = self._log_ts.get(key, 0.0)
        if now - last >= every_sec:
            self._log_ts[key] = now
            return True
        return False

    def _short_cid(self, cid: str) -> str:
        if not cid:
            return "n/a"
        return f"{cid[:10]}..."

    def _token_id_from_cid_side(self, cid: str, side: str) -> str:
        """Derive ERC1155 position token_id from (conditionId, side) for binary markets."""
        try:
            h = str(cid or "").lower().replace("0x", "")
            if len(h) != 64:
                return ""
            idx = 1 if side == "Up" else (2 if side == "Down" else 0)
            if idx == 0:
                return ""
            cid_b = bytes.fromhex(h)
            collection_id = Web3.solidity_keccak(["bytes32", "uint256"], [cid_b, idx])
            position_id = Web3.solidity_keccak(
                ["address", "bytes32"],
                [Web3.to_checksum_address(USDC_E_ADDR), collection_id],
            )
            return str(int.from_bytes(position_id, "big"))
        except Exception:
            return ""

    def _is_exact_round_bounds(self, start_ts: float, end_ts: float, dur: int) -> bool:
        if start_ts <= 0 or end_ts <= 0 or dur <= 0:
            return False
        if abs((end_ts - start_ts) - dur * 60.0) > 2.5:
            return False
        st = datetime.fromtimestamp(start_ts, tz=timezone.utc)
        et = datetime.fromtimestamp(end_ts, tz=timezone.utc)
        if st.second != 0 or et.second != 0:
            return False
        if dur in (5, 15):
            if st.minute % dur != 0 or et.minute % dur != 0:
                return False
        return True

    def _round_bounds_from_question(self, question: str) -> tuple[float, float]:
        """Best-effort exact ET round parsing from market title."""
        if not question:
            return 0.0, 0.0
        # Example: "February 21, 8:30AM-8:45AM ET"
        m = re.search(
            r"([A-Za-z]+)\s+(\d{1,2}),\s*(\d{1,2}:\d{2})(AM|PM)?-(\d{1,2}:\d{2})(AM|PM)\s*ET",
            question,
        )
        if not m:
            return 0.0, 0.0
        month_s, day_s, t1_s, ap1_s, t2_s, ap2_s = m.groups()
        months = {
            "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
            "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
        }
        mon = months.get(month_s.lower())
        if mon is None:
            return 0.0, 0.0
        day = int(day_s)
        ap1 = ap1_s or ap2_s
        if ap1 is None:
            return 0.0, 0.0
        now_et = datetime.now(ZoneInfo("America/New_York"))
        year = now_et.year
        try:
            st_local = datetime.strptime(f"{year}-{mon:02d}-{day:02d} {t1_s}{ap1}", "%Y-%m-%d %I:%M%p").replace(
                tzinfo=ZoneInfo("America/New_York")
            )
            et_local = datetime.strptime(f"{year}-{mon:02d}-{day:02d} {t2_s}{ap2_s}", "%Y-%m-%d %I:%M%p").replace(
                tzinfo=ZoneInfo("America/New_York")
            )
            st_ts = st_local.astimezone(timezone.utc).timestamp()
            et_ts = et_local.astimezone(timezone.utc).timestamp()
            # Handle midnight crossing edge-case.
            if et_ts <= st_ts:
                et_ts = st_ts + 24 * 3600
            return st_ts, et_ts
        except Exception:
            return 0.0, 0.0

    def _apply_exact_window_from_question(self, m: dict, t: dict | None = None) -> bool:
        """Normalize start/end timestamps from explicit ET window in title when possible."""
        if not isinstance(m, dict):
            return False
        dur = int((t or {}).get("duration") or m.get("duration") or 0)
        if dur <= 0:
            return False
        q_start, q_end = self._round_bounds_from_question(m.get("question", ""))
        if not self._is_exact_round_bounds(q_start, q_end, dur):
            return False
        m["start_ts"] = q_start
        m["end_ts"] = q_end
        if t is not None:
            t["end_ts"] = q_end
            t["duration"] = dur
        return True

    def _force_expired_from_question_if_needed(self, m: dict, t: dict | None = None) -> bool:
        """If title window is exact and already expired, force end_ts to that exact end."""
        if not isinstance(m, dict):
            return False
        dur = int((t or {}).get("duration") or m.get("duration") or 0)
        if dur <= 0:
            return False
        q_start, q_end = self._round_bounds_from_question(m.get("question", ""))
        if not self._is_exact_round_bounds(q_start, q_end, dur):
            return False
        now_ts = datetime.now(timezone.utc).timestamp()
        if q_end > now_ts:
            return False
        m["start_ts"] = q_start
        m["end_ts"] = q_end
        if t is not None:
            t["end_ts"] = q_end
            t["duration"] = dur
        return True

    def _round_key(self, cid: str = "", m: dict | None = None, t: dict | None = None) -> str:
        m = m or {}
        t = t or {}
        asset = (t.get("asset") or m.get("asset") or "?").upper()
        dur = int(t.get("duration") or m.get("duration") or 0)
        start_ts = float(m.get("start_ts") or 0)
        end_ts = float(t.get("end_ts") or m.get("end_ts") or 0)
        if not self._is_exact_round_bounds(start_ts, end_ts, dur):
            q_st, q_et = self._round_bounds_from_question(m.get("question", ""))
            if dur <= 0 and q_st > 0 and q_et > q_st:
                q_dur = int(round((q_et - q_st) / 60.0))
                if q_dur in (5, 15):
                    dur = q_dur
            if self._is_exact_round_bounds(q_st, q_et, dur):
                start_ts, end_ts = q_st, q_et
        if self._is_exact_round_bounds(start_ts, end_ts, dur):
            st = datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            et = datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            return f"{asset}-{dur}m-{st}-{et}"
        return f"{asset}-{dur}m-cid{self._short_cid(cid)}"

    def _round_fingerprint(self, cid: str = "", m: dict | None = None, t: dict | None = None) -> str:
        """CID-independent round identity to prevent opposite-side same-round hedging."""
        m = m or {}
        t = t or {}
        asset = (t.get("asset") or m.get("asset") or "?").upper()
        dur = int(t.get("duration") or m.get("duration") or 0)
        start_ts = float(m.get("start_ts") or 0)
        end_ts = float(t.get("end_ts") or m.get("end_ts") or 0)
        if not self._is_exact_round_bounds(start_ts, end_ts, dur):
            q_st, q_et = self._round_bounds_from_question(m.get("question", ""))
            if dur <= 0 and q_st > 0 and q_et > q_st:
                q_dur = int(round((q_et - q_st) / 60.0))
                if q_dur in (5, 15):
                    dur = q_dur
            if self._is_exact_round_bounds(q_st, q_et, dur):
                start_ts, end_ts = q_st, q_et
        if self._is_exact_round_bounds(start_ts, end_ts, dur):
            st = datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            et = datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            return f"{asset}-{dur}m-{st}-{et}"
        q = (m.get("question", "") or "").strip().lower()
        return f"{asset}-{dur}m-q:{q[:64]}"

    async def _http_get_json(self, url: str, params: dict | None = None, timeout: float = 8.0):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        try:
            async with self._session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as r:
                if r.status >= 400:
                    raise RuntimeError(f"http {r.status} {url}")
                return await r.json()
        except Exception as e:
            self._errors.tick("http_get_json", print, err=e, every=20)
            raise

    def _reload_copyflow(self):
        now = _time.time()
        if now - self._copyflow_last_try < COPYFLOW_RELOAD_SEC:
            return
        self._copyflow_last_try = now
        try:
            candidates = [COPYFLOW_FILE, "/app/clawdbot_copyflow.json"]
            src = next((p for p in candidates if p and os.path.exists(p)), "")
            if not src:
                return
            mtime = os.path.getmtime(src)
            if mtime <= self._copyflow_mtime:
                return
            with open(src, "r", encoding="utf-8") as f:
                payload = json.load(f)
            market_flow = payload.get("market_side_flow", {})
            leaders = payload.get("leaders", [])
            if isinstance(market_flow, dict):
                self._copyflow_map = market_flow
                if isinstance(leaders, list):
                    lw = {}
                    for row in leaders:
                        w = (row.get("wallet") or "").lower().strip()
                        sc = float(row.get("score", 0.0) or 0.0)
                        if w and sc > 0:
                            lw[w] = sc
                    self._copyflow_leaders = lw
                self._copyflow_mtime = mtime
                print(
                    f"{B}[COPY]{RS} loaded {len(self._copyflow_map)} market side-flow "
                    f"+ {len(self._copyflow_leaders)} leaders from {src}"
                )
        except Exception as e:
            self._errors.tick("copyflow_reload", print, err=e, every=10)

    async def _copyflow_refresh_loop(self):
        """Periodic copy-wallet recalculation from live on-chain data."""
        if not COPYFLOW_REFRESH_ENABLED or DRY_RUN:
            return
        script_path = "/app/scripts/polymarket_copy_alpha.py"
        out_path = COPYFLOW_FILE
        while True:
            await asyncio.sleep(COPYFLOW_REFRESH_SEC)
            try:
                if not os.path.exists(script_path):
                    if self._should_log("copyflow-no-script", 300):
                        print(f"{Y}[COPY]{RS} refresh skipped: missing {script_path}")
                    continue
                cmd = [
                    "python", script_path,
                    "--wallet", ADDRESS,
                    "--out", out_path,
                    "--min-roi", str(COPYFLOW_MIN_ROI),
                ]
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                out, err = await proc.communicate()
                if proc.returncode == 0:
                    msg = (out.decode("utf-8", "ignore").strip().splitlines() or ["ok"])[0]
                    print(f"{B}[COPY]{RS} refreshed: {msg}")
                else:
                    em = err.decode("utf-8", "ignore").strip()[:180]
                    self._errors.tick("copyflow_refresh", print, err=em, every=5)
            except Exception as e:
                self._errors.tick("copyflow_refresh_loop", print, err=e, every=5)

    def _score_wallet_activity_24h(self, rows: list[dict], now_ts: int) -> tuple[int, int, float, float]:
        """Score one wallet from recent activity: settled count, wins, wr, roi."""
        by_cid = {}
        for e in rows:
            cid = (e.get("conditionId") or "").strip()
            if not cid:
                continue
            ts_raw = float(e.get("timestamp") or 0.0)
            ts = ts_raw / 1000.0 if ts_raw > 1e12 else ts_raw
            if ts <= 0 or now_ts - ts > COPYFLOW_INTEL_WINDOW_SEC:
                continue
            typ = (e.get("type") or "").upper()
            side = (e.get("side") or "").upper()
            usdc = float(e.get("usdcSize") or 0.0)
            d = by_cid.setdefault(cid, {"buy": 0.0, "redeem": 0.0, "last_buy_ts": 0.0})
            if typ in ("TRADE", "BUY", "PURCHASE") and side == "BUY":
                d["buy"] += max(0.0, usdc)
                d["last_buy_ts"] = max(d["last_buy_ts"], ts)
            elif typ == "REDEEM":
                d["redeem"] += max(0.0, usdc)
        settled = wins = 0
        buy_sum = redeem_sum = 0.0
        for d in by_cid.values():
            b = float(d["buy"])
            if b < 1.0:
                continue
            if now_ts - float(d["last_buy_ts"]) < COPYFLOW_INTEL_SETTLE_LAG_SEC:
                continue
            settled += 1
            buy_sum += b
            redeem_sum += float(d["redeem"])
            if float(d["redeem"]) > 0:
                wins += 1
        wr = (wins / settled) if settled > 0 else 0.0
        roi = ((redeem_sum - buy_sum) / buy_sum) if buy_sum > 0 else -1.0
        return settled, wins, wr, roi

    async def _cid_family(self, cid: str) -> tuple[str, int]:
        """Return (asset, duration) for a conditionId using cache + Gamma lookup."""
        if not cid:
            return "?", 0
        cached = self._cid_family_cache.get(cid)
        if cached:
            return cached
        asset, dur = "?", 0
        try:
            rows = await self._http_get_json(
                f"{GAMMA}/markets",
                params={"conditionId": cid},
                timeout=8,
            )
            m = rows[0] if isinstance(rows, list) and rows else (rows if isinstance(rows, dict) else {})
            slug = m.get("seriesSlug") or m.get("series_slug", "")
            if slug in SERIES:
                asset = SERIES[slug]["asset"]
                dur = int(SERIES[slug]["duration"])
            else:
                q = (m.get("question") or "").lower()
                if "bitcoin" in q:
                    asset = "BTC"
                elif "ethereum" in q:
                    asset = "ETH"
                elif "solana" in q:
                    asset = "SOL"
                elif "xrp" in q:
                    asset = "XRP"
                if "5m" in q:
                    dur = 5
                elif "15m" in q:
                    dur = 15
        except Exception:
            pass
        self._cid_family_cache[cid] = (asset, dur)
        return asset, dur

    def _wallet_family_metrics(
        self,
        rows: list[dict],
        now_ts: int,
        cid_family: dict[str, tuple[str, int]],
        target_families: set[str],
    ) -> dict[str, dict[str, float]]:
        """
        Compute family-specific performance for one wallet.
        Returns: fam_key -> {"settled24","wins24","wr24","roi24","settled_all","wins_all","wr_all","roi_all"}.
        """
        by_fam_cid: dict[tuple[str, str], dict[str, float]] = {}
        for e in rows:
            cid = (e.get("conditionId") or "").strip()
            if not cid:
                continue
            fam = cid_family.get(cid, ("?", 0))
            fam_key = f"{fam[0]}-{fam[1]}m"
            if fam_key not in target_families:
                continue
            ts_raw = float(e.get("timestamp") or 0.0)
            ts = ts_raw / 1000.0 if ts_raw > 1e12 else ts_raw
            typ = (e.get("type") or "").upper()
            side = (e.get("side") or "").upper()
            usdc = float(e.get("usdcSize") or 0.0)
            k = (fam_key, cid)
            d = by_fam_cid.setdefault(k, {"buy": 0.0, "redeem": 0.0, "last_buy_ts": 0.0})
            if typ in ("TRADE", "BUY", "PURCHASE") and side == "BUY":
                d["buy"] += max(0.0, usdc)
                d["last_buy_ts"] = max(d["last_buy_ts"], ts)
            elif typ == "REDEEM":
                d["redeem"] += max(0.0, usdc)

        out: dict[str, dict[str, float]] = {}
        for fam_key in target_families:
            settled24 = wins24 = settled_all = wins_all = 0
            buy24 = red24 = buy_all = red_all = 0.0
            for (fk, _cid), d in by_fam_cid.items():
                if fk != fam_key:
                    continue
                b = float(d["buy"])
                if b < 1.0:
                    continue
                last_buy_ts = float(d["last_buy_ts"])
                age = now_ts - last_buy_ts
                if age < COPYFLOW_INTEL_SETTLE_LAG_SEC:
                    continue
                # "all" means all available history in fetched activity rows.
                settled_all += 1
                buy_all += b
                red_all += float(d["redeem"])
                if float(d["redeem"]) > 0:
                    wins_all += 1
                # 24h family performance.
                if age <= COPYFLOW_INTEL_WINDOW_SEC:
                    settled24 += 1
                    buy24 += b
                    red24 += float(d["redeem"])
                    if float(d["redeem"]) > 0:
                        wins24 += 1
            wr24 = (wins24 / settled24) if settled24 > 0 else 0.0
            roi24 = ((red24 - buy24) / buy24) if buy24 > 0 else -1.0
            wr_all = (wins_all / settled_all) if settled_all > 0 else 0.0
            roi_all = ((red_all - buy_all) / buy_all) if buy_all > 0 else -1.0
            out[fam_key] = {
                "settled24": float(settled24),
                "wins24": float(wins24),
                "wr24": float(wr24),
                "roi24": float(roi24),
                "settled_all": float(settled_all),
                "wins_all": float(wins_all),
                "wr_all": float(wr_all),
                "roi_all": float(roi_all),
            }
        return out

    async def _copyflow_intel_loop(self):
        """Discover and rank currently winning wallets from active markets (rolling 24h)."""
        if not COPYFLOW_INTEL_ENABLED or DRY_RUN:
            return
        while True:
            await asyncio.sleep(COPYFLOW_INTEL_REFRESH_SEC)
            try:
                cids = list(self.active_mkts.keys())[: max(1, COPYFLOW_LIVE_MAX_MARKETS)]
                if not cids:
                    continue
                cid_family: dict[str, tuple[str, int]] = {}
                target_families: set[str] = set()
                for cid in cids:
                    m = self.active_mkts.get(cid, {})
                    a = (m.get("asset") or "?").upper()
                    d = int(m.get("duration") or 0)
                    if a != "?" and d in (5, 15):
                        fam = (a, d)
                    else:
                        fam = await self._cid_family(cid)
                    cid_family[cid] = fam
                    target_families.add(f"{fam[0]}-{fam[1]}m")
                tasks = [
                    self._http_get_json(
                        "https://data-api.polymarket.com/trades",
                        params={"market": cid, "limit": str(COPYFLOW_INTEL_TRADES_PER_MARKET)},
                        timeout=10,
                    )
                    for cid in cids
                ]
                rows_list = await asyncio.gather(*tasks, return_exceptions=True)
                wallet_rank = {}
                for rows in rows_list:
                    if isinstance(rows, Exception) or not isinstance(rows, list):
                        continue
                    for tr in rows:
                        if (tr.get("side") or "").upper() != "BUY":
                            continue
                        w = (tr.get("proxyWallet") or "").lower().strip()
                        if not w or w == ADDRESS.lower():
                            continue
                        px = float(tr.get("price") or 0.0)
                        sz = float(tr.get("size") or 0.0)
                        usdc = max(0.0, px * sz)
                        d = wallet_rank.setdefault(w, {"n": 0.0, "usdc": 0.0})
                        d["n"] += 1.0
                        d["usdc"] += usdc
                if not wallet_rank:
                    continue
                ranked = sorted(
                    wallet_rank.items(),
                    key=lambda kv: (kv[1]["n"], kv[1]["usdc"]),
                    reverse=True,
                )[: max(5, COPYFLOW_INTEL_MAX_WALLETS)]
                act_tasks = [
                    self._http_get_json(
                        "https://data-api.polymarket.com/activity",
                        params={"user": w, "limit": str(COPYFLOW_INTEL_ACTIVITY_LIMIT)},
                        timeout=10,
                    )
                    for w, _ in ranked
                ]
                acts = await asyncio.gather(*act_tasks, return_exceptions=True)
                now_ts = int(_time.time())
                live_scores = {}
                family_scores: dict[str, dict[str, float]] = {k: {} for k in target_families}
                unknown_cids = set()
                for rows in acts:
                    if isinstance(rows, Exception) or not isinstance(rows, list):
                        continue
                    for e in rows:
                        cid = (e.get("conditionId") or "").strip()
                        if cid and cid not in cid_family and cid not in self._cid_family_cache:
                            unknown_cids.add(cid)
                if unknown_cids:
                    lookups = list(unknown_cids)[:300]
                    looked = await asyncio.gather(*[self._cid_family(cid) for cid in lookups], return_exceptions=True)
                    for cid, fam in zip(lookups, looked):
                        if isinstance(fam, tuple):
                            cid_family[cid] = fam
                for (w, agg), rows in zip(ranked, acts):
                    if isinstance(rows, Exception) or not isinstance(rows, list):
                        continue
                    settled, wins, wr, roi = self._score_wallet_activity_24h(rows, now_ts)
                    if settled < COPYFLOW_INTEL_MIN_SETTLED or roi < COPYFLOW_INTEL_MIN_ROI:
                        continue
                    sample_n = min(1.0, math.log1p(settled) / math.log1p(80))
                    wr_n = max(0.0, min(1.0, (wr - 0.50) / 0.30))
                    roi_n = max(0.0, min(1.0, (roi - COPYFLOW_INTEL_MIN_ROI) / 0.50))
                    mkt_aff = min(1.0, (agg["n"] / 12.0))
                    score = wr_n * 0.45 + roi_n * 0.35 + sample_n * 0.10 + mkt_aff * 0.10
                    if score > 0:
                        live_scores[w] = round(score, 4)
                    fam_metrics = self._wallet_family_metrics(rows, now_ts, cid_family, target_families)
                    for fam_key, fm in fam_metrics.items():
                        s24 = int(fm["settled24"])
                        sall = int(fm["settled_all"])
                        if s24 < COPYFLOW_INTEL_MIN_SETTLED_FAMILY_24H:
                            continue
                        if sall < COPYFLOW_INTEL_MIN_SETTLED_FAMILY_ALL:
                            continue
                        if fm["roi24"] < COPYFLOW_INTEL_MIN_ROI:
                            continue
                        wr24_n = max(0.0, min(1.0, (fm["wr24"] - 0.50) / 0.30))
                        roi24_n = max(0.0, min(1.0, (fm["roi24"] - COPYFLOW_INTEL_MIN_ROI) / 0.50))
                        wrall_n = max(0.0, min(1.0, (fm["wr_all"] - 0.50) / 0.30))
                        roiall_n = max(0.0, min(1.0, (fm["roi_all"] + 0.05) / 0.55))
                        sample24_n = min(1.0, math.log1p(s24) / math.log1p(40))
                        sampleall_n = min(1.0, math.log1p(sall) / math.log1p(180))
                        fam_score = (
                            (wr24_n * 0.35 + roi24_n * 0.30 + sample24_n * 0.10)
                            + (wrall_n * 0.15 + roiall_n * 0.05 + sampleall_n * 0.05)
                        )
                        if fam_score > 0:
                            family_scores.setdefault(fam_key, {})[w] = round(fam_score, 4)
                if live_scores:
                    self._copyflow_leaders_live = live_scores
                    if self._should_log("copyflow-intel", 120):
                        top = sorted(live_scores.items(), key=lambda kv: kv[1], reverse=True)[:3]
                        top_s = ", ".join(f"{w[:6]}..={s:.2f}" for w, s in top)
                        print(f"{B}[COPY-INTEL]{RS} leaders24h={len(live_scores)} top={top_s}")
                self._copyflow_leaders_family = family_scores
                if self._should_log("copyflow-intel-family", 150):
                    fam_parts = []
                    for fam_key, mp in sorted(family_scores.items()):
                        if not mp:
                            continue
                        topf = sorted(mp.items(), key=lambda kv: kv[1], reverse=True)[:1]
                        fam_parts.append(f"{fam_key}:{topf[0][0][:6]}..={topf[0][1]:.2f}")
                    if fam_parts:
                        print(f"{B}[COPY-FAMILY]{RS} " + " | ".join(fam_parts))
            except Exception as e:
                self._errors.tick("copyflow_intel_loop", print, err=e, every=10)

    async def _copyflow_live_loop(self):
        """Build per-market leader side-flow from latest PM trades for active markets."""
        if not COPYFLOW_LIVE_ENABLED or DRY_RUN:
            return
        while True:
            await asyncio.sleep(COPYFLOW_LIVE_REFRESH_SEC)
            try:
                leaders = dict(self._copyflow_leaders)
                for w, s in self._copyflow_leaders_live.items():
                    leaders[w] = max(float(s), float(leaders.get(w, 0.0) or 0.0))
                if not leaders:
                    continue
                cids = list(self.active_mkts.keys())[: max(1, COPYFLOW_LIVE_MAX_MARKETS)]
                if not cids:
                    continue

                tasks = [
                    self._http_get_json(
                        "https://data-api.polymarket.com/trades",
                        params={"market": cid, "limit": str(COPYFLOW_LIVE_TRADES_LIMIT)},
                        timeout=8,
                    )
                    for cid in cids
                ]
                rows_list = await asyncio.gather(*tasks, return_exceptions=True)
                updated = 0
                for cid, rows in zip(cids, rows_list):
                    if isinstance(rows, Exception) or not isinstance(rows, list):
                        continue
                    fam = self.active_mkts.get(cid, {})
                    fam_key = f"{(fam.get('asset') or '?').upper()}-{int(fam.get('duration') or 0)}m"
                    up = down = 0.0
                    low_w = mid_w = high_w = 0.0
                    px_w_sum = px_w_den = 0.0
                    wallet_buy_n = {}
                    for tr in rows:
                        if (tr.get("side") or "").upper() != "BUY":
                            continue
                        w = (tr.get("proxyWallet") or "").lower().strip()
                        fam_boost = float(self._copyflow_leaders_family.get(fam_key, {}).get(w, 0.0) or 0.0)
                        wt = max(float(leaders.get(w, 0.0) or 0.0), fam_boost * 1.35)
                        if wt <= 0:
                            continue
                        wallet_buy_n[w] = wallet_buy_n.get(w, 0) + 1
                        px = float(tr.get("price") or 0.0)
                        if px > 0:
                            px_w_sum += px * wt
                            px_w_den += wt
                            if px <= 0.30:
                                low_w += wt
                            elif px >= 0.70:
                                high_w += wt
                            else:
                                mid_w += wt
                        out = tr.get("outcome") or ""
                        if out == "Up":
                            up += wt
                        elif out == "Down":
                            down += wt
                    s = up + down
                    if s <= 0:
                        continue
                    self._copyflow_live[cid] = {
                        "Up": round(up / s, 4),
                        "Down": round(down / s, 4),
                        "n": int(round(s * 10)),
                        "avg_entry_c": round((px_w_sum / px_w_den) * 100.0, 1) if px_w_den > 0 else 0.0,
                        "low_c_share": round(low_w / max(1e-9, (low_w + mid_w + high_w)), 4),
                        "high_c_share": round(high_w / max(1e-9, (low_w + mid_w + high_w)), 4),
                        "multibet_ratio": round(
                            (
                                sum(1 for n in wallet_buy_n.values() if n >= 2)
                                / max(1, len(wallet_buy_n))
                            ),
                            4,
                        ),
                        "src": "live",
                    }
                    updated += 1
                if updated and self._should_log("copyflow-live", 60):
                    # Show one sample behavior snapshot to keep logs concise.
                    sample_cid = cids[0]
                    sm = self._copyflow_live.get(sample_cid, {})
                    if sm:
                        print(
                            f"{B}[COPY-LIVE]{RS} refreshed {updated} mkts | "
                            f"avg_entry={sm.get('avg_entry_c',0):.1f}c "
                            f"low/high={sm.get('low_c_share',0):.0%}/{sm.get('high_c_share',0):.0%} "
                            f"multibet={sm.get('multibet_ratio',0):.0%}"
                        )
                    else:
                        print(f"{B}[COPY-LIVE]{RS} refreshed {updated} active markets")
            except Exception as e:
                self._errors.tick("copyflow_live_loop", print, err=e, every=10)

    def _startup_self_check(self):
        rpc_rank = sorted(self._rpc_stats.items(), key=lambda x: x[1])[:3]
        rpc_str = ", ".join(f"{u.split('//')[-1]}={ms:.0f}ms" for u, ms in rpc_rank) if rpc_rank else "n/a"
        print(
            f"{B}[BOOT]{RS} "
            f"trade_all={TRADE_ALL_MARKETS} 5m={ENABLE_5M} assets={','.join(sorted(FIVE_MIN_ASSETS))} "
            f"score_gate(5m/15m)={MIN_SCORE_GATE_5M}/{MIN_SCORE_GATE_15M} "
            f"max_entry={MAX_ENTRY_PRICE:.2f}+tol{MAX_ENTRY_TOL:.2f} payout>={MIN_PAYOUT_MULT:.2f}x "
            f"near_miss_tol={ENTRY_NEAR_MISS_TOL:.3f} "
            f"ev_net>={MIN_EV_NET:.3f} fee={FEE_RATE_EST:.4f} "
            f"risk(max_open/same_dir/cid)={MAX_OPEN}/{MAX_SAME_DIR}/{MAX_CID_EXPOSURE_PCT:.0%} "
            f"block_opp(cid/round)={BLOCK_OPPOSITE_SIDE_SAME_CID}/{BLOCK_OPPOSITE_SIDE_SAME_ROUND}"
        )
        print(
            f"{B}[BOOT]{RS} "
            f"max_win_mode={MAX_WIN_MODE} "
            f"win_prob_min(5m/15m)={WINMODE_MIN_TRUE_PROB_5M:.2f}/{WINMODE_MIN_TRUE_PROB_15M:.2f} "
            f"win_entry_cap(5m/15m)={WINMODE_MAX_ENTRY_5M:.2f}/{WINMODE_MAX_ENTRY_15M:.2f} "
            f"win_edge_min={WINMODE_MIN_EDGE:.3f} cl_agree_required={WINMODE_REQUIRE_CL_AGREE}"
        )
        print(
            f"{B}[BOOT]{RS} "
            f"fast_mode={ORDER_FAST_MODE} maker_wait(5m/15m)={MAKER_WAIT_5M_SEC:.2f}/{MAKER_WAIT_15M_SEC:.2f}s "
            f"maker_poll(5m/15m)={MAKER_POLL_5M_SEC:.2f}/{MAKER_POLL_15M_SEC:.2f}s "
            f"near_end_fok(5m/15m)={FAST_TAKER_NEAR_END_5M_SEC:.0f}/{FAST_TAKER_NEAR_END_15M_SEC:.0f}s "
            f"fast_spread(5m/15m)<={FAST_TAKER_SPREAD_MAX_5M:.3f}/{FAST_TAKER_SPREAD_MAX_15M:.3f} "
            f"fast_score(5m/15m)>={FAST_TAKER_SCORE_5M}/{FAST_TAKER_SCORE_15M} "
            f"early_fok(5m/15m)<={FAST_TAKER_EARLY_WINDOW_SEC_5M:.0f}/{FAST_TAKER_EARLY_WINDOW_SEC_15M:.0f}s "
            f"rpc_probe={RPC_PROBE_COUNT} switch_margin={RPC_SWITCH_MARGIN_MS:.0f}ms"
        )
        print(
            f"{B}[BOOT]{RS} "
            f"latency<= {MAX_SIGNAL_LATENCY_MS:.0f}ms quote_stale<= {MAX_QUOTE_STALENESS_MS:.0f}ms "
            f"min_mins_left(5m/15m)={MIN_MINS_LEFT_5M:.1f}/{MIN_MINS_LEFT_15M:.1f} "
            f"extra_score(BTC5m/XRP15m)=+{EXTRA_SCORE_GATE_BTC_5M}/+{EXTRA_SCORE_GATE_XRP_15M} "
            f"exposure trend(total/side)={EXPOSURE_CAP_TOTAL_TREND:.0%}/{EXPOSURE_CAP_SIDE_TREND:.0%} "
            f"chop(total/side)={EXPOSURE_CAP_TOTAL_CHOP:.0%}/{EXPOSURE_CAP_SIDE_CHOP:.0%}"
        )
        print(
            f"{B}[BOOT]{RS} copyflow_live={COPYFLOW_LIVE_ENABLED} "
            f"copy_intel24h={COPYFLOW_INTEL_ENABLED} "
            f"intel_refresh={COPYFLOW_INTEL_REFRESH_SEC}s"
        )
        print(
            f"{B}[BOOT]{RS} scan_log_change_only={LOG_SCAN_ON_CHANGE_ONLY} "
            f"scan_heartbeat={LOG_SCAN_EVERY_SEC}s"
        )
        print(
            f"{B}[BOOT]{RS} log(flow/round-empty/settle/bank)="
            f"{LOG_FLOW_EVERY_SEC}/{LOG_ROUND_EMPTY_EVERY_SEC}/"
            f"{LOG_SETTLE_FIRST_EVERY_SEC}/{LOG_BANK_EVERY_SEC}s "
            f"bank_delta>={LOG_BANK_MIN_DELTA:.2f}"
        )
        print(
            f"{B}[BOOT]{RS} redeem_poll={REDEEM_POLL_SEC:.1f}s "
            f"force_redeem_scan={FORCE_REDEEM_SCAN_SEC}s"
        )
        print(f"{B}[BOOT]{RS} rpc={self._rpc_url or 'none'} | top={rpc_str}")

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
                self.seen = set(loaded[-2000:] if len(loaded) > 2000 else loaded)
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
        if not ADDRESS:
            raise RuntimeError("Missing POLY_ADDRESS/ADDRESS (wallet address)")
        key_hex = PRIVATE_KEY[2:] if PRIVATE_KEY.startswith("0x") else PRIVATE_KEY
        if PRIVATE_KEY and not re.fullmatch(r"[0-9a-fA-F]{64}", key_hex):
            raise RuntimeError("POLY_PRIVATE_KEY format invalid (expected 64 hex chars)")
        if not PRIVATE_KEY and not (POLY_API_KEY and POLY_API_SECRET and POLY_API_PASSPHRASE):
            raise RuntimeError(
                "Missing auth: set POLY_PRIVATE_KEY or POLY_API_KEY/POLY_API_SECRET/POLY_API_PASSPHRASE"
            )
        self.clob = ClobClient(
            host=CLOB_HOST,
            key=PRIVATE_KEY,
            chain_id=CHAIN_ID,
            signature_type=0,   # EOA (direct wallet, not proxy)
            funder=ADDRESS,     # address holding the USDC
        )
        # Prefer explicit API creds when provided; otherwise derive from private key.
        try:
            if POLY_API_KEY and POLY_API_SECRET and POLY_API_PASSPHRASE:
                creds = ApiCreds(
                    api_key=POLY_API_KEY,
                    api_secret=POLY_API_SECRET,
                    api_passphrase=POLY_API_PASSPHRASE,
                )
                self.clob.set_api_creds(creds)
                print(f"{G}[CLOB] API creds from env OK: {POLY_API_KEY[:8]}...{RS}")
            else:
                creds = self.clob.create_or_derive_api_creds()
                self.clob.set_api_creds(creds)
                print(f"{G}[CLOB] API creds OK: {creds.api_key[:8]}...{RS}")
        except Exception as e:
            print(f"{R}[CLOB] Creds error: {e}{RS}")
            raise RuntimeError("CLOB authentication failed (invalid/missing key or API creds)") from e

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

        # In paper mode, bankroll must stay simulated and never be overridden by CLOB/on-chain.
        if DRY_RUN:
            self.bankroll = BANKROLL
            self.start_bank = BANKROLL
            self.peak_bankroll = BANKROLL
            self.onchain_wallet_usdc = BANKROLL
            self.onchain_open_positions = 0.0
            self.onchain_open_count = 0
            self.onchain_redeemable_count = 0
            self.onchain_total_equity = BANKROLL
            print(f"{B}[PAPER]{RS} DRY_RUN bankroll fixed at ${BANKROLL:.2f} (no CLOB balance sync)")
        else:
            # Sync real USDC balance → override bankroll with actual on-chain value
            try:
                bal = self.clob.get_balance_allowance(
                    BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                )
                usdc = float(bal.get("balance", 0)) / 1e6
                allowances = bal.get("allowances", {})
                allow = max((float(v) for v in allowances.values()), default=0) / 1e6
                if usdc > 0:
                    self.bankroll = usdc
                    if (not self._pnl_baseline_locked) and (not PNL_BASELINE_RESET_ON_BOOT):
                        self.start_bank = usdc
                print(
                    f"{G}[CLOB] USDC balance: ${usdc:.2f}  allowance: ${allow:.2f}  "
                    f"→ bankroll set to ${self.bankroll:.2f}{RS}"
                )
                if usdc < 10:
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
    def _local_position_counts(self):
        """Local counters for logging: only active open positions and redeem queue size."""
        now_ts = _time.time()
        open_local = 0
        for _, (m, _) in list(self.pending.items()):
            end_ts = float(m.get("end_ts", 0) or 0)
            if end_ts <= 0 or end_ts > now_ts:
                open_local += 1
        return open_local, len(self.pending_redeem)

    def status(self):
        el   = datetime.now(timezone.utc) - self.start_time
        h, m = int(el.total_seconds()//3600), int(el.total_seconds()%3600//60)
        wr   = f"{self.wins/self.total*100:.1f}%" if self.total else "–"
        display_bank = self.onchain_total_equity if self.onchain_snapshot_ts > 0 else self.bankroll
        pnl  = display_bank - self.start_bank
        roi  = pnl / self.start_bank * 100 if self.start_bank > 0 else 0
        pc   = G if pnl >= 0 else R
        rs   = G if self.rtds_ok else R
        open_local, settling_local = self._local_position_counts()
        price_str = "  ".join(
            f"{B}{a}:{RS} ${p:,.2f}" for a, p in self.prices.items() if p > 0
        )
        print(
            f"\n{W}{'─'*66}{RS}\n"
            f"  {B}Time:{RS} {h}h{m}m  {rs}RTDS{'✓' if self.rtds_ok else '✗'}{RS}  "
            f"{B}Trades:{RS} {self.total}  {B}Win:{RS} {wr}  "
            f"{B}ROI:{RS} {pc}{roi:+.1f}%{RS}\n"
            f"  {B}Bankroll:{RS} ${display_bank:.2f}  "
            f"{B}P&L:{RS} {pc}${pnl:+.2f}{RS}  "
            f"{B}Network:{RS} {NETWORK}  "
            f"{B}Open(local/onchain):{RS} {open_local}/{self.onchain_open_count}  "
            f"{Y}Settling(local/onchain):{RS} {settling_local}/{self.onchain_redeemable_count}\n"
            f"  {price_str}\n"
            f"{W}{'─'*66}{RS}"
        )
        if self._perf_stats.get("score_n", 0) > 0 or self._perf_stats.get("order_n", 0) > 0:
            rpc_ms = self._rpc_stats.get(self._rpc_url, 0.0)
            print(
                f"  {B}Perf:{RS} score_ema={self._perf_stats.get('score_ms_ema', 0.0):.0f}ms "
                f"order_ema={self._perf_stats.get('order_ms_ema', 0.0):.0f}ms "
                f"{B}RPC:{RS} {self._rpc_url} ({rpc_ms:.0f}ms)"
            )
        if self._bucket_stats.rows:
            top_bucket = sorted(
                self._bucket_stats.rows.items(),
                key=lambda kv: kv[1].get("n", 0),
                reverse=True,
            )[0]
            k, v = top_bucket
            avg_slip = (v["slip_bps"] / v["fills"]) if v.get("fills", 0) else 0.0
            outcomes = int(v.get("outcomes", v.get("wins", 0) + v.get("losses", 0)))
            fills = int(v.get("fills", 0) or 0)
            wr_b = (v["wins"] / outcomes * 100.0) if outcomes > 0 else 0.0
            pf_b = (
                float(v.get("gross_win", 0.0)) / float(v.get("gross_loss", 1e-9))
                if float(v.get("gross_loss", 0.0)) > 0
                else (2.0 if float(v.get("gross_win", 0.0)) > 0 else 1.0)
            )
            exp_b = float(v.get("pnl", 0.0)) / max(1, outcomes)
            print(
                f"  {B}ExecQ:{RS} top={k} fills/outcomes={fills}/{outcomes} pf={pf_b:.2f} exp={exp_b:+.2f} "
                f"wr={wr_b:.1f}% avg_slip={avg_slip:.1f}bps pnl={v['pnl']:+.2f}"
            )
        # Show each open position with current win/loss status
        now_ts = _time.time()
        for cid, (m, t) in list(self.pending.items()):
            self._force_expired_from_question_if_needed(m, t)
            self._apply_exact_window_from_question(m, t)
            asset      = t.get("asset", "?")
            side       = t.get("side", "?")
            size       = t.get("size", 0)
            rk         = self._round_key(cid=cid, m=m, t=t)
            # Use market reference price (Chainlink at market open = Polymarket "price to beat")
            # NOT the bot's trade entry price — market determines outcome from its own start
            open_p     = self.open_prices.get(cid, 0)
            src        = self.open_prices_source.get(cid, "?")
            # If no open price yet, try Polymarket API inline (best effort)
            if open_p <= 0:
                start_ts_m = m.get("start_ts", 0)
                end_ts_m   = m.get("end_ts", 0)
                dur_m      = int(t.get("duration") or m.get("duration") or 15)
                if start_ts_m > 0 and end_ts_m > 0:
                    # status() is sync: avoid blocking I/O here.
                    # open price will be refreshed by scan/market loops.
                    _ = (start_ts_m, end_ts_m, dur_m)
            # Use Chainlink (resolution source) for win/loss; fall back to RTDS if unavailable
            cl_p       = self.cl_prices.get(asset, 0)
            cur_p      = cl_p if cl_p > 0 else self.prices.get(asset, 0)
            end_ts     = t.get("end_ts", 0)
            mins_left  = max(0, (end_ts - now_ts) / 60)
            title      = m.get("question", "")[:38]
            if open_p > 0 and src == "?":
                src = "CL" if cl_p > 0 else "RTDS"
            if open_p > 0 and cur_p > 0:
                pred_winner = "Up" if cur_p >= open_p else "Down"
                projected_win = (side == pred_winner)
                move_pct   = (cur_p - open_p) / open_p * 100
                c          = Y
                status_str = "LIVE"
                payout_est = size / t.get("entry", 0.5) if projected_win else 0
                move_str   = f"({move_pct:+.2f}%)"
                proj_str   = "LEAD" if projected_win else "TRAIL"
            else:
                c          = Y
                status_str = "UNSETTLED"
                payout_est = 0
                move_str   = "(no ref)"
                proj_str   = "NA"
            tok_price = t.get("entry", 0)
            tok_str   = f"@{tok_price*100:.0f}¢→{(1/tok_price):.2f}x" if tok_price > 0 else "@?¢"
            print(f"  {c}[{status_str}]{RS} {asset} {side} | {title} | "
                  f"beat={open_p:.4f}[{src}] now={cur_p:.4f} {move_str} | "
                  f"bet=${size:.2f} {tok_str} est=${payout_est:.2f} proj={proj_str} | "
                  f"{mins_left:.1f}min left | rk={rk} cid={self._short_cid(cid)}")
        # Show settling (pending_redeem) positions
        for cid, val in list(self.pending_redeem.items()):
            if isinstance(val[0], dict):
                m_r, t_r = val
                asset_r = t_r.get("asset", "?")
                side_r  = t_r.get("side", "?")
                size_r  = t_r.get("size", 0)
                title_r = m_r.get("question", "")[:38]
                rk_r = self._round_key(cid=cid, m=m_r, t=t_r)
            else:
                side_r, asset_r = val
                size_r = 0; title_r = ""
                rk_r = self._round_key(cid=cid, m={"asset": asset_r}, t={"asset": asset_r, "side": side_r})
            elapsed_r = (_time.time() - self._redeem_queued_ts.get(cid, _time.time())) / 60
            print(f"  {Y}[SETTLING]{RS} {asset_r} {side_r} | {title_r} | bet=${size_r:.2f} | "
                  f"waiting {elapsed_r:.0f}min | rk={rk_r} cid={self._short_cid(cid)}")

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
                                if now_t - self._last_eval_time.get(cid, 0) < 0.25: continue
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
        rpc_epoch_local = -1
        loop = asyncio.get_event_loop()
        while True:
            if self.w3 is None:
                await asyncio.sleep(3)
                continue
            if rpc_epoch_local != self._rpc_epoch or not contracts:
                contracts = {}
                for asset, addr in CHAINLINK_FEEDS.items():
                    try:
                        contracts[asset] = self.w3.eth.contract(
                            address=Web3.to_checksum_address(addr), abi=CHAINLINK_ABI
                        )
                    except Exception as e:
                        print(f"{Y}[CL] {asset} contract error: {e}{RS}")
                rpc_epoch_local = self._rpc_epoch
                if contracts:
                    ok_assets = list(contracts.keys())
                    print(f"{G}[CL] Chainlink feeds: {', '.join(ok_assets)} | rpc={self._rpc_url}{RS}")
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

    async def _rpc_optimizer_loop(self):
        """Continuously measure RPC latency and switch to fastest healthy endpoint."""
        while True:
            await asyncio.sleep(RPC_OPTIMIZE_SEC)
            try:
                loop = asyncio.get_event_loop()
                best_rpc = self._rpc_url
                best_ms = self._rpc_stats.get(best_rpc, 1e18)
                for rpc in POLYGON_RPCS:
                    try:
                        _w3 = self._build_w3(rpc, timeout=4)
                        samples = []
                        for _ in range(max(1, RPC_PROBE_COUNT)):
                            t0 = _time.perf_counter()
                            await loop.run_in_executor(None, lambda w=_w3: w.eth.block_number)
                            samples.append((_time.perf_counter() - t0) * 1000.0)
                        samples.sort()
                        ms = samples[len(samples) // 2]
                        self._rpc_stats[rpc] = ms
                        if ms < best_ms:
                            best_ms = ms
                            best_rpc = rpc
                    except Exception:
                        continue
                current_ms = self._rpc_stats.get(self._rpc_url, 1e18)
                if best_rpc and best_rpc != self._rpc_url and best_ms + RPC_SWITCH_MARGIN_MS < current_ms:
                    try:
                        nw3 = self._build_w3(best_rpc, timeout=6)
                        _ = await asyncio.get_event_loop().run_in_executor(None, lambda: nw3.eth.block_number)
                        self.w3 = nw3
                        self._rpc_url = best_rpc
                        self._rpc_epoch += 1
                        self._nonce_mgr = NonceManager(self.w3, ADDRESS)
                        print(f"{G}[RPC] Switched to fastest: {best_rpc} ({best_ms:.0f}ms){RS}")
                    except Exception:
                        pass
            except Exception:
                pass

    def _current_price(self, asset: str) -> float:
        """For direction decisions: RTDS (real-time) is current price.
        Chainlink is used only as open_price baseline (set once at market start).
        Fallback to Chainlink if RTDS not yet streaming."""
        rtds = self.prices.get(asset, 0)
        if rtds > 0:
            return rtds
        return self.cl_prices.get(asset, 0)

    def _kelly_size(self, true_prob: float, entry: float, kelly_frac: float) -> float:
        """Raw Kelly bet size before execution/risk floors and caps."""
        if entry <= 0 or entry >= 1:
            return max(0.0, self.bankroll * 0.01)
        b = (1 / entry) - 1
        q = 1 - true_prob
        kelly_f = max(0.0, (true_prob * b - q) / b)
        size  = self.bankroll * kelly_f * kelly_frac * self._kelly_drawdown_scale()
        cap   = self.bankroll * MAX_BANKROLL_PCT
        return round(max(0.0, min(cap, size)), 2)

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
        score_started = _time.perf_counter()
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
        last_tick_ts = self.price_history.get(asset, deque(maxlen=1))[-1][0] if self.price_history.get(asset) else 0
        quote_age_ms = (_time.time() - last_tick_ts) * 1000.0 if last_tick_ts else 9e9
        if quote_age_ms > MAX_QUOTE_STALENESS_MS:
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
        if STRICT_PM_SOURCE and open_src != "PM":
            return None
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
            prior = min(0.68, 0.58 + prev_win_move * 10)   # conservative continuation prior
            if direction == "Up":
                prob_up   = max(prob_up, prior)
                prob_down = 1 - prob_up
            else:
                prob_down = max(prob_down, prior)
                prob_up   = 1 - prob_down

        # Online calibration: shrink overconfident probabilities toward 50% when live WR degrades.
        shrink = self._prob_shrink_factor()
        prob_up = 0.5 + (prob_up - 0.5) * shrink
        prob_up = max(0.05, min(0.95, prob_up))
        prob_down = 1.0 - prob_up

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

        # Max-win profile: bias to higher posterior probability side, not continuation side.
        if MAX_WIN_MODE:
            if prob_up >= prob_down:
                side, edge, true_prob = "Up", edge_up, prob_up
            else:
                side, edge, true_prob = "Down", edge_down, prob_down

        # Optional copyflow signal from externally ranked leader wallets on same market.
        copy_adj = 0
        copy_net = 0.0
        flow = self._copyflow_live.get(cid) or self._copyflow_map.get(cid, {})
        if isinstance(flow, dict):
            up_conf = float(flow.get("Up", flow.get("up", 0.0)) or 0.0)
            dn_conf = float(flow.get("Down", flow.get("down", 0.0)) or 0.0)
            pref = up_conf if side == "Up" else dn_conf
            opp = dn_conf if side == "Up" else up_conf
            copy_net = pref - opp
            copy_adj = int(round(max(-COPYFLOW_BONUS_MAX, min(COPYFLOW_BONUS_MAX, copy_net * COPYFLOW_BONUS_MAX))))
            score += copy_adj
            edge += copy_net * 0.01
            # Leader behavior style signal: are winners buying cheap or paying high cents?
            avg_c = float(flow.get("avg_entry_c", 0.0) or 0.0)
            low_share = float(flow.get("low_c_share", 0.0) or 0.0)
            high_share = float(flow.get("high_c_share", 0.0) or 0.0)
            multibet_ratio = float(flow.get("multibet_ratio", 0.0) or 0.0)
            if avg_c > 0 and high_share >= 0.60 and multibet_ratio >= 0.30:
                score += 1
                edge += 0.005
            elif avg_c > 0 and low_share >= 0.60 and (up_price if side == "Up" else (1 - up_price)) > 0.55:
                # Winners buying low-cents but market now expensive: fade confidence.
                score -= 1
                edge -= 0.005

        # Pre-bid arm: for first seconds after open, bias side/execution to pre-planned signal.
        arm = self._prebid_plan.get(cid, {})
        arm_active = False
        if isinstance(arm, dict) and PREBID_ARM_ENABLED:
            now_ts = _time.time()
            start_ts = float(arm.get("start_ts", 0) or 0)
            conf = float(arm.get("conf", 0.0) or 0.0)
            arm_side = arm.get("side", "")
            if start_ts > 0 and now_ts >= start_ts and (now_ts - start_ts) <= PREBID_ARM_WINDOW_SEC and conf >= PREBID_MIN_CONF:
                arm_active = True
                if arm_side in ("Up", "Down") and arm_side != side:
                    side = arm_side
                    entry = up_price if side == "Up" else (1 - up_price)
                score += 2
                edge += max(0.0, conf - 0.5) * 0.05

        # Bet the signal direction — EV_net and execution quality drive growth.
        entry = up_price if side == "Up" else (1 - up_price)
        # Keep edge/prob aligned with final side after pre-bid/copyflow adjustments.
        base_prob = prob_up if side == "Up" else prob_down
        base_edge = edge_up if side == "Up" else edge_down
        true_prob = max(true_prob, base_prob) if MAX_WIN_MODE else base_prob
        edge = max(edge, base_edge) if MAX_WIN_MODE else base_edge

        if MAX_WIN_MODE:
            if WINMODE_REQUIRE_CL_AGREE and not cl_agree:
                return None
            min_prob_req = WINMODE_MIN_TRUE_PROB_5M if duration <= 5 else WINMODE_MIN_TRUE_PROB_15M
            if true_prob < min_prob_req:
                return None
            if edge < WINMODE_MIN_EDGE:
                return None

        # ── Score gate (duration-aware) ─────────────────────────────────────
        min_score_local = MIN_SCORE_GATE
        if duration <= 5:
            min_score_local = max(min_score_local, MIN_SCORE_GATE_5M)
        else:
            min_score_local = max(min_score_local, MIN_SCORE_GATE_15M)
        if arm_active:
            min_score_local = max(0, min_score_local - 2)
        if score < min_score_local:
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
        # Dynamic max entry: base + tolerance + small conviction slack.
        score_slack = 0.02 if score >= 12 else (0.01 if score >= 9 else 0.0)
        max_entry_allowed = min(0.97, MAX_ENTRY_PRICE + MAX_ENTRY_TOL + score_slack)
        min_entry_allowed = 0.01
        if duration <= 5:
            max_entry_allowed = min(max_entry_allowed, MAX_ENTRY_PRICE_5M)
            min_entry_allowed = max(min_entry_allowed, MIN_ENTRY_PRICE_5M)
            if MAX_WIN_MODE:
                max_entry_allowed = min(max_entry_allowed, WINMODE_MAX_ENTRY_5M)
        elif MAX_WIN_MODE:
            max_entry_allowed = min(max_entry_allowed, WINMODE_MAX_ENTRY_15M)
        # Adaptive model-consistent cap: if conviction is high, allow higher entry as long
        # expected value after fees remains positive.
        min_ev_base = MIN_EV_NET_5M if duration <= 5 else MIN_EV_NET
        model_cap = true_prob / max(1.0 + FEE_RATE_EST + max(0.003, min_ev_base), 1e-9)
        if score >= 9:
            max_entry_allowed = max(max_entry_allowed, min(0.85, model_cap))
        # Adaptive protection against poor payout fills from realized outcomes.
        min_payout_req, min_ev_req, adaptive_hard_cap = self._adaptive_thresholds(duration)
        max_entry_allowed = min(max_entry_allowed, adaptive_hard_cap)
        # High-conviction 15m mode: target lower cents early for better payout.
        hc15 = (
            HC15_ENABLED and duration == 15 and
            score >= HC15_MIN_SCORE and
            true_prob >= HC15_MIN_TRUE_PROB and
            edge >= HC15_MIN_EDGE
        )
        if hc15 and pct_remaining > HC15_FALLBACK_PCT_LEFT and live_entry > HC15_TARGET_ENTRY:
            use_limit = True
            entry = min(HC15_TARGET_ENTRY, max_entry_allowed)
        else:
            if min_entry_allowed <= live_entry <= max_entry_allowed:
                entry = live_entry
            elif PULLBACK_LIMIT_ENABLED and pct_remaining >= PULLBACK_LIMIT_MIN_PCT_LEFT:
                # Don't miss good-payout setups: park a pullback limit at max acceptable entry.
                use_limit = True
                entry = max_entry_allowed
            else:
                if LOG_VERBOSE:
                    print(f"{Y}[SKIP] {asset} {side} entry={live_entry:.3f} outside [{min_entry_allowed:.2f},{max_entry_allowed:.2f}]{RS}")
                return None

        payout_mult = 1.0 / max(entry, 1e-9)
        if payout_mult < min_payout_req:
            if LOG_VERBOSE:
                print(f"{Y}[SKIP] {asset} {side} payout={payout_mult:.2f}x < min={min_payout_req:.2f}x{RS}")
            return None
        ev_net = (true_prob / max(entry, 1e-9)) - 1.0 - FEE_RATE_EST
        if ev_net < min_ev_req:
            if LOG_VERBOSE:
                print(f"{Y}[SKIP] {asset} {side} ev_net={ev_net:.3f} < min={min_ev_req:.3f}{RS}")
            return None
        if self._should_log("flow-thresholds", LOG_FLOW_EVERY_SEC):
            print(
                f"{B}[FLOW]{RS} "
                f"payout>={min_payout_req:.2f}x ev>={min_ev_req:.3f} "
                f"entry=[{min_entry_allowed:.2f},{max_entry_allowed:.2f}]"
            )
        fresh_cl_disagree = (not cl_agree) and (cl_age_s is not None) and (cl_age_s <= 45)

        # ── ENTRY PRICE TIERS ─────────────────────────────────────────────────
        # Higher payout (cheaper tokens) gets larger Kelly fraction.
        # Expensive tokens still traded but with smaller exposure.
        if entry <= 0.20:
            if   score >= 12: kelly_frac, bankroll_pct = 0.20, 0.10
            elif score >= 8:  kelly_frac, bankroll_pct = 0.16, 0.08
            else:             kelly_frac, bankroll_pct = 0.12, 0.06
        elif entry <= 0.30:
            if   score >= 12: kelly_frac, bankroll_pct = 0.16, 0.08
            elif score >= 8:  kelly_frac, bankroll_pct = 0.12, 0.06
            else:             kelly_frac, bankroll_pct = 0.10, 0.05
        elif entry <= 0.40:
            if   score >= 12: kelly_frac, bankroll_pct = 0.12, 0.06
            elif score >= 8:  kelly_frac, bankroll_pct = 0.10, 0.05
            else:             kelly_frac, bankroll_pct = 0.08, 0.04
        elif entry <= 0.55:
            if   score >= 12: kelly_frac, bankroll_pct = 0.08, 0.04
            elif score >= 8:  kelly_frac, bankroll_pct = 0.06, 0.03
            else:             kelly_frac, bankroll_pct = 0.05, 0.025
        else:  # 0.55–0.85
            if   score >= 12: kelly_frac, bankroll_pct = 0.04, 0.02
            elif score >= 8:  kelly_frac, bankroll_pct = 0.03, 0.015
            else:             kelly_frac, bankroll_pct = 0.02, 0.010

        wr_scale   = self._wr_bet_scale()
        oracle_scale = 0.60 if fresh_cl_disagree else (0.80 if not cl_agree else 1.0)
        bucket_scale = self._bucket_size_scale(duration, score, entry)
        raw_size   = self._kelly_size(true_prob, entry, kelly_frac)
        max_single = min(100.0, self.bankroll * bankroll_pct)
        cid_cap    = max(0.50, self.bankroll * MAX_CID_EXPOSURE_PCT)
        hard_cap   = max(0.50, min(max_single, cid_cap, self.bankroll * MAX_BANKROLL_PCT))
        model_size = round(min(hard_cap, raw_size * vol_mult * wr_scale * oracle_scale * bucket_scale), 2)
        dyn_floor  = min(hard_cap, max(MIN_BET_ABS, self.bankroll * MIN_BET_PCT))
        # If model size is too small and setup is not top quality, skip instead of forcing a noisy tiny bet.
        if model_size < dyn_floor:
            hi_conf = (score >= 12 and true_prob >= 0.62 and edge >= 0.03)
            if not hi_conf:
                return None
            size = round(dyn_floor, 2)
        else:
            size = model_size
        size = max(0.50, min(hard_cap, size))

        # Immediate fills: FOK on strong signal, GTC limit otherwise
        # Limit orders (use_limit=True) are always GTC — force_taker stays False
        force_taker = (not use_limit) and (
            (score >= 12 and very_strong_mom and imbalance_confirms and move_pct > 0.0015) or
            (score >= 12 and is_early_continuation)
        )
        if FAST_EXEC_ENABLED and (not use_limit):
            if score >= FAST_EXEC_SCORE and edge >= FAST_EXEC_EDGE and entry <= MAX_ENTRY_PRICE:
                force_taker = True
        if arm_active:
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
            "max_entry_allowed": max_entry_allowed,
            "min_entry_allowed": min_entry_allowed,
            "copy_adj": copy_adj,
            "copy_net": copy_net,
            "quote_age_ms": quote_age_ms,
            "signal_latency_ms": (_time.perf_counter() - score_started) * 1000.0,
            "prebid_arm": arm_active,
        }

    async def _execute_trade(self, sig: dict):
        """Execute a pre-scored signal: log, mark seen, place order, update state."""
        cid = sig["cid"]
        m = sig["m"]
        side = sig.get("side", "")
        round_key = self._round_key(cid=cid, m=m, t=sig)
        round_fp = self._round_fingerprint(cid=cid, m=m, t=sig)
        round_side_key = f"{round_fp}|{side}"
        cid_side_key = f"{cid}|{side}"
        now_attempt = _time.time()
        async with self._exec_lock:
            # Trim expired round-side blocks.
            for k, exp in list(self._round_side_block_until.items()):
                if float(exp or 0) <= now_attempt:
                    self._round_side_block_until.pop(k, None)
            if cid in self.seen or cid in self._executing_cids or len(self.pending) >= MAX_OPEN:
                return
            if float(self._round_side_block_until.get(round_side_key, 0) or 0) > now_attempt:
                return
            # Prevent rapid-fire retries on the same round/side.
            last_try = float(self._round_side_attempt_ts.get(round_side_key, 0) or 0)
            if last_try > 0 and (now_attempt - last_try) < ROUND_RETRY_COOLDOWN_SEC:
                return
            # Also prevent duplicate retries on the same exact CID+side.
            last_try_cid_side = float(self._cid_side_attempt_ts.get(cid_side_key, 0) or 0)
            if last_try_cid_side > 0 and (now_attempt - last_try_cid_side) < ROUND_RETRY_COOLDOWN_SEC:
                return
            # Never re-enter same round/side if already in pending (handles cross-CID drift).
            for cid_p, (m_p, t_p) in list(self.pending.items()):
                if cid_p == cid and t_p.get("side") == side:
                    return
                rk_p = self._round_fingerprint(m=m_p, t=t_p)
                if rk_p == round_fp and t_p.get("side") == side:
                    return
            self._round_side_attempt_ts[round_side_key] = now_attempt
            self._cid_side_attempt_ts[cid_side_key] = now_attempt
            self._executing_cids.add(cid)
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
            cp_tag = f" copy={sig.get('copy_adj',0):+d}" if sig.get("copy_adj", 0) else ""
            print(f"{tag} {sig['asset']} {sig['duration']}m {sig['side']} | "
                  f"score={score} edge={sig['edge']:+.3f} size=${sig['size']:.2f} "
                  f"entry={sig['entry']:.3f} src={sig.get('open_price_source','?')} "
                  f"cl_age={sig.get('chainlink_age_s', -1):.0f}s{hc_tag}{cp_tag}{agree_str}{RS}")

        try:
            duration = int(sig.get("duration", 0) or 0)
            mins_left = float(sig.get("mins_left", 0.0) or 0.0)
            min_left_gate = MIN_MINS_LEFT_5M if duration <= 5 else MIN_MINS_LEFT_15M
            if mins_left <= min_left_gate:
                if LOG_VERBOSE:
                    print(
                        f"{Y}[SKIP]{RS} {sig['asset']} {duration}m {sig['side']} "
                        f"mins_left={mins_left:.1f} <= gate={min_left_gate:.1f}"
                    )
                return
            extra_score_gate = 0
            if sig.get("asset") == "BTC" and duration <= 5:
                extra_score_gate = max(extra_score_gate, EXTRA_SCORE_GATE_BTC_5M)
            if sig.get("asset") == "XRP" and duration > 5:
                extra_score_gate = max(extra_score_gate, EXTRA_SCORE_GATE_XRP_15M)
            if extra_score_gate > 0:
                base_gate = MIN_SCORE_GATE_5M if duration <= 5 else MIN_SCORE_GATE_15M
                req_score = base_gate + extra_score_gate
                if float(sig.get("score", 0.0) or 0.0) < req_score:
                    if LOG_VERBOSE:
                        print(
                            f"{Y}[SKIP]{RS} {sig['asset']} {duration}m {sig['side']} "
                            f"score={sig.get('score', 0):.0f} < req={req_score:.0f}"
                        )
                    return
            if sig.get("quote_age_ms", 0) > MAX_QUOTE_STALENESS_MS:
                # Soft gate: keep trading unless quote is extremely stale.
                if sig.get("quote_age_ms", 0) > MAX_QUOTE_STALENESS_MS * 3:
                    if LOG_VERBOSE:
                        print(f"{Y}[SKIP] very stale quote {sig['asset']} age={sig.get('quote_age_ms', 0):.0f}ms{RS}")
                    return
            if sig.get("signal_latency_ms", 0) > MAX_SIGNAL_LATENCY_MS:
                # Soft gate: keep coverage unless decision latency is extreme.
                if sig.get("signal_latency_ms", 0) > MAX_SIGNAL_LATENCY_MS * 2:
                    if LOG_VERBOSE:
                        print(f"{Y}[SKIP] extreme latency {sig['asset']} signal={sig.get('signal_latency_ms', 0):.0f}ms{RS}")
                    return
            t_ord = _time.perf_counter()
            exec_result = await self._place_order(
                sig["token_id"], sig["side"], sig["entry"], sig["size"],
                sig["asset"], sig["duration"], sig["mins_left"],
                sig["true_prob"], sig["cl_agree"],
                min_edge_req=sig["min_edge"], force_taker=sig["force_taker"],
                score=sig["score"], pm_book_data=sig.get("pm_book_data"),
                use_limit=sig.get("use_limit", False),
                max_entry_allowed=sig.get("max_entry_allowed", MAX_ENTRY_PRICE),
                hc15_mode=sig.get("hc15_mode", False),
                hc15_fallback_cap=HC15_FALLBACK_MAX_ENTRY,
            )
            self._perf_update("order_ms", (_time.perf_counter() - t_ord) * 1000.0)
            order_id = (exec_result or {}).get("order_id", "")
            actual_size_usdc = float((exec_result or {}).get("notional_usdc", sig["size"]) or sig["size"])
            fill_price = float((exec_result or {}).get("fill_price", sig["entry"]) or sig["entry"])
            slip_bps = ((fill_price - sig["entry"]) / max(sig["entry"], 1e-9)) * 10000.0
            stat_bucket = self._bucket_key(sig["duration"], sig["score"], sig["entry"])
            if order_id:
                self._bucket_stats.add_fill(stat_bucket, slip_bps)
            trade = {
                "side": sig["side"], "size": actual_size_usdc, "entry": sig["entry"],
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
                "bucket": stat_bucket,
                "fill_price": fill_price,
                "slip_bps": round(slip_bps, 2),
                "round_key": round_key,
                "placed_ts": _time.time(),
            }
            self._log_onchain_event("ENTRY", cid, {
                "asset": sig["asset"],
                "side": sig["side"],
                "score": sig["score"],
                "size_usdc": actual_size_usdc,
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
                "fill_price": round(fill_price, 6),
                "slippage_bps": round(slip_bps, 2),
                "signal_latency_ms": round(sig.get("signal_latency_ms", 0.0), 2),
                "quote_age_ms": round(sig.get("quote_age_ms", 0.0), 2),
                "bucket": stat_bucket,
                "round_key": round_key,
            })
            if order_id:
                self.seen.add(cid)
                self._save_seen()
                self.pending[cid] = (m, trade)
                block_until = float(m.get("end_ts", 0) or 0)
                if block_until <= now_attempt:
                    block_until = now_attempt + max(60.0, ROUND_RETRY_COOLDOWN_SEC * 2)
                self._round_side_block_until[round_side_key] = block_until + 15.0
                self._save_pending()
                self._log(m, trade)
        finally:
            async with self._exec_lock:
                self._executing_cids.discard(cid)

    async def evaluate(self, m: dict):
        """RTDS fast-path: score a single market and execute if score gate passes."""
        t0 = _time.perf_counter()
        sig = await self._score_market(m)
        self._perf_update("score_ms", (_time.perf_counter() - t0) * 1000.0)
        if sig and sig["score"] >= MIN_SCORE_GATE:
            await self._execute_trade(sig)

    async def _place_order(self, token_id, side, price, size_usdc, asset, duration, mins_left, true_prob=0.5, cl_agree=True, min_edge_req=None, force_taker=False, score=0, pm_book_data=None, use_limit=False, max_entry_allowed=None, hc15_mode=False, hc15_fallback_cap=0.36):
        """Maker-first order strategy:
        1. Post bid at mid-price (best_bid+best_ask)/2 — collect the spread
        2. Wait up to 45s for fill (other market evals run in parallel via asyncio)
        3. If unfilled, cancel and fall back to taker at best_ask+tick
        4. If taker unfilled after 3s, cancel and return None.
        Returns dict {order_id, fill_price, mode} or None."""
        if DRY_RUN:
            fake_id = f"DRY-{asset[:3]}-{int(datetime.now(timezone.utc).timestamp())}"
            # Simulate at AMM price (approximates maker fill quality)
            print(f"{Y}[DRY-RUN]{RS} {side} {asset} {duration}m | ${size_usdc:.2f} @ {price:.3f} | id={fake_id}")
            return {"order_id": fake_id, "fill_price": price, "mode": "dry"}

        for attempt in range(max(1, ORDER_RETRY_MAX)):
            try:
                loop = asyncio.get_event_loop()
                slip_cap_bps = MAX_TAKER_SLIP_BPS_5M if duration <= 5 else MAX_TAKER_SLIP_BPS_15M

                def _slip_bps(exec_price: float, ref_price: float) -> float:
                    return ((exec_price - ref_price) / max(ref_price, 1e-9)) * 10000.0

                def _normalize_order_size(exec_price: float, intended_usdc: float) -> tuple[float, float]:
                    min_shares = max(0.01, MIN_ORDER_SIZE_SHARES + ORDER_SIZE_PAD_SHARES)
                    raw_shares = max(0.0, intended_usdc) / max(exec_price, 1e-9)
                    shares = round(max(raw_shares, min_shares), 2)
                    notional = round(shares * exec_price, 2)
                    return shares, notional

                def _normalize_buy_amount(intended_usdc: float) -> float:
                    # Market BUY maker amount is USDC and must stay at 2 decimals.
                    return float(round(max(0.0, intended_usdc), 2))

                async def _post_limit_fok(exec_price: float) -> tuple[dict, float]:
                    # Strict instant execution with price cap to keep slippage near zero.
                    px = round(max(0.001, min(exec_price, 0.97)), 4)
                    px_order = round(px, 2)
                    amount_usdc = _normalize_buy_amount(size_usdc)
                    order_args = MarketOrderArgs(
                        token_id=token_id,
                        amount=float(amount_usdc),
                        side="BUY",
                        price=float(px_order),
                        order_type=OrderType.FOK,
                    )
                    signed = await loop.run_in_executor(None, lambda: self.clob.create_market_order(order_args))
                    resp = await loop.run_in_executor(None, lambda: self.clob.post_order(signed, OrderType.FOK))
                    return resp, float(px_order)

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
                # Ensure order notional always satisfies CLOB minimum size in shares.
                _, min_notional = _normalize_order_size(best_ask, 0.0)
                if size_usdc < min_notional:
                    if min_notional > self.bankroll:
                        print(
                            f"{Y}[SKIP]{RS} {asset} {side} min-order notional=${min_notional:.2f} "
                            f"> bankroll=${self.bankroll:.2f}"
                        )
                        return None
                    if LOG_VERBOSE:
                        print(
                            f"{Y}[SIZE-ADJ]{RS} {asset} {side} ${size_usdc:.2f} -> "
                            f"${min_notional:.2f} (min {MIN_ORDER_SIZE_SHARES:.0f} shares)"
                        )
                    size_usdc = min_notional
                edge_floor = min_edge_req if min_edge_req is not None else 0.04
                if not cl_agree:
                    edge_floor += 0.02
                base_spread_cap = MAX_BOOK_SPREAD_5M if duration <= 5 else MAX_BOOK_SPREAD_15M
                if (spread - base_spread_cap) > 1e-6 and not use_limit:
                    print(
                        f"{Y}[SKIP]{RS} {asset} {side} spread too wide: "
                        f"{spread:.3f} > cap={base_spread_cap:.3f}"
                    )
                    return None

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
                    print(f"{B}[EXEC-CHECK]{RS} {asset} {side} score={score} maker_edge={maker_edge_est:.3f} taker_edge={taker_edge:.3f}")
                else:
                    # Normal conviction: taker edge gate applies
                    if taker_edge < edge_floor:
                        kind = "disagree" if not cl_agree else "directional"
                        print(f"{Y}[SKIP] {asset} {side} [{kind}]: taker_edge={taker_edge:.3f} < {edge_floor:.2f} "
                              f"(ask={best_ask:.3f} model={true_prob:.3f}){RS}")
                        return None
                    print(f"{B}[EXEC-CHECK]{RS} {asset} {side} edge={taker_edge:.3f} floor={edge_floor:.2f}")

                # High conviction: skip maker, go straight to FOK taker for instant fill
                # FOK = Fill-or-Kill: fills completely at price or cancels instantly — no waiting
                if (not force_taker) and ORDER_FAST_MODE and (not use_limit):
                    eff_max_entry = max_entry_allowed if max_entry_allowed is not None else 0.99
                    fast_spread_cap = FAST_TAKER_SPREAD_MAX_5M if duration <= 5 else FAST_TAKER_SPREAD_MAX_15M
                    score_cap = FAST_TAKER_SCORE_5M if duration <= 5 else FAST_TAKER_SCORE_15M
                    secs_elapsed = max(0.0, duration * 60.0 - max(0.0, mins_left * 60.0))
                    early_cut = FAST_TAKER_EARLY_WINDOW_SEC_5M if duration <= 5 else FAST_TAKER_EARLY_WINDOW_SEC_15M
                    if (
                        score >= score_cap
                        and spread <= fast_spread_cap
                        and best_ask <= eff_max_entry
                        and taker_edge >= (edge_floor + 0.01)
                    ):
                        force_taker = True
                    elif (
                        secs_elapsed <= early_cut
                        and score >= (score_cap + 1)
                        and spread <= (fast_spread_cap + 0.005)
                        and best_ask <= eff_max_entry
                        and taker_edge >= (edge_floor + 0.015)
                    ):
                        force_taker = True
                    elif (
                        best_ask <= eff_max_entry
                        and taker_edge >= edge_floor
                        and (maker_edge_est - taker_edge) <= FAST_TAKER_EDGE_DIFF_MAX
                    ):
                        force_taker = True

                if force_taker:
                    taker_price = round(min(best_ask, max_entry_allowed or 0.99, 0.97), 4)
                    slip_now = _slip_bps(taker_price, price)
                    if slip_now > slip_cap_bps:
                        print(
                            f"{Y}[SLIP-GUARD]{RS} {asset} {side} fast-taker blocked: "
                            f"slip={slip_now:.1f}bps > cap={slip_cap_bps:.0f}bps"
                        )
                        force_taker = False
                    else:
                        print(
                            f"{G}[FAST-PATH]{RS} {asset} {side} spread={spread:.3f} "
                            f"score={score} slip={slip_now:.1f}bps -> limit FOK"
                        )
                        print(f"{G}[FAST-TAKER]{RS} {asset} {side} HIGH-CONV @ {taker_price:.3f} | ${size_usdc:.2f}")
                        resp, _ = await _post_limit_fok(taker_price)
                        order_id = resp.get("orderID") or resp.get("id", "")
                        if resp.get("status") in ("matched", "filled"):
                            self.bankroll -= size_usdc
                            print(f"{G}[FAST-FILL]{RS} {side} {asset} {duration}m | ${size_usdc:.2f} @ {taker_price:.3f} | Bank ${self.bankroll:.2f}")
                            return {"order_id": order_id, "fill_price": taker_price, "mode": "fok", "notional_usdc": size_usdc}
                        # FOK not filled (thin liquidity) — fall through to normal maker/taker flow
                        print(f"{Y}[FOK] unfilled — falling back to maker{RS}")
                        force_taker = False  # reset so we don't loop

                # Near market close, prioritize instant execution over maker wait.
                # Missing the window costs more than paying a small spread.
                if (not force_taker) and ORDER_FAST_MODE:
                    secs_left = max(0.0, mins_left * 60.0)
                    near_end_cut = FAST_TAKER_NEAR_END_5M_SEC if duration <= 5 else FAST_TAKER_NEAR_END_15M_SEC
                    if secs_left <= near_end_cut and taker_edge >= edge_floor and best_ask <= (max_entry_allowed or 0.99):
                        force_taker = True
                        taker_price = round(min(best_ask, max_entry_allowed or 0.99, 0.97), 4)
                        slip_now = _slip_bps(taker_price, price)
                        if slip_now > slip_cap_bps:
                            print(
                                f"{Y}[SLIP-GUARD]{RS} {asset} {side} near-end taker blocked: "
                                f"slip={slip_now:.1f}bps > cap={slip_cap_bps:.0f}bps"
                            )
                            force_taker = False
                        else:
                            print(f"{G}[FAST-TAKER]{RS} {asset} {side} near-end @ {taker_price:.3f} | ${size_usdc:.2f}")
                            resp, _ = await _post_limit_fok(taker_price)
                            order_id = resp.get("orderID") or resp.get("id", "")
                            if resp.get("status") in ("matched", "filled"):
                                self.bankroll -= size_usdc
                                print(f"{G}[FAST-FILL]{RS} {side} {asset} {duration}m | ${size_usdc:.2f} @ {taker_price:.3f} | Bank ${self.bankroll:.2f}")
                                return {"order_id": order_id, "fill_price": taker_price, "mode": "fok_near_end", "notional_usdc": size_usdc}
                            print(f"{Y}[FOK] near-end unfilled — fallback maker{RS}")
                            force_taker = False

                # ── PHASE 1: Maker bid ────────────────────────────────────
                mid          = (best_bid + best_ask) / 2
                if use_limit:
                    # True GTC limit at target price — will fill only on pullback
                    maker_price = round(max(price, tick), 4)
                else:
                    maker_price = round(min(mid + tick, best_ask - tick), 4)
                    maker_price = max(maker_price, tick)
                    entry_cap = round(min(0.97, price + tick * max(0, MAKER_ENTRY_TICK_TOL)), 4)
                    if maker_price > entry_cap:
                        maker_price = max(tick, entry_cap)
                maker_edge   = true_prob - maker_price
                maker_price_order = round(maker_price, 2)
                size_tok_m, maker_notional = _normalize_order_size(maker_price_order, size_usdc)
                size_usdc = maker_notional

                print(f"{G}[MAKER] {asset} {side}: bid={maker_price:.3f} "
                      f"ask={best_ask:.3f} spread={spread:.3f} "
                      f"maker_edge={maker_edge:.3f} taker_edge={taker_edge:.3f}{RS}")

                order_args = OrderArgs(
                    token_id=token_id,
                    price=float(maker_price_order),
                    size=float(round(size_tok_m, 2)),
                    side="BUY",
                )
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
                    return {"order_id": order_id, "fill_price": maker_price, "mode": "maker", "notional_usdc": size_usdc}

                # Ultra-low-latency waits to avoid blocking other opportunities.
                poll_interval = MAKER_POLL_5M_SEC if duration <= 5 else MAKER_POLL_15M_SEC
                base_wait = MAKER_WAIT_5M_SEC if duration <= 5 else MAKER_WAIT_15M_SEC
                max_wait = min(base_wait, max(0.25, mins_left * 60 * 0.02))
                if use_limit:
                    # Pullback limit: do not stall the cycle waiting on far-away price.
                    max_wait = min(max_wait, 0.35 if duration <= 5 else 0.60)
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
                    return {"order_id": order_id, "fill_price": maker_price, "mode": "maker", "notional_usdc": size_usdc}

                # Partial maker fill protection:
                # status may remain "live" while filled_size > 0. Track it so position is not missed.
                try:
                    info = await loop.run_in_executor(None, lambda: self.clob.get_order(order_id))
                    if isinstance(info, dict):
                        filled_sz = float(info.get("filled_size") or info.get("filledSize") or 0.0)
                        if filled_sz > 0:
                            fill_usdc = filled_sz * maker_price
                            if fill_usdc >= DUST_RECOVER_MIN:
                                self.bankroll -= min(size_usdc, fill_usdc)
                                print(f"{Y}[PARTIAL]{RS} {side} {asset} {duration}m | "
                                      f"filled≈${fill_usdc:.2f} @ {maker_price:.3f} | tracking open position")
                                return {"order_id": order_id, "fill_price": maker_price, "mode": "maker_partial", "notional_usdc": min(size_usdc, fill_usdc)}
                except Exception:
                    self._errors.tick("order_partial_check", print, every=50)

                # Cancel maker, fall back to taker with fresh book
                try:
                    await loop.run_in_executor(None, lambda: self.clob.cancel(order_id))
                except Exception:
                    pass

                # ── PHASE 2: price-capped FOK fallback — re-fetch book for fresh ask ──
                try:
                    fresh     = await loop.run_in_executor(None, lambda: self.clob.get_order_book(token_id))
                    f_asks    = sorted(fresh.asks, key=lambda x: float(x.price)) if fresh.asks else []
                    f_bids    = sorted(fresh.bids, key=lambda x: float(x.price), reverse=True) if fresh.bids else []
                    f_tick    = float(fresh.tick_size or tick)
                    fresh_ask = float(f_asks[0].price) if f_asks else best_ask
                    fresh_bid = float(f_bids[0].price) if f_bids else max(0.0, fresh_ask - f_tick)
                    fresh_spread = max(0.0, fresh_ask - fresh_bid)
                    if (fresh_spread - base_spread_cap) > 1e-6:
                        print(
                            f"{Y}[SKIP]{RS} {asset} {side} fallback spread too wide: "
                            f"{fresh_spread:.3f} > cap={base_spread_cap:.3f}"
                        )
                        return None
                    eff_max_entry = max_entry_allowed if max_entry_allowed is not None else MAX_ENTRY_PRICE
                    if fresh_ask > eff_max_entry:
                        if fresh_ask <= (eff_max_entry + ENTRY_NEAR_MISS_TOL):
                            # Near-miss tolerance: allow 1-2 ticks overshoot when still +EV.
                            if LOG_VERBOSE:
                                print(
                                    f"{Y}[NEAR-MISS]{RS} {asset} {side} ask={fresh_ask:.3f} "
                                    f"within tol={ENTRY_NEAR_MISS_TOL:.3f} over max_entry={eff_max_entry:.3f}"
                                )
                        else:
                            print(f"{Y}[SKIP] {asset} {side} pullback missed: ask={fresh_ask:.3f} > max_entry={eff_max_entry:.2f}{RS}")
                            return None
                    fresh_payout = 1.0 / max(fresh_ask, 1e-9)
                    min_payout_fb, min_ev_fb, _ = self._adaptive_thresholds(duration)
                    if fresh_payout < min_payout_fb:
                        print(f"{Y}[SKIP] {asset} {side} fallback payout={fresh_payout:.2f}x < min={min_payout_fb:.2f}x{RS}")
                        return None
                    fresh_ep  = true_prob - fresh_ask
                    if fresh_ep < edge_floor:
                        print(f"{Y}[SKIP] {asset} {side} taker: fresh ask={fresh_ask:.3f} edge={fresh_ep:.3f} < {edge_floor:.2f} — price moved against us{RS}")
                        return None
                    fresh_ev_net = (true_prob / max(fresh_ask, 1e-9)) - 1.0 - FEE_RATE_EST
                    if fresh_ev_net < min_ev_fb:
                        print(f"{Y}[SKIP] {asset} {side} fallback ev_net={fresh_ev_net:.3f} < min={min_ev_fb:.3f}{RS}")
                        return None
                    taker_price = round(min(fresh_ask, eff_max_entry, 0.97), 4)
                    slip_now = _slip_bps(taker_price, price)
                    if slip_now > slip_cap_bps:
                        print(
                            f"{Y}[SLIP-GUARD]{RS} {asset} {side} fallback taker blocked: "
                            f"slip={slip_now:.1f}bps > cap={slip_cap_bps:.0f}bps"
                        )
                        return None
                except Exception:
                    taker_price = round(min(best_ask, max_entry_allowed or 0.99, 0.97), 4)
                    fresh_ask   = best_ask
                print(f"{Y}[MAKER] unfilled — FOK taker @ {taker_price:.3f} (fresh ask={fresh_ask:.3f}){RS}")
                resp, _  = await _post_limit_fok(taker_price)
                order_id = resp.get("orderID") or resp.get("id", "")
                status   = resp.get("status", "")

                if order_id and status in ("matched", "filled"):
                    self.bankroll -= size_usdc
                    print(f"{Y}[TAKER FILL]{RS} {side} {asset} {duration}m | "
                          f"${size_usdc:.2f} @ {taker_price:.3f} | Bank ${self.bankroll:.2f}")
                    return {"order_id": order_id, "fill_price": taker_price, "mode": "fok_fallback", "notional_usdc": size_usdc}

                # FOK should not partially fill, but keep single state-check for exchange race.
                if order_id:
                    try:
                        info = await loop.run_in_executor(None, lambda: self.clob.get_order(order_id))
                        if isinstance(info, dict) and info.get("status") in ("matched", "filled"):
                            self.bankroll -= size_usdc
                            print(f"{Y}[TAKER FILL]{RS} {side} {asset} {duration}m | "
                                  f"${size_usdc:.2f} @ {taker_price:.3f} | Bank ${self.bankroll:.2f}")
                            return {"order_id": order_id, "fill_price": taker_price, "mode": "fok_fallback", "notional_usdc": size_usdc}
                    except Exception:
                        self._errors.tick("order_status_check", print, every=50)
                print(f"{Y}[ORDER] Both maker and FOK taker unfilled — cancelled{RS}")
                return None

            except Exception as e:
                err = str(e)
                err_l = err.lower()
                transient = (
                    "request exception" in err_l
                    or "timed out" in err_l
                    or "timeout" in err_l
                    or "connection reset" in err_l
                    or "temporarily unavailable" in err_l
                    or "502" in err_l
                    or "503" in err_l
                    or "504" in err_l
                )
                if "429" in err or "rate limit" in err_l:
                    wait = min(12.0, 2.0 * (attempt + 1))
                    print(f"{Y}[ORDER-RETRY]{RS} {asset} {side} rate-limit, retry in {wait:.1f}s ({attempt+1}/{ORDER_RETRY_MAX})")
                    await asyncio.sleep(wait)
                    continue
                if transient and attempt < (max(1, ORDER_RETRY_MAX) - 1):
                    wait = min(2.0, ORDER_RETRY_BASE_SEC * (attempt + 1))
                    print(
                        f"{Y}[ORDER-RETRY]{RS} {asset} {side} transient error "
                        f"({attempt+1}/{ORDER_RETRY_MAX}): {e} — retry in {wait:.2f}s"
                    )
                    await asyncio.sleep(wait)
                    continue
                self._errors.tick("place_order", print, err=e, every=10)
                print(f"{R}[ORDER FAILED]{RS} {asset} {side} after {attempt+1} attempts: {e}")
                return None
        return None

    # ── RESOLVE ───────────────────────────────────────────────────────────────
    async def _resolve(self):
        """Queue all expired positions for on-chain resolution.
        Never trust local price comparison — Polymarket resolves on Chainlink
        at the exact expiry timestamp, which may differ from current price."""
        for _, (m_fix, t_fix) in list(self.pending.items()):
            self._force_expired_from_question_if_needed(m_fix, t_fix)
            self._apply_exact_window_from_question(m_fix, t_fix)
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
                self._record_result(asset, trade["side"], won, trade.get("structural", False), pnl=pnl)
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
                    "round_key": self._round_key(cid=k, m=m, t=trade),
                })
                print(
                    f"{B}[RESOLVE]{RS} {asset} {trade['side']} {trade['duration']}m → on-chain queue "
                    f"(checking in {REDEEM_POLL_SEC:.1f}s) | rk={self._round_key(cid=k, m=m, t=trade)} "
                    f"cid={self._short_cid(k)}"
                )

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
            await asyncio.sleep(REDEEM_POLL_SEC)
            if not self.pending_redeem:
                continue
            positions_by_cid = {}
            try:
                pos_rows = await self._http_get_json(
                    "https://data-api.polymarket.com/positions",
                    params={"user": ADDRESS, "sizeThreshold": "0.01"},
                    timeout=8,
                )
                if isinstance(pos_rows, list):
                    positions_by_cid = {
                        p.get("conditionId", ""): p
                        for p in pos_rows if p.get("conditionId")
                    }
            except Exception:
                positions_by_cid = {}
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
                rk = self._round_key(cid=cid, m=m, t=trade)
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
                            print(
                                f"{Y}[WAIT]{RS} {asset} {side} ~${size:.2f} — awaiting oracle "
                                f"({elapsed:.0f}min) | rk={rk} cid={self._short_cid(cid)}"
                            )
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
                        tx_hash_full = ""
                        redeem_confirmed = False
                        usdc_before = 0.0
                        usdc_after = 0.0
                        try:
                            _raw_before = await loop.run_in_executor(
                                None, lambda: _usdc.functions.balanceOf(_addr_cs).call()
                            )
                            usdc_before = (_raw_before or 0) / 1e6
                        except Exception:
                            usdc_before = 0.0
                        try:
                            tx_hash = await self._submit_redeem_tx(
                                ctf=ctf, collat=collat, acct=acct,
                                cid_bytes=cid_bytes, index_set=(1 if side == "Up" else 2),
                                loop=loop
                            )
                            self._redeem_verify_counts.pop(cid, None)
                            tx_hash_full = tx_hash
                            redeem_confirmed = True
                            suffix = f"tx={tx_hash[:16]}"
                        except Exception:
                            # If still claimable, keep in queue and retry later (never miss redeem).
                            if await self._is_redeem_claimable(
                                ctf=ctf, collat=collat, acct_addr=acct.address,
                                cid_bytes=cid_bytes, index_set=(1 if side == "Up" else 2), loop=loop
                            ):
                                print(
                                    f"{Y}[REDEEM-RETRY]{RS} claimable but tx failed; will retry "
                                    f"{asset} {side} | rk={rk} cid={self._short_cid(cid)}"
                                )
                                continue
                            pos = positions_by_cid.get(cid, {})
                            pos_redeemable = bool(pos.get("redeemable", False))
                            pos_val = float(pos.get("currentValue", 0) or 0)
                            if pos_redeemable and pos_val >= 0.01:
                                print(
                                    f"{Y}[REDEEM-PENDING]{RS} still redeemable on API "
                                    f"(value=${pos_val:.2f}) | rk={rk} cid={self._short_cid(cid)}"
                                )
                                continue
                            if REDEEM_REQUIRE_ONCHAIN_CONFIRM:
                                # Strict mode: close only with explicit on-chain confirmation.
                                # If winning token balance is zero on-chain, there is nothing claimable
                                # in this wallet, so position can be finalized as non-wallet-redeem.
                                tok = str(trade.get("token_id", "") or "").strip()
                                tok_bal = -1
                                if tok.isdigit():
                                    try:
                                        tok_bal = await loop.run_in_executor(
                                            None,
                                            lambda ti=int(tok): ctf.functions.balanceOf(_addr_cs, ti).call(),
                                        )
                                    except Exception:
                                        tok_bal = -1
                                if tok_bal == 0:
                                    redeem_confirmed = True
                                    suffix = "onchain-no-wallet-balance"
                                else:
                                    print(
                                        f"{Y}[REDEEM-UNCONFIRMED]{RS} waiting on-chain confirm "
                                        f"(tx missing, token_balance={tok_bal}) | rk={rk} cid={self._short_cid(cid)}"
                                    )
                                    continue
                            else:
                                checks = int(self._redeem_verify_counts.get(cid, 0)) + 1
                                self._redeem_verify_counts[cid] = checks
                                if checks < 3:
                                    print(
                                        f"{Y}[REDEEM-VERIFY]{RS} non-claimable; waiting confirm "
                                        f"({checks}/3) | rk={rk} cid={self._short_cid(cid)}"
                                    )
                                    continue
                                self._redeem_verify_counts.pop(cid, None)

                        # Record win only after strict on-chain confirmation.
                        if REDEEM_REQUIRE_ONCHAIN_CONFIRM and not redeem_confirmed:
                            print(
                                f"{Y}[REDEEM-UNCONFIRMED]{RS} strict mode active; keeping in queue "
                                f"| rk={rk} cid={self._short_cid(cid)}"
                            )
                            continue

                        # Record confirmed win
                        try:
                            _raw = await loop.run_in_executor(
                                None, lambda: _usdc.functions.balanceOf(_addr_cs).call()
                            )
                            if _raw > 0:
                                usdc_after = _raw / 1e6
                                self.bankroll = usdc_after
                        except Exception:
                            pass
                        if usdc_after <= 0:
                            usdc_after = usdc_before
                        self.daily_pnl += pnl
                        self.total += 1; self.wins += 1
                        self._bucket_stats.add_outcome(trade.get("bucket", "unknown"), True, pnl)
                        self._record_result(asset, side, True, trade.get("structural", False), pnl=pnl)
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
                            "round_key": rk,
                        })
                        wr = f"{self.wins/self.total*100:.0f}%" if self.total else "–"
                        usdc_delta = usdc_after - usdc_before
                        print(f"{G}[WIN]{RS} {asset} {side} {trade.get('duration',0)}m | "
                              f"{G}${pnl:+.2f}{RS} | Bank ${self.bankroll:.2f} | WR {wr} | "
                              f"{suffix} | rk={rk} cid={self._short_cid(cid)}")
                        if tx_hash_full:
                            print(
                                f"{G}[REDEEMED-ONCHAIN]{RS} cid={self._short_cid(cid)} "
                                f"tx={tx_hash_full} usdc_delta=${usdc_delta:+.2f} "
                                f"({usdc_before:.2f}->{usdc_after:.2f})"
                            )
                        else:
                            print(
                                f"{Y}[REDEEMED-AUTO]{RS} cid={self._short_cid(cid)} "
                                f"usdc_delta=${usdc_delta:+.2f} ({usdc_before:.2f}->{usdc_after:.2f})"
                            )
                        done.append(cid)
                    else:
                        # Lost on-chain (on-chain is authoritative)
                        if size > 0:
                            pnl = -size
                            self.daily_pnl += pnl
                            self.total += 1
                            self._bucket_stats.add_outcome(trade.get("bucket", "unknown"), False, pnl)
                            self._record_result(asset, side, False, trade.get("structural", False), pnl=pnl)
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
                                "round_key": rk,
                            })
                            wr = f"{self.wins/self.total*100:.0f}%" if self.total else "–"
                            print(f"{R}[LOSS]{RS} {asset} {side} {trade.get('duration',0)}m | "
                                  f"{R}${pnl:+.2f}{RS} | Bank ${self.bankroll:.2f} | WR {wr} | "
                                  f"rk={rk} cid={self._short_cid(cid)}")
                        done.append(cid)
                except Exception as e:
                    print(f"{Y}[REDEEM] {asset}: {e}{RS}")
            changed_pending = False
            for cid in done:
                self.redeemed_cids.add(cid)
                self._redeem_verify_counts.pop(cid, None)
                self.pending_redeem.pop(cid, None)
                if self.pending.pop(cid, None) is not None:
                    changed_pending = True
                # Remove closed cid from live-log caches immediately.
                self._mkt_log_ts.pop(cid, None)
                self.open_prices.pop(cid, None)
                self.open_prices_source.pop(cid, None)
            if changed_pending:
                self._save_pending()

    async def _submit_redeem_tx(self, ctf, collat, acct, cid_bytes: bytes, index_set: int, loop):
        """Submit redeem tx with serialized nonce handling."""
        async with self._redeem_tx_lock:
            last_err = None
            for _ in range(4):
                try:
                    if self._nonce_mgr is None:
                        self._nonce_mgr = NonceManager(self.w3, acct.address)
                    nonce = await self._nonce_mgr.next_nonce(loop)
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
                        if self._nonce_mgr is not None:
                            await self._nonce_mgr.reset_from_chain(loop)
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
                        self._record_result(asset_p, side_p, False, t_p.get("structural", False), pnl=-size_p)
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
            q_st, q_et = self._round_bounds_from_question(title)
            if self._is_exact_round_bounds(q_st, q_et, duration):
                start_ts, end_ts = q_st, q_et
            elif q_et > 0 and q_et <= now:
                end_ts = q_et

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
                    if not self._is_exact_round_bounds(start_ts, end_ts, duration):
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
            except Exception as e:
                self._errors.tick("bnb_seed_depth", print, err=e, every=25)
            try:
                klines = await loop.run_in_executor(None, lambda s=sym_api: _req.get(
                    "https://api.binance.com/api/v3/klines",
                    params={"symbol": s, "interval": "1m", "limit": 33}, timeout=5).json())
                if isinstance(klines, list):
                    self.binance_cache[asset]["klines"] = klines
            except Exception as e:
                self._errors.tick("bnb_seed_klines", print, err=e, every=25)
            try:
                mark = await loop.run_in_executor(None, lambda s=sym_api: _req.get(
                    "https://fapi.binance.com/fapi/v1/premiumIndex",
                    params={"symbol": s}, timeout=5).json())
                self.binance_cache[asset]["mark"]    = float(mark.get("markPrice", 0))
                self.binance_cache[asset]["index"]   = float(mark.get("indexPrice", 0))
                self.binance_cache[asset]["funding"] = float(mark.get("lastFundingRate", 0))
            except Exception as e:
                self._errors.tick("bnb_seed_perp", print, err=e, every=25)
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

    def _bucket_key(self, duration: int, score: int, entry: float) -> str:
        score_bucket = "s12+" if score >= 12 else ("s9-11" if score >= 9 else "s0-8")
        entry_bucket = "<30c" if entry < 0.30 else ("30-50c" if entry <= 0.50 else ">50c")
        return f"{duration}m|{score_bucket}|{entry_bucket}"

    def _growth_snapshot(self) -> dict:
        rows = list(self._bucket_stats.rows.values())
        outcomes = sum(int(r.get("outcomes", r.get("wins", 0) + r.get("losses", 0))) for r in rows)
        gross_win = sum(float(r.get("gross_win", 0.0)) for r in rows)
        gross_loss = sum(float(r.get("gross_loss", 0.0)) for r in rows)
        pnl = sum(float(r.get("pnl", 0.0)) for r in rows)
        fills = sum(int(r.get("fills", 0)) for r in rows)
        slip = sum(float(r.get("slip_bps", 0.0)) for r in rows)
        pf = (gross_win / gross_loss) if gross_loss > 0 else (2.0 if gross_win > 0 else 1.0)
        expectancy = pnl / max(1, outcomes)
        avg_slip = slip / max(1, fills)
        recent_pnl = list(self.recent_pnl)[-20:]
        recent_pf = 1.0
        if recent_pnl:
            rp_win = sum(p for p in recent_pnl if p > 0)
            rp_loss = -sum(p for p in recent_pnl if p < 0)
            recent_pf = (rp_win / rp_loss) if rp_loss > 0 else (2.0 if rp_win > 0 else 1.0)
        return {
            "outcomes": outcomes,
            "gross_win": gross_win,
            "gross_loss": gross_loss,
            "pf": pf,
            "recent_pf": recent_pf,
            "expectancy": expectancy,
            "avg_slip": avg_slip,
        }

    def _bucket_size_scale(self, duration: int, score: int, entry: float) -> float:
        """Adaptive size control from realized EV quality by bucket."""
        k = self._bucket_key(duration, score, entry)
        r = self._bucket_stats.rows.get(k)
        if not r:
            return 1.0
        outcomes = int(r.get("outcomes", r.get("wins", 0) + r.get("losses", 0)))
        if outcomes < 8:
            return 1.0
        pnl = float(r.get("pnl", 0.0))
        gross_win = float(r.get("gross_win", 0.0))
        gross_loss = float(r.get("gross_loss", 0.0))
        pf = (gross_win / gross_loss) if gross_loss > 0 else (2.0 if gross_win > 0 else 1.0)
        exp = pnl / max(1, outcomes)
        fills = int(r.get("fills", 0))
        avg_slip = (float(r.get("slip_bps", 0.0)) / max(1, fills))
        scale = 1.0
        if outcomes >= 12 and (pf < 0.90 or exp < -0.25):
            scale = 0.45
        elif pf < 1.05 or exp < 0.0:
            scale = 0.70
        elif pf > 1.35 and exp > 0.30 and avg_slip < 120.0:
            scale = 1.15
        if avg_slip > 350.0:
            scale *= 0.75
        return max(0.40, min(1.20, scale))

    def _adaptive_thresholds(self, duration: int) -> tuple[float, float, float]:
        """Dynamic payout/EV/entry thresholds from realized quality (PF/expectancy/slippage)."""
        base_payout = MIN_PAYOUT_MULT_5M if duration <= 5 else MIN_PAYOUT_MULT
        base_ev = MIN_EV_NET_5M if duration <= 5 else MIN_EV_NET
        hard_cap = ENTRY_HARD_CAP_5M if duration <= 5 else ENTRY_HARD_CAP_15M

        snap = self._growth_snapshot()
        tightness = 0.0
        if snap["outcomes"] >= 10 and snap["pf"] < 1.0:
            tightness += min(1.2, (1.0 - snap["pf"]) * 1.5)
        if snap["outcomes"] >= 10 and snap["expectancy"] < 0:
            tightness += min(1.0, abs(snap["expectancy"]) / 0.8)
        if snap["avg_slip"] > 220:
            tightness += min(0.6, (snap["avg_slip"] - 220.0) / 600.0)
        if self.consec_losses >= 2:
            tightness += min(1.0, (self.consec_losses - 1) * 0.25)
        if snap["outcomes"] >= 12 and snap["pf"] > 1.35 and snap["expectancy"] > 0.30 and snap["avg_slip"] < 120:
            tightness -= 0.35
        tightness = min(1.8, max(-0.5, tightness))

        min_payout = max(1.55, base_payout + (0.20 * tightness))
        min_ev = max(0.005, base_ev + (0.015 * tightness))
        max_entry_hard = max(0.33, min(0.80, hard_cap - (0.06 * tightness)))

        return min_payout, min_ev, max_entry_hard

    def _prob_shrink_factor(self) -> float:
        """Calibrate model confidence to realized PnL quality (anti-overconfidence)."""
        snap = self._growth_snapshot()
        if snap["outcomes"] < 8:
            return 0.75
        shrink = 0.82
        if snap["pf"] < 1.0:
            shrink -= min(0.25, (1.0 - snap["pf"]) * 0.50)
        if snap["expectancy"] < 0:
            shrink -= min(0.20, abs(snap["expectancy"]) / 1.0)
        if snap["avg_slip"] > 240:
            shrink -= min(0.10, (snap["avg_slip"] - 240.0) / 900.0)
        if snap["pf"] > 1.40 and snap["expectancy"] > 0.20 and snap["avg_slip"] < 120:
            shrink += 0.05
        if self.consec_losses >= 2:
            shrink -= min(0.15, 0.04 * self.consec_losses)
        return max(0.40, min(0.92, shrink))

    def _signal_growth_score(self, sig: dict) -> float:
        """Rank candidates by growth quality (higher is better)."""
        entry = max(float(sig.get("entry", 0.5)), 1e-6)
        score = float(sig.get("score", 0))
        edge = float(sig.get("edge", 0.0))
        cl_bonus = 0.02 if sig.get("cl_agree", True) else -0.03
        payout = (1.0 / entry) - 1.0
        ev_net = (float(sig.get("true_prob", 0.5)) / entry) - 1.0 - FEE_RATE_EST
        q_age = float(sig.get("quote_age_ms", 0.0) or 0.0)
        s_lat = float(sig.get("signal_latency_ms", 0.0) or 0.0)
        lag_penalty = 0.0
        if q_age > 900:
            lag_penalty -= 0.03
        if s_lat > 550:
            lag_penalty -= 0.03
        return ev_net + edge * 0.35 + payout * 0.06 + score * 0.003 + cl_bonus + lag_penalty

    def _regime_caps(self) -> tuple[float, float]:
        vals = []
        for a in BNB_SYM:
            vals.append((self._variance_ratio(a), self._autocorr_regime(a)))
        if not vals:
            return EXPOSURE_CAP_TOTAL_CHOP, EXPOSURE_CAP_SIDE_CHOP
        trending = sum(1 for vr, ac in vals if vr > 1.02 and ac > 0.02)
        if trending >= max(1, len(vals) // 2):
            return EXPOSURE_CAP_TOTAL_TREND, EXPOSURE_CAP_SIDE_TREND
        return EXPOSURE_CAP_TOTAL_CHOP, EXPOSURE_CAP_SIDE_CHOP

    def _exposure_ok(self, sig: dict, active_pending: dict) -> bool:
        cid = sig.get("cid", "")
        side = sig.get("side", "")
        bankroll_ref = max(self.bankroll, 1.0)
        cid_cap = bankroll_ref * MAX_CID_EXPOSURE_PCT
        sig_m = sig.get("m", {}) if isinstance(sig.get("m", {}), dict) else {}
        sig_fp = self._round_fingerprint(cid=cid, m=sig_m, t=sig)
        same_cid = [(m, t) for c, (m, t) in active_pending.items() if c == cid]
        if same_cid:
            same_side_open = sum(max(0.0, t.get("size", 0.0)) for _, t in same_cid if t.get("side") == side)
            opposite_open = sum(max(0.0, t.get("size", 0.0)) for _, t in same_cid if t.get("side") != side)
            if BLOCK_OPPOSITE_SIDE_SAME_CID and opposite_open > 0:
                if LOG_VERBOSE:
                    print(f"{Y}[RISK] skip {sig.get('asset')} {side} opposite-side open on same cid{RS}")
                return False
            if same_side_open + sig.get("size", 0.0) > cid_cap:
                if LOG_VERBOSE:
                    print(f"{Y}[RISK] skip {sig.get('asset')} cid exposure {(same_side_open + sig.get('size', 0.0)):.2f}>{cid_cap:.2f}{RS}")
                return False
        same_round = [
            (c, m, t) for c, (m, t) in active_pending.items()
            if self._round_fingerprint(cid=c, m=m, t=t) == sig_fp
        ]
        if same_round:
            round_same_side = sum(max(0.0, t.get("size", 0.0)) for _, _, t in same_round if t.get("side") == side)
            round_opp_side = sum(max(0.0, t.get("size", 0.0)) for _, _, t in same_round if t.get("side") != side)
            if BLOCK_OPPOSITE_SIDE_SAME_ROUND and round_opp_side > 0:
                if LOG_VERBOSE:
                    print(f"{Y}[RISK] skip {sig.get('asset')} {side} opposite-side open on same round{RS}")
                return False
            if round_same_side + sig.get("size", 0.0) > cid_cap:
                if LOG_VERBOSE:
                    print(f"{Y}[RISK] skip {sig.get('asset')} round exposure {(round_same_side + sig.get('size', 0.0)):.2f}>{cid_cap:.2f}{RS}")
                return False
        total_cap, side_cap = self._regime_caps()
        total_open = sum(max(0.0, t.get("size", 0.0)) for _, t in active_pending.values())
        side_open = sum(
            max(0.0, t.get("size", 0.0))
            for _, t in active_pending.values()
            if t.get("side") == side
        )
        after_total = total_open + sig.get("size", 0.0)
        after_side = side_open + sig.get("size", 0.0)
        if after_total > bankroll_ref * total_cap:
            if LOG_VERBOSE:
                print(f"{Y}[RISK] skip {sig.get('asset')} total exposure {after_total/bankroll_ref:.0%}>{total_cap:.0%}{RS}")
            return False
        if after_side > bankroll_ref * side_cap:
            if LOG_VERBOSE:
                print(f"{Y}[RISK] skip {sig.get('asset')} {sig.get('side')} exposure {after_side/bankroll_ref:.0%}>{side_cap:.0%}{RS}")
            return False
        return True

    def _direction_bias(self, asset: str, side: str) -> float:
        """Small additive bias from realized side expectancy/profit factor."""
        k = f"{asset}|{side}"
        s = self.side_perf.get(k, {})
        n = int(s.get("n", 0))
        if n < 8:
            return 0.0
        gw = float(s.get("gross_win", 0.0))
        gl = float(s.get("gross_loss", 0.0))
        pnl = float(s.get("pnl", 0.0))
        pf = (gw / gl) if gl > 0 else (2.0 if gw > 0 else 1.0)
        exp = pnl / max(1, n)
        bias = (pf - 1.0) * 0.02 + (exp / 8.0)
        return max(-0.06, min(0.06, bias))

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

    def _wr_bet_scale(self) -> float:
        """EV/PF-only size controller for growth. No win-rate input."""
        pnl = list(self.recent_pnl)[-30:]
        if len(pnl) < 10:
            return 1.0
        gross_win = sum(p for p in pnl if p > 0)
        gross_loss = -sum(p for p in pnl if p < 0)
        pf = (gross_win / gross_loss) if gross_loss > 0 else (2.0 if gross_win > 0 else 1.0)
        exp = sum(pnl) / len(pnl)
        if pf >= 1.35 and exp >= 0.40:
            return 1.35
        if pf >= 1.25 and exp >= 0.25:
            return 1.20
        if pf < 1.10 or exp < 0.10:
            return 0.90
        return 1.0

    def _adaptive_min_edge(self) -> float:
        """CLOB edge sanity floor — keeps us from buying at obviously bad prices.
        Range: 3-8% only. Never blocks trading on its own."""
        snap = self._growth_snapshot()
        edge = 0.045
        if snap["avg_slip"] > 250:
            edge += min(0.015, (snap["avg_slip"] - 250.0) / 25000.0)
        if snap["outcomes"] >= 10 and snap["pf"] < 1.0:
            edge += min(0.015, (1.0 - snap["pf"]) * 0.03)
        if snap["outcomes"] >= 10 and snap["pf"] > 1.35 and snap["expectancy"] > 0.2:
            edge -= 0.008
        return max(0.03, min(0.08, edge))

    def _adaptive_momentum_weight(self) -> float:
        """Shift toward momentum only when realized expectancy degrades."""
        pnl = list(self.recent_pnl)[-10:]
        if len(pnl) < 5:
            return MOMENTUM_WEIGHT
        exp = sum(pnl) / max(1, len(pnl))
        if exp < 0:
            return min(0.65, MOMENTUM_WEIGHT + 0.15)
        return MOMENTUM_WEIGHT

    def _load_stats(self):
        try:
            with open(STATS_FILE) as f:
                data = json.load(f)
            self.stats        = data.get("stats", {})
            self.recent_trades = deque(data.get("recent", []), maxlen=30)
            self.recent_pnl = deque(data.get("recent_pnl", []), maxlen=40)
            self.side_perf = data.get("side_perf", {})
            total = sum(s.get("total",0) for a in self.stats.values() for s in a.values())
            wins  = sum(s.get("wins",0)  for a in self.stats.values() for s in a.values())
            if total and LOG_VERBOSE:
                print(f"{B}[STATS-LOCAL]{RS} cache={total} wr={wins/total*100:.1f}% (not on-chain)")
        except Exception:
            self.stats        = {}
            self.recent_trades = deque(maxlen=30)
            self.recent_pnl = deque(maxlen=40)
            self.side_perf = {}

    def _load_pnl_baseline(self):
        """Load persistent on-chain PnL baseline unless reset is requested on boot."""
        if PNL_BASELINE_RESET_ON_BOOT:
            self._pnl_baseline_locked = False
            self._pnl_baseline_ts = ""
            return
        try:
            with open(PNL_BASELINE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            sb = float(data.get("start_bank", 0.0) or 0.0)
            ts = str(data.get("baseline_ts", "") or "")
            if sb > 0:
                self.start_bank = sb
                self._pnl_baseline_locked = True
                self._pnl_baseline_ts = ts
                print(f"{B}[P&L-BASELINE]{RS} loaded on-chain baseline ${sb:.2f} ts={ts or 'n/a'}")
        except Exception:
            self._pnl_baseline_locked = False
            self._pnl_baseline_ts = ""

    def _save_pnl_baseline(self, total_equity: float, wallet_usdc: float):
        try:
            rec = {
                "start_bank": round(float(total_equity), 2),
                "wallet_usdc": round(float(wallet_usdc), 2),
                "baseline_ts": self._pnl_baseline_ts,
            }
            with open(PNL_BASELINE_FILE, "w", encoding="utf-8") as f:
                json.dump(rec, f)
        except Exception:
            pass

    def _save_stats(self):
        try:
            with open(STATS_FILE, "w") as f:
                json.dump(
                    {
                        "stats": self.stats,
                        "recent": list(self.recent_trades),
                        "recent_pnl": list(self.recent_pnl),
                        "side_perf": self.side_perf,
                    },
                    f,
                )
        except Exception:
            pass

    def _record_result(self, asset: str, side: str, won: bool, structural: bool = False, pnl: float = 0.0):
        if asset not in self.stats:
            self.stats[asset] = {}
        if side not in self.stats[asset]:
            self.stats[asset][side] = {"wins": 0, "total": 0}
        self.stats[asset][side]["total"] += 1
        if won:
            self.stats[asset][side]["wins"] += 1
        self.recent_trades.append(1 if won else 0)
        self.recent_pnl.append(float(pnl))
        sp_key = f"{asset}|{side}"
        row = self.side_perf.get(sp_key, {"n": 0, "gross_win": 0.0, "gross_loss": 0.0, "pnl": 0.0})
        row["n"] = int(row.get("n", 0)) + 1
        if pnl >= 0:
            row["gross_win"] = float(row.get("gross_win", 0.0)) + float(pnl)
        else:
            row["gross_loss"] = float(row.get("gross_loss", 0.0)) + abs(float(pnl))
        row["pnl"] = float(row.get("pnl", 0.0)) + float(pnl)
        self.side_perf[sp_key] = row
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
        me   = self._adaptive_min_edge()
        snap = self._growth_snapshot()
        last5 = list(self.recent_pnl)[-5:]
        streak = "".join("+" if x > 0 else "-" for x in last5) if last5 else ""
        wr_scale = self._wr_bet_scale()
        wr_str   = f"  BetScale={wr_scale:.1f}x" if wr_scale > 1.0 else ""
        print(
            f"{B}[ADAPT]{RS} Last5PnL={streak or '–'} PF={snap['recent_pf']:.2f} "
            f"Exp={sum(last5)/max(1,len(last5)):+.2f} MinEdge={me:.2f} "
            f"Streak={self.consec_losses}L DrawdownScale={self._kelly_drawdown_scale():.0%}{wr_str}"
        )

    # ── CHAINLINK HISTORICAL PRICE ────────────────────────────────────────────
    async def _get_polymarket_open_price(self, asset: str, start_ts: float, end_ts: float, duration: int = 15) -> float:
        """Call Polymarket's own price API to get the authoritative 'price to beat'.
        Returns openPrice float or 0.0 on any error."""
        try:
            from datetime import datetime, timezone as _tz
            sym = asset  # BTC, ETH, SOL, XRP
            dur = 5 if int(duration or 0) <= 5 else 15

            # Canonical PM round boundaries: start at exact 5m/15m slot.
            st_dt = datetime.fromtimestamp(start_ts, tz=_tz.utc)
            st_floor = st_dt.replace(minute=(st_dt.minute // dur) * dur, second=0, microsecond=0)
            et_floor = st_floor.timestamp() + dur * 60
            st = st_floor.strftime("%Y-%m-%dT%H:%M:%SZ")
            et = datetime.fromtimestamp(et_floor, tz=_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            variants = ["five", "fifteen"] if dur == 5 else ["fifteen"]
            for variant in variants:
                data = await self._http_get_json(
                    "https://polymarket.com/api/crypto/crypto-price",
                    params={"symbol": sym, "eventStartTime": st, "variant": variant, "endDate": et},
                    timeout=6,
                )
                price = (data or {}).get("openPrice", 0.0) if isinstance(data, dict) else 0.0
                if price:
                    return float(price)
            return 0.0
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
            self._reload_copyflow()
            # Cleanup stale pre-bid plans.
            now_epoch = _time.time()
            for pcid, plan in list(self._prebid_plan.items()):
                st = float((plan or {}).get("start_ts", 0) or 0)
                if st <= 0 or now_epoch > (st + PREBID_ARM_WINDOW_SEC + 120):
                    self._prebid_plan.pop(pcid, None)
            # Settlement always has priority over new entries.
            await self._resolve()
            if self.pending_redeem:
                if self._should_log("settle-first", LOG_SETTLE_FIRST_EVERY_SEC):
                    print(
                        f"{Y}[SETTLE-FIRST]{RS} pending_redeem={len(self.pending_redeem)} "
                        f"— skipping new entries until win/loss is finalized on-chain"
                    )
                await asyncio.sleep(SCAN_INTERVAL)
                continue
            markets = await self.fetch_markets()
            now     = datetime.now(timezone.utc).timestamp()
            open_local, settling_local = self._local_position_counts()
            scan_state = (
                len(markets),
                open_local,
                int(self.onchain_open_count),
                settling_local,
                int(self.onchain_redeemable_count),
            )
            state_changed = scan_state != self._scan_state_last
            if (not LOG_SCAN_ON_CHANGE_ONLY) or state_changed or self._should_log("scan-heartbeat", LOG_SCAN_EVERY_SEC):
                print(
                    f"{B}[SCAN]{RS} Live markets: {len(markets)} | "
                    f"Open(local/onchain): {open_local}/{self.onchain_open_count} | "
                    f"Settling(local/onchain): {settling_local}/{self.onchain_redeemable_count}"
                )
            self._scan_state_last = scan_state

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
                if m["start_ts"] > now:
                    sec_to_start = m["start_ts"] - now
                    if sec_to_start <= 120:
                        flow_pre = self._copyflow_live.get(cid) or self._copyflow_map.get(cid, {})
                        if isinstance(flow_pre, dict):
                            upc = float(flow_pre.get("Up", 0.0) or 0.0)
                            dnc = float(flow_pre.get("Down", 0.0) or 0.0)
                            if (upc + dnc) > 0 and self._should_log(f"prebid:{cid}", 30):
                                side_pre = "Up" if upc >= dnc else "Down"
                                conf_pre = max(upc, dnc)
                                if PREBID_ARM_ENABLED and conf_pre >= PREBID_MIN_CONF:
                                    self._prebid_plan[cid] = {
                                        "side": side_pre,
                                        "conf": conf_pre,
                                        "start_ts": float(m.get("start_ts", now)),
                                        "armed_at": _time.time(),
                                    }
                                print(
                                    f"{B}[PRE-BID]{RS} {m.get('asset','?')} {m.get('duration',0)}m "
                                    f"starts in {sec_to_start:.0f}s | side={side_pre} conf={conf_pre:.2f} "
                                    f"rk={self._round_key(cid=cid, m=m)}"
                                )
                    continue
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
                    ref = await self._get_polymarket_open_price(asset, start_ts, end_ts_m, dur)
                    if ref > 0:
                        src = "PM"
                    else:
                        # Fallback to Chainlink
                        ref, src = await self._get_chainlink_at(asset, start_ts)
                    if src == "not-ready":
                        print(
                            f"{W}[NEW MARKET] {asset} {dur}m | {title_s} | waiting for first round... | "
                            f"rk={self._round_key(cid=cid, m=m)} cid={self._short_cid(cid)}{RS}"
                        )
                    elif ref <= 0:
                        print(
                            f"{Y}[NEW MARKET] {asset} {dur}m | {title_s} | no open price yet — retry | "
                            f"rk={self._round_key(cid=cid, m=m)} cid={self._short_cid(cid)}{RS}"
                        )
                    else:
                        if asset in self.asset_cur_open:
                            self.asset_prev_open[asset] = self.asset_cur_open[asset]
                        self.asset_cur_open[asset]   = ref
                        self.open_prices[cid]        = ref
                        self.open_prices_source[cid] = src
                        print(
                            f"{W}[NEW MARKET] {asset} {dur}m | {title_s} | beat=${ref:,.4f} [{src}] | "
                            f"{m['mins_left']:.1f}min left | rk={self._round_key(cid=cid, m=m)} "
                            f"cid={self._short_cid(cid)}{RS}"
                        )
                else:
                    # Already known — but if source isn't PM yet, retry authoritative API
                    src = self.open_prices_source.get(cid, "?")
                    if src != "PM":
                        start_ts = m.get("start_ts", now)
                        end_ts_m = m.get("end_ts", now + dur * 60)
                        pm_ref = await self._get_polymarket_open_price(asset, start_ts, end_ts_m, dur)
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
                t_score = _time.perf_counter()
                signals = list(await asyncio.gather(*[self._score_market(m) for m in candidates]))
                elapsed_ms = (_time.perf_counter() - t_score) * 1000.0
                if candidates:
                    self._perf_update("score_ms", elapsed_ms / max(1, len(candidates)))
                valid   = sorted([s for s in signals if s is not None], key=lambda x: -x["score"])
                if valid:
                    best = valid[0]
                    other_strs = " | others: " + ", ".join(s["asset"] + "=" + str(s["score"]) for s in valid[1:]) if len(valid) > 1 else ""
                    best_sig = f"{best.get('cid','')}|{best['asset']}|{best['side']}|{best['score']}"
                    if best_sig != self._last_round_best or self._should_log("round-best-heartbeat", 20):
                        self._last_round_best = best_sig
                        print(f"{B}[ROUND] Best signal: {best['asset']} {best['side']} score={best['score']}{other_strs}{RS}")
                elif self._should_log("round-empty", LOG_ROUND_EMPTY_EVERY_SEC):
                    print(f"{Y}[ROUND]{RS} no executable signal")
                active_pending = {c: (m2, t) for c, (m2, t) in self.pending.items() if m2.get("end_ts", 0) > now}
                shadow_pending = dict(active_pending)
                slots = max(0, MAX_OPEN - len(active_pending))
                to_exec = []
                selected = valid
                if ROUND_BEST_ONLY and valid:
                    best_5m = max((s for s in valid if s.get("duration", 0) <= 5), key=self._signal_growth_score, default=None)
                    best_15m = max((s for s in valid if s.get("duration", 0) > 5), key=self._signal_growth_score, default=None)
                    selected = [s for s in (best_5m, best_15m) if s is not None]
                    if selected:
                        picks = " | ".join(
                            f"{s['asset']} {s['duration']}m {s['side']} g={self._signal_growth_score(s):+.3f}"
                            for s in selected
                        )
                        if picks != self._last_round_pick or self._should_log("round-pick-heartbeat", 20):
                            self._last_round_pick = picks
                            print(f"{B}[ROUND-PICK]{RS} {picks}")
                for sig in selected:
                    if len(to_exec) >= slots:
                        break
                    if not self._exposure_ok(sig, shadow_pending):
                        continue
                    if not TRADE_ALL_MARKETS:
                        pending_up = sum(1 for _, t in shadow_pending.values() if t.get("side") == "Up")
                        pending_dn = sum(1 for _, t in shadow_pending.values() if t.get("side") == "Down")
                        if sig["side"] == "Up" and pending_up >= MAX_SAME_DIR:
                            continue
                        if sig["side"] == "Down" and pending_dn >= MAX_SAME_DIR:
                            continue
                    to_exec.append(sig)
                    m_sig = sig.get("m", {}) if isinstance(sig.get("m", {}), dict) else {}
                    t_sig = {
                        "side": sig.get("side", ""),
                        "size": float(sig.get("size", 0.0) or 0.0),
                        "asset": sig.get("asset", ""),
                        "duration": int(sig.get("duration", 0) or 0),
                        "end_ts": float(m_sig.get("end_ts", now + 60) or (now + 60)),
                    }
                    shadow_pending[f"__pick__{len(to_exec)}"] = (m_sig, t_sig)
                if to_exec:
                    await asyncio.gather(*[self._execute_trade(sig) for sig in to_exec])

            await asyncio.sleep(SCAN_INTERVAL)
          except Exception as e:
            print(f"{R}[SCAN] Error (continuing): {e}{RS}")
            await asyncio.sleep(SCAN_INTERVAL)

    async def _refresh_balance(self):
        """Always sync bankroll, trades and win rate from on-chain truth.
        Never skips — no local accounting assumptions."""
        USDC_E = USDC_E_ADDR
        ERC20_ABI = [{"inputs":[{"name":"account","type":"address"}],
                      "name":"balanceOf","outputs":[{"name":"","type":"uint256"}],
                      "stateMutability":"view","type":"function"}]
        CTF_BAL_ABI = [
            {"inputs":[{"name":"owner","type":"address"},{"name":"id","type":"uint256"}],
             "name":"balanceOf","outputs":[{"name":"","type":"uint256"}],
             "stateMutability":"view","type":"function"},
            {"inputs":[{"name":"conditionId","type":"bytes32"}],
             "name":"payoutDenominator","outputs":[{"name":"","type":"uint256"}],
             "stateMutability":"view","type":"function"},
            {"inputs":[{"name":"conditionId","type":"bytes32"},{"name":"index","type":"uint256"}],
             "name":"payoutNumerators","outputs":[{"name":"","type":"uint256"}],
             "stateMutability":"view","type":"function"},
        ]
        usdc_contract = None
        ctf_bal_contract = None
        addr_cs = None
        if self.w3 is not None:
            try:
                usdc_contract = self.w3.eth.contract(
                    address=Web3.to_checksum_address(USDC_E), abi=ERC20_ABI
                )
                cfg = get_contract_config(CHAIN_ID, neg_risk=False)
                ctf_bal_contract = self.w3.eth.contract(
                    address=Web3.to_checksum_address(cfg.conditional_tokens),
                    abi=CTF_BAL_ABI,
                )
                addr_cs = Web3.to_checksum_address(ADDRESS)
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

                # 1. On-chain USDC.e balance
                usdc_raw = await loop.run_in_executor(
                    None, lambda: usdc_contract.functions.balanceOf(
                        addr_cs
                    ).call()
                ) if usdc_contract else 0
                usdc = usdc_raw / 1e6

                # 2. Strict on-chain CID valuation from wallet token balances.
                open_val = 0.0
                open_count = 0
                settling_claim_val = 0.0
                settling_claim_count = 0
                onchain_open_cids = set()
                if ctf_bal_contract is not None and addr_cs is not None:
                    # Open (unresolved) positions from local tracked cids, valued by token price cache.
                    for cid, (m_o, t_o) in list(self.pending.items()):
                        tok = str(t_o.get("token_id", "") or "").strip()
                        if not tok.isdigit():
                            tok = self._token_id_from_cid_side(cid, str(t_o.get("side", "")))
                        if not tok.isdigit():
                            continue
                        try:
                            bal_raw = await loop.run_in_executor(
                                None,
                                lambda ti=int(tok): ctf_bal_contract.functions.balanceOf(addr_cs, ti).call(),
                            )
                            qty = bal_raw / 1e6
                            if qty <= 0:
                                continue
                            px = float(self.token_prices.get(tok) or 0.0)
                            if px <= 0:
                                up_px = float(m_o.get("up_price") or t_o.get("mkt_price") or 0.5)
                                px = up_px if t_o.get("side") == "Up" else max(0.0, 1.0 - up_px)
                            open_val += qty * px
                            open_count += 1
                            onchain_open_cids.add(cid)
                        except Exception:
                            continue

                    # Settling (resolved winners not redeemed yet): claimable notional from on-chain numerators.
                    for cid, val in list(self.pending_redeem.items()):
                        if isinstance(val[0], dict):
                            m_s, t_s = val
                            side_s = t_s.get("side", "")
                            tok = str(t_s.get("token_id", "") or "").strip()
                        else:
                            side_s, _ = val
                            m_s = {"conditionId": cid}
                            tok = ""
                        if not tok.isdigit():
                            tok = self._token_id_from_cid_side(cid, str(side_s))
                        if not tok.isdigit():
                            continue
                        try:
                            cid_bytes = bytes.fromhex(cid.lower().replace("0x", "").zfill(64))
                            denom = await loop.run_in_executor(
                                None, lambda b=cid_bytes: ctf_bal_contract.functions.payoutDenominator(b).call()
                            )
                            if int(denom) == 0:
                                continue
                            n0 = await loop.run_in_executor(
                                None, lambda b=cid_bytes: ctf_bal_contract.functions.payoutNumerators(b, 0).call()
                            )
                            n1 = await loop.run_in_executor(
                                None, lambda b=cid_bytes: ctf_bal_contract.functions.payoutNumerators(b, 1).call()
                            )
                            winner = "Up" if n0 > 0 and n1 == 0 else "Down" if n1 > 0 and n0 == 0 else ""
                            if winner != side_s:
                                continue
                            bal_raw = await loop.run_in_executor(
                                None,
                                lambda ti=int(tok): ctf_bal_contract.functions.balanceOf(addr_cs, ti).call(),
                            )
                            qty = bal_raw / 1e6
                            if qty <= 0:
                                continue
                            settling_claim_val += qty
                            settling_claim_count += 1
                        except Exception:
                            continue

                total = round(usdc + open_val + settling_claim_val, 2)
                self.onchain_wallet_usdc = round(usdc, 2)
                self.onchain_open_positions = round(open_val, 2)
                self.onchain_open_count = int(open_count)
                self.onchain_redeemable_count = int(settling_claim_count)
                self.onchain_total_equity = total
                self.onchain_snapshot_ts = _time.time()
                bank_state = (
                    round(usdc, 2),
                    round(open_val, 2),
                    int(open_count),
                    round(settling_claim_val, 2),
                    int(settling_claim_count),
                    round(total, 2),
                )
                prev_bank_state = self._bank_state_last
                state_changed = prev_bank_state is None or any(
                    abs(float(a) - float(b)) >= LOG_BANK_MIN_DELTA
                    if isinstance(a, (int, float)) and isinstance(b, (int, float))
                    else a != b
                    for a, b in zip(bank_state, prev_bank_state)
                )
                if state_changed or self._should_log("bank-heartbeat", LOG_BANK_EVERY_SEC):
                    print(
                        f"{B}[BANK]{RS} on-chain USDC=${usdc:.2f}  "
                        f"open_positions=${open_val:.2f} ({open_count})  "
                        f"settling_claimable=${settling_claim_val:.2f} ({settling_claim_count})  "
                        f"total=${total:.2f}"
                    )
                self._bank_state_last = bank_state
                if total > 0:
                    self.bankroll = total
                    if total > self.peak_bankroll:
                        self.peak_bankroll = total
                    if not self._pnl_baseline_locked:
                        self.start_bank = total
                        self._pnl_baseline_locked = True
                        self._pnl_baseline_ts = datetime.now(timezone.utc).isoformat()
                        self._save_pnl_baseline(total, usdc)
                        print(
                            f"{B}[P&L-BASELINE]{RS} set on-chain baseline=${total:.2f} "
                            f"at {self._pnl_baseline_ts}"
                        )

                # 3. API recovery/stats only (does not drive bankroll valuation)
                positions = await self._http_get_json(
                    "https://data-api.polymarket.com/positions",
                    params={"user": ADDRESS, "sizeThreshold": "0.01"},
                    timeout=10,
                )
                if not isinstance(positions, list):
                    positions = []
                api_active_cids = {
                    p.get("conditionId", "")
                    for p in positions
                    if not p.get("redeemable", False)
                    and p.get("outcome", "")
                    and float(p.get("currentValue", 0) or 0) >= DUST_RECOVER_MIN
                    and p.get("conditionId", "")
                }
                # On-chain-first cleanup: if a local pending CID is neither on-chain nor API-active
                # after a grace window, it is stale and removed from local state.
                prune_n = 0
                now_ts = datetime.now(timezone.utc).timestamp()
                for cid, (m_p, t_p) in list(self.pending.items()):
                    if cid in onchain_open_cids or cid in self.pending_redeem or cid in api_active_cids:
                        continue
                    placed_ts = float(t_p.get("placed_ts", 0) or 0)
                    if placed_ts > 0 and (now_ts - placed_ts) < 90:
                        continue
                    # Keep very fresh windows briefly even without placed_ts.
                    end_ts = float(m_p.get("end_ts", 0) or 0)
                    if end_ts > 0 and (end_ts - now_ts) > 0 and (end_ts - now_ts) < 90:
                        continue
                    self.pending.pop(cid, None)
                    prune_n += 1
                if prune_n:
                    self._save_pending()
                    print(f"{Y}[SYNC] pruned stale local pending: {prune_n} (absent on-chain/API){RS}")
                # Recover any on-chain positions not tracked in pending (every tick = 30s)
                for p in positions:
                    cid  = p.get("conditionId", "")
                    rdm  = p.get("redeemable", False)
                    val  = float(p.get("currentValue", 0))
                    side = p.get("outcome", "")
                    title = p.get("title", "")
                    if rdm or not side or not cid or val < DUST_RECOVER_MIN:
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
                        mkt_data = await self._http_get_json(
                            f"{GAMMA}/markets", params={"conditionId": cid}, timeout=8
                        )
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
                    q_st, q_et = self._round_bounds_from_question(title)
                    if self._is_exact_round_bounds(q_st, q_et, duration):
                        start_ts, end_ts = q_st, q_et
                    elif q_et > 0 and q_et <= now_ts:
                        end_ts = q_et
                    if end_ts > 0 and end_ts <= now_ts:
                        stored_end = self.pending.get(cid, ({},))[0].get("end_ts", 0)
                        if stored_end > now_ts:
                            end_ts = stored_end  # trust stored real end_ts
                        elif end_ts % 86400 < 3600:  # midnight UTC — Gamma sent date-only, false expiry
                            if not self._is_exact_round_bounds(start_ts, end_ts, duration):
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
                    await self._sync_stats_from_api()

            except Exception as e:
                self._errors.tick("refresh_balance", print, err=e, every=10)
                print(f"{Y}[BANK] refresh error: {e}{RS}")

    async def _sync_stats_from_api(self):
        """Sync win/loss stats from Polymarket activity API (on-chain truth).
        BUY = bet placed, REDEEM = win. Also syncs recent_trades deque for last-5 gate."""
        try:
            activity = await self._http_get_json(
                "https://data-api.polymarket.com/activity",
                params={"user": ADDRESS, "limit": "500"},
                timeout=10,
            )
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
            self._errors.tick("sync_stats_api", print, err=e, every=10)
            print(f"{Y}[STATS] activity sync error: {e}{RS}")

    async def _redeemable_scan(self):
        """Every 60s, re-scan Polymarket API for redeemable positions not yet queued.
        Catches winners that weren't on-chain resolved when _sync_redeemable ran at startup."""
        if DRY_RUN or self.w3 is None:
            return
        while True:
            try:
                positions = await self._http_get_json(
                    "https://data-api.polymarket.com/positions",
                    params={"user": ADDRESS, "sizeThreshold": "0.01"},
                    timeout=10,
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
                self._errors.tick("redeemable_scan", print, err=e, every=10)
                print(f"{Y}[SCAN-REDEEM] Error: {e}{RS}")
            await asyncio.sleep(REDEEMABLE_SCAN_SEC)

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
            try:
                activity = await self._http_get_json(
                    "https://data-api.polymarket.com/activity",
                    params={"user": ADDRESS, "limit": "600"},
                    timeout=12,
                )
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
                        print(
                            f"{G}[FORCE-REDEEM]{RS} {winner} {d['title'][:36]} | tx={tx_hash[:16]} | "
                            f"cid={self._short_cid(cid)}"
                        )
                    except Exception as e:
                        print(f"{Y}[FORCE-REDEEM] retry later {self._short_cid(cid)}: {e}{RS}")
            except Exception as e:
                self._errors.tick("force_redeem_backfill", print, err=e, every=10)
                print(f"{Y}[FORCE-REDEEM] Error: {e}{RS}")
            await asyncio.sleep(FORCE_REDEEM_SCAN_SEC)

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
                    if val < DUST_RECOVER_MIN:   # tiny dust — mark seen only, don't track
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
                    q_st, q_et = self._round_bounds_from_question(title)
                    if self._is_exact_round_bounds(q_st, q_et, duration):
                        start_ts, end_ts = q_st, q_et
                    elif q_et > 0 and q_et <= now:
                        end_ts = q_et
                    # If already expired: check if it's a Gamma date-only midnight UTC false alarm.
                    if end_ts > 0 and end_ts <= now:
                        stored_end = self.pending.get(cid, ({},))[0].get("end_ts", 0)
                        if stored_end > now:
                            end_ts = stored_end  # trust stored real end_ts
                        elif end_ts % 86400 < 3600:  # midnight UTC — date-only field, false expiry
                            if not self._is_exact_round_bounds(start_ts, end_ts, duration):
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
║  Bankroll: sync on-chain at startup                          ║
║  {'DRY-RUN (simulated, no real orders)' if DRY_RUN else 'LIVE — ordini reali su Polymarket':<44}║
╚══════════════════════════════════════════════════════════════╝{RS}
        """)
        self._startup_self_check()
        while True:
            try:
                self.init_clob()
                break
            except Exception as e:
                print(f"{R}[BOOT]{RS} CLOB init failed: {e} — retry in 20s")
                await asyncio.sleep(20)
        self._sync_redeemable()   # redeem any wins from previous runs
        # Sync stats from API immediately so first status print shows real data
        await self._sync_stats_from_api()
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
            _guard("_copyflow_refresh_loop", self._copyflow_refresh_loop),
            _guard("_copyflow_live_loop",   self._copyflow_live_loop),
            _guard("_copyflow_intel_loop",  self._copyflow_intel_loop),
            _guard("_rpc_optimizer_loop",   self._rpc_optimizer_loop),
            _guard("_redeemable_scan",      self._redeemable_scan),
            _guard("_position_sync_loop",   self._position_sync_loop),
        )


if __name__ == "__main__":
    try:
        asyncio.run(LiveTrader().run())
    except KeyboardInterrupt:
        print(f"\n{Y}[STOP] Log: {LOG_FILE}{RS}")
