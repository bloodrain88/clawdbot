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
import sys
import threading
import time as _time
from collections import deque, defaultdict
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
MIN_EXEC_NOTIONAL_USDC = float(os.environ.get("MIN_EXEC_NOTIONAL_USDC", "5.0"))
MIN_ORDER_SIZE_SHARES = float(os.environ.get("MIN_ORDER_SIZE_SHARES", "5.0"))
ORDER_SIZE_PAD_SHARES = float(os.environ.get("ORDER_SIZE_PAD_SHARES", "0.02"))
MIN_BET_PCT    = float(os.environ.get("MIN_BET_PCT", "0.022"))
DUST_RECOVER_MIN = float(os.environ.get("DUST_RECOVER_MIN", "0.50"))
MIN_PARTIAL_TRACK_USDC = float(os.environ.get("MIN_PARTIAL_TRACK_USDC", "5.0"))
# Presence threshold for on-chain position visibility/sync.
# Keep this low and independent from recovery sizing thresholds.
OPEN_PRESENCE_MIN = float(os.environ.get("OPEN_PRESENCE_MIN", "0.01"))
MAX_ABS_BET    = float(os.environ.get("MAX_ABS_BET", "14.0"))     # hard ceiling
MAX_BANKROLL_PCT = 0.25   # never risk more than 25% of bankroll on a single bet
MAX_OPEN       = int(os.environ.get("MAX_OPEN", "8"))
MAX_SAME_DIR   = int(os.environ.get("MAX_SAME_DIR", "8"))
MAX_CID_EXPOSURE_PCT = float(os.environ.get("MAX_CID_EXPOSURE_PCT", "0.10"))
BLOCK_OPPOSITE_SIDE_SAME_CID = os.environ.get("BLOCK_OPPOSITE_SIDE_SAME_CID", "true").lower() == "true"
BLOCK_OPPOSITE_SIDE_SAME_ROUND = os.environ.get("BLOCK_OPPOSITE_SIDE_SAME_ROUND", "true").lower() == "true"
ROUND_STACK_SIZE_DECAY = float(os.environ.get("ROUND_STACK_SIZE_DECAY", "0.72"))
ROUND_STACK_SIZE_MIN = float(os.environ.get("ROUND_STACK_SIZE_MIN", "0.45"))
# Extra round-level decay to reduce clustered exposure across multiple assets
# in the same 15m window while keeping trading active.
ROUND_TOTAL_SIZE_DECAY = float(os.environ.get("ROUND_TOTAL_SIZE_DECAY", "0.82"))
ROUND_TOTAL_SIZE_MIN = float(os.environ.get("ROUND_TOTAL_SIZE_MIN", "0.40"))
ROUND_CORR_SAME_SIDE_DECAY = float(os.environ.get("ROUND_CORR_SAME_SIDE_DECAY", "0.70"))
ROUND_CORR_SAME_SIDE_MIN = float(os.environ.get("ROUND_CORR_SAME_SIDE_MIN", "0.35"))
LOW_ENTRY_SIZE_HAIRCUT_ENABLED = os.environ.get("LOW_ENTRY_SIZE_HAIRCUT_ENABLED", "true").lower() == "true"
LOW_ENTRY_SIZE_HAIRCUT_PX = float(os.environ.get("LOW_ENTRY_SIZE_HAIRCUT_PX", "0.30"))
LOW_ENTRY_SIZE_HAIRCUT_MULT = float(os.environ.get("LOW_ENTRY_SIZE_HAIRCUT_MULT", "0.65"))
LOW_ENTRY_SIZE_HAIRCUT_KEEP_SCORE = int(os.environ.get("LOW_ENTRY_SIZE_HAIRCUT_KEEP_SCORE", "18"))
LOW_ENTRY_SIZE_HAIRCUT_KEEP_PROB = float(os.environ.get("LOW_ENTRY_SIZE_HAIRCUT_KEEP_PROB", "0.80"))
TRADE_ALL_MARKETS = os.environ.get("TRADE_ALL_MARKETS", "true").lower() == "true"
ROUND_BEST_ONLY = os.environ.get("ROUND_BEST_ONLY", "false").lower() == "true"
MIN_SCORE_GATE = int(os.environ.get("MIN_SCORE_GATE", "0"))
MIN_SCORE_GATE_5M = int(os.environ.get("MIN_SCORE_GATE_5M", "8"))
MIN_SCORE_GATE_15M = int(os.environ.get("MIN_SCORE_GATE_15M", "7"))
MAX_ENTRY_PRICE = float(os.environ.get("MAX_ENTRY_PRICE", "0.45"))
MAX_ENTRY_TOL = float(os.environ.get("MAX_ENTRY_TOL", "0.015"))
MIN_ENTRY_PRICE_15M = float(os.environ.get("MIN_ENTRY_PRICE_15M", "0.20"))
MIN_ENTRY_PRICE_5M = float(os.environ.get("MIN_ENTRY_PRICE_5M", "0.35"))
MAX_ENTRY_PRICE_5M = float(os.environ.get("MAX_ENTRY_PRICE_5M", "0.52"))
MIN_PAYOUT_MULT = float(os.environ.get("MIN_PAYOUT_MULT", "1.85"))
MIN_EV_NET = float(os.environ.get("MIN_EV_NET", "0.019"))
FEE_RATE_EST = float(os.environ.get("FEE_RATE_EST", "0.0156"))
HC15_ENABLED = os.environ.get("HC15_ENABLED", "false").lower() == "true"
HC15_MIN_SCORE = int(os.environ.get("HC15_MIN_SCORE", "10"))
HC15_MIN_TRUE_PROB = float(os.environ.get("HC15_MIN_TRUE_PROB", "0.62"))
HC15_MIN_EDGE = float(os.environ.get("HC15_MIN_EDGE", "0.10"))
HC15_TARGET_ENTRY = float(os.environ.get("HC15_TARGET_ENTRY", "0.30"))
HC15_FALLBACK_PCT_LEFT = float(os.environ.get("HC15_FALLBACK_PCT_LEFT", "0.35"))
HC15_FALLBACK_MAX_ENTRY = float(os.environ.get("HC15_FALLBACK_MAX_ENTRY", "0.36"))
MIN_PAYOUT_MULT_5M = float(os.environ.get("MIN_PAYOUT_MULT_5M", "1.75"))
MIN_EV_NET_5M = float(os.environ.get("MIN_EV_NET_5M", "0.019"))
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
MAX_ORDERBOOK_AGE_MS = float(os.environ.get("MAX_ORDERBOOK_AGE_MS", "500"))
MIN_MINS_LEFT_5M = float(os.environ.get("MIN_MINS_LEFT_5M", "1.2"))
MIN_MINS_LEFT_15M = float(os.environ.get("MIN_MINS_LEFT_15M", "3.0"))
LATE_DIR_LOCK_ENABLED = os.environ.get("LATE_DIR_LOCK_ENABLED", "true").lower() == "true"
LATE_DIR_LOCK_MIN_LEFT_5M = float(os.environ.get("LATE_DIR_LOCK_MIN_LEFT_5M", "2.2"))
LATE_DIR_LOCK_MIN_LEFT_15M = float(os.environ.get("LATE_DIR_LOCK_MIN_LEFT_15M", "6.0"))
LATE_DIR_LOCK_MIN_MOVE_PCT = float(os.environ.get("LATE_DIR_LOCK_MIN_MOVE_PCT", "0.0002"))
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
PING_INTERVAL  = int(os.environ.get("PING_INTERVAL", "5"))
STATUS_INTERVAL= int(os.environ.get("STATUS_INTERVAL", "15"))
ONCHAIN_SYNC_SEC = float(os.environ.get("ONCHAIN_SYNC_SEC", "2.0"))
_DATA_DIR      = os.environ.get("DATA_DIR", os.path.expanduser("~"))
LOG_FILE       = os.path.join(_DATA_DIR, "clawdbot_live_trades.csv")
PENDING_FILE   = os.path.join(_DATA_DIR, "clawdbot_pending.json")
SEEN_FILE      = os.path.join(_DATA_DIR, "clawdbot_seen.json")
STATS_FILE     = os.path.join(_DATA_DIR, "clawdbot_stats.json")
METRICS_FILE   = os.path.join(_DATA_DIR, "clawdbot_onchain_metrics.jsonl")
RUNTIME_JSON_LOG_FILE = os.path.join(_DATA_DIR, "clawdbot_runtime_logs.jsonl")
PNL_BASELINE_FILE = os.path.join(_DATA_DIR, "clawdbot_pnl_baseline.json")
SETTLED_FILE   = os.path.join(_DATA_DIR, "clawdbot_settled_cids.json")
PNL_BASELINE_RESET_ON_BOOT = os.environ.get("PNL_BASELINE_RESET_ON_BOOT", "false").lower() == "true"
LOSS_STREAK_PAUSE_ENABLED = os.environ.get("LOSS_STREAK_PAUSE_ENABLED", "true").lower() == "true"
LOSS_STREAK_PAUSE_N = int(os.environ.get("LOSS_STREAK_PAUSE_N", "4"))
LOSS_STREAK_PAUSE_SEC = float(os.environ.get("LOSS_STREAK_PAUSE_SEC", "900"))
RUNTIME_JSON_LOG_ENABLED = os.environ.get("RUNTIME_JSON_LOG_ENABLED", "true").lower() == "true"
RUNTIME_JSON_LOG_ROTATE_DAILY = os.environ.get("RUNTIME_JSON_LOG_ROTATE_DAILY", "true").lower() == "true"

DRY_RUN   = os.environ.get("DRY_RUN", "true").lower() == "true"
STRICT_ONCHAIN_STATE = os.environ.get("STRICT_ONCHAIN_STATE", "true").lower() == "true"
LOG_VERBOSE = os.environ.get("LOG_VERBOSE", "true").lower() == "true"
LOG_STATS_LOCAL = os.environ.get("LOG_STATS_LOCAL", "false").lower() == "true"
LOG_SCAN_EVERY_SEC = int(os.environ.get("LOG_SCAN_EVERY_SEC", "30"))
LOG_SCAN_ON_CHANGE_ONLY = os.environ.get("LOG_SCAN_ON_CHANGE_ONLY", "true").lower() == "true"
LOG_FLOW_EVERY_SEC = int(os.environ.get("LOG_FLOW_EVERY_SEC", "300"))
LOG_ROUND_EMPTY_EVERY_SEC = int(os.environ.get("LOG_ROUND_EMPTY_EVERY_SEC", "30"))
LOG_SETTLE_FIRST_EVERY_SEC = int(os.environ.get("LOG_SETTLE_FIRST_EVERY_SEC", "20"))
SETTLE_FIRST_REQUIRE_ONCHAIN_CLAIMABLE = os.environ.get("SETTLE_FIRST_REQUIRE_ONCHAIN_CLAIMABLE", "true").lower() == "true"
SETTLE_FIRST_FORCE_BLOCK_AFTER_MIN = float(os.environ.get("SETTLE_FIRST_FORCE_BLOCK_AFTER_MIN", "6.0"))
LOG_BANK_EVERY_SEC = int(os.environ.get("LOG_BANK_EVERY_SEC", "45"))
LOG_BANK_MIN_DELTA = float(os.environ.get("LOG_BANK_MIN_DELTA", "0.50"))
LOG_MARKET_EVERY_SEC = int(os.environ.get("LOG_MARKET_EVERY_SEC", "90"))
LOG_LIVE_DETAIL = os.environ.get("LOG_LIVE_DETAIL", "true").lower() == "true"
LOG_LIVE_RK_MAX = int(os.environ.get("LOG_LIVE_RK_MAX", "12"))
LIVE_RK_REAL_ONLY = os.environ.get("LIVE_RK_REAL_ONLY", "false").lower() == "true"
LOG_TAIL_EQ_ENABLED = os.environ.get("LOG_TAIL_EQ_ENABLED", "true").lower() == "true"
LOG_TAIL_EQ_EVERY_SEC = int(os.environ.get("LOG_TAIL_EQ_EVERY_SEC", "15"))
LOG_TAIL_EQ_MAX_ROWS = int(os.environ.get("LOG_TAIL_EQ_MAX_ROWS", "20"))
# Keep freshly-filled local positions visible while on-chain position APIs catch up.
LIVE_LOCAL_GRACE_SEC = float(os.environ.get("LIVE_LOCAL_GRACE_SEC", "120"))
LOG_ROUND_SIGNAL_MIN_SEC = float(os.environ.get("LOG_ROUND_SIGNAL_MIN_SEC", "5"))
LOG_DEBUG_EVERY_SEC = int(os.environ.get("LOG_DEBUG_EVERY_SEC", "60"))
LOG_ROUND_SIGNAL_EVERY_SEC = int(os.environ.get("LOG_ROUND_SIGNAL_EVERY_SEC", "30"))
LOG_SKIP_EVERY_SEC = int(os.environ.get("LOG_SKIP_EVERY_SEC", "30"))
LOG_EDGE_EVERY_SEC = int(os.environ.get("LOG_EDGE_EVERY_SEC", "30"))
LOG_EXEC_EVERY_SEC = int(os.environ.get("LOG_EXEC_EVERY_SEC", "30"))
SKIP_STATS_WINDOW_SEC = int(os.environ.get("SKIP_STATS_WINDOW_SEC", "900"))
SKIP_STATS_TOP_N = int(os.environ.get("SKIP_STATS_TOP_N", "6"))
WS_HEALTH_REQUIRED = os.environ.get("WS_HEALTH_REQUIRED", "true").lower() == "true"
WS_HEALTH_MIN_FRESH_RATIO = float(os.environ.get("WS_HEALTH_MIN_FRESH_RATIO", "0.50"))
WS_HEALTH_MAX_MED_AGE_MS = float(os.environ.get("WS_HEALTH_MAX_MED_AGE_MS", "7000"))
WS_GATE_WARMUP_SEC = float(os.environ.get("WS_GATE_WARMUP_SEC", "45"))
SEEN_MAX_KEEP = int(os.environ.get("SEEN_MAX_KEEP", "600"))
LOG_OPEN_WAIT_EVERY_SEC = int(os.environ.get("LOG_OPEN_WAIT_EVERY_SEC", "120"))
LOG_REDEEM_WAIT_EVERY_SEC = int(os.environ.get("LOG_REDEEM_WAIT_EVERY_SEC", "180"))
REDEEM_POLL_SEC = float(os.environ.get("REDEEM_POLL_SEC", "2.0"))
REDEEM_POLL_SEC_ACTIVE = float(os.environ.get("REDEEM_POLL_SEC_ACTIVE", "0.25"))
REDEEM_REQUIRE_ONCHAIN_CONFIRM = os.environ.get("REDEEM_REQUIRE_ONCHAIN_CONFIRM", "true").lower() == "true"
LOG_MKT_MOVE_THRESHOLD_PCT = float(os.environ.get("LOG_MKT_MOVE_THRESHOLD_PCT", "0.15"))
FORCE_REDEEM_SCAN_SEC = int(os.environ.get("FORCE_REDEEM_SCAN_SEC", "5"))
REDEEMABLE_SCAN_SEC = int(os.environ.get("REDEEMABLE_SCAN_SEC", "10"))
ROUND_RETRY_COOLDOWN_SEC = float(os.environ.get("ROUND_RETRY_COOLDOWN_SEC", "12"))
RPC_OPTIMIZE_SEC = int(os.environ.get("RPC_OPTIMIZE_SEC", "45"))
RPC_PROBE_COUNT = int(os.environ.get("RPC_PROBE_COUNT", "3"))
RPC_SWITCH_MARGIN_MS = float(os.environ.get("RPC_SWITCH_MARGIN_MS", "15"))
HTTP_CONN_LIMIT = int(os.environ.get("HTTP_CONN_LIMIT", "80"))
HTTP_CONN_PER_HOST = int(os.environ.get("HTTP_CONN_PER_HOST", "30"))
HTTP_DNS_TTL_SEC = int(os.environ.get("HTTP_DNS_TTL_SEC", "300"))
HTTP_KEEPALIVE_SEC = float(os.environ.get("HTTP_KEEPALIVE_SEC", "30"))
BOOK_CACHE_TTL_MS = float(os.environ.get("BOOK_CACHE_TTL_MS", "180"))
BOOK_CACHE_MAX = int(os.environ.get("BOOK_CACHE_MAX", "256"))
BOOK_FETCH_CONCURRENCY = int(os.environ.get("BOOK_FETCH_CONCURRENCY", "16"))
CLOB_HEARTBEAT_SEC = float(os.environ.get("CLOB_HEARTBEAT_SEC", "6"))
USER_EVENTS_ENABLED = os.environ.get("USER_EVENTS_ENABLED", "true").lower() == "true"
USER_EVENTS_POLL_SEC = float(os.environ.get("USER_EVENTS_POLL_SEC", "0.8"))
USER_EVENTS_CACHE_TTL_SEC = float(os.environ.get("USER_EVENTS_CACHE_TTL_SEC", "180"))
CLOB_MARKET_WS_ENABLED = os.environ.get("CLOB_MARKET_WS_ENABLED", "true").lower() == "true"
CLOB_MARKET_WS_SYNC_SEC = float(os.environ.get("CLOB_MARKET_WS_SYNC_SEC", "2.0"))
CLOB_MARKET_WS_MAX_AGE_MS = float(os.environ.get("CLOB_MARKET_WS_MAX_AGE_MS", "2000"))
CLOB_MARKET_WS_SOFT_AGE_MS = float(os.environ.get("CLOB_MARKET_WS_SOFT_AGE_MS", "6000"))
CLOB_REST_FALLBACK_ENABLED = os.environ.get("CLOB_REST_FALLBACK_ENABLED", "true").lower() == "true"
CLOB_REST_FRESH_MAX_AGE_MS = float(os.environ.get("CLOB_REST_FRESH_MAX_AGE_MS", "1500"))
CLOB_WS_STALE_HEAL_HITS = int(os.environ.get("CLOB_WS_STALE_HEAL_HITS", "3"))
CLOB_WS_STALE_HEAL_COOLDOWN_SEC = float(os.environ.get("CLOB_WS_STALE_HEAL_COOLDOWN_SEC", "20"))
COPYFLOW_FILE = os.environ.get("COPYFLOW_FILE", os.path.join(_DATA_DIR, "clawdbot_copyflow.json"))
COPYFLOW_RELOAD_SEC = int(os.environ.get("COPYFLOW_RELOAD_SEC", "20"))
COPYFLOW_BONUS_MAX = int(os.environ.get("COPYFLOW_BONUS_MAX", "2"))
COPYFLOW_REFRESH_ENABLED = os.environ.get("COPYFLOW_REFRESH_ENABLED", "true").lower() == "true"
COPYFLOW_REFRESH_SEC = int(os.environ.get("COPYFLOW_REFRESH_SEC", "300"))
COPYFLOW_MIN_ROI = float(os.environ.get("COPYFLOW_MIN_ROI", "-0.03"))
COPYFLOW_LIVE_ENABLED = os.environ.get("COPYFLOW_LIVE_ENABLED", "true").lower() == "true"
COPYFLOW_LIVE_REFRESH_SEC = int(os.environ.get("COPYFLOW_LIVE_REFRESH_SEC", "5"))
COPYFLOW_LIVE_TRADES_LIMIT = int(os.environ.get("COPYFLOW_LIVE_TRADES_LIMIT", "80"))
COPYFLOW_LIVE_MAX_MARKETS = int(os.environ.get("COPYFLOW_LIVE_MAX_MARKETS", "8"))
COPYFLOW_LIVE_MAX_AGE_SEC = float(os.environ.get("COPYFLOW_LIVE_MAX_AGE_SEC", "180"))
COPYFLOW_FETCH_RETRIES = int(os.environ.get("COPYFLOW_FETCH_RETRIES", "3"))
COPYFLOW_CID_ONDEMAND_ENABLED = os.environ.get("COPYFLOW_CID_ONDEMAND_ENABLED", "true").lower() == "true"
COPYFLOW_CID_ONDEMAND_COOLDOWN_SEC = float(os.environ.get("COPYFLOW_CID_ONDEMAND_COOLDOWN_SEC", "4"))
COPYFLOW_HEALTH_FORCE_REFRESH_SEC = float(os.environ.get("COPYFLOW_HEALTH_FORCE_REFRESH_SEC", "12"))
LOG_HEALTH_EVERY_SEC = int(os.environ.get("LOG_HEALTH_EVERY_SEC", "30"))
COPYFLOW_INTEL_ENABLED = os.environ.get("COPYFLOW_INTEL_ENABLED", "true").lower() == "true"
COPYFLOW_INTEL_REFRESH_SEC = int(os.environ.get("COPYFLOW_INTEL_REFRESH_SEC", "60"))
COPYFLOW_INTEL_TRADES_PER_MARKET = int(os.environ.get("COPYFLOW_INTEL_TRADES_PER_MARKET", "120"))
COPYFLOW_INTEL_MAX_WALLETS = int(os.environ.get("COPYFLOW_INTEL_MAX_WALLETS", "40"))
COPYFLOW_INTEL_ACTIVITY_LIMIT = int(os.environ.get("COPYFLOW_INTEL_ACTIVITY_LIMIT", "250"))
COPYFLOW_INTEL_MIN_SETTLED = int(os.environ.get("COPYFLOW_INTEL_MIN_SETTLED", "12"))
COPYFLOW_INTEL_MIN_ROI = float(os.environ.get("COPYFLOW_INTEL_MIN_ROI", "0.02"))
COPYFLOW_INTEL_WINDOW_SEC = int(os.environ.get("COPYFLOW_INTEL_WINDOW_SEC", "86400"))
COPYFLOW_INTEL_SETTLE_LAG_SEC = int(os.environ.get("COPYFLOW_INTEL_SETTLE_LAG_SEC", "1200"))
COPYFLOW_INTEL_MIN_SETTLED_FAMILY_24H = int(os.environ.get("COPYFLOW_INTEL_MIN_SETTLED_FAMILY_24H", "6"))
COPYFLOW_INTEL_MIN_SETTLED_FAMILY_ALL = int(os.environ.get("COPYFLOW_INTEL_MIN_SETTLED_FAMILY_ALL", "20"))
COPYFLOW_HTTP_MAX_PARALLEL = int(os.environ.get("COPYFLOW_HTTP_MAX_PARALLEL", "8"))
MARKET_LEADER_FOLLOW_ENABLED = os.environ.get("MARKET_LEADER_FOLLOW_ENABLED", "true").lower() == "true"
MARKET_LEADER_MIN_NET = float(os.environ.get("MARKET_LEADER_MIN_NET", "0.15"))
MARKET_LEADER_MIN_N = int(os.environ.get("MARKET_LEADER_MIN_N", "30"))
MARKET_LEADER_SCORE_BONUS = int(os.environ.get("MARKET_LEADER_SCORE_BONUS", "2"))
MARKET_LEADER_EDGE_BONUS = float(os.environ.get("MARKET_LEADER_EDGE_BONUS", "0.010"))
REQUIRE_LEADER_FLOW = os.environ.get("REQUIRE_LEADER_FLOW", "true").lower() == "true"
LEADER_SYNTHETIC_ENABLED = os.environ.get("LEADER_SYNTHETIC_ENABLED", "false").lower() == "true"
LEADER_SYNTH_MIN_NET = float(os.environ.get("LEADER_SYNTH_MIN_NET", "0.08"))
LEADER_SYNTH_SCORE_PENALTY = int(os.environ.get("LEADER_SYNTH_SCORE_PENALTY", "1"))
LEADER_SYNTH_EDGE_PENALTY = float(os.environ.get("LEADER_SYNTH_EDGE_PENALTY", "0.005"))
LEADER_FRESH_SIZE_SCALE = float(os.environ.get("LEADER_FRESH_SIZE_SCALE", "1.00"))
LEADER_STALE_SIZE_SCALE = float(os.environ.get("LEADER_STALE_SIZE_SCALE", "0.75"))
LEADER_SYNTH_SIZE_SCALE = float(os.environ.get("LEADER_SYNTH_SIZE_SCALE", "0.55"))
REQUIRE_ORDERBOOK_WS = os.environ.get("REQUIRE_ORDERBOOK_WS", "true").lower() == "true"
WS_BOOK_FALLBACK_ENABLED = os.environ.get("WS_BOOK_FALLBACK_ENABLED", "false").lower() == "true"
WS_BOOK_FALLBACK_MAX_AGE_MS = float(os.environ.get("WS_BOOK_FALLBACK_MAX_AGE_MS", "2500"))
PM_BOOK_FALLBACK_ENABLED = os.environ.get("PM_BOOK_FALLBACK_ENABLED", "false").lower() == "true"
LEADER_FLOW_FALLBACK_ENABLED = os.environ.get("LEADER_FLOW_FALLBACK_ENABLED", "true").lower() == "true"
LEADER_FLOW_FALLBACK_MAX_AGE_SEC = float(os.environ.get("LEADER_FLOW_FALLBACK_MAX_AGE_SEC", "90"))
REQUIRE_VOLUME_SIGNAL = os.environ.get("REQUIRE_VOLUME_SIGNAL", "true").lower() == "true"
STRICT_REQUIRE_FRESH_LEADER = os.environ.get("STRICT_REQUIRE_FRESH_LEADER", "false").lower() == "true"
STRICT_REQUIRE_FRESH_BOOK_WS = os.environ.get("STRICT_REQUIRE_FRESH_BOOK_WS", "true").lower() == "true"
MIN_ANALYSIS_QUALITY = float(os.environ.get("MIN_ANALYSIS_QUALITY", "0.55"))
MIN_ANALYSIS_CONVICTION = float(os.environ.get("MIN_ANALYSIS_CONVICTION", "0.50"))
WS_BOOK_SOFT_MAX_AGE_MS = float(os.environ.get("WS_BOOK_SOFT_MAX_AGE_MS", "20000"))
ANALYSIS_PROB_SCALE_MIN = float(os.environ.get("ANALYSIS_PROB_SCALE_MIN", "0.65"))
ANALYSIS_PROB_SCALE_MAX = float(os.environ.get("ANALYSIS_PROB_SCALE_MAX", "1.20"))
# Small tolerance for payout threshold to avoid dead-zone misses (e.g. 1.98x vs 2.00x).
PAYOUT_NEAR_MISS_TOL = float(os.environ.get("PAYOUT_NEAR_MISS_TOL", "0.03"))
ADAPTIVE_PAYOUT_MAX_UPSHIFT_15M = float(os.environ.get("ADAPTIVE_PAYOUT_MAX_UPSHIFT_15M", "0.05"))
ADAPTIVE_PAYOUT_MAX_UPSHIFT_5M = float(os.environ.get("ADAPTIVE_PAYOUT_MAX_UPSHIFT_5M", "0.05"))
# Mid-round booster (15m only): small additive bet at high payout/high conviction.
MID_BOOSTER_ENABLED = os.environ.get("MID_BOOSTER_ENABLED", "true").lower() == "true"
MID_BOOSTER_ANYTIME_15M = os.environ.get("MID_BOOSTER_ANYTIME_15M", "true").lower() == "true"
MID_BOOSTER_MIN_LEFT_15M = float(os.environ.get("MID_BOOSTER_MIN_LEFT_15M", "6.0"))
MID_BOOSTER_MAX_LEFT_15M = float(os.environ.get("MID_BOOSTER_MAX_LEFT_15M", "10.5"))
MID_BOOSTER_MIN_LEFT_HARD_15M = float(os.environ.get("MID_BOOSTER_MIN_LEFT_HARD_15M", "2.2"))
MID_BOOSTER_MIN_SCORE = int(os.environ.get("MID_BOOSTER_MIN_SCORE", "11"))
MID_BOOSTER_MIN_TRUE_PROB = float(os.environ.get("MID_BOOSTER_MIN_TRUE_PROB", "0.60"))
MID_BOOSTER_MIN_EDGE = float(os.environ.get("MID_BOOSTER_MIN_EDGE", "0.025"))
MID_BOOSTER_MIN_EV_NET = float(os.environ.get("MID_BOOSTER_MIN_EV_NET", "0.050"))
MID_BOOSTER_MIN_PAYOUT = float(os.environ.get("MID_BOOSTER_MIN_PAYOUT", "2.80"))
MID_BOOSTER_MAX_ENTRY = float(os.environ.get("MID_BOOSTER_MAX_ENTRY", "0.35"))
MID_BOOSTER_SIZE_PCT = float(os.environ.get("MID_BOOSTER_SIZE_PCT", "0.008"))
MID_BOOSTER_SIZE_PCT_HIGH = float(os.environ.get("MID_BOOSTER_SIZE_PCT_HIGH", "0.020"))
MID_BOOSTER_MAX_PER_CID = int(os.environ.get("MID_BOOSTER_MAX_PER_CID", "1"))
MID_BOOSTER_LOSS_STREAK_LOCK = int(os.environ.get("MID_BOOSTER_LOSS_STREAK_LOCK", "4"))
MID_BOOSTER_LOCK_HOURS = float(os.environ.get("MID_BOOSTER_LOCK_HOURS", "24"))
CONTINUATION_PRIOR_MAX_BOOST = float(os.environ.get("CONTINUATION_PRIOR_MAX_BOOST", "0.06"))
LOW_CENT_ONLY_ON_EXISTING_POSITION = os.environ.get("LOW_CENT_ONLY_ON_EXISTING_POSITION", "true").lower() == "true"
LOW_CENT_ENTRY_THRESHOLD = float(os.environ.get("LOW_CENT_ENTRY_THRESHOLD", "0.10"))
# Dynamic sizing guardrail for cheap-entry tails:
# - keep default risk around LOW_ENTRY_BASE_SOFT_MAX
# - allow growth toward LOW_ENTRY_HIGH_CONV_SOFT_MAX only on strong conviction
LOW_ENTRY_SOFT_THRESHOLD = float(os.environ.get("LOW_ENTRY_SOFT_THRESHOLD", "0.35"))
LOW_ENTRY_BASE_SOFT_MAX = float(os.environ.get("LOW_ENTRY_BASE_SOFT_MAX", "10.0"))
LOW_ENTRY_HIGH_CONV_SOFT_MAX = float(os.environ.get("LOW_ENTRY_HIGH_CONV_SOFT_MAX", "30.0"))
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
FAST_FOK_MIN_DEPTH_RATIO = float(os.environ.get("FAST_FOK_MIN_DEPTH_RATIO", "1.00"))
MAX_TAKER_SLIP_BPS_5M = float(os.environ.get("MAX_TAKER_SLIP_BPS_5M", "80"))
MAX_TAKER_SLIP_BPS_15M = float(os.environ.get("MAX_TAKER_SLIP_BPS_15M", "70"))
MAX_BOOK_SPREAD_5M = float(os.environ.get("MAX_BOOK_SPREAD_5M", "0.060"))
MAX_BOOK_SPREAD_15M = float(os.environ.get("MAX_BOOK_SPREAD_15M", "0.120"))
MAKER_ENTRY_TICK_TOL = int(os.environ.get("MAKER_ENTRY_TICK_TOL", "1"))
ORDER_RETRY_MAX = int(os.environ.get("ORDER_RETRY_MAX", "4"))
ORDER_RETRY_BASE_SEC = float(os.environ.get("ORDER_RETRY_BASE_SEC", "0.35"))
MAKER_PULLBACK_MAX_GAP_TICKS_5M = int(os.environ.get("MAKER_PULLBACK_MAX_GAP_TICKS_5M", "4"))
MAKER_PULLBACK_MAX_GAP_TICKS_15M = int(os.environ.get("MAKER_PULLBACK_MAX_GAP_TICKS_15M", "6"))

# Entry timing optimizer (15m):
# keep initial strong thesis, but allow a short wait for better pricing.
ENTRY_WAIT_ENABLED = os.environ.get("ENTRY_WAIT_ENABLED", "true").lower() == "true"
ENTRY_WAIT_WINDOW_15M_SEC = float(os.environ.get("ENTRY_WAIT_WINDOW_15M_SEC", "2.0"))
ENTRY_WAIT_POLL_SEC = float(os.environ.get("ENTRY_WAIT_POLL_SEC", "0.20"))
ENTRY_WAIT_MIN_LEFT_SEC = float(os.environ.get("ENTRY_WAIT_MIN_LEFT_SEC", "360"))
ENTRY_WAIT_MIN_SCORE = int(os.environ.get("ENTRY_WAIT_MIN_SCORE", "12"))
ENTRY_WAIT_MIN_TRUE_PROB = float(os.environ.get("ENTRY_WAIT_MIN_TRUE_PROB", "0.66"))
ENTRY_WAIT_MIN_ENTRY = float(os.environ.get("ENTRY_WAIT_MIN_ENTRY", "0.55"))
ENTRY_WAIT_MAX_ENTRY = float(os.environ.get("ENTRY_WAIT_MAX_ENTRY", "0.78"))
ENTRY_WAIT_MIN_IMPROVE_TICKS = int(os.environ.get("ENTRY_WAIT_MIN_IMPROVE_TICKS", "1"))
ENTRY_WAIT_MIN_IMPROVE_ABS = float(os.environ.get("ENTRY_WAIT_MIN_IMPROVE_ABS", "0.005"))
ENTRY_WAIT_TARGET_DROP_ABS = float(os.environ.get("ENTRY_WAIT_TARGET_DROP_ABS", "0.02"))
ENTRY_WAIT_MAX_SCORE_DECAY = int(os.environ.get("ENTRY_WAIT_MAX_SCORE_DECAY", "2"))
ENTRY_WAIT_MAX_PROB_DECAY = float(os.environ.get("ENTRY_WAIT_MAX_PROB_DECAY", "0.03"))
ENTRY_WAIT_MAX_EDGE_DECAY = float(os.environ.get("ENTRY_WAIT_MAX_EDGE_DECAY", "0.02"))
CONSISTENCY_CORE_ENABLED = os.environ.get("CONSISTENCY_CORE_ENABLED", "true").lower() == "true"
CONSISTENCY_REQUIRE_CL_AGREE_15M = os.environ.get("CONSISTENCY_REQUIRE_CL_AGREE_15M", "true").lower() == "true"
CONSISTENCY_MIN_TRUE_PROB_15M = float(os.environ.get("CONSISTENCY_MIN_TRUE_PROB_15M", "0.64"))
CONSISTENCY_MIN_EXEC_EV_15M = float(os.environ.get("CONSISTENCY_MIN_EXEC_EV_15M", "0.024"))
CONSISTENCY_MIN_TF_VOTES_15M = int(os.environ.get("CONSISTENCY_MIN_TF_VOTES_15M", "2"))
CONSISTENCY_MAX_ENTRY_15M = float(os.environ.get("CONSISTENCY_MAX_ENTRY_15M", "0.60"))
CONSISTENCY_MIN_PAYOUT_15M = float(os.environ.get("CONSISTENCY_MIN_PAYOUT_15M", "1.90"))
EV_FRONTIER_ENABLED = os.environ.get("EV_FRONTIER_ENABLED", "true").lower() == "true"
EV_FRONTIER_MARGIN_BASE = float(os.environ.get("EV_FRONTIER_MARGIN_BASE", "0.010"))
EV_FRONTIER_MARGIN_HIGH_ENTRY = float(os.environ.get("EV_FRONTIER_MARGIN_HIGH_ENTRY", "0.050"))
HIGH_EV_SIZE_BOOST_ENABLED = os.environ.get("HIGH_EV_SIZE_BOOST_ENABLED", "true").lower() == "true"
HIGH_EV_SIZE_BOOST = float(os.environ.get("HIGH_EV_SIZE_BOOST", "1.25"))
HIGH_EV_SIZE_BOOST_MAX = float(os.environ.get("HIGH_EV_SIZE_BOOST_MAX", "1.40"))
HIGH_EV_MIN_EXEC_EV = float(os.environ.get("HIGH_EV_MIN_EXEC_EV", "0.035"))
HIGH_EV_MIN_SCORE = int(os.environ.get("HIGH_EV_MIN_SCORE", "14"))
SUPER_BET_MIN_SIZE_ENABLED = os.environ.get("SUPER_BET_MIN_SIZE_ENABLED", "true").lower() == "true"
SUPER_BET_ENTRY_MAX = float(os.environ.get("SUPER_BET_ENTRY_MAX", "0.30"))
SUPER_BET_MIN_PAYOUT = float(os.environ.get("SUPER_BET_MIN_PAYOUT", "3.00"))
SUPER_BET_MIN_SIZE_USDC = float(os.environ.get("SUPER_BET_MIN_SIZE_USDC", "5.0"))
SUPER_BET_MAX_SIZE_ENABLED = os.environ.get("SUPER_BET_MAX_SIZE_ENABLED", "true").lower() == "true"
SUPER_BET_MAX_SIZE_USDC = float(os.environ.get("SUPER_BET_MAX_SIZE_USDC", "10.0"))
SUPER_BET_MAX_BANKROLL_PCT = float(os.environ.get("SUPER_BET_MAX_BANKROLL_PCT", "0.02"))
# Hard absolute cap for very cheap-tail entries after all tuning steps.
TAIL_ENTRY_ABS_CAP_ENABLED = os.environ.get("TAIL_ENTRY_ABS_CAP_ENABLED", "true").lower() == "true"
TAIL_ENTRY_CAP_PX = float(os.environ.get("TAIL_ENTRY_CAP_PX", "0.30"))
TAIL_ENTRY_MAX_SIZE_USDC = float(os.environ.get("TAIL_ENTRY_MAX_SIZE_USDC", "10.0"))
ROLLING_15M_CALIB_ENABLED = os.environ.get("ROLLING_15M_CALIB_ENABLED", "true").lower() == "true"
ROLLING_15M_CALIB_MIN_N = int(os.environ.get("ROLLING_15M_CALIB_MIN_N", "20"))
ROLLING_15M_CALIB_WINDOW = int(os.environ.get("ROLLING_15M_CALIB_WINDOW", "400"))
PM_PATTERN_ENABLED = os.environ.get("PM_PATTERN_ENABLED", "false").lower() == "true"
PM_PATTERN_MIN_N = int(os.environ.get("PM_PATTERN_MIN_N", "6"))
PM_PATTERN_MAX_EDGE_ADJ = float(os.environ.get("PM_PATTERN_MAX_EDGE_ADJ", "0.010"))
PM_PUBLIC_PATTERN_ENABLED = os.environ.get("PM_PUBLIC_PATTERN_ENABLED", "false").lower() == "true"
PM_PUBLIC_PATTERN_MIN_N = int(os.environ.get("PM_PUBLIC_PATTERN_MIN_N", "24"))
PM_PUBLIC_PATTERN_MAX_EDGE_ADJ = float(os.environ.get("PM_PUBLIC_PATTERN_MAX_EDGE_ADJ", "0.006"))
PM_PUBLIC_PATTERN_DOM_MED = float(os.environ.get("PM_PUBLIC_PATTERN_DOM_MED", "0.12"))
PM_PUBLIC_PATTERN_DOM_STRONG = float(os.environ.get("PM_PUBLIC_PATTERN_DOM_STRONG", "0.22"))
RECENT_SIDE_PRIOR_ENABLED = os.environ.get("RECENT_SIDE_PRIOR_ENABLED", "true").lower() == "true"
RECENT_SIDE_PRIOR_MIN_N = int(os.environ.get("RECENT_SIDE_PRIOR_MIN_N", "8"))
RECENT_SIDE_PRIOR_LOOKBACK_H = float(os.environ.get("RECENT_SIDE_PRIOR_LOOKBACK_H", "12"))
RECENT_SIDE_PRIOR_MAX_PROB_ADJ = float(os.environ.get("RECENT_SIDE_PRIOR_MAX_PROB_ADJ", "0.025"))
RECENT_SIDE_PRIOR_MAX_EDGE_ADJ = float(os.environ.get("RECENT_SIDE_PRIOR_MAX_EDGE_ADJ", "0.010"))
RECENT_SIDE_PRIOR_MAX_SCORE_ADJ = int(os.environ.get("RECENT_SIDE_PRIOR_MAX_SCORE_ADJ", "2"))
CONSISTENCY_TRAIL_ALLOW_MIN_PCT_LEFT_15M = float(os.environ.get("CONSISTENCY_TRAIL_ALLOW_MIN_PCT_LEFT_15M", "0.78"))
CONSISTENCY_STRONG_MIN_SCORE_15M = int(os.environ.get("CONSISTENCY_STRONG_MIN_SCORE_15M", "14"))
CONSISTENCY_STRONG_MIN_TRUE_PROB_15M = float(os.environ.get("CONSISTENCY_STRONG_MIN_TRUE_PROB_15M", "0.72"))
CONSISTENCY_STRONG_MIN_EXEC_EV_15M = float(os.environ.get("CONSISTENCY_STRONG_MIN_EXEC_EV_15M", "0.030"))
ASSET_QUALITY_FILTER_ENABLED = os.environ.get("ASSET_QUALITY_FILTER_ENABLED", "false").lower() == "true"
ASSET_QUALITY_MIN_TRADES = int(os.environ.get("ASSET_QUALITY_MIN_TRADES", "12"))
ASSET_QUALITY_MIN_PF = float(os.environ.get("ASSET_QUALITY_MIN_PF", "0.98"))
ASSET_QUALITY_MIN_EXP = float(os.environ.get("ASSET_QUALITY_MIN_EXP", "0.00"))
ASSET_QUALITY_SCORE_PENALTY = int(os.environ.get("ASSET_QUALITY_SCORE_PENALTY", "2"))
ASSET_QUALITY_EDGE_PENALTY = float(os.environ.get("ASSET_QUALITY_EDGE_PENALTY", "0.010"))
ASSET_QUALITY_HARD_BLOCK_ENABLED = os.environ.get("ASSET_QUALITY_HARD_BLOCK_ENABLED", "false").lower() == "true"
ASSET_QUALITY_BLOCK_PF = float(os.environ.get("ASSET_QUALITY_BLOCK_PF", "0.88"))
ASSET_QUALITY_BLOCK_EXP = float(os.environ.get("ASSET_QUALITY_BLOCK_EXP", "-0.25"))
MAX_WIN_MODE = os.environ.get("MAX_WIN_MODE", "true").lower() == "true"
WINMODE_MIN_TRUE_PROB_5M = float(os.environ.get("WINMODE_MIN_TRUE_PROB_5M", "0.58"))
WINMODE_MIN_TRUE_PROB_15M = float(os.environ.get("WINMODE_MIN_TRUE_PROB_15M", "0.62"))
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
CLOB_MARKET_WSS = os.environ.get("CLOB_MARKET_WSS", "wss://ws-subscriptions-clob.polymarket.com/ws/market")

# Binance symbols per asset (spot + futures share same symbol)
BNB_SYM = {info["asset"]: info["asset"].lower() + "usdt" for info in SERIES.values()}
# e.g. {"BTC": "btcusdt", "ETH": "ethusdt", ...}

G="\033[92m"; R="\033[91m"; Y="\033[93m"; B="\033[94m"; W="\033[97m"; RS="\033[0m"


class _JsonLogTee:
    """Mirror stdout/stderr to a persistent JSONL journal file."""

    def __init__(self, stream, stream_name: str, path: str, meta: dict, rotate_daily: bool = False):
        self._stream = stream
        self._stream_name = stream_name
        self._base_path = path
        self._meta = dict(meta or {})
        self._buf = ""
        self._lock = threading.Lock()
        self._rotate_daily = bool(rotate_daily)
        self._active_day = ""
        self._active_path = path

    def _daily_path(self, day: str) -> str:
        d = os.path.dirname(self._base_path) or "."
        fn = os.path.basename(self._base_path)
        stem, ext = os.path.splitext(fn)
        if not ext:
            ext = ".jsonl"
        return os.path.join(d, f"{stem}-{day}{ext}")

    def _ensure_target_path(self, ts_now: datetime):
        if not self._rotate_daily:
            self._active_path = self._base_path
            return
        day = ts_now.strftime("%Y-%m-%d")
        if day != self._active_day:
            self._active_day = day
            self._active_path = self._daily_path(day)

    def _append_json_line(self, msg: str, ts_now: datetime):
        if not msg:
            return
        self._ensure_target_path(ts_now)
        rec = {
            "ts": ts_now.isoformat(),
            "stream": self._stream_name,
            "msg": msg,
            **self._meta,
        }
        try:
            with open(self._active_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=True) + "\n")
        except Exception:
            # never break runtime printing on journal write issues
            pass

    def write(self, data):
        s = "" if data is None else str(data)
        with self._lock:
            self._stream.write(s)
            self._stream.flush()
            self._buf += s
            while "\n" in self._buf:
                line, self._buf = self._buf.split("\n", 1)
                self._append_json_line(line.rstrip("\r"), datetime.now(timezone.utc))
        return len(s)

    def flush(self):
        with self._lock:
            self._stream.flush()
            if self._buf:
                self._append_json_line(self._buf.rstrip("\r"), datetime.now(timezone.utc))
                self._buf = ""

    def isatty(self):
        try:
            return self._stream.isatty()
        except Exception:
            return False


def _install_runtime_json_log():
    if not RUNTIME_JSON_LOG_ENABLED:
        return
    try:
        os.makedirs(os.path.dirname(RUNTIME_JSON_LOG_FILE) or ".", exist_ok=True)
    except Exception:
        pass
    meta = {
        "service": os.environ.get("NF_SERVICE_ID", "") or os.environ.get("SERVICE_ID", ""),
        "instance": os.environ.get("HOSTNAME", ""),
        "deploy_sha": os.environ.get("NF_DEPLOYED_SHA", "") or os.environ.get("GIT_COMMIT", ""),
        "build_id": os.environ.get("NF_BUILD_ID", "") or os.environ.get("NORTHFLANK_BUILD_ID", ""),
        "pid": os.getpid(),
    }
    sys.stderr = _JsonLogTee(
        sys.stderr,
        "stderr",
        RUNTIME_JSON_LOG_FILE,
        meta,
        rotate_daily=RUNTIME_JSON_LOG_ROTATE_DAILY,
    )
    sys.stdout = _JsonLogTee(
        sys.stdout,
        "stdout",
        RUNTIME_JSON_LOG_FILE,
        meta,
        rotate_daily=RUNTIME_JSON_LOG_ROTATE_DAILY,
    )
    log_pattern = RUNTIME_JSON_LOG_FILE
    if RUNTIME_JSON_LOG_ROTATE_DAILY:
        d = os.path.dirname(RUNTIME_JSON_LOG_FILE) or "."
        fn = os.path.basename(RUNTIME_JSON_LOG_FILE)
        stem, ext = os.path.splitext(fn)
        if not ext:
            ext = ".jsonl"
        log_pattern = os.path.join(d, f"{stem}-YYYY-MM-DD{ext}")
    print(f"{B}[LOG-JSON]{RS} {log_pattern}")

# ─────────────────────────────────────────────────────────────────────────────

class LiveTrader:
    def __init__(self):
        self._boot_ts = _time.time()
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
        self._live_rk_repair_ts = 0.0
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
        self._pending_absent_counts = {}  # cid -> consecutive sync cycles absent from on-chain/API
        self.seen            = set()
        self._session        = None   # persistent aiohttp session
        self._book_cache     = {}     # token_id -> {"ts_ms": float, "book": OrderBook}
        self._book_sem       = asyncio.Semaphore(max(1, BOOK_FETCH_CONCURRENCY))
        self._clob_market_ws = None
        self._clob_ws_books  = {}     # token_id -> {"ts_ms": float, "best_bid": float, "best_ask": float, "tick": float, "asks": [(p,s)]}
        self._clob_ws_assets_subscribed = set()
        self._heartbeat_last_ok = 0.0
        self._heartbeat_id   = ""
        self._order_event_cache = {}  # order_id -> {"status": str, "filled_size": float, "ts": float}
        self.cl_prices       = {}    # Chainlink oracle prices (resolution source)
        self.cl_updated      = {}    # Chainlink last update timestamp per asset
        self.bankroll        = BANKROLL
        self.start_bank      = BANKROLL
        self._pnl_baseline_locked = False
        self._pnl_baseline_ts = ""
        self.onchain_wallet_usdc = BANKROLL
        self.onchain_open_positions = 0.0
        # Canonical aliases used by status/log renderer.
        self.onchain_usdc_balance = BANKROLL
        self.onchain_open_stake_total = 0.0
        self.onchain_redeemable_usdc = 0.0
        self.onchain_open_mark_value = 0.0
        self.onchain_open_count = 0
        self.onchain_redeemable_count = 0
        self.onchain_open_cids = set()
        self.onchain_open_usdc_by_cid = {}
        self.onchain_open_stake_by_cid = {}
        self.onchain_open_shares_by_cid = {}
        self.onchain_open_meta_by_cid = {}
        self.onchain_settling_usdc_by_cid = {}
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
        self._resolved_samples = deque(maxlen=2000)  # rolling settled outcomes for 15m calibration
        self._pm_pattern_stats = {}
        self.side_perf       = {}                 # "ASSET|SIDE" -> {n, gross_win, gross_loss, pnl}
        self._last_eval_time    = {}              # cid → last RTDS-triggered evaluate() timestamp
        self._exec_lock         = asyncio.Lock()
        self._executing_cids    = set()
        self._round_side_attempt_ts = {}          # "round_key|side" → last attempt ts
        self._cid_side_attempt_ts = {}            # "cid|side" → last attempt ts
        self._round_side_block_until = {}         # "round_fingerprint|side" → ttl epoch
        self._booster_used_by_cid = {}            # cid -> count of executed booster add-ons
        self._booster_consec_losses = 0
        self._booster_lock_until = 0.0
        self._redeem_tx_lock    = asyncio.Lock()  # serialize redeem txs to avoid nonce clashes
        self._nonce_mgr         = None
        self._errors            = ErrorTracker()
        self._bucket_stats      = BucketStats()
        self._copyflow_map      = {}
        self._copyflow_leaders  = {}
        self._copyflow_leaders_live = {}
        self._copyflow_leaders_family = {}
        self._copyflow_live     = {}
        self._copyflow_live_last_update_ts = 0.0
        self._copyflow_live_zero_streak = 0
        self._copyflow_force_refresh_ts = 0.0
        self._copyflow_cid_on_demand_ts = {}
        self._ws_stale_hits = 0
        self._ws_last_heal_ts = 0.0
        self._skip_events = deque(maxlen=8000)  # (ts, reason)
        self._cid_family_cache  = {}
        self._prebid_plan       = {}
        self._copyflow_mtime    = 0.0
        self._copyflow_last_try = 0.0
        self.peak_bankroll      = BANKROLL           # track peak for drawdown guard
        self.consec_losses      = 0                  # consecutive resolved losses counter
        self._pause_entries_until = 0.0
        self._settled_outcomes = {}
        self._last_round_best   = ""
        self._last_round_pick   = ""
        self._last_round_best_ts = 0.0
        self._last_round_pick_ts = 0.0
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
        self._load_settled_outcomes()
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

    def _noisy_log_enabled(self, key: str, every_sec: float = LOG_DEBUG_EVERY_SEC) -> bool:
        # Operator mode: keep logs clean by default; show extra diagnostics sparsely.
        return LOG_VERBOSE or LOG_LIVE_DETAIL or self._should_log(key, every_sec)

    def _skip_tick(self, reason: str):
        try:
            self._skip_events.append((_time.time(), str(reason or "unknown")))
        except Exception:
            pass

    def _skip_top(self, window_sec: int = SKIP_STATS_WINDOW_SEC, top_n: int = SKIP_STATS_TOP_N):
        now = _time.time()
        while self._skip_events and (now - float(self._skip_events[0][0] or 0.0)) > max(60, int(window_sec)):
            self._skip_events.popleft()
        counts = defaultdict(int)
        for _, r in self._skip_events:
            counts[r] += 1
        total = sum(counts.values())
        top = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[: max(1, int(top_n))]
        return total, top

    def _feed_health_snapshot(self):
        now = _time.time()
        tids = list(self._trade_focus_token_ids() or self._active_token_ids())
        active_tokens = len(tids)
        fresh_books = 0
        ws_ages = []
        for tid in tids:
            age = self._clob_ws_book_age_ms(tid)
            ws_ages.append(age)
            if age <= CLOB_MARKET_WS_MAX_AGE_MS:
                fresh_books += 1
        ws_med = sorted(ws_ages)[len(ws_ages) // 2] if ws_ages else 9e9
        # Market-level WS health (preferred for trade gate):
        # one fresh side per active CID is enough to keep realtime decision path alive.
        active_markets = len(self.active_mkts)
        fresh_markets = 0
        ws_market_ages = []
        for m in self.active_mkts.values():
            tu = str(m.get("token_up", "") or "").strip()
            td = str(m.get("token_down", "") or "").strip()
            if not tu and not td:
                continue
            au = self._clob_ws_book_age_ms(tu) if tu else 9e9
            ad = self._clob_ws_book_age_ms(td) if td else 9e9
            best_age = min(au, ad)
            ws_market_ages.append(best_age)
            if best_age <= CLOB_MARKET_WS_MAX_AGE_MS:
                fresh_markets += 1
        ws_market_med = (
            sorted(ws_market_ages)[len(ws_market_ages) // 2]
            if ws_market_ages else 9e9
        )

        cids = list(self.active_mkts.keys())
        active_cids = len(cids)
        fresh_leaders = 0
        leader_ages = []
        for cid in cids:
            row = self._copyflow_live.get(cid, {}) if isinstance(self._copyflow_live, dict) else {}
            ts = float(row.get("ts", 0.0) or 0.0)
            n = int(row.get("n", 0) or 0)
            age = (now - ts) if ts > 0 else 9e9
            leader_ages.append(age)
            if ts > 0 and n > 0 and age <= COPYFLOW_LIVE_MAX_AGE_SEC:
                fresh_leaders += 1
        leader_med = sorted(leader_ages)[len(leader_ages) // 2] if leader_ages else 9e9

        rtds_ages = []
        for a in ("BTC", "ETH", "SOL", "XRP"):
            ts = float(self._ema_ts.get(a, 0.0) or 0.0)
            if ts > 0:
                rtds_ages.append(now - ts)
        rtds_med = sorted(rtds_ages)[len(rtds_ages) // 2] if rtds_ages else 9e9

        cl_ages = []
        for a in ("BTC", "ETH", "SOL", "XRP"):
            ts = float(self.cl_updated.get(a, 0.0) or 0.0)
            if ts > 0:
                cl_ages.append(now - ts)
        cl_med = sorted(cl_ages)[len(cl_ages) // 2] if cl_ages else 9e9

        return {
            "active_tokens": active_tokens,
            "fresh_books": fresh_books,
            "ws_med_ms": ws_med,
            "active_markets": active_markets,
            "fresh_markets": fresh_markets,
            "ws_market_med_ms": ws_market_med,
            "active_cids": active_cids,
            "fresh_leaders": fresh_leaders,
            "leader_med_s": leader_med,
            "rtds_med_s": rtds_med,
            "cl_med_s": cl_med,
        }

    def _ws_trade_gate_ok(self):
        hs = self._feed_health_snapshot()
        am = int(hs.get("active_markets", 0) or 0)
        if am <= 0:
            return False, hs
        fresh = int(hs.get("fresh_markets", 0) or 0)
        ratio = fresh / max(1, am)
        ws_med = float(hs.get("ws_market_med_ms", 9e9) or 9e9)
        ok = (ratio >= WS_HEALTH_MIN_FRESH_RATIO) and (ws_med <= WS_HEALTH_MAX_MED_AGE_MS)
        return ok, hs

    def _booster_locked(self) -> bool:
        return _time.time() < float(self._booster_lock_until or 0.0)

    def _short_cid(self, cid: str) -> str:
        if not cid:
            return "n/a"
        return f"{cid[:10]}..."

    def _as_float(self, v, default: float = 0.0) -> float:
        try:
            if v is None:
                return float(default)
            return float(v)
        except Exception:
            return float(default)

    def _extract_usdc_in_from_tx(self, tx_hash: str) -> float:
        """Exact USDC amount received by wallet from tx receipt Transfer logs."""
        if not tx_hash or not self.w3:
            return 0.0
        try:
            rcpt = self.w3.eth.get_transaction_receipt(tx_hash)
            logs = list(getattr(rcpt, "logs", None) or rcpt.get("logs", []) or [])
            transfer_sig = Web3.keccak(text="Transfer(address,address,uint256)").hex().lower()
            usdc_addr = Web3.to_checksum_address(USDC_E_ADDR).lower()
            me = Web3.to_checksum_address(ADDRESS).lower()
            got = 0
            for lg in logs:
                lg_addr = str(lg.get("address", "")).lower()
                if lg_addr != usdc_addr:
                    continue
                topics = lg.get("topics", []) or []
                if len(topics) < 3:
                    continue
                t0 = str(topics[0].hex() if hasattr(topics[0], "hex") else topics[0]).lower()
                if t0 != transfer_sig:
                    continue
                to_topic = str(topics[2].hex() if hasattr(topics[2], "hex") else topics[2]).lower()
                to_addr = "0x" + to_topic[-40:]
                if to_addr.lower() != me:
                    continue
                data = lg.get("data", "0x0")
                raw = int(data.hex(), 16) if hasattr(data, "hex") else int(str(data), 16)
                got += raw
            return got / 1e6
        except Exception:
            return 0.0

    def _count_pending_redeem_by_rk(self, rk: str) -> int:
        n = 0
        for cid_x, val_x in list(self.pending_redeem.items()):
            if isinstance(val_x[0], dict):
                m_x, t_x = val_x
            else:
                side_x, asset_x = val_x
                m_x = {"conditionId": cid_x, "asset": asset_x}
                t_x = {"side": side_x, "asset": asset_x, "duration": 0}
            if self._round_key(cid=cid_x, m=m_x, t=t_x) == rk:
                n += 1
        return n

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

    @staticmethod
    def _normalize_side_label(side_raw: str) -> str:
        s = str(side_raw or "").strip().lower()
        if s in ("up", "yes", "higher", "above"):
            return "Up"
        if s in ("down", "no", "lower", "below"):
            return "Down"
        return str(side_raw or "").strip()

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

    def _coerce_json_list(self, v):
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            try:
                x = json.loads(v)
                return x if isinstance(x, list) else []
            except Exception:
                return []
        return []

    def _map_updown_market_fields(self, market: dict, fallback: dict | None = None) -> tuple[float, str, str]:
        """Return (up_price, token_up, token_down) robustly from outcomes/tokens.
        Prevents inverted side mapping when API ordering is not strictly [Up, Down]."""
        fb = fallback or {}
        outcomes = self._coerce_json_list((market or {}).get("outcomes")) or self._coerce_json_list(fb.get("outcomes"))
        prices = self._coerce_json_list((market or {}).get("outcomePrices")) or self._coerce_json_list(fb.get("outcomePrices"))
        tokens = self._coerce_json_list((market or {}).get("clobTokenIds")) or self._coerce_json_list(fb.get("clobTokenIds"))

        idx_up = None
        idx_down = None
        for i, raw in enumerate(outcomes):
            s = str(raw or "").strip().lower()
            if idx_up is None and s in ("up", "higher", "above", "yes"):
                idx_up = i
            if idx_down is None and s in ("down", "lower", "below", "no"):
                idx_down = i
        if idx_up is None and idx_down is not None and len(tokens) >= 2:
            idx_up = 1 - idx_down
        if idx_down is None and idx_up is not None and len(tokens) >= 2:
            idx_down = 1 - idx_up
        if idx_up is None and len(tokens) > 0:
            idx_up = 0
        if idx_down is None and len(tokens) > 1:
            idx_down = 1 if idx_up == 0 else 0

        token_up = str(tokens[idx_up]) if (idx_up is not None and idx_up < len(tokens)) else (str(tokens[0]) if len(tokens) > 0 else "")
        token_down = str(tokens[idx_down]) if (idx_down is not None and idx_down < len(tokens)) else (str(tokens[1]) if len(tokens) > 1 else "")

        up_price = 0.5
        pick = idx_up if idx_up is not None else 0
        if len(prices) > pick:
            try:
                up_price = float(prices[pick])
            except Exception:
                up_price = 0.5
        return up_price, token_up, token_down

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

    def _is_historical_expired_position(self, pos: dict, now_ts: float, grace_sec: float = 1200.0) -> bool:
        """True for clearly old/expired market windows to avoid replaying stale losses."""
        try:
            end_ts = 0.0
            for ke in ("endDate", "end_date", "eventEndTime"):
                s = pos.get(ke)
                if isinstance(s, str) and s:
                    try:
                        end_ts = datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
                        break
                    except Exception:
                        pass
            q_st, q_et = self._round_bounds_from_question(str(pos.get("title", "") or ""))
            if q_et > 0:
                end_ts = q_et
            return end_ts > 0 and now_ts > (end_ts + max(60.0, float(grace_sec)))
        except Exception:
            return False

    async def _http_get_json(self, url: str, params: dict | None = None, timeout: float = 8.0):
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=max(1, HTTP_CONN_LIMIT),
                limit_per_host=max(1, HTTP_CONN_PER_HOST),
                ttl_dns_cache=max(0, HTTP_DNS_TTL_SEC),
                enable_cleanup_closed=True,
                keepalive_timeout=max(5.0, HTTP_KEEPALIVE_SEC),
            )
            self._session = aiohttp.ClientSession(
                connector=connector,
                headers={"User-Agent": "clawdbot-live/1.0"},
            )
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

    async def _gather_bounded(self, coros, limit: int):
        """Run awaitables with bounded concurrency to avoid network bursts."""
        sem = asyncio.Semaphore(max(1, int(limit)))

        async def _run(coro):
            async with sem:
                return await coro

        return await asyncio.gather(*[_run(c) for c in coros], return_exceptions=True)

    def _prune_order_event_cache(self):
        cutoff = _time.time() - max(10.0, USER_EVENTS_CACHE_TTL_SEC)
        for oid, ev in list(self._order_event_cache.items()):
            if float(ev.get("ts", 0.0) or 0.0) < cutoff:
                self._order_event_cache.pop(oid, None)

    def _cache_order_event(self, oid: str, status: str = "", filled_size: float = 0.0):
        oid = (oid or "").strip()
        if not oid:
            return
        st = (status or "").lower().strip()
        prev = self._order_event_cache.get(oid, {})
        fs_prev = float(prev.get("filled_size", 0.0) or 0.0)
        self._order_event_cache[oid] = {
            "status": st or str(prev.get("status", "")),
            "filled_size": max(fs_prev, float(filled_size or 0.0)),
            "ts": _time.time(),
        }

    def _extract_order_event(self, row: dict):
        """Best-effort normalize notifications row into (order_id, status, filled_size)."""
        if not isinstance(row, dict):
            return "", "", 0.0
        payload = row.get("payload")
        if not isinstance(payload, dict):
            payload = {}
        o = row.get("order") if isinstance(row.get("order"), dict) else {}
        oid = (
            row.get("order_id")
            or row.get("orderID")
            or row.get("orderId")
            or payload.get("order_id")
            or payload.get("orderID")
            or payload.get("orderId")
            or o.get("id")
            or o.get("order_id")
            or ""
        )
        status = (
            row.get("status")
            or row.get("event_type")
            or row.get("type")
            or payload.get("status")
            or payload.get("event_type")
            or payload.get("type")
            or o.get("status")
            or ""
        )
        filled_size = (
            row.get("filled_size")
            or row.get("filledSize")
            or payload.get("filled_size")
            or payload.get("filledSize")
            or o.get("filled_size")
            or o.get("filledSize")
            or 0.0
        )
        try:
            filled_size = float(filled_size or 0.0)
        except Exception:
            filled_size = 0.0
        st = str(status or "").lower()
        # Normalize common variants.
        if st in ("match", "matched", "fill", "filled", "trade"):
            st = "filled"
        elif st in ("cancel", "canceled", "cancelled", "killed", "rejected"):
            st = "canceled"
        elif st in ("open", "live", "placed", "created"):
            st = "live"
        return str(oid or ""), st, filled_size

    async def _get_order_book(self, token_id: str, force_fresh: bool = False):
        """Low-latency orderbook fetch with tiny TTL cache to avoid duplicate roundtrips."""
        if not token_id:
            return None
        now_ms = _time.time() * 1000.0
        if not force_fresh:
            cached = self._book_cache.get(token_id)
            if cached and (now_ms - float(cached.get("ts_ms", 0.0))) <= BOOK_CACHE_TTL_MS:
                return cached.get("book")
        loop = asyncio.get_event_loop()
        async with self._book_sem:
            if not force_fresh:
                now_ms = _time.time() * 1000.0
                cached = self._book_cache.get(token_id)
                if cached and (now_ms - float(cached.get("ts_ms", 0.0))) <= BOOK_CACHE_TTL_MS:
                    return cached.get("book")
            book = await loop.run_in_executor(None, lambda: self.clob.get_order_book(token_id))
            self._book_cache[token_id] = {"ts_ms": _time.time() * 1000.0, "book": book}
            if len(self._book_cache) > max(16, BOOK_CACHE_MAX):
                # Evict oldest entries to cap memory/lookup overhead.
                oldest = sorted(
                    self._book_cache.items(),
                    key=lambda kv: float(kv[1].get("ts_ms", 0.0))
                )[: max(1, len(self._book_cache) - BOOK_CACHE_MAX)]
                for k, _ in oldest:
                    self._book_cache.pop(k, None)
            return book

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
                    self._fetch_condition_trades(
                        cid, COPYFLOW_INTEL_TRADES_PER_MARKET, timeout=10
                    )
                    for cid in cids
                ]
                rows_list = await self._gather_bounded(tasks, COPYFLOW_HTTP_MAX_PARALLEL)
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
                acts = await self._gather_bounded(act_tasks, COPYFLOW_HTTP_MAX_PARALLEL)
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
                    looked = await self._gather_bounded(
                        [self._cid_family(cid) for cid in lookups],
                        COPYFLOW_HTTP_MAX_PARALLEL,
                    )
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

    async def _copyflow_live_refresh_once(self, reason: str = "loop") -> int:
        leaders = dict(self._copyflow_leaders)
        for w, s in self._copyflow_leaders_live.items():
            leaders[w] = max(float(s), float(leaders.get(w, 0.0) or 0.0))
        cids = list(self.active_mkts.keys())[: max(1, COPYFLOW_LIVE_MAX_MARKETS)]
        if not cids:
            self._copyflow_live_zero_streak += 1
            return 0
        if not leaders:
            # Fallback 1: seed from family leaders (asset-duration specialists).
            for cid in cids:
                fam = self.active_mkts.get(cid, {})
                fam_key = f"{(fam.get('asset') or '?').upper()}-{int(fam.get('duration') or 0)}m"
                fam_map = self._copyflow_leaders_family.get(fam_key, {})
                if not isinstance(fam_map, dict):
                    continue
                for w, s in fam_map.items():
                    try:
                        leaders[w] = max(float(leaders.get(w, 0.0) or 0.0), float(s or 0.0) * 0.90)
                    except Exception:
                        continue
        if not leaders:
            # Fallback 2: bootstrap recent active wallets from current market flow.
            boot_count = {}
            for cid in cids:
                try:
                    rows_boot = await self._fetch_condition_trades(
                        cid, max(30, COPYFLOW_LIVE_TRADES_LIMIT // 2), timeout=6
                    )
                except Exception:
                    rows_boot = []
                if not isinstance(rows_boot, list):
                    continue
                for tr in rows_boot:
                    if (tr.get("side") or "").upper() != "BUY":
                        continue
                    w = (tr.get("proxyWallet") or "").lower().strip()
                    if not w:
                        continue
                    boot_count[w] = int(boot_count.get(w, 0)) + 1
            for w, c in boot_count.items():
                if c >= 1:
                    leaders[w] = max(float(leaders.get(w, 0.0) or 0.0), min(0.25, 0.08 + 0.02 * c))
        if not leaders:
            self._copyflow_live_zero_streak += 1
            return 0

        tasks = [
            self._fetch_condition_trades(
                cid, COPYFLOW_LIVE_TRADES_LIMIT, timeout=8
            )
            for cid in cids
        ]
        rows_list = await self._gather_bounded(tasks, COPYFLOW_HTTP_MAX_PARALLEL)
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
                "ts": _time.time(),
                "src": "live",
            }
            updated += 1
        if updated > 0:
            self._copyflow_live_last_update_ts = _time.time()
            self._copyflow_live_zero_streak = 0
        else:
            self._copyflow_live_zero_streak += 1
        if updated and self._should_log("copyflow-live", 60):
            sample_cid = cids[0]
            sm = self._copyflow_live.get(sample_cid, {})
            if sm:
                print(
                    f"{B}[COPY-LIVE]{RS} refreshed {updated} mkts | "
                    f"avg_entry={sm.get('avg_entry_c',0):.1f}c "
                    f"low/high={sm.get('low_c_share',0):.0%}/{sm.get('high_c_share',0):.0%} "
                    f"multibet={sm.get('multibet_ratio',0):.0%} | reason={reason}"
                )
            else:
                print(f"{B}[COPY-LIVE]{RS} refreshed {updated} active markets | reason={reason}")
        return updated

    async def _fetch_condition_trades(self, cid: str, limit: int, timeout: int = 8):
        """Fetch trades for a specific conditionId and hard-filter by conditionId.
        Data API may return unrelated rows when params are malformed; never trust unfiltered output.
        """
        cid_l = str(cid or "").lower().strip()
        if not cid_l:
            return []
        lim = max(1, int(limit))
        attempts = [
            {"conditionId": cid, "limit": str(lim)},
            {"market": cid, "limit": str(lim)},
            {"condition_id": cid, "limit": str(lim)},
        ]
        rows_all = []
        for _ in range(max(1, COPYFLOW_FETCH_RETRIES)):
            for prm in attempts:
                try:
                    rows = await self._http_get_json(
                        "https://data-api.polymarket.com/trades",
                        params=prm,
                        timeout=timeout,
                    )
                except Exception:
                    rows = []
                if isinstance(rows, list) and rows:
                    rows_all.extend(rows)
            if rows_all:
                break
        if not rows_all:
            return []

        out = []
        seen = set()
        for tr in rows_all:
            row = tr or {}
            c = str(row.get("conditionId", "")).lower().strip()
            if c != cid_l:
                continue
            k = (
                str(row.get("transactionHash", "")).lower().strip(),
                str(row.get("outcome", "")),
                str(row.get("proxyWallet", "")).lower().strip(),
                str(row.get("price", "")),
                str(row.get("size", "")),
                str(row.get("timestamp", "")),
            )
            if k in seen:
                continue
            seen.add(k)
            out.append(row)
        return out

    async def _fetch_recent_trades(self, limit: int = 300, timeout: int = 8):
        try:
            rows = await self._http_get_json(
                "https://data-api.polymarket.com/trades",
                params={"limit": str(max(1, int(limit)))},
                timeout=timeout,
            )
        except Exception:
            rows = []
        return rows if isinstance(rows, list) else []

    def _trade_matches_family(self, tr: dict, asset: str, duration: int) -> bool:
        """Match a trade row to current market family (asset + 15m/5m) using API metadata."""
        try:
            a = str(asset or "").upper()
            d = int(duration or 0)
            slug = str((tr or {}).get("slug", "") or "").lower()
            title = str((tr or {}).get("title", "") or "").lower()
            if a and a.lower() not in slug and a.lower() not in title:
                return False
            if d <= 5:
                return ("5m" in slug) or ("5 minutes" in title) or ("5 min" in title)
            return ("15m" in slug) or ("15 minutes" in title) or ("15 min" in title)
        except Exception:
            return False

    async def _copyflow_refresh_cid_once(self, cid: str, reason: str = "cid") -> int:
        """Targeted leader-flow refresh for one CID used by scorer on missing/stale leader data."""
        cid = str(cid or "").strip()
        if not cid:
            return 0
        leaders = dict(self._copyflow_leaders)
        for w, s in self._copyflow_leaders_live.items():
            leaders[w] = max(float(s), float(leaders.get(w, 0.0) or 0.0))
        use_unweighted_flow = False

        # Fallback: bootstrap candidate leaders directly from the same CID recent buys.
        if not leaders:
            boot_count = {}
            rows_boot = await self._fetch_condition_trades(
                cid, max(30, COPYFLOW_LIVE_TRADES_LIMIT // 2), timeout=6
            )
            for tr in rows_boot:
                if (tr.get("side") or "").upper() != "BUY":
                    continue
                w = (tr.get("proxyWallet") or "").lower().strip()
                if not w:
                    continue
                boot_count[w] = int(boot_count.get(w, 0)) + 1
            for w, c in boot_count.items():
                if c >= 1:
                    leaders[w] = max(float(leaders.get(w, 0.0) or 0.0), min(0.25, 0.08 + 0.02 * c))
        if not leaders:
            # No ranked leaders available right now: continue with real market flow (unweighted).
            use_unweighted_flow = True

        rows = await self._fetch_condition_trades(cid, COPYFLOW_LIVE_TRADES_LIMIT, timeout=8)
        if not rows:
            # Source-level fallback (real data): use most recent trades from same family.
            fam = self.active_mkts.get(cid, {})
            fam_asset = (fam.get("asset") or "").upper()
            fam_dur = int(fam.get("duration") or 0)
            if fam_asset and fam_dur in (5, 15):
                rows_all = await self._fetch_recent_trades(
                    max(200, COPYFLOW_LIVE_TRADES_LIMIT * 2), timeout=8
                )
                rows = [tr for tr in rows_all if self._trade_matches_family(tr, fam_asset, fam_dur)]
        if not rows:
            self._copyflow_live_zero_streak += 1
            return 0

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
            if use_unweighted_flow:
                wt = 1.0
            else:
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
            self._copyflow_live_zero_streak += 1
            return 0
        self._copyflow_live[cid] = {
            "Up": round(up / s, 4),
            "Down": round(down / s, 4),
            "n": int(round(s * 10)),
            "avg_entry_c": round((px_w_sum / px_w_den) * 100.0, 1) if px_w_den > 0 else 0.0,
            "low_c_share": round(low_w / max(1e-9, (low_w + mid_w + high_w)), 4),
            "high_c_share": round(high_w / max(1e-9, (low_w + mid_w + high_w)), 4),
            "multibet_ratio": round(
                (sum(1 for n in wallet_buy_n.values() if n >= 2) / max(1, len(wallet_buy_n))), 4
            ),
            "ts": _time.time(),
            "src": "live-unweighted" if use_unweighted_flow else "live",
        }
        self._copyflow_live_last_update_ts = _time.time()
        self._copyflow_live_zero_streak = 0
        if self._should_log(f"copyflow-cid-refresh:{cid}", 20):
            row = self._copyflow_live.get(cid, {})
            print(
                f"{B}[COPY-LIVE]{RS} cid refresh ok | cid={self._short_cid(cid)} "
                f"n={int(row.get('n',0) or 0)} up={float(row.get('Up',0.0) or 0.0):.2f} "
                f"dn={float(row.get('Down',0.0) or 0.0):.2f} | reason={reason}"
            )
        return 1

    async def _copyflow_live_loop(self):
        """Build per-market leader side-flow from latest PM trades for active markets."""
        if not COPYFLOW_LIVE_ENABLED or DRY_RUN:
            return
        while True:
            await asyncio.sleep(COPYFLOW_LIVE_REFRESH_SEC)
            try:
                await self._copyflow_live_refresh_once(reason="loop")
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
            f"{B}[BOOT]{RS} leader_follow={MARKET_LEADER_FOLLOW_ENABLED} "
            f"leader_min_net={MARKET_LEADER_MIN_NET:.2f} leader_min_n={MARKET_LEADER_MIN_N} "
            f"leader_bonus(score/edge)={MARKET_LEADER_SCORE_BONUS}/{MARKET_LEADER_EDGE_BONUS:.3f}"
        )
        print(
            f"{B}[BOOT]{RS} require(leader/book/vol)="
            f"{REQUIRE_LEADER_FLOW}/{REQUIRE_ORDERBOOK_WS}/{REQUIRE_VOLUME_SIGNAL}"
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
            f"book_stale<= {MAX_ORDERBOOK_AGE_MS:.0f}ms "
            f"min_mins_left(5m/15m)={MIN_MINS_LEFT_5M:.1f}/{MIN_MINS_LEFT_15M:.1f} "
            f"late_dir_lock={LATE_DIR_LOCK_ENABLED} "
            f"lock_mins(5m/15m)={LATE_DIR_LOCK_MIN_LEFT_5M:.1f}/{LATE_DIR_LOCK_MIN_LEFT_15M:.1f} "
            f"lock_move>={LATE_DIR_LOCK_MIN_MOVE_PCT*100:.03f}% "
            f"extra_score(BTC5m/XRP15m)=+{EXTRA_SCORE_GATE_BTC_5M}/+{EXTRA_SCORE_GATE_XRP_15M} "
            f"exposure trend(total/side)={EXPOSURE_CAP_TOTAL_TREND:.0%}/{EXPOSURE_CAP_SIDE_TREND:.0%} "
            f"chop(total/side)={EXPOSURE_CAP_TOTAL_CHOP:.0%}/{EXPOSURE_CAP_SIDE_CHOP:.0%}"
        )
        print(
            f"{B}[BOOT]{RS} copyflow_live={COPYFLOW_LIVE_ENABLED} "
            f"copy_intel24h={COPYFLOW_INTEL_ENABLED} "
            f"live_refresh={COPYFLOW_LIVE_REFRESH_SEC}s "
            f"live_max_age={COPYFLOW_LIVE_MAX_AGE_SEC:.0f}s "
            f"health_refresh={COPYFLOW_HEALTH_FORCE_REFRESH_SEC:.0f}s "
            f"intel_refresh={COPYFLOW_INTEL_REFRESH_SEC}s "
            f"copy_http_parallel={COPYFLOW_HTTP_MAX_PARALLEL}"
        )
        print(
            f"{B}[BOOT]{RS} leader_tiers "
            f"require={REQUIRE_LEADER_FLOW} "
            f"synth={LEADER_SYNTHETIC_ENABLED} "
            f"synth_min_net={LEADER_SYNTH_MIN_NET:.2f} "
            f"penalty(score/edge)={LEADER_SYNTH_SCORE_PENALTY}/{LEADER_SYNTH_EDGE_PENALTY:.3f} "
            f"size_scale(fresh/stale/synth)="
            f"{LEADER_FRESH_SIZE_SCALE:.2f}/{LEADER_STALE_SIZE_SCALE:.2f}/{LEADER_SYNTH_SIZE_SCALE:.2f}"
        )
        print(
            f"{B}[BOOT]{RS} clob_market_ws={CLOB_MARKET_WS_ENABLED} "
            f"sync={CLOB_MARKET_WS_SYNC_SEC:.1f}s "
            f"book_ws_age(strict/soft)<={CLOB_MARKET_WS_MAX_AGE_MS:.0f}/{CLOB_MARKET_WS_SOFT_AGE_MS:.0f}ms "
            f"heal(hits/cooldown)={CLOB_WS_STALE_HEAL_HITS}/{CLOB_WS_STALE_HEAL_COOLDOWN_SEC:.0f}s "
            f"pm_fallback={PM_BOOK_FALLBACK_ENABLED} "
            f"ws_gate={WS_HEALTH_REQUIRED} ratio>={WS_HEALTH_MIN_FRESH_RATIO:.2f} "
            f"ws_med<={WS_HEALTH_MAX_MED_AGE_MS:.0f}ms"
        )
        print(
            f"{B}[BOOT]{RS} scan_log_change_only={LOG_SCAN_ON_CHANGE_ONLY} "
            f"scan_heartbeat={LOG_SCAN_EVERY_SEC}s"
        )
        print(
            f"{B}[BOOT]{RS} log(flow/round-empty/settle/bank)="
            f"{LOG_FLOW_EVERY_SEC}/{LOG_ROUND_EMPTY_EVERY_SEC}/"
            f"{LOG_SETTLE_FIRST_EVERY_SEC}/{LOG_BANK_EVERY_SEC}s "
            f"bank_delta>={LOG_BANK_MIN_DELTA:.2f} "
            f"live_detail={LOG_LIVE_DETAIL} debug_every={LOG_DEBUG_EVERY_SEC}s "
            f"round_signal_every={LOG_ROUND_SIGNAL_EVERY_SEC}s "
            f"skip/edge/exec={LOG_SKIP_EVERY_SEC}/{LOG_EDGE_EVERY_SEC}/{LOG_EXEC_EVERY_SEC}s"
        )
        print(
            f"{B}[BOOT]{RS} redeem_poll={REDEEM_POLL_SEC:.1f}s "
            f"redeem_poll_active={REDEEM_POLL_SEC_ACTIVE:.2f}s "
            f"force_redeem_scan={FORCE_REDEEM_SCAN_SEC}s "
            f"onchain_sync={ONCHAIN_SYNC_SEC:.1f}s "
            f"heartbeat={CLOB_HEARTBEAT_SEC:.1f}s "
            f"book_cache={BOOK_CACHE_TTL_MS:.0f}ms/{BOOK_CACHE_MAX}"
        )
        print(
            f"{B}[BOOT]{RS} user_events={USER_EVENTS_ENABLED} "
            f"poll={USER_EVENTS_POLL_SEC:.2f}s cache_ttl={USER_EVENTS_CACHE_TTL_SEC:.0f}s"
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
                # Keep a shorter rolling window to reduce false blocked_seen after restarts.
                keep_n = max(100, int(SEEN_MAX_KEEP))
                self.seen = set(loaded[-keep_n:] if len(loaded) > keep_n else loaded)
                print(f"{Y}[RESUME] Loaded {len(self.seen)} seen markets from disk{RS}")
            except Exception:
                pass
        # Load only *currently open* cids from positions API.
        # Do not import historical/closed cids into `seen`, otherwise fresh markets
        # can be incorrectly blocked from candidacy after restart.
        # DO NOT add to self.pending here; _sync_open_positions (called in init_clob)
        # fetches real end_ts from Gamma API and adds them properly.
        try:
            import requests as _req
            positions = _req.get(
                "https://data-api.polymarket.com/positions",
                params={"user": ADDRESS, "sizeThreshold": "0.01", "redeemable": "false"},
                timeout=8
            ).json()
            added_open = 0
            for p in positions:
                cid        = p.get("conditionId", "")
                redeemable = p.get("redeemable", False)
                outcome    = self._normalize_side_label(p.get("outcome", ""))
                val        = float(p.get("currentValue", 0))
                title      = p.get("title", "")
                # Keep seen aligned to active exposure only.
                if (not redeemable) and outcome and cid and val >= OPEN_PRESENCE_MIN:
                    self.seen.add(cid)
                    added_open += 1
                if not redeemable and outcome and cid:
                    print(f"{Y}[RESUME] Position: {title[:45]} {outcome} ~${val:.2f}{RS}")
            print(f"{Y}[RESUME] Seen+open from API: +{added_open} | total seen={len(self.seen)}{RS}")
        except Exception:
            pass
        # Load pending trades
        if STRICT_ONCHAIN_STATE:
            return
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
            self.onchain_usdc_balance = BANKROLL
            self.onchain_open_stake_total = 0.0
            self.onchain_redeemable_usdc = 0.0
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
            f"\n{W}{'─'*72}{RS}\n"
            f"  {B}SESSION{RS}  {B}Time:{RS} {h}h{m}m  {rs}RTDS{'✓' if self.rtds_ok else '✗'}{RS}  "
            f"{B}Network:{RS} {NETWORK}\n"
            f"  {B}PERF{RS}     {B}Trades:{RS} {self.total}  {B}Win:{RS} {wr}  "
            f"{B}ROI:{RS} {pc}{roi:+.1f}%{RS}  {B}P&L:{RS} {pc}${pnl:+.2f}{RS}\n"
            f"  {B}ONCHAIN{RS}  {B}USDC:{RS} ${self.onchain_usdc_balance:.2f}  "
            f"{Y}OPEN_STAKE:{RS} ${self.onchain_open_stake_total:.2f} ({self.onchain_open_count})  "
            f"{B}OPEN_MARK:{RS} ${self.onchain_open_mark_value:.2f}  "
            f"{Y}SETTLING:{RS} ${self.onchain_redeemable_usdc:.2f} ({self.onchain_redeemable_count})  "
            f"{B}TOTAL:{RS} ${display_bank:.2f}\n"
            f"  {price_str}\n"
            f"{W}{'─'*72}{RS}"
        )
        show_debug = LOG_VERBOSE or self._should_log("debug-status", LOG_DEBUG_EVERY_SEC)
        if show_debug and (self._perf_stats.get("score_n", 0) > 0 or self._perf_stats.get("order_n", 0) > 0):
            rpc_ms = self._rpc_stats.get(self._rpc_url, 0.0)
            print(
                f"  {B}Perf:{RS} score_ema={self._perf_stats.get('score_ms_ema', 0.0):.0f}ms "
                f"order_ema={self._perf_stats.get('order_ms_ema', 0.0):.0f}ms "
                f"{B}RPC:{RS} {self._rpc_url} ({rpc_ms:.0f}ms)"
            )
        if show_debug and self._bucket_stats.rows:
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
        if show_debug and self._should_log("status-health", LOG_HEALTH_EVERY_SEC):
            hs = self._feed_health_snapshot()
            print(
                f"  {B}Health:{RS} book_fresh={hs['fresh_books']}/{hs['active_tokens']} "
                f"leader_fresh={hs['fresh_leaders']}/{hs['active_cids']} "
                f"ws_med={hs['ws_med_ms']:.0f}ms rtds_med={hs['rtds_med_s']:.1f}s cl_med={hs['cl_med_s']:.1f}s"
            )
        if show_debug and self._should_log("status-skip-top", LOG_HEALTH_EVERY_SEC):
            sk_total, sk_top = self._skip_top(SKIP_STATS_WINDOW_SEC, SKIP_STATS_TOP_N)
            if sk_total > 0 and sk_top:
                sk_str = ", ".join(f"{k}:{v}" for k, v in sk_top)
                print(
                    f"  {B}SkipTop:{RS} last{SKIP_STATS_WINDOW_SEC//60}m total={sk_total} | {sk_str}"
                )
        # Show each open position with current win/loss status
        now_ts = _time.time()
        shown_live_cids = set()
        live_by_rk = {}
        tail_rows = []
        for cid, (m, t) in list(self.pending.items()):
            recently_filled_local = False
            if self.onchain_snapshot_ts > 0 and cid not in self.onchain_open_cids:
                placed_ts = float((t or {}).get("placed_ts", 0.0) or 0.0)
                recently_filled_local = (
                    placed_ts > 0
                    and (now_ts - placed_ts) <= LIVE_LOCAL_GRACE_SEC
                    and float((t or {}).get("size", 0.0) or 0.0) > 0
                )
                if not recently_filled_local:
                    continue
            self._force_expired_from_question_if_needed(m, t)
            self._apply_exact_window_from_question(m, t)
            asset      = t.get("asset", "?")
            side       = t.get("side", "?")
            stake = float(self.onchain_open_stake_by_cid.get(cid, 0.0) or 0.0)
            if stake <= 0:
                stake = float(self.onchain_open_usdc_by_cid.get(cid, 0.0) or 0.0)
            if stake <= 0 and recently_filled_local:
                stake = float((t or {}).get("size", 0.0) or 0.0)
            if stake <= 0:
                continue
            meta_cid = self.onchain_open_meta_by_cid.get(cid, {}) if isinstance(self.onchain_open_meta_by_cid, dict) else {}
            stake_src = str(meta_cid.get("stake_source", "onchain"))
            if recently_filled_local and cid not in self.onchain_open_cids:
                stake_src = "local_fill_grace"
            value_now = float(self.onchain_open_usdc_by_cid.get(cid, 0.0) or 0.0)
            if value_now <= 0 and recently_filled_local:
                value_now = float((t or {}).get("size", 0.0) or 0.0)
            shares = float(self.onchain_open_shares_by_cid.get(cid, 0.0) or 0.0)
            if shares <= 0 and recently_filled_local:
                fill_px = float((t or {}).get("fill_price", (t or {}).get("entry", 0.0)) or 0.0)
                if fill_px > 0 and stake > 0:
                    shares = stake / max(fill_px, 1e-9)
            rk         = self._round_key(cid=cid, m=m, t=t)
            # Use market reference price (Chainlink at market open = Polymarket "price to beat")
            # NOT the bot's trade entry price — market determines outcome from its own start
            open_p     = float(self.open_prices.get(cid, 0.0) or 0.0)
            src        = self.open_prices_source.get(cid, "?")
            if open_p <= 0:
                trade_open = float(
                    (t or {}).get("open_price", 0.0)
                    or meta_cid.get("open_price", 0.0)
                    or (m or {}).get("open_price", 0.0)
                    or 0.0
                )
                if trade_open > 0:
                    open_p = trade_open
                    src = "TRADE"
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
                payout_est = shares if (projected_win and shares > 0) else (stake / max(float(meta_cid.get("entry", t.get("entry", 0.5)) or 0.5), 1e-9) if projected_win else 0)
                move_str   = f"({move_pct:+.2f}%)"
                proj_str   = "LEAD" if projected_win else "TRAIL"
            else:
                c          = Y
                status_str = "UNSETTLED"
                payout_est = 0
                move_str   = "(no ref)"
                proj_str   = "NA"
            eff_entry = (stake / shares) if shares > 0 else float(meta_cid.get("entry", t.get("entry", 0.0)) or 0.0)
            tok_str   = f"@{eff_entry*100:.0f}¢→{(shares/max(stake,1e-9)):.2f}x" if shares > 0 and stake > 0 else (f"@{eff_entry*100:.0f}¢→{(1/eff_entry):.2f}x" if eff_entry > 0 else "@?¢")
            stake_label = "bet"
            if stake_src == "value_fallback":
                stake_label = "value_now"
                tok_str = "@n/a"
            if LOG_LIVE_DETAIL:
                print(f"  {c}[{status_str}]{RS} {asset} {side} | {title} | "
                      f"beat={open_p:.4f}[{src}] now={cur_p:.4f} {move_str} | "
                      f"{stake_label}=${(value_now if stake_label=='value_now' else stake):.2f} {tok_str} est=${payout_est:.2f} proj={proj_str} | "
                      f"{mins_left:.1f}min left | rk={rk} cid={self._short_cid(cid)}")
            shown_live_cids.add(cid)
            agg = live_by_rk.setdefault(
                rk,
                {
                    "n": 0,
                    "stake": 0.0,
                    "value_now": 0.0,
                    "win_payout": 0.0,
                    "lead": 0,
                    "up_n": 0,
                    "down_n": 0,
                    "up_stake": 0.0,
                    "down_stake": 0.0,
                },
            )
            agg["n"] += 1
            agg["stake"] += float(stake)
            agg["value_now"] += float(value_now)
            agg["win_payout"] += float(shares if shares > 0 else payout_est)
            if str(side).lower() == "up":
                agg["up_n"] += 1
                agg["up_stake"] += float(stake)
            else:
                agg["down_n"] += 1
                agg["down_stake"] += float(stake)
            if proj_str == "LEAD":
                agg["lead"] += 1
        # On-chain open positions not yet hydrated into local pending.
        # Keep visibility strictly on-chain to avoid "missing live trade" blind spots.
        for cid, meta in list(self.onchain_open_meta_by_cid.items()):
            if cid in shown_live_cids:
                continue
            stake = float(meta.get("stake_usdc", 0.0) or 0.0)
            if stake <= 0:
                stake = float(self.onchain_open_stake_by_cid.get(cid, 0.0) or 0.0)
            if stake <= 0:
                stake = float(self.onchain_open_usdc_by_cid.get(cid, 0.0) or 0.0)
            if stake <= 0:
                continue
            stake_src = str(meta.get("stake_source", "local"))
            value_now = float(self.onchain_open_usdc_by_cid.get(cid, 0.0) or 0.0)
            shares = float(meta.get("shares", 0.0) or 0.0)
            if shares <= 0:
                shares = float(self.onchain_open_shares_by_cid.get(cid, 0.0) or 0.0)
            asset = str(meta.get("asset", "?") or "?")
            side = str(meta.get("side", "?") or "?")
            title = str(meta.get("title", "") or "")[:38]
            end_ts = float(meta.get("end_ts", 0) or 0)
            mins_left = max(0.0, (end_ts - now_ts) / 60.0) if end_ts > 0 else 0.0
            if end_ts > 0 and end_ts <= now_ts and cid not in self.pending_redeem:
                continue
            open_p = float(self.open_prices.get(cid, 0.0) or 0.0)
            src = self.open_prices_source.get(cid, "?")
            if open_p <= 0:
                meta_open = float(meta.get("open_price", 0.0) or 0.0)
                if meta_open > 0:
                    open_p = meta_open
                    src = "TRADE"
            cl_p = float(self.cl_prices.get(asset, 0.0) or 0.0)
            cur_p = cl_p if cl_p > 0 else float(self.prices.get(asset, 0.0) or 0.0)
            if open_p > 0 and cur_p > 0:
                pred_winner = "Up" if cur_p >= open_p else "Down"
                projected_win = (side == pred_winner)
                move_pct = (cur_p - open_p) / open_p * 100.0
                payout_est = shares if (projected_win and shares > 0) else (stake / max(float(meta.get("entry", 0.5) or 0.5), 1e-9) if projected_win else 0.0)
                move_str = f"({move_pct:+.2f}%)"
                proj_str = "LEAD" if projected_win else "TRAIL"
            else:
                payout_est = 0.0
                move_str = "(no ref)"
                proj_str = "NA"
            rk = self._round_key(
                cid=cid,
                m={"asset": asset, "duration": int(meta.get("duration", 15) or 15), "end_ts": end_ts},
                t={"asset": asset, "side": side, "duration": int(meta.get("duration", 15) or 15), "end_ts": end_ts},
            )
            if LOG_LIVE_DETAIL:
                print(
                    f"  {Y}[LIVE-PENDING]{RS} {asset} {side} | {title} | "
                    f"beat={open_p:.4f}[{src}] now={cur_p:.4f} {move_str} | "
                    f"{('value_now' if stake_src=='value_fallback' else 'bet')}=${(value_now if stake_src=='value_fallback' else stake):.2f} "
                    f"{('@n/a' if stake_src=='value_fallback' else '')} est=${payout_est:.2f} proj={proj_str} | "
                    f"{mins_left:.1f}min left | rk={rk} cid={self._short_cid(cid)}"
                )
            agg = live_by_rk.setdefault(
                rk,
                {
                    "n": 0,
                    "stake": 0.0,
                    "value_now": 0.0,
                    "win_payout": 0.0,
                    "lead": 0,
                    "up_n": 0,
                    "down_n": 0,
                    "up_stake": 0.0,
                    "down_stake": 0.0,
                },
            )
            agg["n"] += 1
            agg["stake"] += float(stake)
            agg["value_now"] += float(value_now)
            agg["win_payout"] += float(shares if shares > 0 else payout_est)
            if str(side).lower() == "up":
                agg["up_n"] += 1
                agg["up_stake"] += float(stake)
            else:
                agg["down_n"] += 1
                agg["down_stake"] += float(stake)
            if proj_str == "LEAD":
                agg["lead"] += 1
        if live_by_rk:
            for rk, row in sorted(live_by_rk.items(), key=lambda kv: kv[0])[:max(1, LOG_LIVE_RK_MAX)]:
                spent = float(row.get("stake", 0.0) or 0.0)
                value_now = float(row.get("value_now", 0.0) or 0.0)
                win_payout = float(row.get("win_payout", 0.0) or 0.0)
                win_profit = win_payout - spent
                mult = (win_payout / spent) if spent > 0 else 0.0
                avg_entry = (spent / win_payout) if win_payout > 0 else 0.0
                lead = int(row.get("lead", 0) or 0)
                n = int(row.get("n", 0) or 0)
                up_n = int(row.get("up_n", 0) or 0)
                down_n = int(row.get("down_n", 0) or 0)
                up_stake = float(row.get("up_stake", 0.0) or 0.0)
                down_stake = float(row.get("down_stake", 0.0) or 0.0)
                c_pl = G if win_profit > 0 else (R if win_profit < 0 else Y)
                c_lead = G if lead > 0 else R
                if up_n > 0 and down_n == 0:
                    side_lbl = "UP"
                    side_stake = up_stake
                elif down_n > 0 and up_n == 0:
                    side_lbl = "DOWN"
                    side_stake = down_stake
                else:
                    side_lbl = f"MIX U{up_n}/D{down_n}"
                    side_stake = spent
                mark_pnl = value_now - spent
                c_mark = G if mark_pnl > 0 else (R if mark_pnl < 0 else Y)
                mark_roi = (mark_pnl / spent * 100.0) if spent > 0 else 0.0
                if lead >= n and n > 0:
                    event_state = "LEAD"
                    c_event = G
                elif lead <= 0:
                    event_state = "TRAIL"
                    c_event = R
                else:
                    event_state = "MIXED"
                    c_event = Y
                if mark_pnl > 0:
                    mark_state = "GREEN"
                    c_mark_state = G
                elif mark_pnl < 0:
                    mark_state = "RED"
                    c_mark_state = R
                else:
                    mark_state = "FLAT"
                    c_mark_state = Y
                if LIVE_RK_REAL_ONLY:
                    print(
                        f"  {B}[LIVE-RK]{RS} {rk} | state=OPEN(unsettled) | trades={n} | "
                        f"{Y}SIDE{RS}={side_lbl} (${side_stake:.2f}) | "
                        f"{c_event}EVENT_NOW{RS}={c_event}{event_state}{RS}({lead}/{n}) | "
                        f"{c_mark_state}MARK_NOW{RS}={c_mark_state}{mark_state}{RS} | "
                        f"{Y}SPENT{RS}=${spent:.2f} | "
                        f"{B}REAL_MARK{RS}=${value_now:.2f} | "
                        f"{c_mark}REAL_PNL_NOW{RS}={c_mark}${mark_pnl:+.2f}{RS} | "
                        f"{c_mark}REAL_ROI_NOW{RS}={c_mark}{mark_roi:+.1f}%{RS} | "
                        f"{B}AVG_ENTRY{RS}={(avg_entry*100):.1f}c"
                    )
                else:
                    print(
                        f"  {B}[LIVE-RK]{RS} {rk} | state=OPEN(unsettled) | trades={n} | "
                        f"{Y}SIDE{RS}={side_lbl} (${side_stake:.2f}) | "
                        f"{c_event}EVENT_NOW{RS}={c_event}{event_state}{RS}({lead}/{n}) | "
                        f"{c_mark_state}MARK_NOW{RS}={c_mark_state}{mark_state}{RS} | "
                        f"{Y}SPENT{RS}=${spent:.2f} | "
                        f"{B}REAL_MARK{RS}=${value_now:.2f} | "
                        f"{c_mark}REAL_PNL_NOW{RS}={c_mark}${mark_pnl:+.2f}{RS} | "
                        f"{c_mark}REAL_ROI_NOW{RS}={c_mark}{mark_roi:+.1f}%{RS} | "
                        f"{G}SCENARIO_IF_WIN{RS}=${win_payout:.2f} | "
                        f"{c_pl}SCENARIO_PROFIT_IF_WIN{RS}={c_pl}${win_profit:+.2f}{RS} | "
                        f"{B}x{RS}{mult:.2f} | "
                        f"{B}AVG_ENTRY{RS}={(avg_entry*100):.1f}c"
                    )
                tail_rows.append({
                    "rk": rk,
                    "state": "OPEN(unsettled)",
                    "trades": n,
                    "side": side_lbl,
                    "side_stake": round(side_stake, 6),
                    "event_now": event_state,
                    "event_lead": lead,
                    "event_total": n,
                    "mark_now": mark_state,
                    "spent": round(spent, 6),
                    "real_mark": round(value_now, 6),
                    "real_pnl_now": round(mark_pnl, 6),
                    "real_roi_now_pct": round(mark_roi, 4),
                    "scenario_if_win": round(win_payout, 6),
                    "scenario_profit_if_win": round(win_profit, 6),
                    "scenario_mult_if_win": round(mult, 6),
                    "avg_entry_c": round(avg_entry * 100.0, 4),
                })
        elif self._should_log("live-rk-empty", 15):
            print(f"  {Y}[LIVE-RK]{RS} none | trades=0 | no active on-chain positions")
            now_fix = _time.time()
            if int(self.onchain_open_count or 0) > 0 and (now_fix - float(self._live_rk_repair_ts or 0.0)) >= 20.0:
                self._live_rk_repair_ts = now_fix
                print(
                    f"{Y}[LIVE-RK-MISMATCH]{RS} onchain_open={self.onchain_open_count} "
                    f"but no live rows — forcing open position resync"
                )
                try:
                    self._sync_open_positions()
                except Exception as e:
                    self._errors.tick("live_rk_repair", print, err=e, every=10)
        if LOG_TAIL_EQ_ENABLED and self._should_log("tail-eq", max(1, LOG_TAIL_EQ_EVERY_SEC)):
            try:
                rows_sorted = sorted(
                    tail_rows,
                    key=lambda r: abs(float(r.get("real_pnl_now", 0.0) or 0.0)),
                    reverse=True,
                )[:max(1, LOG_TAIL_EQ_MAX_ROWS)]
                payload = {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "network": NETWORK,
                    "session_min": int(max(0.0, (datetime.now(timezone.utc) - self.start_time).total_seconds()) // 60),
                    "onchain_usdc": round(float(self.onchain_usdc_balance or 0.0), 6),
                    "onchain_open_stake": round(float(self.onchain_open_stake_total or 0.0), 6),
                    "onchain_open_mark": round(float(self.onchain_open_mark_value or 0.0), 6),
                    "onchain_settling_claimable": round(float(self.onchain_redeemable_usdc or 0.0), 6),
                    "onchain_open_count": int(self.onchain_open_count or 0),
                    "onchain_redeemable_count": int(self.onchain_redeemable_count or 0),
                    "pending_redeem_count": int(len(self.pending_redeem)),
                    "total_trades": int(self.total),
                    "wins": int(self.wins),
                    "win_rate_pct": round((self.wins / self.total * 100.0), 4) if self.total > 0 else 0.0,
                    "rows": rows_sorted,
                }
                print(f"{B}[TAIL-EQ]{RS} " + json.dumps(payload, separators=(",", ":")))
            except Exception as e:
                self._errors.tick("tail_eq_log", print, err=e, every=10)
        # Show settling (pending_redeem) positions
        for cid, val in list(self.pending_redeem.items()):
            if isinstance(val[0], dict):
                m_r, t_r = val
                asset_r = t_r.get("asset", "?")
                side_r  = t_r.get("side", "?")
                size_r  = float(self.onchain_settling_usdc_by_cid.get(cid, 0.0))
                title_r = m_r.get("question", "")[:38]
                rk_r = self._round_key(cid=cid, m=m_r, t=t_r)
            else:
                side_r, asset_r = val
                size_r = float(self.onchain_settling_usdc_by_cid.get(cid, 0.0)); title_r = ""
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
                            try: await ws.send("PING")
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
                err_s = str(e).lower()
                fails = int(getattr(self, "_rtds_fails", 0) or 0) + 1
                self._rtds_fails = fails
                if "429" in err_s or "too many requests" in err_s:
                    # Upstream throttling: back off harder to recover stable freshness.
                    wait_s = min(90.0, 12.0 * (1.6 ** min(6, fails - 1)))
                else:
                    wait_s = min(30.0, 2.0 * (2 ** min(5, fails - 1)))
                await asyncio.sleep(wait_s)
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
                connector = aiohttp.TCPConnector(
                    limit=max(1, HTTP_CONN_LIMIT),
                    limit_per_host=max(1, HTTP_CONN_PER_HOST),
                    ttl_dns_cache=max(0, HTTP_DNS_TTL_SEC),
                    enable_cleanup_closed=True,
                    keepalive_timeout=max(5.0, HTTP_KEEPALIVE_SEC),
                )
                self._session = aiohttp.ClientSession(
                    connector=connector,
                    headers={"User-Agent": "clawdbot-live/1.0"},
                )
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
                if not cid or not end_str or not start_str:
                    continue
                try:
                    end_ts   = datetime.fromisoformat(end_str.replace("Z","+00:00")).timestamp()
                    start_ts = datetime.fromisoformat(start_str.replace("Z","+00:00")).timestamp()
                except:
                    continue
                if end_ts <= now or start_ts > now + 60:
                    continue
                up_price, token_up, token_down = self._map_updown_market_fields(m_data, fallback=ev)
                result[cid] = {
                    "conditionId": cid,
                    "question":    q,
                    "asset":       info["asset"],
                    "duration":    info["duration"],
                    "end_ts":      end_ts,
                    "start_ts":    start_ts,
                    "up_price":    up_price,
                    "mins_left":   (end_ts - now) / 60,
                    "token_up":    token_up,
                    "token_down":  token_down,
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

    def _active_token_ids(self) -> set[str]:
        toks = set()
        for m in self.active_mkts.values():
            tu = str(m.get("token_up", "") or "").strip()
            td = str(m.get("token_down", "") or "").strip()
            if tu:
                toks.add(tu)
            if td:
                toks.add(td)
        return toks

    def _trade_focus_token_ids(self) -> set[str]:
        """Token set for execution freshness: subscribe both sides for every active market."""
        toks = set()
        for cid, m in self.active_mkts.items():
            tu = str(m.get("token_up", "") or "").strip()
            td = str(m.get("token_down", "") or "").strip()
            if not tu and not td:
                continue
            if tu:
                toks.add(tu)
            if td:
                toks.add(td)
        return toks

    def _normalize_book_levels(self, levels) -> list[tuple[float, float]]:
        out = []
        if not isinstance(levels, list):
            return out
        for lv in levels:
            p = s = 0.0
            try:
                if isinstance(lv, dict):
                    p = float(lv.get("price", lv.get("p", 0.0)) or 0.0)
                    s = float(lv.get("size", lv.get("s", lv.get("quantity", 0.0))) or 0.0)
                elif isinstance(lv, (list, tuple)) and len(lv) >= 2:
                    p = float(lv[0] or 0.0)
                    s = float(lv[1] or 0.0)
            except Exception:
                p = s = 0.0
            if p > 0 and s > 0:
                out.append((p, s))
        return out

    def _ingest_clob_ws_event(self, row: dict):
        if not isinstance(row, dict):
            return
        aid = str(
            row.get("asset_id")
            or row.get("assetId")
            or row.get("token_id")
            or row.get("tokenId")
            or row.get("market")
            or ""
        ).strip()
        if not aid:
            return
        prev = self._clob_ws_books.get(aid, {})
        asks = self._normalize_book_levels(
            row.get("asks", row.get("sells", row.get("sell", [])))
        )
        bids = self._normalize_book_levels(
            row.get("bids", row.get("buys", row.get("buy", [])))
        )
        best_ask = 0.0
        best_bid = 0.0
        if asks:
            asks = sorted(asks, key=lambda x: x[0])[:20]
            best_ask = asks[0][0]
        else:
            try:
                best_ask = float(
                    row.get("best_ask", row.get("bestAsk", row.get("ask", 0.0))) or 0.0
                )
            except Exception:
                best_ask = 0.0
            asks = list(prev.get("asks") or [])
        if bids:
            bids = sorted(bids, key=lambda x: x[0], reverse=True)
            best_bid = bids[0][0]
        else:
            try:
                best_bid = float(
                    row.get("best_bid", row.get("bestBid", row.get("bid", 0.0))) or 0.0
                )
            except Exception:
                best_bid = 0.0
            if best_bid <= 0:
                best_bid = float(prev.get("best_bid", 0.0) or 0.0)
        if best_ask <= 0:
            best_ask = float(prev.get("best_ask", 0.0) or 0.0)
        tick = float(row.get("tick_size", row.get("tick", prev.get("tick", 0.01))) or 0.01)
        if best_ask > 0:
            self._clob_ws_books[aid] = {
                "ts_ms": _time.time() * 1000.0,
                "best_bid": best_bid if best_bid > 0 else max(0.0, best_ask - tick),
                "best_ask": best_ask,
                "tick": tick,
                "asks": asks,
            }
            if len(self._clob_ws_books) > max(64, BOOK_CACHE_MAX * 2):
                keys = sorted(
                    self._clob_ws_books.items(),
                    key=lambda kv: kv[1].get("ts_ms", 0.0),
                    reverse=True,
                )[: max(1, BOOK_CACHE_MAX * 2)]
                keep = {k for k, _ in keys}
                for k in list(self._clob_ws_books.keys()):
                    if k not in keep:
                        self._clob_ws_books.pop(k, None)

    def _get_clob_ws_book(self, token_id: str, max_age_ms: float | None = None):
        if not token_id:
            return None
        row = self._clob_ws_books.get(token_id)
        if not row:
            return None
        age_cap = float(max_age_ms if max_age_ms is not None else CLOB_MARKET_WS_MAX_AGE_MS)
        age_ms = (_time.time() * 1000.0) - float(row.get("ts_ms", 0.0) or 0.0)
        if age_ms > age_cap:
            return None
        best_ask = float(row.get("best_ask", 0.0) or 0.0)
        if best_ask <= 0:
            return None
        return {
            "best_bid": float(row.get("best_bid", max(0.0, best_ask - 0.01)) or 0.0),
            "best_ask": best_ask,
            "tick": float(row.get("tick", 0.01) or 0.01),
            "asks": list(row.get("asks") or []),
            "ts": float(row.get("ts_ms", 0.0) or 0.0) / 1000.0,
            "source": "clob-ws",
        }

    def _clob_ws_book_age_ms(self, token_id: str) -> float:
        """Best-effort age for diagnostics; returns large number if missing."""
        row = self._clob_ws_books.get(token_id or "")
        if not row:
            return 9e9
        return (_time.time() * 1000.0) - float(row.get("ts_ms", 0.0) or 0.0)

    async def _stream_clob_market_book(self):
        """Realtime CLOB market channel: keep per-token best bid/ask + shallow asks in memory."""
        if DRY_RUN or (not CLOB_MARKET_WS_ENABLED):
            return
        import websockets as _ws
        delay = 2
        while True:
            try:
                async with _ws.connect(
                    CLOB_MARKET_WSS,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=2**22,
                ) as ws:
                    self._clob_market_ws = ws
                    self._clob_ws_assets_subscribed = set()
                    print(f"{G}[CLOB-WS] market connected{RS}")
                    delay = 2
                    async def _app_heartbeat():
                        # Polymarket market WS expects app-level "PING" heartbeat every ~10s.
                        while True:
                            await asyncio.sleep(10.0)
                            try:
                                await ws.send("PING")
                            except Exception:
                                break
                    hb_task = asyncio.create_task(_app_heartbeat())
                    try:
                        while True:
                            desired = self._trade_focus_token_ids() or self._active_token_ids()
                            add = desired - self._clob_ws_assets_subscribed
                            rem = self._clob_ws_assets_subscribed - desired
                            if add:
                                await ws.send(
                                    json.dumps(
                                        {
                                            "assets_ids": sorted(add),
                                            "type": "market",
                                            "custom_feature_enabled": True,
                                        }
                                    )
                                )
                                self._clob_ws_assets_subscribed |= set(add)
                            if rem:
                                await ws.send(
                                    json.dumps(
                                        {
                                            "assets_ids": sorted(rem),
                                            "type": "market",
                                            "operation": "unsubscribe",
                                        }
                                    )
                                )
                                self._clob_ws_assets_subscribed -= set(rem)
                            try:
                                raw = await asyncio.wait_for(ws.recv(), timeout=max(0.5, CLOB_MARKET_WS_SYNC_SEC))
                            except asyncio.TimeoutError:
                                continue
                            try:
                                msg = json.loads(raw)
                            except Exception:
                                # Server may return plain-text PONG to app heartbeat.
                                if str(raw).strip().upper() == "PONG":
                                    continue
                                continue
                            rows = msg if isinstance(msg, list) else [msg]
                            for row in rows:
                                if isinstance(row, dict) and isinstance(row.get("data"), (list, dict)):
                                    sub = row.get("data")
                                    if isinstance(sub, list):
                                        for r2 in sub:
                                            self._ingest_clob_ws_event(r2)
                                    else:
                                        self._ingest_clob_ws_event(sub)
                                else:
                                    self._ingest_clob_ws_event(row)
                    finally:
                        hb_task.cancel()
            except Exception as e:
                self._errors.tick("clob_market_ws", print, err=e, every=8)
                print(f"{Y}[CLOB-WS] {e} — reconnect in {delay}s{RS}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 20)
            finally:
                self._clob_market_ws = None
                self._clob_ws_assets_subscribed = set()

    # ── SCORE + EXECUTE ───────────────────────────────────────────────────────
    async def _fetch_pm_book_safe(self, token_id: str):
        """Fetch Polymarket CLOB book; prefer WS, fallback to fresh CLOB REST snapshot."""
        if not token_id:
            return None
        ws_book = self._get_clob_ws_book(token_id, max_age_ms=CLOB_MARKET_WS_MAX_AGE_MS)
        if ws_book is not None:
            return ws_book
        if not CLOB_REST_FALLBACK_ENABLED:
            return None
        loop = asyncio.get_event_loop()
        try:
            book     = await self._get_order_book(token_id, force_fresh=True)
            tick     = float(book.tick_size or "0.01")
            asks     = sorted(book.asks, key=lambda x: float(x.price)) if book.asks else []
            bids     = sorted(book.bids, key=lambda x: float(x.price), reverse=True) if book.bids else []
            if not asks:
                return None
            best_ask = float(asks[0].price)
            best_bid = float(bids[0].price) if bids else best_ask - 0.10
            asks_compact = []
            for lv in asks[:20]:
                try:
                    asks_compact.append((float(lv.price), float(lv.size)))
                except Exception:
                    continue
            return {
                "best_bid": best_bid,
                "best_ask": best_ask,
                "tick": tick,
                "asks": asks_compact,
                "ts": _time.time(),
                "source": "clob-rest",
            }
        except Exception:
            return None

    async def _score_market(self, m: dict) -> dict | None:
        """Score a market opportunity. Returns signal dict or None if hard-blocked.
        Pure analysis — no side effects, no order placement."""
        score_started = _time.perf_counter()
        cid       = m["conditionId"]
        booster_eval = False
        booster_side_locked = ""
        if cid in self.seen:
            if MID_BOOSTER_ENABLED and cid in self.pending:
                _pm, _pt = self.pending.get(cid, ({}, {}))
                _pside = str((_pt or {}).get("side", "") or "")
                _is_core = bool((_pt or {}).get("core_position", True))
                _stake_onchain = float(self.onchain_open_stake_by_cid.get(cid, 0.0) or 0.0)
                if _pside in ("Up", "Down") and _is_core and _stake_onchain > 0:
                    booster_eval = True
                    booster_side_locked = _pside
                else:
                    return None
            else:
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

        # Previous window direction from CL prices (used as one signal among others).
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
        # Keep initialized for any early quality penalties before final side selection.
        edge = 0.0

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
        # Empirical guard from live outcomes: core 15m trades with missing CL age
        # are materially worse. Keep block for missing age, allow mildly stale.
        if duration >= 15 and (not booster_eval):
            if cl_age_s is None:
                if self._noisy_log_enabled(f"skip-core-source-age:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
                    a = -1.0 if cl_age_s is None else float(cl_age_s)
                    print(f"{Y}[SKIP] {asset} {duration}m core source-age invalid (cl_age={a:.1f}s){RS}")
                self._skip_tick("core_source_age_invalid")
                return None
        score += onchain_adj

        # ── Binance signals from WS cache (instant) + PM book fetch (async ~36ms) ─
        ob_imbalance               = self._binance_imbalance(asset)
        (taker_ratio, vol_ratio)   = self._binance_taker_flow(asset)
        (perp_basis, funding_rate) = self._binance_perp_signals(asset)
        (vwap_dev, vol_mult)       = self._binance_window_stats(asset, m["start_ts"])
        _pm_book = await self._fetch_pm_book_safe(prefetch_token)
        ws_book_now = self._get_clob_ws_book(prefetch_token, max_age_ms=CLOB_MARKET_WS_MAX_AGE_MS)
        ws_book_soft = self._get_clob_ws_book(prefetch_token, max_age_ms=WS_BOOK_SOFT_MAX_AGE_MS)
        ws_book_strict = ws_book_now
        if ws_book_now is None:
            # Try alternate side token before declaring WS missing.
            tok_u = str(m.get("token_up", "") or "")
            tok_d = str(m.get("token_down", "") or "")
            alt = tok_d if prefetch_token == tok_u else tok_u
            if alt:
                ws_book_now = self._get_clob_ws_book(alt, max_age_ms=CLOB_MARKET_WS_MAX_AGE_MS)
                ws_book_strict = ws_book_now
                if ws_book_soft is None:
                    ws_book_soft = self._get_clob_ws_book(alt, max_age_ms=WS_BOOK_SOFT_MAX_AGE_MS)
        # Never trade on soft-stale WS books: keep strict-fresh only for entries.
        # Older books can still be observed by health checker, but not used in scoring/execution.

        # Realtime triad gating: orderbook + leader + volume signals.
        if REQUIRE_ORDERBOOK_WS and ws_book_now is None:
            # Soft fallback: if REST/PM book is fresh enough, continue with a small quality penalty.
            pm_ok = False
            clob_rest_ok = False
            pm_age_ms = 9e9
            if isinstance(_pm_book, dict):
                pm_ts = float(_pm_book.get("ts", 0.0) or 0.0)
                pm_age_ms = ((_time.time() - pm_ts) * 1000.0) if pm_ts > 0 else 9e9
                pm_best_ask = float(_pm_book.get("best_ask", 0.0) or 0.0)
                pm_src = str(_pm_book.get("source", "") or "")
                pm_ok = pm_best_ask > 0 and pm_age_ms <= WS_BOOK_FALLBACK_MAX_AGE_MS
                clob_rest_ok = (
                    pm_src == "clob-rest"
                    and pm_best_ask > 0
                    and pm_age_ms <= CLOB_REST_FRESH_MAX_AGE_MS
                )
            if clob_rest_ok or (WS_BOOK_FALLBACK_ENABLED and PM_BOOK_FALLBACK_ENABLED and pm_ok):
                score -= 1
                ws_book_now = _pm_book
                if self._noisy_log_enabled(f"ws-fallback:{asset}:{duration}", LOG_SKIP_EVERY_SEC):
                    if clob_rest_ok:
                        print(
                            f"{B}[CLOB-REST]{RS} {asset} {duration}m using fresh CLOB REST book "
                            f"(age={pm_age_ms:.0f}ms) — WS stale"
                        )
                    else:
                        print(
                            f"{Y}[WS-FALLBACK]{RS} {asset} {duration}m using PM book "
                            f"(age={pm_age_ms:.0f}ms) — no fresh CLOB WS"
                        )
            else:
                # Last-resort soft WS fallback (short stale window) to avoid hard stalls.
                if isinstance(ws_book_soft, dict):
                    ws_book_now = ws_book_soft
                    score -= 2
                    if self._noisy_log_enabled(f"ws-soft:{asset}:{duration}", LOG_SKIP_EVERY_SEC):
                        ws_age_pref = self._clob_ws_book_age_ms(prefetch_token)
                        print(
                            f"{Y}[WS-SOFT]{RS} {asset} {duration}m using older CLOB WS book "
                            f"(age={ws_age_pref:.0f}ms) — strict/REST unavailable"
                        )
                else:
                    if self._noisy_log_enabled(f"skip-no-ws-book:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
                        ws_age_pref = self._clob_ws_book_age_ms(prefetch_token)
                        print(
                            f"{Y}[SKIP] {asset} {duration}m missing fresh CLOB WS book "
                            f"(ws_age={ws_age_pref:.0f}ms pm_age={pm_age_ms:.0f}ms){RS}"
                        )
                    self._skip_tick("book_ws_missing")
                    return None
        if REQUIRE_ORDERBOOK_WS and STRICT_REQUIRE_FRESH_BOOK_WS and ws_book_strict is None:
            allow_strict_rest = (
                isinstance(ws_book_now, dict)
                and str(ws_book_now.get("source", "") or "") == "clob-rest"
                and ((_time.time() - float(ws_book_now.get("ts", 0.0) or 0.0)) * 1000.0) <= CLOB_REST_FRESH_MAX_AGE_MS
            )
            if not allow_strict_rest:
                if self._noisy_log_enabled(f"skip-strict-ws:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
                    print(
                        f"{Y}[SKIP] {asset} {duration}m strict WS required (no fresh strict book){RS}"
                    )
                self._skip_tick("book_ws_strict_required")
                return None

        # Additional instant signals from Binance cache (zero latency)
        dw_ob     = self._ob_depth_weighted(asset)
        autocorr  = self._autocorr_regime(asset)
        vr_ratio  = self._variance_ratio(asset)
        is_jump, jump_dir, jump_z = self._jump_detect(asset)
        btc_lead_p = self._btc_lead_signal(asset)
        cache_a = self.binance_cache.get(asset, {})
        volume_ready = bool(cache_a.get("depth_bids")) and bool(cache_a.get("depth_asks")) and (len(cache_a.get("klines", [])) >= 10)
        if REQUIRE_VOLUME_SIGNAL and not volume_ready:
            if self._noisy_log_enabled(f"skip-no-vol:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
                print(f"{Y}[SKIP] {asset} {duration}m missing live Binance depth/volume cache{RS}")
            self._skip_tick("volume_missing")
            return None

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

        # VWAP: momentum signal (−2 to +2 pts)
        # Price ABOVE window VWAP in our direction = momentum confirms = good
        # Price BELOW window VWAP in our direction = momentum weak = bad
        vwap_net = vwap_dev if is_up else -vwap_dev   # positive = price above VWAP in bet direction
        if   vwap_net >  0.0015: score += 2   # strongly above VWAP → momentum confirms bet
        elif vwap_net >  0.0008: score += 1
        elif vwap_net < -0.0015: score -= 2   # strongly below VWAP → momentum against bet
        elif vwap_net < -0.0008: score -= 1

        # Vol-normalized displacement signal (−2 to +2 pts)
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

        # Previous-window continuation (data-driven, low weight).
        # Never use a fixed global continuation prior; require realtime corroboration.
        if prev_win_dir is not None:
            if prev_win_dir == direction:
                conf_hits = 0
                conf_hits += 1 if tf_votes >= 3 else 0
                conf_hits += 1 if ((is_up and taker_ratio > 0.53) or ((not is_up) and taker_ratio < 0.47)) else 0
                conf_hits += 1 if ((is_up and ob_sig > 0.08) or ((not is_up) and ob_sig < -0.08)) else 0
                if conf_hits >= 2:
                    score += 2 if pct_remaining > 0.80 else 1
            else:
                score -= 1

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
        bias_up   = self._direction_bias(asset, "Up", duration)
        bias_down = self._direction_bias(asset, "Down", duration)
        prob_up   = max(0.05, min(0.95, p_up_ll + bias_up))
        prob_down = max(0.05, min(0.95, 1.0 - p_up_ll + bias_down))

        # Early continuation prior boost (bounded, realtime-confirmed).
        if prev_win_dir == direction and pct_remaining > 0.80:
            prior_boost = 0.0
            if tf_votes >= 3:
                prior_boost += 0.015
            if ((is_up and taker_ratio > 0.54) or ((not is_up) and taker_ratio < 0.46)):
                prior_boost += 0.015
            if ((is_up and ob_sig > 0.10) or ((not is_up) and ob_sig < -0.10)):
                prior_boost += 0.010
            if cl_agree:
                prior_boost += 0.010
            prior_boost = max(0.0, min(CONTINUATION_PRIOR_MAX_BOOST, prior_boost))
            if direction == "Up":
                prob_up = max(prob_up, min(0.95, prob_up + prior_boost))
                prob_down = 1 - prob_up
            else:
                prob_down = max(prob_down, min(0.95, prob_down + prior_boost))
                prob_up   = 1 - prob_down

        # Online calibration: shrink overconfident probabilities toward 50% when live WR degrades.
        shrink = self._prob_shrink_factor()
        prob_up = 0.5 + (prob_up - 0.5) * shrink
        prob_up = max(0.05, min(0.95, prob_up))
        prob_down = 1.0 - prob_up

        # Recent on-chain side prior (asset+duration+side), non-blocking directional calibration.
        up_recent = self._recent_side_profile(asset, duration, "Up")
        dn_recent = self._recent_side_profile(asset, duration, "Down")
        up_adj = float(up_recent.get("prob_adj", 0.0) or 0.0)
        dn_adj = float(dn_recent.get("prob_adj", 0.0) or 0.0)
        if abs(up_adj) > 1e-9 or abs(dn_adj) > 1e-9:
            pu = max(0.01, min(0.99, prob_up + up_adj))
            pd = max(0.01, min(0.99, prob_down + dn_adj))
            z = pu + pd
            if z > 0:
                prob_up = max(0.05, min(0.95, pu / z))
                prob_down = 1.0 - prob_up

        edge_up   = prob_up   - up_price
        edge_down = prob_down - (1 - up_price)
        min_edge   = self._adaptive_min_edge()
        pre_filter = max(0.02, min_edge * 0.25)   # loose pre-filter vs AMM

        # Lock side to price direction when move is clear.
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
            # Momentum/CL direction wins over weak market-pricing noise.
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

        # Keep signal components aligned to the effective side.
        side_up = side == "Up"
        recent_side = up_recent if side_up else dn_recent
        recent_side_n = int(recent_side.get("n", 0) or 0)
        recent_side_exp = float(recent_side.get("exp", 0.0) or 0.0)
        recent_side_wr_lb = float(recent_side.get("wr_lb", 0.5) or 0.5)
        recent_side_score_adj = int(recent_side.get("score_adj", 0) or 0)
        recent_side_edge_adj = float(recent_side.get("edge_adj", 0.0) or 0.0)
        recent_side_prob_adj = float(recent_side.get("prob_adj", 0.0) or 0.0)
        if recent_side_score_adj != 0 or abs(recent_side_edge_adj) > 1e-9:
            score += recent_side_score_adj
            edge += recent_side_edge_adj
            if self._noisy_log_enabled(f"recent-side:{asset}:{side}", LOG_FLOW_EVERY_SEC):
                print(
                    f"{B}[RECENT-SIDE]{RS} {asset} {duration}m {side} "
                    f"adj(score={recent_side_score_adj:+d},edge={recent_side_edge_adj:+.3f},prob={recent_side_prob_adj:+.3f}) "
                    f"n={recent_side_n} exp={recent_side_exp:+.2f} wr_lb={recent_side_wr_lb:.2f}"
                )
        tf_votes = tf_up_votes if side_up else tf_dn_votes
        ob_sig = dw_ob if side_up else -dw_ob
        cl_agree = True
        if cl_now > 0 and open_price > 0:
            cl_agree = (cl_now > open_price) == side_up
        if ASSET_QUALITY_FILTER_ENABLED:
            q_n, q_pf, q_exp = self._asset_side_quality(asset, side)
            if q_n >= max(1, ASSET_QUALITY_MIN_TRADES):
                if q_pf <= ASSET_QUALITY_BLOCK_PF and q_exp <= ASSET_QUALITY_BLOCK_EXP:
                    if ASSET_QUALITY_HARD_BLOCK_ENABLED:
                        if self._noisy_log_enabled(f"skip-asset-quality-block:{asset}:{side}", LOG_SKIP_EVERY_SEC):
                            print(
                                f"{Y}[SKIP] {asset} {duration}m quality-block {side} "
                                f"(n={q_n} pf={q_pf:.2f} exp={q_exp:+.2f}){RS}"
                            )
                        self._skip_tick("asset_quality_block")
                        return None
                    # Default: don't freeze execution; apply a stronger penalty only.
                    score -= max(3, ASSET_QUALITY_SCORE_PENALTY + 1)
                    edge -= max(0.015, ASSET_QUALITY_EDGE_PENALTY + 0.005)
                    if self._noisy_log_enabled(f"asset-quality-softblock:{asset}:{side}", LOG_FLOW_EVERY_SEC):
                        print(
                            f"{Y}[ASSET-QUALITY]{RS} {asset} {duration}m {side} soft-block "
                            f"(n={q_n} pf={q_pf:.2f} exp={q_exp:+.2f})"
                        )
                if q_pf < ASSET_QUALITY_MIN_PF or q_exp < ASSET_QUALITY_MIN_EXP:
                    score -= ASSET_QUALITY_SCORE_PENALTY
                    edge -= ASSET_QUALITY_EDGE_PENALTY
                    if self._noisy_log_enabled(f"asset-quality-pen:{asset}:{side}", LOG_FLOW_EVERY_SEC):
                        print(
                            f"{Y}[ASSET-QUALITY]{RS} {asset} {duration}m {side} "
                            f"penalty n={q_n} pf={q_pf:.2f} exp={q_exp:+.2f} "
                            f"(-{ASSET_QUALITY_SCORE_PENALTY} score)"
                        )
        imbalance_confirms = ob_sig > 0.10

        # Anti-random guard: require multi-signal confluence before any sizing.
        conf = 0
        if ob_sig > 0.05:
            conf += 1
        if (side_up and taker_ratio > 0.52) or ((not side_up) and taker_ratio < 0.48):
            conf += 1
        if tf_votes >= 2:
            conf += 1
        if cl_agree:
            conf += 1
        if conf < 2:
            if self._noisy_log_enabled(f"skip-low-confluence:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
                print(
                    f"{Y}[SKIP]{RS} {asset} {duration}m low confluence "
                    f"(conf={conf}/4 side={side})"
                )
            self._skip_tick("low_confluence")
            return None

        # Late-window direction lock: avoid betting against the beat direction
        # when the move is already clear near expiry.
        if (
            LATE_DIR_LOCK_ENABLED
            and open_price > 0
            and current > 0
            and duration in (5, 15)
        ):
            lock_mins = LATE_DIR_LOCK_MIN_LEFT_5M if duration <= 5 else LATE_DIR_LOCK_MIN_LEFT_15M
            move_abs = abs((current - open_price) / max(open_price, 1e-9))
            if mins_left <= lock_mins and move_abs >= LATE_DIR_LOCK_MIN_MOVE_PCT:
                beat_dir = "Up" if current >= open_price else "Down"
                if side != beat_dir:
                    if self._noisy_log_enabled(f"late-lock-score:{asset}:{side}", LOG_SKIP_EVERY_SEC):
                        print(
                            f"{Y}[SKIP]{RS} {asset} {duration}m {side} late-lock: "
                            f"beat_dir={beat_dir} mins_left={mins_left:.1f} move={move_abs*100:.2f}%"
                        )
                    self._skip_tick("late_lock")
                    return None

        # Optional copyflow signal from externally ranked leader wallets on same market.
        copy_adj = 0
        copy_net = 0.0
        leader_ready = False
        leader_soft_ready = False
        signal_tier = "TIER-C"
        signal_source = "synthetic"
        leader_size_scale = LEADER_SYNTH_SIZE_SCALE
        flow_n = 0
        flow_age_s = 9e9
        up_conf = 0.0
        dn_conf = 0.0
        flow = self._copyflow_live.get(cid) or self._copyflow_map.get(cid, {})
        pm_pattern_key = ""
        pm_pattern_score_adj = 0
        pm_pattern_edge_adj = 0.0
        pm_pattern_n = 0
        pm_pattern_exp = 0.0
        pm_pattern_wr_lb = 0.5
        pm_public_pattern_score_adj = 0
        pm_public_pattern_edge_adj = 0.0
        pm_public_pattern_n = 0
        pm_public_pattern_dom = 0.0
        pm_public_pattern_avg_c = 0.0
        recent_side_n = 0
        recent_side_exp = 0.0
        recent_side_wr_lb = 0.5
        recent_side_score_adj = 0
        recent_side_edge_adj = 0.0
        recent_side_prob_adj = 0.0
        if isinstance(flow, dict):
            up_conf = float(flow.get("Up", flow.get("up", 0.0)) or 0.0)
            dn_conf = float(flow.get("Down", flow.get("down", 0.0)) or 0.0)
            flow_n = int(flow.get("n", 0) or 0)
            flow_ts = float(flow.get("ts", 0.0) or 0.0)
            flow_age_s = (_time.time() - flow_ts) if flow_ts > 0 else 9e9
            flow_fresh = flow_age_s <= COPYFLOW_LIVE_MAX_AGE_SEC
            leader_net = up_conf - dn_conf
            # "Leader-flow present" should reflect data availability, not strict sample quality.
            # Keep MARKET_LEADER_MIN_N only for force-follow logic below.
            leader_ready = flow_fresh and (flow_n >= 1)
            leader_soft_ready = (flow_n >= MARKET_LEADER_MIN_N) and (
                flow_age_s <= LEADER_FLOW_FALLBACK_MAX_AGE_SEC
            )
            # Strong per-market leader consensus: follow the market leaders on this CID.
            if (
                MARKET_LEADER_FOLLOW_ENABLED
                and flow_fresh
                and flow_n >= MARKET_LEADER_MIN_N
                and abs(leader_net) >= MARKET_LEADER_MIN_NET
            ):
                leader_side = "Up" if leader_net > 0 else "Down"
                leader_entry = up_price if leader_side == "Up" else (1 - up_price)
                leader_entry_cap = min(0.97, MAX_ENTRY_PRICE + MAX_ENTRY_TOL + 0.03)
                if leader_entry <= leader_entry_cap:
                    if side != leader_side:
                        if self._noisy_log_enabled(f"leader-follow:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
                            print(
                                f"{B}[LEADER-FOLLOW]{RS} {asset} {duration}m force {side}->{leader_side} "
                                f"(up={up_conf:.2f} down={dn_conf:.2f} n={flow_n} entry={leader_entry:.3f})"
                            )
                        side = leader_side
                    score += MARKET_LEADER_SCORE_BONUS
                    edge += MARKET_LEADER_EDGE_BONUS
                    signal_tier = "TIER-A"
                    signal_source = "leader-fresh"
                    leader_size_scale = LEADER_FRESH_SIZE_SCALE
                elif self._noisy_log_enabled(f"leader-follow-bypass:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
                    print(
                        f"{Y}[LEADER-BYPASS]{RS} {asset} {duration}m leader={leader_side} "
                        f"entry={leader_entry:.3f} > cap={leader_entry_cap:.3f}"
                    )
            elif MARKET_LEADER_FOLLOW_ENABLED and (not flow_fresh) and flow_n > 0:
                # Leader-flow stale is informational only: never block/penalize execution.
                pass
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
            if PM_PATTERN_ENABLED:
                pm_pattern_key = self._pm_pattern_key(asset, duration, side, copy_net, flow)
                patt = self._pm_pattern_profile(pm_pattern_key)
                pm_pattern_score_adj = int(patt.get("score_adj", 0) or 0)
                pm_pattern_edge_adj = float(patt.get("edge_adj", 0.0) or 0.0)
                pm_pattern_n = int(patt.get("n", 0) or 0)
                pm_pattern_exp = float(patt.get("exp", 0.0) or 0.0)
                pm_pattern_wr_lb = float(patt.get("wr_lb", 0.5) or 0.5)
                if pm_pattern_score_adj != 0 or abs(pm_pattern_edge_adj) > 1e-9:
                    score += pm_pattern_score_adj
                    edge += pm_pattern_edge_adj
                    if self._noisy_log_enabled(f"pm-pattern:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
                        print(
                            f"{B}[PM-PATTERN]{RS} {asset} {duration}m {side} "
                            f"adj(score={pm_pattern_score_adj:+d},edge={pm_pattern_edge_adj:+.3f}) "
                            f"n={int(patt.get('n',0) or 0)} exp={float(patt.get('exp',0.0) or 0.0):+.2f} "
                            f"wr_lb={float(patt.get('wr_lb',0.5) or 0.5):.2f}"
                        )
                elif (
                    int(patt.get("n", 0) or 0) > 0
                    and int(patt.get("n", 0) or 0) < int(patt.get("min_n", PM_PATTERN_MIN_N) or PM_PATTERN_MIN_N)
                    and self._noisy_log_enabled(f"pm-pattern-warmup:{asset}:{cid}", LOG_FLOW_EVERY_SEC)
                ):
                    print(
                        f"{Y}[PM-PATTERN-WARMUP]{RS} {asset} {duration}m {side} "
                        f"n={int(patt.get('n',0) or 0)}/{int(patt.get('min_n', PM_PATTERN_MIN_N) or PM_PATTERN_MIN_N)} "
                        f"exp={float(patt.get('exp',0.0) or 0.0):+.2f} "
                        f"wr_lb={float(patt.get('wr_lb',0.5) or 0.5):.2f}"
                    )
            if PM_PUBLIC_PATTERN_ENABLED:
                pub = self._pm_public_pattern_profile(side, flow, (up_price if side == "Up" else (1.0 - up_price)))
                pub_score_adj = int(pub.get("score_adj", 0) or 0)
                pub_edge_adj = float(pub.get("edge_adj", 0.0) or 0.0)
                pm_public_pattern_score_adj = pub_score_adj
                pm_public_pattern_edge_adj = pub_edge_adj
                pm_public_pattern_n = int(pub.get("n", 0) or 0)
                pm_public_pattern_dom = float(pub.get("dom", 0.0) or 0.0)
                pm_public_pattern_avg_c = float(pub.get("avg_c", 0.0) or 0.0)
                if pub_score_adj != 0 or abs(pub_edge_adj) > 1e-9:
                    score += pub_score_adj
                    edge += pub_edge_adj
                    if self._noisy_log_enabled(f"pm-public-pattern:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
                        if float(pub.get("avg_c", 0.0) or 0.0) >= 85.0 and abs(float(pub.get("dom", 0.0) or 0.0)) >= PM_PUBLIC_PATTERN_DOM_MED:
                            print(
                                f"{Y}[PM-PUBLIC-OCROWD]{RS} {asset} {duration}m {side} "
                                f"high-cent crowd mode avg={float(pub.get('avg_c',0.0) or 0.0):.1f}c "
                                f"dom={float(pub.get('dom',0.0) or 0.0):+.2f}"
                            )
                        print(
                            f"{B}[PM-PUBLIC-PATTERN]{RS} {asset} {duration}m {side} "
                            f"adj(score={pub_score_adj:+d},edge={pub_edge_adj:+.3f}) "
                            f"n={int(pub.get('n',0) or 0)} dom={float(pub.get('dom',0.0) or 0.0):+.2f} "
                            f"avg={float(pub.get('avg_c',0.0) or 0.0):.1f}c src={str(flow.get('src','?') or '?')}"
                        )
                elif (
                    int(pub.get("n", 0) or 0) > 0
                    and int(pub.get("n", 0) or 0) < int(pub.get("min_n", PM_PUBLIC_PATTERN_MIN_N) or PM_PUBLIC_PATTERN_MIN_N)
                    and self._noisy_log_enabled(f"pm-public-pattern-warmup:{asset}:{cid}", LOG_FLOW_EVERY_SEC)
                ):
                    print(
                        f"{Y}[PM-PUBLIC-WARMUP]{RS} {asset} {duration}m {side} "
                        f"n={int(pub.get('n',0) or 0)}/{int(pub.get('min_n', PM_PUBLIC_PATTERN_MIN_N) or PM_PUBLIC_PATTERN_MIN_N)} "
                        f"dom={float(pub.get('dom',0.0) or 0.0):+.2f} avg={float(pub.get('avg_c',0.0) or 0.0):.1f}c"
                    )

        # Real-time CID refresh on missing/stale leader-flow before any gating decision.
        if COPYFLOW_CID_ONDEMAND_ENABLED and (
            flow_n <= 0 or flow_age_s > COPYFLOW_LIVE_MAX_AGE_SEC
        ):
            now_ts = _time.time()
            last_ts = float(self._copyflow_cid_on_demand_ts.get(cid, 0.0) or 0.0)
            if (now_ts - last_ts) >= COPYFLOW_CID_ONDEMAND_COOLDOWN_SEC:
                self._copyflow_cid_on_demand_ts[cid] = now_ts
                try:
                    await self._copyflow_refresh_cid_once(cid, reason="score-miss")
                except Exception:
                    pass
                flow = self._copyflow_live.get(cid) or self._copyflow_map.get(cid, {})
                if isinstance(flow, dict):
                    up_conf = float(flow.get("Up", flow.get("up", 0.0)) or 0.0)
                    dn_conf = float(flow.get("Down", flow.get("down", 0.0)) or 0.0)
                    flow_n = int(flow.get("n", 0) or 0)
                    flow_ts = float(flow.get("ts", 0.0) or 0.0)
                    flow_age_s = (_time.time() - flow_ts) if flow_ts > 0 else 9e9
                    leader_net = up_conf - dn_conf
                    leader_ready = flow_age_s <= COPYFLOW_LIVE_MAX_AGE_SEC and flow_n >= 1
                    leader_soft_ready = (flow_n >= MARKET_LEADER_MIN_N) and (
                        flow_age_s <= LEADER_FLOW_FALLBACK_MAX_AGE_SEC
                    )
                    pref = up_conf if side == "Up" else dn_conf
                    opp = dn_conf if side == "Up" else up_conf
                    copy_net = pref - opp
        if leader_ready and signal_source != "leader-fresh":
            signal_tier = "TIER-A"
            signal_source = "leader-live"
            leader_size_scale = LEADER_FRESH_SIZE_SCALE
        if REQUIRE_LEADER_FLOW and (not leader_ready):
            # Leader wallets are optional alpha, never a hard gate.
            # We continue with strict realtime technical stack.
            signal_tier = "TIER-B"
            signal_source = "tech-realtime-no-leader"
            leader_size_scale = min(leader_size_scale, 0.90)

        # Leader/prebid can change side: refresh side-aligned metrics.
        side_up = side == "Up"
        tf_votes = tf_up_votes if side_up else tf_dn_votes
        ob_sig = dw_ob if side_up else -dw_ob
        cl_agree = True
        if cl_now > 0 and open_price > 0:
            cl_agree = (cl_now > open_price) == side_up
        imbalance_confirms = ob_sig > 0.10

        # Source-quality + signal-conviction analysis (real data only).
        # This is the primary anti-random layer: trade only when data is both fresh and coherent.
        cl_fresh = (cl_age_s is not None) and (cl_age_s <= 35.0)
        quote_fresh = quote_age_ms <= min(MAX_QUOTE_STALENESS_MS, 1200.0)
        ws_fresh = ws_book_strict is not None
        rest_fresh = (
            isinstance(ws_book_now, dict)
            and str(ws_book_now.get("source", "") or "") in ("clob-rest", "pm")
            and ((_time.time() - float(ws_book_now.get("ts", 0.0) or 0.0)) * 1000.0)
            <= max(CLOB_REST_FRESH_MAX_AGE_MS, WS_BOOK_FALLBACK_MAX_AGE_MS)
        )
        book_fresh = ws_fresh or rest_fresh
        vol_fresh = bool(volume_ready)
        leader_fresh = bool(leader_ready)
        analysis_quality = (
            ((0.25 if ws_fresh else (0.22 if rest_fresh else 0.0)))
            + (0.20 if leader_fresh else 0.0)
            + (0.20 if cl_fresh else 0.0)
            + (0.15 if quote_fresh else 0.0)
            + (0.20 if vol_fresh else 0.0)
        )

        dir_sign = 1.0 if side == "Up" else -1.0
        ob_c = max(0.0, min(1.0, (ob_sig + 0.10) / 0.30))
        tk_signed = (taker_ratio - 0.5) * dir_sign
        tk_c = max(0.0, min(1.0, (tk_signed + 0.05) / 0.18))
        tf_c = max(0.0, min(1.0, (float(tf_votes) - 1.0) / 3.0))
        basis_c = max(0.0, min(1.0, ((dir_sign * perp_basis) + 0.0002) / 0.0010))
        vwap_c = max(0.0, min(1.0, ((dir_sign * vwap_dev) + 0.0008) / 0.0024))
        cl_c = 1.0 if cl_agree else 0.0
        leader_c = 0.5
        if leader_fresh:
            leader_c = max(0.0, min(1.0, ((copy_net) + 0.12) / 0.34))
        analysis_conviction = (
            0.18 * ob_c
            + 0.18 * tk_c
            + 0.18 * tf_c
            + 0.12 * basis_c
            + 0.12 * vwap_c
            + 0.12 * cl_c
            + 0.10 * leader_c
        )

        quality_floor = float(MIN_ANALYSIS_QUALITY)
        conviction_floor = float(MIN_ANALYSIS_CONVICTION)
        # Dynamic gates by realtime data quality and timing (position-aware, not fixed-only).
        if book_fresh and cl_fresh and vol_fresh:
            quality_floor -= 0.03
        if (not leader_fresh) and book_fresh and cl_fresh:
            quality_floor -= 0.02
        if mins_left <= (3.5 if duration >= 15 else 1.8):
            quality_floor -= 0.02
            conviction_floor -= 0.01
        if quote_age_ms > 900:
            quality_floor += 0.02
        if not book_fresh:
            quality_floor += 0.03
            conviction_floor += 0.02
        elif rest_fresh and (not ws_fresh):
            # REST fresh is acceptable, but slightly weaker than strict WS.
            conviction_floor += 0.005
        if not cl_fresh:
            conviction_floor += 0.03
        quality_floor = max(0.45, min(0.75, quality_floor))
        conviction_floor = max(0.45, min(0.72, conviction_floor))

        if analysis_quality + 1e-6 < quality_floor:
            if self._noisy_log_enabled(f"skip-analysis-quality:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
                print(
                    f"{Y}[SKIP] {asset} {duration}m low analysis quality "
                    f"q={analysis_quality:.3f}<{quality_floor:.3f}{RS}"
                )
            self._skip_tick("analysis_quality_low")
            return None
        if analysis_conviction + 1e-6 < conviction_floor:
            if self._noisy_log_enabled(f"skip-analysis-conv:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
                print(
                    f"{Y}[SKIP] {asset} {duration}m low analysis conviction "
                    f"c={analysis_conviction:.3f}<{conviction_floor:.3f}{RS}"
                )
            self._skip_tick("analysis_conviction_low")
            return None

        # Recalibrate posterior using measured analysis quality.
        # Higher quality allows stronger posterior; weaker quality shrinks toward 50%.
        quality_scale = ANALYSIS_PROB_SCALE_MIN + (
            (ANALYSIS_PROB_SCALE_MAX - ANALYSIS_PROB_SCALE_MIN) * max(0.0, min(1.0, analysis_quality))
        )
        prob_up = 0.5 + (prob_up - 0.5) * quality_scale
        prob_up = max(0.05, min(0.95, prob_up))
        prob_down = 1.0 - prob_up
        # Apply recent side prior again after quality rescale (keeps final posterior aligned to live outcomes).
        up_recent = self._recent_side_profile(asset, duration, "Up")
        dn_recent = self._recent_side_profile(asset, duration, "Down")
        up_adj = float(up_recent.get("prob_adj", 0.0) or 0.0)
        dn_adj = float(dn_recent.get("prob_adj", 0.0) or 0.0)
        if abs(up_adj) > 1e-9 or abs(dn_adj) > 1e-9:
            pu = max(0.01, min(0.99, prob_up + up_adj))
            pd = max(0.01, min(0.99, prob_down + dn_adj))
            z = pu + pd
            if z > 0:
                prob_up = max(0.05, min(0.95, pu / z))
                prob_down = 1.0 - prob_up
        edge_up = prob_up - up_price
        edge_down = prob_down - (1 - up_price)

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

        # Pre-bid may override side: refresh aligned metrics for downstream gates/sizing.
        side_up = side == "Up"
        tf_votes = tf_up_votes if side_up else tf_dn_votes
        ob_sig = dw_ob if side_up else -dw_ob
        cl_agree = True
        if cl_now > 0 and open_price > 0:
            cl_agree = (cl_now > open_price) == side_up
        imbalance_confirms = ob_sig > 0.10

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
        if pm_book_data is None:
            # Ensure entry/payout gates use the actual traded side book, not synthetic 1-up_price.
            ws_side = self._get_clob_ws_book(token_id, max_age_ms=CLOB_MARKET_WS_MAX_AGE_MS)
            if ws_side is not None:
                pm_book_data = ws_side
            else:
                pm_book_data = await self._fetch_pm_book_safe(token_id)
        # Side/book sanity check: if chosen token ask is far from expected side price
        # and opposite token aligns much better, swap to prevent inverted side execution.
        def _best_ask_from(book):
            if not book:
                return 0.0
            if isinstance(book, dict):
                return float(book.get("best_ask", 0.0) or 0.0)
            try:
                _b, _a, _t = book
                return float(_a or 0.0)
            except Exception:
                return 0.0

        fair_side_entry = up_price if side == "Up" else (1.0 - up_price)
        cur_ask = _best_ask_from(pm_book_data)
        if fair_side_entry > 0 and cur_ask > 0 and abs(cur_ask - fair_side_entry) > 0.18:
            alt_token_id = m["token_down"] if side == "Up" else m["token_up"]
            alt_book = self._get_clob_ws_book(alt_token_id, max_age_ms=CLOB_MARKET_WS_MAX_AGE_MS)
            if alt_book is None:
                alt_book = await self._fetch_pm_book_safe(alt_token_id)
            alt_ask = _best_ask_from(alt_book)
            if alt_ask > 0:
                cur_err = abs(cur_ask - fair_side_entry)
                alt_err = abs(alt_ask - fair_side_entry)
                if (alt_err + 0.03) < cur_err:
                    if self._noisy_log_enabled(f"token-swap:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
                        print(
                            f"{Y}[TOKEN-SWAP]{RS} {asset} {duration}m {side} "
                            f"ask(cur={cur_ask:.3f},alt={alt_ask:.3f},fair={fair_side_entry:.3f})"
                        )
                    token_id = alt_token_id
                    pm_book_data = alt_book

        # ── Live CLOB price (more accurate than Gamma up_price) ──────────────
        live_entry = entry
        book_age_ms = 0.0
        if pm_book_data is not None:
            if isinstance(pm_book_data, dict):
                book_ts = float(pm_book_data.get("ts", 0.0) or 0.0)
                book_age_ms = ((_time.time() - book_ts) * 1000.0) if book_ts > 0 else 9e9
                if book_age_ms > MAX_ORDERBOOK_AGE_MS:
                    pm_book_data = None
                else:
                    clob_ask = float(pm_book_data.get("best_ask", 0.0) or 0.0)
                    live_entry = clob_ask
            else:
                # Backward compatibility with old tuple format.
                _, clob_ask, _ = pm_book_data
                live_entry = clob_ask

        # ── Entry strategy ────────────────────────────────────────────────────
        # Trade every eligible market while still preferring higher-payout entries.
        use_limit = False
        # Dynamic max entry: base + tolerance + small conviction slack.
        score_slack = 0.02 if score >= 12 else (0.01 if score >= 9 else 0.0)
        max_entry_allowed = min(0.97, MAX_ENTRY_PRICE + MAX_ENTRY_TOL + score_slack)
        min_entry_allowed = 0.01
        base_min_entry_allowed = 0.01
        base_max_entry_allowed = max_entry_allowed
        if duration <= 5:
            max_entry_allowed = min(max_entry_allowed, MAX_ENTRY_PRICE_5M)
            min_entry_allowed = max(min_entry_allowed, MIN_ENTRY_PRICE_5M)
            base_min_entry_allowed = min_entry_allowed
            base_max_entry_allowed = max_entry_allowed
            if MAX_WIN_MODE:
                max_entry_allowed = min(max_entry_allowed, WINMODE_MAX_ENTRY_5M)
        else:
            min_entry_allowed = max(min_entry_allowed, MIN_ENTRY_PRICE_15M)
            base_min_entry_allowed = min_entry_allowed
            base_max_entry_allowed = max_entry_allowed
            if MAX_WIN_MODE:
                max_entry_allowed = min(max_entry_allowed, WINMODE_MAX_ENTRY_15M)
        # Adaptive model-consistent cap: if conviction is high, allow higher entry as long
        # expected value after fees remains positive.
        min_ev_base = MIN_EV_NET_5M if duration <= 5 else MIN_EV_NET
        base_min_ev_req = float(min_ev_base)
        base_min_payout_req = float(MIN_PAYOUT_MULT_5M if duration <= 5 else MIN_PAYOUT_MULT)
        model_cap = true_prob / max(1.0 + FEE_RATE_EST + max(0.003, min_ev_base), 1e-9)
        if score >= 9:
            max_entry_allowed = max(max_entry_allowed, min(0.85, model_cap))
        # Adaptive protection against poor payout fills from realized outcomes.
        min_payout_req, min_ev_req, adaptive_hard_cap = self._adaptive_thresholds(duration)
        rolling_profile = self._rolling_15m_profile(duration, live_entry, open_src, cl_age_s)
        if duration >= 15 and (not booster_eval):
            min_ev_req = max(0.005, min_ev_req + float(rolling_profile.get("ev_add", 0.0) or 0.0))
        # Dynamic flow constraints by current setup quality (avoid static hardcoded behavior).
        setup_q = max(0.0, min(1.0, (analysis_quality * 0.55) + (analysis_conviction * 0.45)))
        q_relax = max(0.0, setup_q - 0.55)
        min_payout_req = max(1.55, min_payout_req - (0.20 * q_relax))
        # Additional payout relaxation only for strong, side-aligned 15m setups.
        # This reduces "no-trade" dead-zones without opening weak entries.
        if (
            duration >= 15
            and score >= 12
            and true_prob >= 0.82
            and setup_q >= 0.70
            and cl_agree
        ):
            extra_relax = min(0.06, max(0.0, setup_q - 0.70) * 0.30)
            min_payout_req = max(1.70, min_payout_req - extra_relax)
        # Core 15m EV floor: avoid low-multiple core entries.
        if duration >= 15 and (not booster_eval):
            min_payout_req = max(min_payout_req, 1.70)
        min_ev_req = max(0.005, min_ev_req - (0.012 * q_relax))
        if ws_fresh and cl_fresh and vol_fresh and mins_left >= (4.0 if duration >= 15 else 2.0):
            max_entry_allowed = min(0.90, max_entry_allowed + 0.02)
        max_entry_allowed = min(max_entry_allowed, adaptive_hard_cap)
        # Dynamic min-entry (not fixed hard floor): adapt to setup quality + microstructure.
        # High-quality setup can dip lower for super-payout entries; weaker setup is stricter.
        min_entry_dyn = float(base_min_entry_allowed)
        if setup_q >= 0.72 and vol_ratio >= 1.10 and tf_votes >= 2:
            relax = min(0.08, (setup_q - 0.72) * 0.20)  # up to -8c
            min_entry_dyn = max(0.01, min_entry_dyn - relax)
        elif setup_q <= 0.58:
            tighten = min(0.06, (0.58 - setup_q) * 0.25)  # up to +6c
            min_entry_dyn = min(0.45, min_entry_dyn + tighten)
        if mins_left <= (3.5 if duration >= 15 else 1.8):
            # Near expiry, avoid ultra-low entries that are mostly noise/fill artifacts.
            min_entry_dyn = max(min_entry_dyn, base_min_entry_allowed + 0.01)
        if not ws_fresh:
            min_entry_dyn = max(min_entry_dyn, base_min_entry_allowed + 0.01)
        min_entry_allowed = max(0.01, min(min_entry_dyn, max_entry_allowed - 0.01))
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
            elif PULLBACK_LIMIT_ENABLED and pct_remaining >= (
                PULLBACK_LIMIT_MIN_PCT_LEFT if duration <= 5 else max(PULLBACK_LIMIT_MIN_PCT_LEFT, 0.45)
            ):
                # Don't miss good-payout setups: park a pullback limit at max acceptable entry.
                use_limit = True
                entry = max_entry_allowed
            else:
                if self._noisy_log_enabled(f"skip-score-entry:{asset}:{side}", LOG_SKIP_EVERY_SEC):
                    print(f"{Y}[SKIP] {asset} {side} entry={live_entry:.3f} outside [{min_entry_allowed:.2f},{max_entry_allowed:.2f}]{RS}")
                self._skip_tick("entry_outside")
                return None

        payout_mult = 1.0 / max(entry, 1e-9)
        if payout_mult < min_payout_req:
            if payout_mult >= max(1.0, (min_payout_req - PAYOUT_NEAR_MISS_TOL)):
                if self._noisy_log_enabled(f"payout-near-miss:{asset}:{side}", LOG_SKIP_EVERY_SEC):
                    print(
                        f"{Y}[PAYOUT-TOL]{RS} {asset} {side} payout={payout_mult:.2f}x "
                        f"near min={min_payout_req:.2f}x (tol={PAYOUT_NEAR_MISS_TOL:.2f}x)"
                    )
            else:
                if self._noisy_log_enabled(f"skip-score-payout:{asset}:{side}", LOG_SKIP_EVERY_SEC):
                    print(
                        f"{Y}[SKIP] {asset} {side} payout={payout_mult:.3f}x "
                        f"< min={min_payout_req:.3f}x{RS}"
                    )
                self._skip_tick("payout_below")
                return None
        ev_net = (true_prob / max(entry, 1e-9)) - 1.0 - FEE_RATE_EST
        exec_slip_cost, exec_nofill_penalty, exec_fill_ratio = self._execution_penalties(duration, score, entry)
        execution_ev = ev_net - exec_slip_cost - exec_nofill_penalty
        if execution_ev < min_ev_req:
            if self._noisy_log_enabled(f"skip-score-ev:{asset}:{side}", LOG_SKIP_EVERY_SEC):
                print(
                    f"{Y}[SKIP] {asset} {side} exec_ev={execution_ev:.3f} "
                    f"(ev={ev_net:.3f} slip={exec_slip_cost:.3f} nofill={exec_nofill_penalty:.3f}) "
                    f"< min={min_ev_req:.3f}{RS}"
                )
            self._skip_tick("ev_below")
            return None
        if duration >= 15 and (not booster_eval) and CONSISTENCY_CORE_ENABLED:
            core_payout = 1.0 / max(entry, 1e-9)
            dyn_prob_floor = max(
                0.50,
                min(
                    0.90,
                    CONSISTENCY_MIN_TRUE_PROB_15M + float(rolling_profile.get("prob_add", 0.0) or 0.0),
                ),
            )
            dyn_ev_floor = max(
                0.005,
                min(
                    0.060,
                    CONSISTENCY_MIN_EXEC_EV_15M + float(rolling_profile.get("ev_add", 0.0) or 0.0),
                ),
            )
            if EV_FRONTIER_ENABLED:
                # Entry-aware minimum probability: allow sub-2x only when posterior is strong enough.
                # required_prob = breakeven(entry+fees) + safety margin increasing with expensive entries.
                req_prob = (
                    (entry * (1.0 + FEE_RATE_EST))
                    + EV_FRONTIER_MARGIN_BASE
                    + max(0.0, entry - 0.50) * EV_FRONTIER_MARGIN_HIGH_ENTRY
                )
                if true_prob + 1e-9 < req_prob:
                    if self._noisy_log_enabled(f"skip-ev-frontier:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
                        print(
                            f"{Y}[SKIP] {asset} {duration}m ev-frontier prob "
                            f"{true_prob:.3f}<{req_prob:.3f} (entry={entry:.3f}){RS}"
                        )
                    self._skip_tick("ev_frontier_prob_low")
                    return None
            core_lead_now = (
                (side == "Up" and current >= open_price) or
                (side == "Down" and current <= open_price)
            )
            core_strong = (
                score >= CONSISTENCY_STRONG_MIN_SCORE_15M
                and true_prob >= CONSISTENCY_STRONG_MIN_TRUE_PROB_15M
                and execution_ev >= CONSISTENCY_STRONG_MIN_EXEC_EV_15M
                and tf_votes >= 3
                and cl_agree
            )
            if core_payout + 1e-9 < CONSISTENCY_MIN_PAYOUT_15M:
                if self._noisy_log_enabled(f"skip-consistency-payout:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
                    print(
                        f"{Y}[SKIP] {asset} {duration}m consistency payout="
                        f"{core_payout:.3f}x<{CONSISTENCY_MIN_PAYOUT_15M:.3f}x{RS}"
                    )
                self._skip_tick("consistency_payout_low")
                return None
            if CONSISTENCY_REQUIRE_CL_AGREE_15M and (not cl_agree):
                if self._noisy_log_enabled(f"skip-consistency-cl:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
                    print(f"{Y}[SKIP] {asset} {duration}m consistency: CL disagree on core trade{RS}")
                self._skip_tick("consistency_cl_disagree")
                return None
            if true_prob + 1e-9 < dyn_prob_floor:
                if self._noisy_log_enabled(f"skip-consistency-prob:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
                    print(
                        f"{Y}[SKIP] {asset} {duration}m consistency prob "
                        f"{true_prob:.3f}<{dyn_prob_floor:.3f}{RS}"
                    )
                self._skip_tick("consistency_prob_low")
                return None
            if execution_ev + 1e-9 < dyn_ev_floor:
                if self._noisy_log_enabled(f"skip-consistency-ev:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
                    print(
                        f"{Y}[SKIP] {asset} {duration}m consistency exec_ev "
                        f"{execution_ev:.3f}<{dyn_ev_floor:.3f}{RS}"
                    )
                self._skip_tick("consistency_ev_low")
                return None
            if tf_votes < CONSISTENCY_MIN_TF_VOTES_15M:
                if self._noisy_log_enabled(f"skip-consistency-tf:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
                    print(
                        f"{Y}[SKIP] {asset} {duration}m consistency tf_votes "
                        f"{tf_votes}<{CONSISTENCY_MIN_TF_VOTES_15M}{RS}"
                    )
                self._skip_tick("consistency_tf_low")
                return None
            # Propagate strict payout rule to execution layer so maker/fallback cannot overpay.
            core_entry_cap = min(0.99, 1.0 / max(1e-9, CONSISTENCY_MIN_PAYOUT_15M))
            if max_entry_allowed > core_entry_cap:
                max_entry_allowed = core_entry_cap
                if min_entry_allowed >= max_entry_allowed:
                    min_entry_allowed = max(0.01, max_entry_allowed - 0.01)
            if entry > CONSISTENCY_MAX_ENTRY_15M and (not core_strong):
                if self._noisy_log_enabled(f"skip-consistency-entry:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
                    print(
                        f"{Y}[SKIP] {asset} {duration}m consistency entry={entry:.3f} "
                        f"> cap={CONSISTENCY_MAX_ENTRY_15M:.3f} (not strong-core){RS}"
                    )
                self._skip_tick("consistency_entry_high")
                return None
            if not core_lead_now:
                trail_ok = pct_remaining >= CONSISTENCY_TRAIL_ALLOW_MIN_PCT_LEFT_15M and core_strong
                if not trail_ok:
                    if self._noisy_log_enabled(f"skip-consistency-trail:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
                        print(
                            f"{Y}[SKIP] {asset} {duration}m consistency trailing weak "
                            f"(pct_left={pct_remaining:.2f} strong={core_strong}){RS}"
                        )
                    self._skip_tick("consistency_trail_weak")
                    return None
        if LOW_CENT_ONLY_ON_EXISTING_POSITION and entry <= LOW_CENT_ENTRY_THRESHOLD:
            if not booster_eval:
                if self._noisy_log_enabled(f"skip-lowcent-new:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
                    print(
                        f"{Y}[SKIP]{RS} {asset} {duration}m low-cent entry={entry:.3f} "
                        f"allowed only as add-on to existing position"
                    )
                self._skip_tick("lowcent_requires_existing")
                return None
            # Add-on low-cent requires that current side is already leading.
            is_leading_now = (
                (side == "Up" and current > open_price) or
                (side == "Down" and current < open_price)
            )
            if not is_leading_now:
                if self._noisy_log_enabled(f"skip-lowcent-notlead:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
                    print(
                        f"{Y}[SKIP]{RS} {asset} {duration}m low-cent add-on blocked "
                        f"(not leading now)"
                    )
                self._skip_tick("lowcent_not_leading")
                return None
        if self._noisy_log_enabled("flow-thresholds", LOG_FLOW_EVERY_SEC):
            print(
                f"{B}[FLOW]{RS} "
                f"payout>={base_min_payout_req:.3f}x→{min_payout_req:.3f}x "
                f"ev>={base_min_ev_req:.3f}→{min_ev_req:.3f} "
                f"entry=[{base_min_entry_allowed:.2f},{base_max_entry_allowed:.2f}]→"
                f"[{min_entry_allowed:.2f},{max_entry_allowed:.2f}]"
            )
        booster_mode = False
        booster_note = ""
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
        # Risk-aware size decay for tail-priced entries and near-expiry windows.
        # This preserves signal direction while preventing oversized bets on 2c/7c tails.
        cents_scale = 1.0
        if entry <= 0.03:
            cents_scale = 0.12
        elif entry <= 0.05:
            cents_scale = 0.18
        elif entry <= 0.10:
            cents_scale = 0.28
        elif entry <= 0.20:
            cents_scale = 0.45
        time_scale = 1.0
        if duration >= 15:
            if mins_left <= 2.5:
                time_scale = 0.20
            elif mins_left <= 3.5:
                time_scale = 0.35
            elif mins_left <= 5.0:
                time_scale = 0.55
        raw_size   = self._kelly_size(true_prob, entry, kelly_frac)
        max_single = min(100.0, self.bankroll * bankroll_pct)
        cid_cap    = max(0.50, self.bankroll * MAX_CID_EXPOSURE_PCT)
        hard_cap   = max(0.50, min(max_single, cid_cap, self.bankroll * MAX_BANKROLL_PCT, float(MAX_ABS_BET)))
        # Avoid oversized tail bets: these entries can look high-multiple but are low-quality fills.
        if entry <= 0.05:
            hard_cap = min(hard_cap, max(MIN_BET_ABS, self.bankroll * 0.015))
        elif entry <= 0.10:
            hard_cap = min(hard_cap, max(MIN_BET_ABS, self.bankroll * 0.020))
        # Soft cap curve for cheap entries:
        # default around $10, but scale toward ~$30 only if conviction is truly strong.
        if entry <= LOW_ENTRY_SOFT_THRESHOLD:
            score_n = max(0.0, min(1.0, (float(score) - 8.0) / 10.0))
            prob_n = max(0.0, min(1.0, (float(true_prob) - 0.56) / 0.20))
            edge_n = max(0.0, min(1.0, (float(edge) - 0.02) / 0.12))
            # copy_net > 0 means leaders agree with chosen side; <0 penalizes conviction.
            leader_n = max(0.0, min(1.0, float(copy_net) / 0.40))
            conviction = (
                0.35 * score_n
                + 0.35 * prob_n
                + 0.20 * edge_n
                + 0.10 * leader_n
            )
            soft_cap = LOW_ENTRY_BASE_SOFT_MAX + (
                (LOW_ENTRY_HIGH_CONV_SOFT_MAX - LOW_ENTRY_BASE_SOFT_MAX) * conviction
            )
            hard_cap = min(hard_cap, max(MIN_BET_ABS, soft_cap))
        # Risk cap when leader flow is not fresh: avoid oversized bets on cheap entries.
        # Deterministic cap from signal quality (score/prob/edge), no random guard.
        if duration >= 15 and entry <= 0.40 and (not leader_ready):
            score_q = max(0.0, min(1.0, (float(score) - 9.0) / 9.0))
            prob_q = max(0.0, min(1.0, (float(true_prob) - 0.58) / 0.20))
            edge_q = max(0.0, min(1.0, (float(edge) - 0.02) / 0.10))
            qual = 0.45 * score_q + 0.35 * prob_q + 0.20 * edge_q
            # Cap range: ~$6 (weak) .. ~$12 (strong but no fresh leaders).
            no_leader_cap = 6.0 + (6.0 * qual)
            # Near expiry tighten further.
            if mins_left <= 3.5:
                no_leader_cap = min(no_leader_cap, 8.0)
            hard_cap = min(hard_cap, max(MIN_BET_ABS, no_leader_cap))
        model_size = round(
            min(
                hard_cap,
                raw_size * vol_mult * wr_scale * oracle_scale * bucket_scale * cents_scale * time_scale * leader_size_scale,
            ),
            2,
        )
        if duration >= 15 and (not booster_eval):
            model_size = round(
                min(
                    hard_cap,
                    model_size * float(rolling_profile.get("size_mult", 1.0) or 1.0),
                ),
                2,
            )
        if (
            HIGH_EV_SIZE_BOOST_ENABLED
            and duration >= 15
            and (not booster_eval)
            and score >= HIGH_EV_MIN_SCORE
            and execution_ev >= HIGH_EV_MIN_EXEC_EV
            and entry <= 0.50
        ):
            boost = min(HIGH_EV_SIZE_BOOST_MAX, max(1.0, HIGH_EV_SIZE_BOOST))
            model_size = round(min(hard_cap, model_size * boost), 2)
        dyn_floor  = min(hard_cap, max(MIN_BET_ABS, self.bankroll * MIN_BET_PCT))
        # Mid-entry high-conviction floor:
        # avoid dust-size core bets on solid 15m setups around 30c-55c.
        if (
            duration >= 15
            and (not booster_eval)
            and 0.30 <= entry <= 0.55
            and score >= 12
            and true_prob >= 0.70
            and execution_ev >= max(min_ev_req + 0.008, 0.020)
            and cl_agree
        ):
            dyn_floor = max(
                dyn_floor,
                min(hard_cap, max(4.0, self.bankroll * 0.012)),
            )
        # Never force a big floor size on ultra-cheap tails or near-expiry entries.
        if entry <= 0.10 or (duration >= 15 and mins_left <= 3.5):
            dyn_floor = min(dyn_floor, MIN_BET_ABS)
        # If model size is too small and setup is not top quality, skip instead of forcing a noisy tiny bet.
        if model_size < dyn_floor:
            hi_conf = (score >= 12 and true_prob >= 0.62 and edge >= 0.03)
            if not hi_conf:
                return None
            size = round(dyn_floor, 2)
        else:
            size = model_size
        size = max(0.50, min(hard_cap, size))

        # Super-bet floor:
        # for high-multiple entries (very low price), keep at least a meaningful notional.
        if SUPER_BET_MIN_SIZE_ENABLED:
            payout_mult = (1.0 / max(entry, 1e-9))
            if (
                duration >= 15
                and entry <= SUPER_BET_ENTRY_MAX
                and payout_mult >= SUPER_BET_MIN_PAYOUT
                and size < SUPER_BET_MIN_SIZE_USDC
            ):
                old_size = size
                size = min(hard_cap, max(size, SUPER_BET_MIN_SIZE_USDC))
                if self._noisy_log_enabled(f"superbet-floor:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
                    print(
                        f"{Y}[SIZE-TUNE]{RS} {asset} {duration}m {side} superbet floor "
                        f"x={payout_mult:.2f} entry={entry:.3f} "
                        f"${old_size:.2f}->${size:.2f}"
                    )

        # Super-bet cap:
        # keep very-high-multiple tails from over-allocating notional when liquidity/noise is high.
        if SUPER_BET_MAX_SIZE_ENABLED:
            payout_mult = (1.0 / max(entry, 1e-9))
            if duration >= 15 and entry <= SUPER_BET_ENTRY_MAX and payout_mult >= SUPER_BET_MIN_PAYOUT:
                max_super = max(0.50, min(SUPER_BET_MAX_SIZE_USDC, self.bankroll * SUPER_BET_MAX_BANKROLL_PCT))
                if size > max_super:
                    old_size = size
                    size = round(max(0.50, max_super), 2)
                    if self._noisy_log_enabled(f"superbet-cap:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
                        print(
                            f"{Y}[SIZE-TUNE]{RS} {asset} {duration}m {side} superbet cap "
                            f"x={payout_mult:.2f} entry={entry:.3f} "
                            f"${old_size:.2f}->${size:.2f}"
                        )

        if size < MIN_EXEC_NOTIONAL_USDC:
            return None

        # Mid-round (or anytime) additive booster on existing same-side position.
        # This is intentionally a separate, small bet with stricter quality filters.
        if booster_eval and MID_BOOSTER_ENABLED:
            if self._booster_locked():
                if self._noisy_log_enabled(f"booster-lock:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
                    rem_h = max(0.0, (self._booster_lock_until - _time.time()) / 3600.0)
                    print(f"{Y}[BOOST-SKIP]{RS} locked {rem_h:.1f}h | cid={self._short_cid(cid)}")
                return None
            if duration != 15:
                return None
            if side != booster_side_locked:
                return None
            if int(self._booster_used_by_cid.get(cid, 0) or 0) >= max(1, MID_BOOSTER_MAX_PER_CID):
                return None
            if mins_left < MID_BOOSTER_MIN_LEFT_HARD_15M:
                return None
            in_ideal_window = MID_BOOSTER_MIN_LEFT_15M <= mins_left <= MID_BOOSTER_MAX_LEFT_15M
            if (not MID_BOOSTER_ANYTIME_15M) and (not in_ideal_window):
                return None

            taker_conf = (side_up and taker_ratio > 0.54) or ((not side_up) and taker_ratio < 0.46)
            # Mathematical conviction model (15m intraround):
            # combines microstructure + trend persistence + oracle alignment.
            ob_c = max(-1.0, min(1.0, ob_sig / 0.35))
            tf_c = max(0.0, min(1.0, (tf_votes - 1.0) / 3.0))
            flow_c = max(-1.0, min(1.0, ((taker_ratio - 0.5) * 2.0) if side_up else ((0.5 - taker_ratio) * 2.0)))
            vol_c = max(0.0, min(1.0, (vol_ratio - 1.0) / 1.5))
            basis_signed = perp_basis if side_up else -perp_basis
            basis_c = max(-1.0, min(1.0, basis_signed / 0.0008))
            vwap_signed = vwap_dev if side_up else -vwap_dev
            vwap_c = max(-1.0, min(1.0, vwap_signed / 0.0012))
            oracle_c = 1.0 if cl_agree else -1.0
            booster_conv = (
                0.24 * tf_c
                + 0.20 * max(0.0, ob_c)
                + 0.16 * max(0.0, flow_c)
                + 0.12 * vol_c
                + 0.10 * max(0.0, basis_c)
                + 0.10 * max(0.0, vwap_c)
                + 0.08 * max(0.0, oracle_c)
            )
            base_quality = (
                score >= MID_BOOSTER_MIN_SCORE
                and true_prob >= MID_BOOSTER_MIN_TRUE_PROB
                and edge >= MID_BOOSTER_MIN_EDGE
                and execution_ev >= MID_BOOSTER_MIN_EV_NET
                and payout_mult >= MID_BOOSTER_MIN_PAYOUT
                and entry <= MID_BOOSTER_MAX_ENTRY
                and cl_agree
                and imbalance_confirms
                and taker_conf
                and vol_ratio >= 1.05
                and booster_conv >= 0.58
            )
            if not base_quality:
                return None
            # Outside ideal window keep booster much stricter.
            if not in_ideal_window:
                if not (
                    score >= (MID_BOOSTER_MIN_SCORE + 2)
                    and true_prob >= (MID_BOOSTER_MIN_TRUE_PROB + 0.02)
                    and execution_ev >= (MID_BOOSTER_MIN_EV_NET + 0.015)
                    and booster_conv >= 0.66
                ):
                    return None

            prev_size = float((self.pending.get(cid, ({}, {}))[1] or {}).get("size", 0.0) or 0.0)
            b_pct = MID_BOOSTER_SIZE_PCT
            if score >= (MID_BOOSTER_MIN_SCORE + 3) and true_prob >= (MID_BOOSTER_MIN_TRUE_PROB + 0.03):
                b_pct = MID_BOOSTER_SIZE_PCT_HIGH
            b_size = round(max(MIN_BET_ABS, self.bankroll * b_pct), 2)
            # Keep additive booster small vs existing exposure.
            if prev_size > 0:
                b_size = min(b_size, max(MIN_BET_ABS, prev_size * 0.35))
            size = max(MIN_BET_ABS, min(size, b_size, hard_cap))
            booster_mode = True
            booster_note = "mid" if in_ideal_window else "anytime"
            signal_tier = f"{signal_tier}+BOOST"
            signal_source = f"{signal_source}+booster"
            if self._noisy_log_enabled(f"booster-conv:{asset}:{cid}", LOG_EDGE_EVERY_SEC):
                print(
                    f"{B}[BOOST-CHECK]{RS} {asset} 15m {side} "
                    f"conv={booster_conv:.2f} score={score} ev={execution_ev:.3f} "
                    f"payout={payout_mult:.2f}x entry={entry:.3f} "
                    f"tf={tf_votes} ob={ob_sig:+.2f} tk={taker_ratio:.2f} vol={vol_ratio:.2f}x"
                )

        # Live EV tuning (non-blocking):
        # 1) Reduce tail-risk sizing on very low-cent entries unless conviction is exceptional.
        if (
            LOW_ENTRY_SIZE_HAIRCUT_ENABLED
            and duration >= 15
            and (not booster_mode)
            and entry <= LOW_ENTRY_SIZE_HAIRCUT_PX
            and not (score >= LOW_ENTRY_SIZE_HAIRCUT_KEEP_SCORE and true_prob >= LOW_ENTRY_SIZE_HAIRCUT_KEEP_PROB)
        ):
            old_size = float(size)
            size = max(float(MIN_EXEC_NOTIONAL_USDC), round(old_size * LOW_ENTRY_SIZE_HAIRCUT_MULT, 2))
            if self._noisy_log_enabled(f"low-entry-haircut:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
                print(
                    f"{Y}[SIZE-TUNE]{RS} {asset} {duration}m {side} low-entry haircut "
                    f"entry={entry:.3f} size=${old_size:.2f}->${size:.2f}"
                )

        # 2) Same-round concentration decay: keep trading, but scale down 2nd/3rd/... leg.
        if duration >= 15 and (not booster_mode):
            sig_fp = self._round_fingerprint(
                cid=cid,
                m=m,
                t={"asset": asset, "side": side, "duration": duration},
            )
            same_round_same_side = 0
            for c_p, (m_p, t_p) in self.pending.items():
                try:
                    if self._round_fingerprint(cid=c_p, m=m_p, t=t_p) != sig_fp:
                        continue
                    if str(t_p.get("side", "")) != side:
                        continue
                    same_round_same_side += 1
                except Exception:
                    continue
            if same_round_same_side > 0:
                decay = max(0.05, min(1.0, ROUND_STACK_SIZE_DECAY))
                floor = max(0.05, min(1.0, ROUND_STACK_SIZE_MIN))
                mult = max(floor, decay ** same_round_same_side)
                old_size = float(size)
                size = max(float(MIN_EXEC_NOTIONAL_USDC), round(old_size * mult, 2))
                if self._noisy_log_enabled(f"round-stack-size:{asset}:{side}:{cid}", LOG_FLOW_EVERY_SEC):
                    print(
                        f"{Y}[SIZE-TUNE]{RS} {asset} {duration}m {side} round-stack x{mult:.2f} "
                        f"(legs={same_round_same_side+1}) ${old_size:.2f}->${size:.2f}"
                    )
            # Additional non-blocking round concentration decay:
            # keep entries active but damp clustered same-window risk across assets.
            same_round_total = 0
            same_round_corr_side = 0
            for c_p, (m_p, t_p) in self.pending.items():
                try:
                    if self._round_fingerprint(cid=c_p, m=m_p, t=t_p) != sig_fp:
                        continue
                    same_round_total += 1
                    if str(t_p.get("side", "")) == side:
                        same_round_corr_side += 1
                except Exception:
                    continue
            if same_round_total > 0:
                d_tot = max(0.05, min(1.0, ROUND_TOTAL_SIZE_DECAY))
                f_tot = max(0.05, min(1.0, ROUND_TOTAL_SIZE_MIN))
                mult_tot = max(f_tot, d_tot ** same_round_total)
                old_size = float(size)
                size = max(float(MIN_EXEC_NOTIONAL_USDC), round(old_size * mult_tot, 2))
                if self._noisy_log_enabled(f"round-total-size:{asset}:{side}:{cid}", LOG_FLOW_EVERY_SEC):
                    print(
                        f"{Y}[SIZE-TUNE]{RS} {asset} {duration}m {side} round-total x{mult_tot:.2f} "
                        f"(open_legs={same_round_total}) ${old_size:.2f}->${size:.2f}"
                    )
            if same_round_corr_side > 0:
                d_cor = max(0.05, min(1.0, ROUND_CORR_SAME_SIDE_DECAY))
                f_cor = max(0.05, min(1.0, ROUND_CORR_SAME_SIDE_MIN))
                mult_cor = max(f_cor, d_cor ** same_round_corr_side)
                old_size = float(size)
                size = max(float(MIN_EXEC_NOTIONAL_USDC), round(old_size * mult_cor, 2))
                if self._noisy_log_enabled(f"round-corr-size:{asset}:{side}:{cid}", LOG_FLOW_EVERY_SEC):
                    print(
                        f"{Y}[SIZE-TUNE]{RS} {asset} {duration}m {side} round-corr x{mult_cor:.2f} "
                        f"(same_side_legs={same_round_corr_side}) ${old_size:.2f}->${size:.2f}"
                    )

        # Final superbet/tail normalization after all decays.
        payout_mult = (1.0 / max(entry, 1e-9))
        if SUPER_BET_MIN_SIZE_ENABLED:
            if (
                duration >= 15
                and entry <= SUPER_BET_ENTRY_MAX
                and payout_mult >= SUPER_BET_MIN_PAYOUT
                and size < SUPER_BET_MIN_SIZE_USDC
            ):
                old_size = float(size)
                size = max(float(MIN_EXEC_NOTIONAL_USDC), min(hard_cap, float(SUPER_BET_MIN_SIZE_USDC)))
                if self._noisy_log_enabled(f"superbet-floor-final:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
                    print(
                        f"{Y}[SIZE-TUNE]{RS} {asset} {duration}m {side} superbet floor(final) "
                        f"x={payout_mult:.2f} ${old_size:.2f}->${size:.2f}"
                    )
        if SUPER_BET_MAX_SIZE_ENABLED:
            if duration >= 15 and entry <= SUPER_BET_ENTRY_MAX and payout_mult >= SUPER_BET_MIN_PAYOUT:
                max_super = max(0.50, min(SUPER_BET_MAX_SIZE_USDC, self.bankroll * SUPER_BET_MAX_BANKROLL_PCT))
                if size > max_super:
                    old_size = float(size)
                    size = round(max(0.50, max_super), 2)
                    if self._noisy_log_enabled(f"superbet-cap-final:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
                        print(
                            f"{Y}[SIZE-TUNE]{RS} {asset} {duration}m {side} superbet cap(final) "
                            f"x={payout_mult:.2f} ${old_size:.2f}->${size:.2f}"
                        )
        if TAIL_ENTRY_ABS_CAP_ENABLED and duration >= 15 and entry <= TAIL_ENTRY_CAP_PX:
            tail_cap = max(0.50, min(float(TAIL_ENTRY_MAX_SIZE_USDC), hard_cap))
            if size > tail_cap:
                old_size = float(size)
                size = round(tail_cap, 2)
                if self._noisy_log_enabled(f"tail-cap-final:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
                    print(
                        f"{Y}[SIZE-TUNE]{RS} {asset} {duration}m {side} tail-cap(final) "
                        f"entry={entry:.3f} ${old_size:.2f}->${size:.2f}"
                    )

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
            "ev_net": ev_net,
            "execution_ev": execution_ev,
            "execution_slip_cost": exec_slip_cost,
            "execution_nofill_penalty": exec_nofill_penalty,
            "execution_fill_ratio": exec_fill_ratio,
            "copy_adj": copy_adj,
            "copy_net": copy_net,
            "pm_pattern_key": pm_pattern_key,
            "pm_pattern_score_adj": pm_pattern_score_adj,
            "pm_pattern_edge_adj": pm_pattern_edge_adj,
            "pm_pattern_n": pm_pattern_n,
            "pm_pattern_exp": pm_pattern_exp,
            "pm_pattern_wr_lb": pm_pattern_wr_lb,
            "pm_public_pattern_score_adj": pm_public_pattern_score_adj,
            "pm_public_pattern_edge_adj": pm_public_pattern_edge_adj,
            "pm_public_pattern_n": pm_public_pattern_n,
            "pm_public_pattern_dom": pm_public_pattern_dom,
            "pm_public_pattern_avg_c": pm_public_pattern_avg_c,
            "recent_side_n": recent_side_n,
            "recent_side_exp": recent_side_exp,
            "recent_side_wr_lb": recent_side_wr_lb,
            "recent_side_score_adj": recent_side_score_adj,
            "recent_side_edge_adj": recent_side_edge_adj,
            "recent_side_prob_adj": recent_side_prob_adj,
            "rolling_n": int(rolling_profile.get("n", 0) or 0),
            "rolling_exp": float(rolling_profile.get("exp", 0.0) or 0.0),
            "rolling_wr_lb": float(rolling_profile.get("wr_lb", 0.5) or 0.5),
            "rolling_prob_add": float(rolling_profile.get("prob_add", 0.0) or 0.0),
            "rolling_ev_add": float(rolling_profile.get("ev_add", 0.0) or 0.0),
            "rolling_size_mult": float(rolling_profile.get("size_mult", 1.0) or 1.0),
            "min_payout_req": min_payout_req,
            "min_ev_req": min_ev_req,
            "analysis_quality": analysis_quality,
            "analysis_conviction": analysis_conviction,
            "analysis_prob_scale": quality_scale,
            "signal_tier": signal_tier,
            "signal_source": signal_source,
            "leader_size_scale": leader_size_scale,
            "booster_mode": booster_mode,
            "booster_note": booster_note,
            "book_age_ms": book_age_ms,
            "quote_age_ms": quote_age_ms,
            "signal_latency_ms": (_time.perf_counter() - score_started) * 1000.0,
            "prebid_arm": arm_active,
        }

    async def _execute_trade(self, sig: dict):
        """Execute a pre-scored signal: log, mark seen, place order, update state."""
        cid = sig["cid"]
        m = sig["m"]
        side = sig.get("side", "")
        is_booster = bool(sig.get("booster_mode", False))
        round_key = self._round_key(cid=cid, m=m, t=sig)
        round_fp = self._round_fingerprint(cid=cid, m=m, t=sig)
        round_side_key = f"{round_fp}|{side}|{'B' if is_booster else 'N'}"
        cid_side_key = f"{cid}|{side}|{'B' if is_booster else 'N'}"
        now_attempt = _time.time()
        async with self._exec_lock:
            # Trim expired round-side blocks.
            for k, exp in list(self._round_side_block_until.items()):
                if float(exp or 0) <= now_attempt:
                    self._round_side_block_until.pop(k, None)
            if cid in self._executing_cids:
                return
            if (not is_booster) and (cid in self.seen or len(self.pending) >= MAX_OPEN):
                return
            if is_booster:
                if self._booster_locked():
                    return
                if cid not in self.pending or cid in self.pending_redeem:
                    return
                p_side = str((self.pending.get(cid, ({}, {}))[1] or {}).get("side", "") or "")
                if p_side != side:
                    return
                if int(self._booster_used_by_cid.get(cid, 0) or 0) >= max(1, MID_BOOSTER_MAX_PER_CID):
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
            tier_tag = f" {sig.get('signal_tier','TIER-?')}/{sig.get('signal_source','na')}"
            scale_tag = f" szx={float(sig.get('leader_size_scale', 1.0) or 1.0):.2f}"
            booster_tag = f" BOOST({sig.get('booster_note','')})" if is_booster else ""
            print(f"{tag} {sig['asset']} {sig['duration']}m {sig['side']} | "
                  f"score={score} edge={sig['edge']:+.3f} size=${sig['size']:.2f} "
                  f"entry={sig['entry']:.3f} src={sig.get('open_price_source','?')} "
                  f"cl_age={sig.get('chainlink_age_s', -1):.0f}s{tier_tag}{scale_tag}"
                  f" q={float(sig.get('analysis_quality',0.0) or 0.0):.2f}"
                  f" c={float(sig.get('analysis_conviction',0.0) or 0.0):.2f}"
                  f"{booster_tag}{hc_tag}{cp_tag}{agree_str}{RS}")

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
            if (
                LATE_DIR_LOCK_ENABLED
                and duration in (5, 15)
                and sig.get("open_price", 0) > 0
            ):
                lock_mins = LATE_DIR_LOCK_MIN_LEFT_5M if duration <= 5 else LATE_DIR_LOCK_MIN_LEFT_15M
                if mins_left <= lock_mins:
                    cur_now = self._current_price(sig["asset"]) or sig.get("current", 0)
                    open_p = float(sig.get("open_price", 0) or 0)
                    if cur_now > 0 and open_p > 0:
                        move_abs = abs((cur_now - open_p) / max(open_p, 1e-9))
                        if move_abs >= LATE_DIR_LOCK_MIN_MOVE_PCT:
                            beat_dir = "Up" if cur_now >= open_p else "Down"
                            if sig.get("side") != beat_dir:
                                if self._noisy_log_enabled(f"late-lock-exec:{sig['asset']}:{sig.get('side','')}", LOG_SKIP_EVERY_SEC):
                                    print(
                                        f"{Y}[SKIP]{RS} {sig['asset']} {duration}m {sig.get('side','')} "
                                        f"late-lock(exec): beat_dir={beat_dir} mins_left={mins_left:.1f} "
                                        f"move={move_abs*100:.2f}%"
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
            sig = await self._maybe_wait_for_better_entry(sig)
            if sig is None:
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
                core_position=(not is_booster),
            )
            self._perf_update("order_ms", (_time.perf_counter() - t_ord) * 1000.0)
            order_id = (exec_result or {}).get("order_id", "")
            filled = exec_result is not None
            actual_size_usdc = float((exec_result or {}).get("notional_usdc", sig["size"]) or sig["size"])
            fill_price = float((exec_result or {}).get("fill_price", sig["entry"]) or sig["entry"])
            slip_bps = ((fill_price - sig["entry"]) / max(sig["entry"], 1e-9)) * 10000.0
            stat_bucket = self._bucket_key(sig["duration"], sig["score"], sig["entry"])
            if filled:
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
                "execution_ev": sig.get("execution_ev", 0.0),
                "ev_net": sig.get("ev_net", 0.0),
                "pm_pattern_key": sig.get("pm_pattern_key", ""),
                "pm_pattern_score_adj": sig.get("pm_pattern_score_adj", 0),
                "pm_pattern_edge_adj": sig.get("pm_pattern_edge_adj", 0.0),
                "pm_pattern_n": sig.get("pm_pattern_n", 0),
                "pm_pattern_exp": sig.get("pm_pattern_exp", 0.0),
                "pm_pattern_wr_lb": sig.get("pm_pattern_wr_lb", 0.5),
                "pm_public_pattern_score_adj": sig.get("pm_public_pattern_score_adj", 0),
                "pm_public_pattern_edge_adj": sig.get("pm_public_pattern_edge_adj", 0.0),
                "pm_public_pattern_n": sig.get("pm_public_pattern_n", 0),
                "pm_public_pattern_dom": sig.get("pm_public_pattern_dom", 0.0),
                "pm_public_pattern_avg_c": sig.get("pm_public_pattern_avg_c", 0.0),
                "recent_side_n": sig.get("recent_side_n", 0),
                "recent_side_exp": sig.get("recent_side_exp", 0.0),
                "recent_side_wr_lb": sig.get("recent_side_wr_lb", 0.5),
                "recent_side_score_adj": sig.get("recent_side_score_adj", 0),
                "recent_side_edge_adj": sig.get("recent_side_edge_adj", 0.0),
                "recent_side_prob_adj": sig.get("recent_side_prob_adj", 0.0),
                "rolling_n": sig.get("rolling_n", 0),
                "rolling_exp": sig.get("rolling_exp", 0.0),
                "rolling_wr_lb": sig.get("rolling_wr_lb", 0.5),
                "rolling_prob_add": sig.get("rolling_prob_add", 0.0),
                "rolling_ev_add": sig.get("rolling_ev_add", 0.0),
                "rolling_size_mult": sig.get("rolling_size_mult", 1.0),
                "min_payout_req": sig.get("min_payout_req", 0.0),
                "min_ev_req": sig.get("min_ev_req", 0.0),
                "bucket": stat_bucket,
                "fill_price": fill_price,
                "slip_bps": round(slip_bps, 2),
                "round_key": round_key,
                "placed_ts": _time.time(),
                "booster_mode": bool(is_booster),
                "booster_note": sig.get("booster_note", ""),
                "booster_count": 1 if is_booster else 0,
                "booster_stake_usdc": actual_size_usdc if is_booster else 0.0,
                "addon_count": 1 if is_booster else 0,
                "addon_stake_usdc": actual_size_usdc if is_booster else 0.0,
                "core_position": (not is_booster),
                "core_entry_locked": (not is_booster),
                "core_entry": (fill_price if (not is_booster) else 0.0),
                "core_size_usdc": (actual_size_usdc if (not is_booster) else 0.0),
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
                "placed": bool(filled),
                "order_id": order_id or "",
                "fill_price": round(fill_price, 6),
                "slippage_bps": round(slip_bps, 2),
                "signal_latency_ms": round(sig.get("signal_latency_ms", 0.0), 2),
                "quote_age_ms": round(sig.get("quote_age_ms", 0.0), 2),
                "bucket": stat_bucket,
                "round_key": round_key,
                "booster_mode": bool(is_booster),
                "booster_note": sig.get("booster_note", ""),
            })
            if filled and self._noisy_log_enabled(f"entry-stats:{sig['asset']}:{cid}", LOG_FLOW_EVERY_SEC):
                print(
                    f"{B}[ENTRY-STATS]{RS} {sig['asset']} {sig['duration']}m {sig['side']} "
                    f"ev={float(sig.get('execution_ev',0.0) or 0.0):+.3f} "
                    f"payout={1.0/max(float(sig.get('entry',0.5) or 0.5),1e-9):.2f}x "
                    f"min(payout/ev)={float(sig.get('min_payout_req',0.0) or 0.0):.2f}x/"
                    f"{float(sig.get('min_ev_req',0.0) or 0.0):.3f} "
                    f"pm={int(sig.get('pm_pattern_score_adj',0) or 0):+d}/{float(sig.get('pm_pattern_edge_adj',0.0) or 0.0):+.3f}"
                    f"(n={int(sig.get('pm_pattern_n',0) or 0)} exp={float(sig.get('pm_pattern_exp',0.0) or 0.0):+.2f}) "
                    f"pub={int(sig.get('pm_public_pattern_score_adj',0) or 0):+d}/{float(sig.get('pm_public_pattern_edge_adj',0.0) or 0.0):+.3f}"
                    f"(n={int(sig.get('pm_public_pattern_n',0) or 0)} dom={float(sig.get('pm_public_pattern_dom',0.0) or 0.0):+.2f}) "
                    f"rec={int(sig.get('recent_side_score_adj',0) or 0):+d}/{float(sig.get('recent_side_edge_adj',0.0) or 0.0):+.3f}"
                    f"(n={int(sig.get('recent_side_n',0) or 0)} exp={float(sig.get('recent_side_exp',0.0) or 0.0):+.2f} "
                    f"wr_lb={float(sig.get('recent_side_wr_lb',0.5) or 0.5):.2f}) "
                    f"roll(n={int(sig.get('rolling_n',0) or 0)} exp={float(sig.get('rolling_exp',0.0) or 0.0):+.2f} "
                    f"wr_lb={float(sig.get('rolling_wr_lb',0.5) or 0.5):.2f})"
                )
            if filled:
                if is_booster and cid in self.pending:
                    old_m, old_t = self.pending.get(cid, (m, {}))
                    old_size = float((old_t or {}).get("size", 0.0) or 0.0)
                    new_size = float(actual_size_usdc or 0.0)
                    # Keep core position immutable locally: no weighted-average assimilation.
                    # Add-ons are tracked in separate fields while on-chain remains aggregate truth.
                    old_t["size"] = round(old_size, 6)
                    old_t["fill_price"] = round(fill_price, 6)
                    old_t["slip_bps"] = round(float(trade.get("slip_bps", 0.0) or 0.0), 2)
                    old_t["placed_ts"] = _time.time()
                    old_t["booster_mode"] = True
                    old_t["booster_note"] = sig.get("booster_note", "")
                    old_t["booster_count"] = int(old_t.get("booster_count", 0) or 0) + 1
                    old_t["booster_stake_usdc"] = round(float(old_t.get("booster_stake_usdc", 0.0) or 0.0) + new_size, 6)
                    old_t["addon_count"] = int(old_t.get("addon_count", 0) or 0) + 1
                    old_t["addon_stake_usdc"] = round(float(old_t.get("addon_stake_usdc", 0.0) or 0.0) + new_size, 6)
                    old_t["core_position"] = bool(old_t.get("core_position", True))
                    old_t["core_entry_locked"] = bool(old_t.get("core_entry_locked", True))
                    self.pending[cid] = (old_m, old_t)
                    self._booster_used_by_cid[cid] = int(self._booster_used_by_cid.get(cid, 0) or 0) + 1
                    self._save_pending()
                    print(
                        f"{G}[BOOST-FILL]{RS} {sig['asset']} {sig['duration']}m {side} "
                        f"| +${new_size:.2f} @ {fill_price:.3f} | core=${old_size:.2f} "
                        f"| addon_total=${old_t.get('addon_stake_usdc', 0.0):.2f} "
                        f"| cid={self._short_cid(cid)}"
                    )
                else:
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

    async def _maybe_wait_for_better_entry(self, sig: dict) -> dict | None:
        """Short price-improvement wait while preserving initial conviction snapshot.

        For strong 15m setups only, wait a brief window for a better entry.
        Enter only if thesis quality is preserved (score/prob/edge decay bounded).
        """
        try:
            if not ENTRY_WAIT_ENABLED:
                return sig
            if bool(sig.get("booster_mode", False)) or bool(sig.get("use_limit", False)):
                return sig
            duration = int(sig.get("duration", 0) or 0)
            if duration != 15:
                return sig
            mins_left = float(sig.get("mins_left", 0.0) or 0.0)
            if mins_left * 60.0 <= ENTRY_WAIT_MIN_LEFT_SEC:
                return sig
            score0 = int(sig.get("score", 0) or 0)
            p0 = float(sig.get("true_prob", 0.0) or 0.0)
            edge0 = float(sig.get("edge", 0.0) or 0.0)
            e0 = float(sig.get("entry", 0.0) or 0.0)
            if score0 < ENTRY_WAIT_MIN_SCORE or p0 < ENTRY_WAIT_MIN_TRUE_PROB:
                return sig
            if e0 < ENTRY_WAIT_MIN_ENTRY or e0 > ENTRY_WAIT_MAX_ENTRY:
                return sig

            m = sig.get("m")
            if not isinstance(m, dict):
                return sig

            deadline = _time.time() + max(0.0, ENTRY_WAIT_WINDOW_15M_SEC)
            best = sig
            improved = False
            while _time.time() < deadline:
                await asyncio.sleep(max(0.05, ENTRY_WAIT_POLL_SEC))
                rsig = await self._score_market(m)
                if not rsig:
                    continue
                if rsig.get("cid") != sig.get("cid") or rsig.get("side") != sig.get("side"):
                    continue
                score_t = int(rsig.get("score", 0) or 0)
                p_t = float(rsig.get("true_prob", 0.0) or 0.0)
                edge_t = float(rsig.get("edge", 0.0) or 0.0)
                if (
                    score_t < (score0 - ENTRY_WAIT_MAX_SCORE_DECAY)
                    or p_t < (p0 - ENTRY_WAIT_MAX_PROB_DECAY)
                    or edge_t < (edge0 - ENTRY_WAIT_MAX_EDGE_DECAY)
                ):
                    if self._noisy_log_enabled(f"entry-wait-decay:{sig.get('asset','?')}:{sig.get('side','?')}", LOG_SKIP_EVERY_SEC):
                        print(
                            f"{Y}[ENTRY-WAIT]{RS} {sig.get('asset','?')} {sig.get('side','?')} "
                            f"thesis decayed: score {score_t}/{score0} prob {p_t:.3f}/{p0:.3f} edge {edge_t:.3f}/{edge0:.3f}"
                        )
                    return None
                e_t = float(rsig.get("entry", 1.0) or 1.0)
                tick = float(((rsig.get("pm_book_data") or {}).get("tick", 0.01)) or 0.01)
                min_improve = max(tick * max(1, ENTRY_WAIT_MIN_IMPROVE_TICKS), ENTRY_WAIT_MIN_IMPROVE_ABS)
                if e_t <= (float(best.get("entry", 1.0) or 1.0) - min_improve):
                    best = rsig
                    improved = True
                    if self._noisy_log_enabled(f"entry-wait-improve:{sig.get('asset','?')}:{sig.get('side','?')}", LOG_EDGE_EVERY_SEC):
                        print(
                            f"{B}[ENTRY-WAIT]{RS} {sig.get('asset','?')} {sig.get('side','?')} "
                            f"entry {float(sig.get('entry',0))*100:.1f}c -> {e_t*100:.1f}c "
                            f"(score={score_t} prob={p_t:.3f} edge={edge_t:.3f})"
                        )
                    if e_t <= (e0 - ENTRY_WAIT_TARGET_DROP_ABS):
                        break
            return best if improved else sig
        except Exception as e:
            self._errors.tick("entry_wait", print, err=e, every=20)
            return sig

    async def evaluate(self, m: dict):
        """RTDS fast-path: score a single market and execute if score gate passes."""
        t0 = _time.perf_counter()
        sig = await self._score_market(m)
        self._perf_update("score_ms", (_time.perf_counter() - t0) * 1000.0)
        if sig and sig["score"] >= MIN_SCORE_GATE:
            await self._execute_trade(sig)

    async def _place_order(self, token_id, side, price, size_usdc, asset, duration, mins_left, true_prob=0.5, cl_agree=True, min_edge_req=None, force_taker=False, score=0, pm_book_data=None, use_limit=False, max_entry_allowed=None, hc15_mode=False, hc15_fallback_cap=0.36, core_position=True):
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

        # Execution backstop: never allow micro-notional orders even if upstream scoring/sync
        # accidentally emits a tiny size. This is the final safety net before on-chain send.
        hard_min_notional = max(float(MIN_EXEC_NOTIONAL_USDC), float(MIN_BET_ABS), 1.0)
        if float(size_usdc or 0.0) < hard_min_notional:
            print(
                f"{Y}[SKIP]{RS} {asset} {side} size=${float(size_usdc or 0.0):.2f} "
                f"< hard_min=${hard_min_notional:.2f} (exec backstop)"
            )
            return None

        for attempt in range(max(1, ORDER_RETRY_MAX)):
            try:
                loop = asyncio.get_event_loop()
                slip_cap_bps = MAX_TAKER_SLIP_BPS_5M if duration <= 5 else MAX_TAKER_SLIP_BPS_15M

                def _slip_bps(exec_price: float, ref_price: float) -> float:
                    return ((exec_price - ref_price) / max(ref_price, 1e-9)) * 10000.0

                def _normalize_order_size(exec_price: float, intended_usdc: float) -> tuple[float, float]:
                    min_shares = max(0.01, MIN_ORDER_SIZE_SHARES + ORDER_SIZE_PAD_SHARES)
                    px = max(exec_price, 1e-9)
                    raw_shares = max(0.0, intended_usdc) / px
                    # Execution hard-min is in USDC, not only in shares.
                    min_notional = max(hard_min_notional, min_shares * px)
                    shares_floor = min_notional / px
                    shares = round(max(raw_shares, shares_floor), 2)
                    notional = round(shares * px, 2)
                    return shares, notional

                def _normalize_buy_amount(intended_usdc: float) -> float:
                    # Market BUY maker amount is USDC and must stay at 2 decimals.
                    return float(round(max(0.0, intended_usdc), 2))

                def _book_depth_usdc(levels, price_cap: float) -> float:
                    depth = 0.0
                    for lv in (levels or []):
                        try:
                            if isinstance(lv, (tuple, list)) and len(lv) >= 2:
                                p = float(lv[0])
                                s = float(lv[1])
                            else:
                                p = float(lv.price)
                                s = float(lv.size)
                            if p > price_cap + 1e-9:
                                break
                            depth += p * s
                        except Exception:
                            continue
                    return float(depth)

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
                    try:
                        resp = await loop.run_in_executor(None, lambda: self.clob.post_order(signed, OrderType.FOK))
                    except Exception as e:
                        # FOK semantics: if not fully matched immediately, exchange returns a kill error.
                        # Treat this as unfilled (not a hard order failure).
                        msg = str(e).lower()
                        if (
                            "fully filled or killed" in msg
                            or "couldn't be fully filled" in msg
                            or "could not be fully filled" in msg
                        ):
                            return {"status": "killed", "orderID": "", "id": ""}, float(px_order)
                        raise
                    return resp, float(px_order)

                # Use pre-fetched book from scoring phase (free — ran in parallel with Binance signals)
                # or fetch fresh if not cached (~36ms)
                if pm_book_data is not None:
                    if isinstance(pm_book_data, dict):
                        bts = float(pm_book_data.get("ts", 0.0) or 0.0)
                        bage_ms = ((_time.time() - bts) * 1000.0) if bts > 0 else 9e9
                        if bage_ms > MAX_ORDERBOOK_AGE_MS:
                            pm_book_data = None
                        else:
                            best_bid = float(pm_book_data.get("best_bid", 0.0) or 0.0)
                            best_ask = float(pm_book_data.get("best_ask", 0.0) or 0.0)
                            tick = float(pm_book_data.get("tick", 0.01) or 0.01)
                            asks = list(pm_book_data.get("asks") or [])
                            bids = []
                    else:
                        # Backward compatibility with old tuple format.
                        best_bid, best_ask, tick = pm_book_data
                        asks = []
                        bids = []
                    if pm_book_data is not None and best_ask > 0:
                        spread = best_ask - best_bid
                    else:
                        pm_book_data = None
                if pm_book_data is None:
                    ws_book = self._get_clob_ws_book(token_id, max_age_ms=CLOB_MARKET_WS_MAX_AGE_MS)
                    if ws_book is not None:
                        best_bid = float(ws_book.get("best_bid", 0.0) or 0.0)
                        best_ask = float(ws_book.get("best_ask", 0.0) or 0.0)
                        tick = float(ws_book.get("tick", 0.01) or 0.01)
                        asks = list(ws_book.get("asks") or [])
                        bids = []
                        spread = (best_ask - best_bid) if best_ask > 0 else 0.0
                        pm_book_data = ws_book
                if pm_book_data is None:
                    book     = await self._get_order_book(token_id, force_fresh=False)
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
                strong_exec = (
                    (score >= 12)
                    and cl_agree
                    and (taker_edge >= (edge_floor + 0.01))
                )
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
                    # Execution-first path: when signal is strong and book is tradable, prefer instant fill.
                    if (
                        strong_exec
                        and best_ask <= eff_max_entry
                        and spread <= (fast_spread_cap + 0.01)
                    ):
                        force_taker = True
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
                    # Execution slippage must be measured against current executable ask,
                    # not signal/model entry price from scoring time.
                    slip_now = _slip_bps(taker_price, best_ask)
                    needed_notional = _normalize_buy_amount(size_usdc) * FAST_FOK_MIN_DEPTH_RATIO
                    depth_now = _book_depth_usdc(asks, taker_price)
                    if depth_now < needed_notional:
                        try:
                            ob2 = await self._get_order_book(token_id, force_fresh=True)
                            asks2 = sorted(ob2.asks, key=lambda x: float(x.price)) if ob2.asks else []
                            depth_now = _book_depth_usdc(asks2, taker_price)
                        except Exception:
                            pass
                    if depth_now < needed_notional:
                        print(
                            f"{Y}[FAST-DEPTH]{RS} {asset} {side} insufficient depth "
                            f"${depth_now:.2f} < need ${needed_notional:.2f} @<= {taker_price:.3f}"
                        )
                        force_taker = False
                    elif slip_now > slip_cap_bps:
                        print(
                            f"{Y}[SLIP-GUARD]{RS} {asset} {side} fast-taker blocked: "
                            f"slip={slip_now:.1f}bps(ref_ask={best_ask:.3f}) > cap={slip_cap_bps:.0f}bps"
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
                        slip_now = _slip_bps(taker_price, best_ask)
                        needed_notional = _normalize_buy_amount(size_usdc) * FAST_FOK_MIN_DEPTH_RATIO
                        depth_now = _book_depth_usdc(asks, taker_price)
                        if depth_now < needed_notional:
                            try:
                                ob2 = await self._get_order_book(token_id, force_fresh=True)
                                asks2 = sorted(ob2.asks, key=lambda x: float(x.price)) if ob2.asks else []
                                depth_now = _book_depth_usdc(asks2, taker_price)
                            except Exception:
                                pass
                        if depth_now < needed_notional:
                            print(
                                f"{Y}[FAST-DEPTH]{RS} {asset} {side} near-end insufficient depth "
                                f"${depth_now:.2f} < need ${needed_notional:.2f} @<= {taker_price:.3f}"
                            )
                            force_taker = False
                        elif slip_now > slip_cap_bps:
                            print(
                                f"{Y}[SLIP-GUARD]{RS} {asset} {side} near-end taker blocked: "
                                f"slip={slip_now:.1f}bps(ref_ask={best_ask:.3f}) > cap={slip_cap_bps:.0f}bps"
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
                # If maker pullback target is too far from current book, skip futile maker post.
                # In that case try immediate taker only if still executable and +edge.
                if (not use_limit) and (not force_taker):
                    gap_ticks = 0.0
                    if tick > 0:
                        gap_ticks = max(0.0, (best_bid - maker_price) / tick)
                    max_gap_ticks = MAKER_PULLBACK_MAX_GAP_TICKS_5M if duration <= 5 else MAKER_PULLBACK_MAX_GAP_TICKS_15M
                    if gap_ticks > float(max_gap_ticks):
                        eff_max_entry = max_entry_allowed if max_entry_allowed is not None else 0.99
                        if best_ask <= eff_max_entry and taker_edge >= edge_floor:
                            taker_price = round(min(best_ask, eff_max_entry, 0.97), 4)
                            slip_now = _slip_bps(taker_price, best_ask)
                            needed_notional = _normalize_buy_amount(size_usdc) * FAST_FOK_MIN_DEPTH_RATIO
                            depth_now = _book_depth_usdc(asks, taker_price)
                            if depth_now >= needed_notional and slip_now <= slip_cap_bps:
                                print(
                                    f"{Y}[MAKER-BYPASS]{RS} {asset} {side} pullback too far "
                                    f"(gap={gap_ticks:.1f} ticks>{max_gap_ticks}) -> FOK @ {taker_price:.3f}"
                                )
                                resp, _ = await _post_limit_fok(taker_price)
                                order_id = resp.get("orderID") or resp.get("id", "")
                                if resp.get("status") in ("matched", "filled"):
                                    self.bankroll -= size_usdc
                                    print(f"{G}[FAST-FILL]{RS} {side} {asset} {duration}m | ${size_usdc:.2f} @ {taker_price:.3f} | Bank ${self.bankroll:.2f}")
                                    return {"order_id": order_id, "fill_price": taker_price, "mode": "fok_maker_bypass", "notional_usdc": size_usdc}
                                print(f"{Y}[EXEC-RESULT]{RS} {asset} {side} no-fill reason=maker_bypass_fok_unfilled")
                                return None
                        print(
                            f"{Y}[SKIP]{RS} {asset} {side} maker pullback too far "
                            f"(gap={gap_ticks:.1f} ticks>{max_gap_ticks})"
                        )
                        self._skip_tick("maker_pullback_too_far")
                        return None
                size_tok_m, maker_notional = _normalize_order_size(maker_price_order, size_usdc)
                size_usdc = maker_notional
                if size_usdc < hard_min_notional:
                    print(
                        f"{Y}[SKIP]{RS} {asset} {side} normalized size=${size_usdc:.2f} "
                        f"< hard_min=${hard_min_notional:.2f} (post-normalize)"
                    )
                    return None

                print(f"{G}[MAKER] {asset} {side}: px={maker_price:.3f} "
                      f"book_bid={best_bid:.3f} book_ask={best_ask:.3f} spread={spread:.3f} "
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
                elif strong_exec and not use_limit:
                    # For strong setups, do not spend too long in maker limbo.
                    max_wait = min(max_wait, 0.25 if duration <= 5 else 0.30)
                polls     = max(1, int(max_wait / poll_interval))
                print(f"{G}[MAKER] posted {asset} {side} @ {maker_price:.3f} — "
                      f"waiting up to {polls*poll_interval}s for fill...{RS}")

                filled = False
                for i in range(polls):
                    await asyncio.sleep(poll_interval)
                    ev = self._order_event_cache.get(order_id, {})
                    ev_status = str(ev.get("status", "") or "").lower()
                    if ev_status == "filled":
                        filled = True
                        break
                    if ev_status == "canceled":
                        break
                    try:
                        # Poll fallback only every other tick when user-events cache is active.
                        if i % 2 == 0:
                            info = await loop.run_in_executor(None, lambda: self.clob.get_order(order_id))
                        else:
                            info = None
                        if isinstance(info, dict) and info.get("status") in ("matched", "filled"):
                            filled = True
                            self._cache_order_event(
                                order_id,
                                "filled",
                                float(info.get("filled_size") or info.get("filledSize") or 0.0),
                            )
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
                    ev = self._order_event_cache.get(order_id, {})
                    ev_fill = float(ev.get("filled_size", 0.0) or 0.0)
                    info = None
                    if ev_fill <= 0:
                        info = await loop.run_in_executor(None, lambda: self.clob.get_order(order_id))
                    if isinstance(info, dict):
                        filled_sz = float(info.get("filled_size") or info.get("filledSize") or 0.0)
                    else:
                        filled_sz = ev_fill
                    if filled_sz > 0:
                        self._cache_order_event(order_id, "live", filled_sz)
                        fill_usdc = filled_sz * maker_price
                        partial_track_floor = max(float(DUST_RECOVER_MIN), float(MIN_PARTIAL_TRACK_USDC))
                        if fill_usdc >= partial_track_floor:
                            self.bankroll -= min(size_usdc, fill_usdc)
                            print(f"{Y}[PARTIAL]{RS} {side} {asset} {duration}m | "
                                  f"filled≈${fill_usdc:.2f} @ {maker_price:.3f} | tracking open position")
                            return {"order_id": order_id, "fill_price": maker_price, "mode": "maker_partial", "notional_usdc": min(size_usdc, fill_usdc)}
                        print(
                            f"{Y}[PARTIAL-DUST]{RS} {side} {asset} {duration}m | "
                            f"filled≈${fill_usdc:.2f} @ {maker_price:.3f} < track_min=${partial_track_floor:.2f} "
                            f"| ignore partial tracking"
                        )
                except Exception:
                    self._errors.tick("order_partial_check", print, every=50)

                # Cancel maker, fall back to taker with fresh book
                try:
                    await loop.run_in_executor(None, lambda: self.clob.cancel(order_id))
                except Exception:
                    pass

                # ── PHASE 2: price-capped FOK fallback — re-fetch book for fresh ask ──
                try:
                    fresh     = await self._get_order_book(token_id, force_fresh=True)
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
                        self._skip_tick("fallback_spread")
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
                            self._skip_tick("pullback_missed")
                            return None
                    fresh_payout = 1.0 / max(fresh_ask, 1e-9)
                    min_payout_fb, min_ev_fb, _ = self._adaptive_thresholds(duration)
                    if core_position and duration >= 15 and CONSISTENCY_CORE_ENABLED:
                        # Keep execution fully aligned with core 15m consistency floor.
                        min_payout_fb = max(min_payout_fb, CONSISTENCY_MIN_PAYOUT_15M)
                    elif strong_exec:
                        # Non-core fallback can relax slightly to preserve executable flow.
                        min_payout_fb = max(1.65, min_payout_fb - 0.08)
                    if fresh_payout < min_payout_fb:
                        if fresh_payout >= max(1.0, (min_payout_fb - PAYOUT_NEAR_MISS_TOL)):
                            if self._noisy_log_enabled(f"payout-near-miss-fb:{asset}:{side}", LOG_SKIP_EVERY_SEC):
                                print(
                                    f"{Y}[PAYOUT-TOL]{RS} {asset} {side} fallback payout={fresh_payout:.2f}x "
                                    f"near min={min_payout_fb:.2f}x (tol={PAYOUT_NEAR_MISS_TOL:.2f}x)"
                                )
                        else:
                            print(f"{Y}[SKIP] {asset} {side} fallback payout={fresh_payout:.2f}x < min={min_payout_fb:.2f}x{RS}")
                            self._skip_tick("fallback_payout_below")
                            print(f"{Y}[EXEC-RESULT]{RS} {asset} {side} no-fill reason=fallback_payout_below")
                            return None
                    fresh_ep  = true_prob - fresh_ask
                    if fresh_ep < edge_floor:
                        print(f"{Y}[SKIP] {asset} {side} taker: fresh ask={fresh_ask:.3f} edge={fresh_ep:.3f} < {edge_floor:.2f} — price moved against us{RS}")
                        self._skip_tick("fallback_edge_below")
                        return None
                    fresh_ev_net = (true_prob / max(fresh_ask, 1e-9)) - 1.0 - FEE_RATE_EST
                    if fresh_ev_net < min_ev_fb:
                        print(f"{Y}[SKIP] {asset} {side} fallback ev_net={fresh_ev_net:.3f} < min={min_ev_fb:.3f}{RS}")
                        self._skip_tick("fallback_ev_below")
                        return None
                    taker_price = round(min(fresh_ask, eff_max_entry, 0.97), 4)
                    slip_now = _slip_bps(taker_price, fresh_ask)
                    if slip_now > slip_cap_bps:
                        print(
                            f"{Y}[SLIP-GUARD]{RS} {asset} {side} fallback taker blocked: "
                            f"slip={slip_now:.1f}bps(ref_ask={fresh_ask:.3f}) > cap={slip_cap_bps:.0f}bps"
                        )
                        self._skip_tick("fallback_slip_guard")
                        print(f"{Y}[EXEC-RESULT]{RS} {asset} {side} no-fill reason=fallback_slip_guard")
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
                    self._cache_order_event(order_id, "filled", 0.0)
                    print(f"{Y}[TAKER FILL]{RS} {side} {asset} {duration}m | "
                          f"${size_usdc:.2f} @ {taker_price:.3f} | Bank ${self.bankroll:.2f}")
                    return {"order_id": order_id, "fill_price": taker_price, "mode": "fok_fallback", "notional_usdc": size_usdc}

                # FOK should not partially fill, but keep single state-check for exchange race.
                if order_id:
                    try:
                        ev = self._order_event_cache.get(order_id, {})
                        ev_status = str(ev.get("status", "") or "").lower()
                        if ev_status == "filled":
                            info = {"status": "filled"}
                        else:
                            info = await loop.run_in_executor(None, lambda: self.clob.get_order(order_id))
                        if isinstance(info, dict) and info.get("status") in ("matched", "filled"):
                            self.bankroll -= size_usdc
                            self._cache_order_event(order_id, "filled", 0.0)
                            print(f"{Y}[TAKER FILL]{RS} {side} {asset} {duration}m | "
                                  f"${size_usdc:.2f} @ {taker_price:.3f} | Bank ${self.bankroll:.2f}")
                            return {"order_id": order_id, "fill_price": taker_price, "mode": "fok_fallback", "notional_usdc": size_usdc}
                    except Exception:
                        self._errors.tick("order_status_check", print, every=50)
                print(f"{Y}[ORDER] Both maker and FOK taker unfilled — cancelled{RS}")
                print(f"{Y}[EXEC-RESULT]{RS} {asset} {side} no-fill reason=maker_and_fok_unfilled")
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
            poll_sleep = REDEEM_POLL_SEC_ACTIVE if self.pending_redeem else REDEEM_POLL_SEC
            await asyncio.sleep(max(0.05, poll_sleep))
            if not self.pending_redeem:
                continue
            positions_by_cid = {}
            try:
                pos_rows = await self._http_get_json(
                    "https://data-api.polymarket.com/positions",
                    params={"user": ADDRESS, "sizeThreshold": "0.01", "redeemable": "true"},
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
                            size_local = float(trade.get("size", 0) or 0.0)
                            size_onchain = float(self.onchain_open_stake_by_cid.get(cid, 0.0) or 0.0)
                            size = size_onchain if size_onchain > 0 else size_local
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
                    size_f = float(size or 0.0)
                    # Idempotency guard: avoid re-reconciling the same settled CID
                    # across overlapping loops/restarts/API lag windows.
                    prev_settle = self._settled_outcomes.get(cid, {})
                    prev_res = str((prev_settle or {}).get("result", "") or "").upper()
                    if prev_res in ("WIN", "LOSS"):
                        prev_rk = str((prev_settle or {}).get("rk", "") or "")
                        if self._noisy_log_enabled(f"settle-dupe:{cid}", LOG_REDEEM_WAIT_EVERY_SEC):
                            print(
                                f"{Y}[SETTLE-DUPE]{RS} skip {asset} {side} "
                                f"result={prev_res} | rk={prev_rk or rk} cid={self._short_cid(cid)}"
                            )
                        done.append(cid)
                        continue

                    # For CLOB positions: CTF tokens are held by the exchange contract,
                    # not the user wallet. Always try redeemPositions — if Polymarket
                    # already auto-redeemed, the tx reverts harmlessly; if not, we collect.
                    if won and size_f > 0:
                        fee    = size_f * 0.0156 * (1 - abs(trade.get("mkt_price", 0.5) - 0.5) * 2)
                        payout = size_f / max(float(entry or 0.5), 1e-9) - fee
                        pnl    = payout - size_f

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
                        stake_out = max(size_f, float(self.onchain_open_stake_by_cid.get(cid, 0.0) or 0.0))
                        redeem_in = 0.0
                        if tx_hash_full:
                            redeem_in = self._extract_usdc_in_from_tx(tx_hash_full)
                        if redeem_in <= 0:
                            usdc_delta = usdc_after - usdc_before
                            redeem_in = usdc_delta if usdc_delta > 0 else float(payout)
                        pnl = redeem_in - stake_out
                        order_id_u = str(trade.get("order_id", "") or "").upper()
                        reconcile_only = (
                            bool(trade.get("historical_sync"))
                            or order_id_u.startswith("SYNC")
                            or order_id_u.startswith("RECOVER")
                            or order_id_u.startswith("ONCHAIN-REDEEM-QUEUE")
                        )
                        if int(trade.get("booster_count", 0) or 0) > 0:
                            self._booster_consec_losses = 0
                            self._booster_lock_until = 0.0
                        if not reconcile_only:
                            self.daily_pnl += pnl
                            self.total += 1; self.wins += 1
                            self._bucket_stats.add_outcome(trade.get("bucket", "unknown"), True, pnl)
                            self._record_result(asset, side, True, trade.get("structural", False), pnl=pnl)
                            self._record_resolved_sample(trade, pnl, True)
                            self._record_pm_pattern_outcome(trade, pnl, True)
                        self._log(m, trade, "WIN", pnl)
                        self._log_onchain_event("RESOLVE", cid, {
                            "asset": asset,
                            "side": side,
                            "result": "WIN",
                            "winner_side": winner,
                            "winner_source": winner_source,
                            "size_usdc": size_f,
                            "entry_price": entry,
                            "stake_out_usdc": round(stake_out, 6),
                            "redeem_in_usdc": round(redeem_in, 6),
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
                        rk_n = self._count_pending_redeem_by_rk(rk)
                        usdc_delta = usdc_after - usdc_before
                        tag = "[WIN-RECONCILE]" if reconcile_only else "[WIN]"
                        print(f"{G}{tag}{RS} {asset} {side} {trade.get('duration',0)}m | "
                              f"{G}${pnl:+.2f}{RS} | stake=${stake_out:.2f} redeem=${redeem_in:.2f} "
                              f"| rk_trades={rk_n} | Bank ${self.bankroll:.2f} | WR {wr} | "
                              f"{suffix} | rk={rk} cid={self._short_cid(cid)}")
                        if not reconcile_only:
                            print(
                                f"{B}[OUTCOME-STATS]{RS} {asset} {side} {trade.get('duration',0)}m "
                                f"ev={float(trade.get('execution_ev',0.0) or 0.0):+.3f} "
                                f"payout={1.0/max(float(trade.get('entry',0.5) or 0.5),1e-9):.2f}x "
                                f"pm={int(trade.get('pm_pattern_score_adj',0) or 0):+d}/{float(trade.get('pm_pattern_edge_adj',0.0) or 0.0):+.3f} "
                                f"pub={int(trade.get('pm_public_pattern_score_adj',0) or 0):+d}/{float(trade.get('pm_public_pattern_edge_adj',0.0) or 0.0):+.3f} "
                                f"rec={int(trade.get('recent_side_score_adj',0) or 0):+d}/{float(trade.get('recent_side_edge_adj',0.0) or 0.0):+.3f} "
                                f"roll_n={int(trade.get('rolling_n',0) or 0)} roll_exp={float(trade.get('rolling_exp',0.0) or 0.0):+.2f}"
                            )
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
                        self._settled_outcomes[cid] = {
                            "result": "WIN",
                            "side": side,
                            "rk": rk,
                            "pnl": round(float(pnl), 6),
                            "ts": _time.time(),
                        }
                        self._save_settled_outcomes()
                        done.append(cid)
                    else:
                        # Lost on-chain (on-chain is authoritative)
                        if size_f > 0:
                            stake_loss = max(size_f, float(self.onchain_open_stake_by_cid.get(cid, 0.0) or 0.0))
                            pnl = -stake_loss
                            order_id_u = str(trade.get("order_id", "") or "").upper()
                            reconcile_only = (
                                bool(trade.get("historical_sync"))
                                or order_id_u.startswith("SYNC")
                                or order_id_u.startswith("RECOVER")
                                or order_id_u.startswith("ONCHAIN-REDEEM-QUEUE")
                            )
                            if int(trade.get("booster_count", 0) or 0) > 0:
                                self._booster_consec_losses = int(self._booster_consec_losses or 0) + 1
                                lock_n = max(1, MID_BOOSTER_LOSS_STREAK_LOCK)
                                if self._booster_consec_losses >= lock_n:
                                    self._booster_lock_until = _time.time() + max(1.0, MID_BOOSTER_LOCK_HOURS) * 3600.0
                                    rem_h = max(0.0, (self._booster_lock_until - _time.time()) / 3600.0)
                                    print(
                                        f"{Y}[BOOST-LOCK]{RS} disabled for {rem_h:.1f}h "
                                        f"after {self._booster_consec_losses} booster losses"
                                    )
                            if not reconcile_only:
                                self.daily_pnl += pnl
                                self.total += 1
                                self._bucket_stats.add_outcome(trade.get("bucket", "unknown"), False, pnl)
                                self._record_result(asset, side, False, trade.get("structural", False), pnl=pnl)
                                self._record_resolved_sample(trade, pnl, False)
                                self._record_pm_pattern_outcome(trade, pnl, False)
                            self._log(m, trade, "LOSS", pnl)
                            self._log_onchain_event("RESOLVE", cid, {
                                "asset": asset,
                                "side": side,
                                "result": "LOSS",
                                "winner_side": winner,
                                "winner_source": winner_source,
                                "size_usdc": stake_loss,
                                "entry_price": entry,
                                "stake_out_usdc": round(stake_loss, 6),
                                "redeem_in_usdc": 0.0,
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
                            rk_n = self._count_pending_redeem_by_rk(rk)
                            tag = "[LOSS-RECONCILE]" if reconcile_only else "[LOSS]"
                            print(f"{R}{tag}{RS} {asset} {side} {trade.get('duration',0)}m | "
                                  f"{R}${pnl:+.2f}{RS} | stake=${stake_loss:.2f} redeem=$0.00 "
                                  f"| rk_trades={rk_n} | Bank ${self.bankroll:.2f} | WR {wr} | "
                                  f"rk={rk} cid={self._short_cid(cid)}")
                            if not reconcile_only:
                                print(
                                    f"{B}[OUTCOME-STATS]{RS} {asset} {side} {trade.get('duration',0)}m "
                                    f"ev={float(trade.get('execution_ev',0.0) or 0.0):+.3f} "
                                    f"payout={1.0/max(float(trade.get('entry',0.5) or 0.5),1e-9):.2f}x "
                                    f"pm={int(trade.get('pm_pattern_score_adj',0) or 0):+d}/{float(trade.get('pm_pattern_edge_adj',0.0) or 0.0):+.3f} "
                                    f"pub={int(trade.get('pm_public_pattern_score_adj',0) or 0):+d}/{float(trade.get('pm_public_pattern_edge_adj',0.0) or 0.0):+.3f} "
                                    f"rec={int(trade.get('recent_side_score_adj',0) or 0):+d}/{float(trade.get('recent_side_edge_adj',0.0) or 0.0):+.3f} "
                                    f"roll_n={int(trade.get('rolling_n',0) or 0)} roll_exp={float(trade.get('rolling_exp',0.0) or 0.0):+.2f}"
                                )
                            self._settled_outcomes[cid] = {
                                "result": "LOSS",
                                "side": side,
                                "rk": rk,
                                "pnl": round(float(pnl), 6),
                                "ts": _time.time(),
                            }
                            self._save_settled_outcomes()
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
                self.onchain_open_usdc_by_cid.pop(cid, None)
                self.onchain_open_stake_by_cid.pop(cid, None)
                self.onchain_open_shares_by_cid.pop(cid, None)
                self.onchain_open_meta_by_cid.pop(cid, None)
                self._booster_used_by_cid.pop(cid, None)
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
                params={"user": ADDRESS, "sizeThreshold": "0.01", "redeemable": "false"},
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
                    # Record loss only for non-historical rows; skip replaying old history as live loss.
                    if not self._is_historical_expired_position(pos_data, now, grace_sec=1800.0):
                        size_p = t_p.get("size", 0)
                        if size_p > 0:
                            self.total += 1
                            self._record_result(asset_p, side_p, False, t_p.get("structural", False), pnl=-size_p)
                self._save_pending()

        for pos in positions:
            cid        = pos.get("conditionId", "")
            redeemable = pos.get("redeemable", False)
            outcome    = self._normalize_side_label(pos.get("outcome", ""))   # canonical Up/Down
            val        = float(pos.get("currentValue", 0))
            size_tok   = float(pos.get("size", 0))
            title      = pos.get("title", "")

            # Skip resolved/redeemable or incomplete
            if redeemable or not outcome or not cid:
                continue

            self.seen.add(cid)

            # Keep all materially present on-chain positions for sync/reconcile.
            size_tok = self._as_float(pos.get("size", 0.0), 0.0)
            spent_probe = 0.0
            for k_st in ("initialValue", "costBasis", "totalBought", "amountSpent", "spent"):
                vv = self._as_float(pos.get(k_st, 0.0), 0.0)
                if vv > 0:
                    spent_probe = vv
                    break
            if not ((size_tok > 0) or (spent_probe >= OPEN_PRESENCE_MIN) or (val >= OPEN_PRESENCE_MIN)):
                continue
            if self._is_historical_expired_position(pos, now, grace_sec=1800.0) and val < OPEN_PRESENCE_MIN:
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
                _, token_up, token_down = self._map_updown_market_fields(mkt)
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
            avg_px = self._as_float(pos.get("avgPrice", 0.0), 0.0)
            rec_spent = 0.0
            for k_st in ("initialValue", "costBasis", "totalBought", "amountSpent", "spent"):
                vv = self._as_float(pos.get(k_st, 0.0), 0.0)
                if vv > 0:
                    rec_spent = vv
                    break
            if rec_spent <= 0 and size_tok > 0 and avg_px > 0:
                rec_spent = size_tok * avg_px
            if rec_spent <= 0:
                rec_spent = val
            entry = avg_px if avg_px > 0 else ((rec_spent / size_tok) if size_tok > 0 else 0.5)
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
                prev_side = str(old_t.get("side", "") or "")
                old_m.update({"end_ts": end_ts, "start_ts": start_ts, "duration": duration,
                               "asset": asset, "token_up": token_up, "token_down": token_down})
                old_t.update({"end_ts": end_ts, "asset": asset, "duration": duration,
                               "side": outcome,
                               "token_id": token_up if outcome == "Up" else token_down})
                if prev_side and prev_side != outcome:
                    print(f"{Y}[SYNC-SIDE]{RS} {title[:40]} side corrected {prev_side}->{outcome}")
                # Keep local core parameters stable once locked (no local average overwrite).
                if not bool(old_t.get("core_entry_locked", False)):
                    if rec_spent > 0:
                        old_t["size"] = rec_spent
                    if entry > 0:
                        old_t["entry"] = entry
                old_t["onchain_stake_usdc"] = round(rec_spent, 6)
                old_t["onchain_entry"] = round(entry, 6)
                print(f"{Y}[SYNC] Updated: {title[:40]} {outcome} | spent=${old_t.get('size',0):.2f} mark=${val:.2f} | {duration}m ends in {mins_left:.1f}min{RS}")
            else:
                trade = {"side": outcome, "size": rec_spent, "entry": entry,
                         "open_price": 0, "current_price": 0, "true_prob": 0.5,
                         "mkt_price": entry, "edge": 0, "mins_left": mins_left,
                         "end_ts": end_ts, "asset": asset, "duration": duration,
                         "token_id": token_up if outcome == "Up" else token_down,
                         "order_id": "SYNCED",
                         "core_position": True,
                         "core_entry_locked": True,
                         "core_entry": entry,
                         "core_size_usdc": rec_spent,
                         "addon_count": 0,
                         "addon_stake_usdc": 0.0}
                if mins_left <= 0:
                    self.pending_redeem[cid] = (m, trade)
                    print(f"{Y}[SYNC] Restored->settling: {title[:40]} {outcome} | spent=${rec_spent:.2f} mark=${val:.2f} | {duration}m ended {abs(mins_left):.1f}min ago{RS}")
                else:
                    self.pending[cid] = (m, trade)
                    synced += 1
                    print(f"{Y}[SYNC] Restored: {title[:40]} {outcome} | spent=${rec_spent:.2f} mark=${val:.2f} | {duration}m ends in {mins_left:.1f}min{RS}")

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
                params={"user": ADDRESS, "sizeThreshold": "0.01", "redeemable": "true"},
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
            outcome    = self._normalize_side_label(pos.get("outcome", ""))
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

    @staticmethod
    def _entry_band(entry: float) -> str:
        e = float(entry or 0.0)
        if e <= 0:
            return "missing"
        if e < 0.45:
            return "<45c"
        if e < 0.55:
            return "45-55c"
        if e < 0.65:
            return "55-65c"
        return "65c+"

    def _record_resolved_sample(self, trade: dict, pnl: float, won: bool):
        try:
            self._resolved_samples.append({
                "ts": _time.time(),
                "asset": str(trade.get("asset", "") or ""),
                "side": str(trade.get("side", "") or ""),
                "duration": int(trade.get("duration", 0) or 0),
                "entry": float(trade.get("entry", 0.0) or 0.0),
                "source": str(trade.get("open_price_source", "?") or "?"),
                "cl_age_s": trade.get("chainlink_age_s", None),
                "pnl": float(pnl),
                "won": bool(won),
            })
        except Exception:
            pass

    def _pm_pattern_key(self, asset: str, duration: int, side: str, copy_net: float, flow: dict) -> str:
        low_share = float((flow or {}).get("low_c_share", 0.0) or 0.0)
        high_share = float((flow or {}).get("high_c_share", 0.0) or 0.0)
        multibet = float((flow or {}).get("multibet_ratio", 0.0) or 0.0)
        if abs(copy_net) >= 0.35:
            dom = "dom-strong"
        elif abs(copy_net) >= 0.15:
            dom = "dom-med"
        else:
            dom = "dom-weak"
        if low_share >= 0.55:
            px_style = "lowcent"
        elif high_share >= 0.55:
            px_style = "highcent"
        else:
            px_style = "midcent"
        mb = "multibet-high" if multibet >= 0.30 else "multibet-low"
        return f"{asset}|{duration}m|{side}|{dom}|{px_style}|{mb}"

    def _pm_pattern_profile(self, pattern_key: str) -> dict:
        row = self._pm_pattern_stats.get(pattern_key, {})
        n = int(row.get("n", 0) or 0)
        if n <= 0:
            return {"score_adj": 0, "edge_adj": 0.0, "n": 0, "exp": 0.0, "wr_lb": 0.5, "min_n": PM_PATTERN_MIN_N}
        wins = int(row.get("wins", 0) or 0)
        pnl = float(row.get("pnl", 0.0) or 0.0)
        exp = pnl / max(1, n)
        wr_lb = self._wilson_lower_bound(wins, n)
        score_adj = 0
        edge_adj = 0.0
        if n >= PM_PATTERN_MIN_N:
            if exp >= 0.25 and wr_lb >= 0.52:
                score_adj = 2
                edge_adj = min(PM_PATTERN_MAX_EDGE_ADJ, 0.004 + min(0.006, exp / 80.0))
            elif exp >= 0.08 and wr_lb >= 0.50:
                score_adj = 1
                edge_adj = min(PM_PATTERN_MAX_EDGE_ADJ, 0.002 + min(0.004, exp / 120.0))
            elif exp <= -0.20 and wr_lb < 0.45:
                score_adj = -2
                edge_adj = -min(PM_PATTERN_MAX_EDGE_ADJ, 0.004 + min(0.006, abs(exp) / 80.0))
            elif exp < 0.0 and wr_lb < 0.48:
                score_adj = -1
                edge_adj = -min(PM_PATTERN_MAX_EDGE_ADJ, 0.002 + min(0.004, abs(exp) / 120.0))
        return {
            "score_adj": score_adj,
            "edge_adj": edge_adj,
            "n": n,
            "exp": exp,
            "wr_lb": wr_lb,
            "min_n": PM_PATTERN_MIN_N,
        }

    def _record_pm_pattern_outcome(self, trade: dict, pnl: float, won: bool):
        try:
            key = str(trade.get("pm_pattern_key", "") or "")
            if not key:
                return
            row = self._pm_pattern_stats.get(key, {"n": 0, "wins": 0, "pnl": 0.0})
            row["n"] = int(row.get("n", 0) or 0) + 1
            row["wins"] = int(row.get("wins", 0) or 0) + (1 if won else 0)
            row["pnl"] = float(row.get("pnl", 0.0) or 0.0) + float(pnl)
            self._pm_pattern_stats[key] = row
        except Exception:
            pass

    def _pm_public_pattern_profile(self, side: str, flow: dict, entry_px: float) -> dict:
        """Pattern prior from public PM activity (leader/live flow), independent from own outcomes."""
        try:
            f = flow or {}
            n = int(f.get("n", 0) or 0)
            up = float(f.get("Up", f.get("up", 0.0)) or 0.0)
            dn = float(f.get("Down", f.get("down", 0.0)) or 0.0)
            avg_c = float(f.get("avg_entry_c", 0.0) or 0.0)
            low_share = float(f.get("low_c_share", 0.0) or 0.0)
            high_share = float(f.get("high_c_share", 0.0) or 0.0)
            multibet = float(f.get("multibet_ratio", 0.0) or 0.0)
            pref = up if side == "Up" else dn
            opp = dn if side == "Up" else up
            dom = pref - opp
            if n <= 0:
                return {"score_adj": 0, "edge_adj": 0.0, "n": 0, "dom": 0.0, "avg_c": 0.0, "min_n": PM_PUBLIC_PATTERN_MIN_N}
            if n < PM_PUBLIC_PATTERN_MIN_N:
                return {
                    "score_adj": 0,
                    "edge_adj": 0.0,
                    "n": n,
                    "dom": dom,
                    "avg_c": avg_c,
                    "min_n": PM_PUBLIC_PATTERN_MIN_N,
                }
            score_adj = 0
            edge_adj = 0.0
            # Crowd-extreme filter: when public flow is concentrated at very high cents,
            # treat matching-side dominance as overpay risk (anti-EV), not a buy signal.
            overcrowded_highcent = (
                (avg_c >= 90.0 and abs(dom) >= PM_PUBLIC_PATTERN_DOM_MED)
                or (avg_c >= 85.0 and high_share >= 0.55 and abs(dom) >= PM_PUBLIC_PATTERN_DOM_MED)
            )
            if overcrowded_highcent:
                if dom >= PM_PUBLIC_PATTERN_DOM_MED:
                    score_adj -= 1
                    edge_adj -= min(PM_PUBLIC_PATTERN_MAX_EDGE_ADJ, 0.003 + (dom - PM_PUBLIC_PATTERN_DOM_MED) * 0.010)
                elif dom <= -PM_PUBLIC_PATTERN_DOM_MED:
                    score_adj += 1
                    edge_adj += min(PM_PUBLIC_PATTERN_MAX_EDGE_ADJ, 0.002 + (abs(dom) - PM_PUBLIC_PATTERN_DOM_MED) * 0.008)
            else:
                # Dominance prior from public side-bias (weighted by ranked wallets/live flow),
                # only outside overheated high-cent regimes.
                if dom >= PM_PUBLIC_PATTERN_DOM_STRONG:
                    score_adj += 1
                    edge_adj += min(PM_PUBLIC_PATTERN_MAX_EDGE_ADJ, 0.003 + (dom - PM_PUBLIC_PATTERN_DOM_STRONG) * 0.015)
                elif dom <= -PM_PUBLIC_PATTERN_DOM_STRONG:
                    score_adj -= 1
                    edge_adj -= min(PM_PUBLIC_PATTERN_MAX_EDGE_ADJ, 0.003 + (abs(dom) - PM_PUBLIC_PATTERN_DOM_STRONG) * 0.015)
                elif dom >= PM_PUBLIC_PATTERN_DOM_MED:
                    edge_adj += min(PM_PUBLIC_PATTERN_MAX_EDGE_ADJ, 0.0015 + (dom - PM_PUBLIC_PATTERN_DOM_MED) * 0.010)
                elif dom <= -PM_PUBLIC_PATTERN_DOM_MED:
                    edge_adj -= min(PM_PUBLIC_PATTERN_MAX_EDGE_ADJ, 0.0015 + (abs(dom) - PM_PUBLIC_PATTERN_DOM_MED) * 0.010)
            # Entry-style prior: prefer side aligned with public low-cent behavior at better pricing.
            if avg_c > 0 and avg_c <= 65.0 and low_share >= 0.55 and 0.30 <= entry_px <= 0.55:
                score_adj += 1
                edge_adj += 0.0015
            elif avg_c > 0 and avg_c >= 75.0 and high_share >= 0.60 and entry_px >= 0.62:
                score_adj -= 1
                edge_adj -= 0.0025
            # Repeated same-wallet stacking is useful, but keep impact small.
            if multibet >= 0.35 and 25.0 <= avg_c <= 70.0:
                edge_adj += 0.001
            score_adj = max(-2, min(2, score_adj))
            edge_adj = max(-PM_PUBLIC_PATTERN_MAX_EDGE_ADJ, min(PM_PUBLIC_PATTERN_MAX_EDGE_ADJ, edge_adj))
            return {
                "score_adj": score_adj,
                "edge_adj": edge_adj,
                "n": n,
                "dom": dom,
                "avg_c": avg_c,
                "min_n": PM_PUBLIC_PATTERN_MIN_N,
            }
        except Exception:
            return {"score_adj": 0, "edge_adj": 0.0, "n": 0, "dom": 0.0, "avg_c": 0.0, "min_n": PM_PUBLIC_PATTERN_MIN_N}

    def _recent_side_profile(self, asset: str, duration: int, side: str) -> dict:
        """Recent on-chain-only side quality profile for adaptive scoring (non-blocking)."""
        if not RECENT_SIDE_PRIOR_ENABLED:
            return {"score_adj": 0, "edge_adj": 0.0, "prob_adj": 0.0, "n": 0, "exp": 0.0, "wr_lb": 0.5}
        try:
            now_ts = _time.time()
            lookback_s = max(0.0, float(RECENT_SIDE_PRIOR_LOOKBACK_H) * 3600.0)
            vals = []
            for s in list(self._resolved_samples)[-600:]:
                if str(s.get("asset", "") or "") != asset:
                    continue
                if int(s.get("duration", 0) or 0) != int(duration):
                    continue
                if str(s.get("side", "") or "") != side:
                    continue
                ts = float(s.get("ts", 0.0) or 0.0)
                if ts <= 0:
                    continue
                if lookback_s > 0 and (now_ts - ts) > lookback_s:
                    continue
                vals.append(s)
            n = len(vals)
            if n < max(1, RECENT_SIDE_PRIOR_MIN_N):
                return {"score_adj": 0, "edge_adj": 0.0, "prob_adj": 0.0, "n": n, "exp": 0.0, "wr_lb": 0.5}
            wins = sum(1 for x in vals if bool(x.get("won", False)))
            pnl = sum(float(x.get("pnl", 0.0) or 0.0) for x in vals)
            exp = pnl / max(1, n)
            wr_lb = self._wilson_lower_bound(wins, n)
            score_adj = 0
            edge_adj = 0.0
            prob_adj = 0.0
            if wr_lb >= 0.55 and exp >= 0.20:
                score_adj = min(RECENT_SIDE_PRIOR_MAX_SCORE_ADJ, 2)
                edge_adj = min(RECENT_SIDE_PRIOR_MAX_EDGE_ADJ, 0.004 + min(0.006, exp / 80.0))
                prob_adj = min(RECENT_SIDE_PRIOR_MAX_PROB_ADJ, 0.010 + min(0.015, exp / 40.0))
            elif wr_lb >= 0.51 and exp > 0.0:
                score_adj = min(RECENT_SIDE_PRIOR_MAX_SCORE_ADJ, 1)
                edge_adj = min(RECENT_SIDE_PRIOR_MAX_EDGE_ADJ, 0.003)
                prob_adj = min(RECENT_SIDE_PRIOR_MAX_PROB_ADJ, 0.008)
            elif wr_lb <= 0.45 and exp <= -0.20:
                score_adj = -min(RECENT_SIDE_PRIOR_MAX_SCORE_ADJ, 2)
                edge_adj = -min(RECENT_SIDE_PRIOR_MAX_EDGE_ADJ, 0.004 + min(0.006, abs(exp) / 80.0))
                prob_adj = -min(RECENT_SIDE_PRIOR_MAX_PROB_ADJ, 0.010 + min(0.015, abs(exp) / 40.0))
            elif wr_lb < 0.49 and exp < 0.0:
                score_adj = -min(RECENT_SIDE_PRIOR_MAX_SCORE_ADJ, 1)
                edge_adj = -min(RECENT_SIDE_PRIOR_MAX_EDGE_ADJ, 0.003)
                prob_adj = -min(RECENT_SIDE_PRIOR_MAX_PROB_ADJ, 0.008)
            return {
                "score_adj": int(score_adj),
                "edge_adj": float(edge_adj),
                "prob_adj": float(prob_adj),
                "n": int(n),
                "exp": float(exp),
                "wr_lb": float(wr_lb),
            }
        except Exception:
            return {"score_adj": 0, "edge_adj": 0.0, "prob_adj": 0.0, "n": 0, "exp": 0.0, "wr_lb": 0.5}

    def _rolling_15m_profile(self, duration: int, entry: float, open_src: str, cl_age_s) -> dict:
        if (not ROLLING_15M_CALIB_ENABLED) or duration < 15:
            return {"prob_add": 0.0, "ev_add": 0.0, "size_mult": 1.0, "n": 0, "exp": 0.0, "wr_lb": 0.5}
        band = self._entry_band(entry)
        src_ok = (open_src == "PM") and (cl_age_s is not None) and (float(cl_age_s) <= 45.0)
        vals = []
        for s in list(self._resolved_samples)[-max(50, ROLLING_15M_CALIB_WINDOW):]:
            if int(s.get("duration", 0) or 0) < 15:
                continue
            if self._entry_band(float(s.get("entry", 0.0) or 0.0)) != band:
                continue
            s_src = str(s.get("source", "?") or "?")
            s_age = s.get("cl_age_s", None)
            s_ok = (s_src == "PM") and (s_age is not None) and (float(s_age) <= 45.0)
            if s_ok != src_ok:
                continue
            vals.append(s)
        n = len(vals)
        if n < ROLLING_15M_CALIB_MIN_N:
            return {"prob_add": 0.0, "ev_add": 0.0, "size_mult": 1.0, "n": n, "exp": 0.0, "wr_lb": 0.5}
        wins = sum(1 for x in vals if bool(x.get("won", False)))
        pnl = sum(float(x.get("pnl", 0.0) or 0.0) for x in vals)
        exp = pnl / max(1, n)
        gw = sum(float(x.get("pnl", 0.0) or 0.0) for x in vals if float(x.get("pnl", 0.0) or 0.0) > 0)
        gl = -sum(float(x.get("pnl", 0.0) or 0.0) for x in vals if float(x.get("pnl", 0.0) or 0.0) < 0)
        pf = (gw / gl) if gl > 0 else (2.0 if gw > 0 else 1.0)
        wr_lb = self._wilson_lower_bound(wins, n)
        prob_add = 0.0
        ev_add = 0.0
        size_mult = 1.0
        if exp <= -0.30 or wr_lb < 0.45 or pf < 0.90:
            prob_add += 0.025
            ev_add += 0.006
            size_mult *= 0.75
        elif exp >= 0.20 and wr_lb >= 0.52 and pf >= 1.10:
            prob_add -= 0.008
            ev_add -= 0.001
            size_mult *= 1.12
        if (not src_ok) and (exp < 0):
            prob_add += 0.020
            ev_add += 0.004
            size_mult *= 0.85
        return {
            "prob_add": max(-0.02, min(0.05, prob_add)),
            "ev_add": max(-0.004, min(0.012, ev_add)),
            "size_mult": max(0.65, min(1.25, size_mult)),
            "n": n,
            "exp": exp,
            "wr_lb": wr_lb,
        }

    @staticmethod
    def _wilson_lower_bound(wins: int, n: int, z: float = 1.96) -> float:
        """Conservative confidence lower-bound for Bernoulli win-rate."""
        if n <= 0:
            return 0.0
        phat = max(0.0, min(1.0, wins / max(1, n)))
        z2 = z * z
        denom = 1.0 + z2 / n
        center = phat + z2 / (2.0 * n)
        margin = z * math.sqrt((phat * (1.0 - phat) + z2 / (4.0 * n)) / n)
        return max(0.0, min(1.0, (center - margin) / denom))

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
        recent_trades = list(self.recent_trades)[-20:]
        recent_n = len(recent_trades)
        recent_wins = int(sum(1 for x in recent_trades if x))
        recent_wr = (recent_wins / recent_n) if recent_n > 0 else 0.5
        recent_wr_lb = self._wilson_lower_bound(recent_wins, recent_n)
        return {
            "outcomes": outcomes,
            "gross_win": gross_win,
            "gross_loss": gross_loss,
            "pf": pf,
            "recent_pf": recent_pf,
            "expectancy": expectancy,
            "avg_slip": avg_slip,
            "recent_n": recent_n,
            "recent_wr": recent_wr,
            "recent_wr_lb": recent_wr_lb,
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
        payout_upshift_cap = (
            ADAPTIVE_PAYOUT_MAX_UPSHIFT_5M
            if duration <= 5
            else ADAPTIVE_PAYOUT_MAX_UPSHIFT_15M
        )

        snap = self._growth_snapshot()
        tightness = 0.0
        if snap["outcomes"] >= 10 and snap["pf"] < 1.0:
            tightness += min(1.2, (1.0 - snap["pf"]) * 1.5)
        if snap["outcomes"] >= 10 and snap["expectancy"] < 0:
            tightness += min(1.0, abs(snap["expectancy"]) / 0.8)
        # Conservative confidence guard: if the lower confidence bound of recent WR drops below 50%,
        # tighten aggressively to protect EV under uncertainty.
        if snap["recent_n"] >= 10 and snap["recent_wr_lb"] < 0.50:
            tightness += min(1.0, (0.50 - snap["recent_wr_lb"]) * 4.0)
        if snap["avg_slip"] > 220:
            tightness += min(0.6, (snap["avg_slip"] - 220.0) / 600.0)
        if self.consec_losses >= 2:
            tightness += min(1.0, (self.consec_losses - 1) * 0.25)
        if snap["outcomes"] >= 12 and snap["pf"] > 1.35 and snap["expectancy"] > 0.30 and snap["avg_slip"] < 120:
            tightness -= 0.35
        # Momentum regime relaxation: require both observed WR and confidence.
        if (
            snap["outcomes"] >= 12
            and snap["pf"] >= 1.15
            and snap["recent_pf"] >= 1.10
            and snap["expectancy"] >= 0.10
            and snap["avg_slip"] < 180
            and snap["recent_n"] >= 12
            and snap["recent_wr"] >= 0.58
            and snap["recent_wr_lb"] >= 0.50
            and self.consec_losses == 0
        ):
            tightness -= 0.15
        tightness = min(1.8, max(-0.5, tightness))

        min_payout_raw = base_payout + (0.20 * tightness)
        if duration >= 15:
            # Keep 15m payout floor strict: adapt upward only, never below base.
            min_payout = max(base_payout, min(base_payout + payout_upshift_cap, min_payout_raw))
        else:
            min_payout = max(1.55, min(base_payout + payout_upshift_cap, min_payout_raw))
        min_ev = max(0.005, base_ev + (0.015 * tightness))
        max_entry_hard = max(0.33, min(0.80, hard_cap - (0.06 * tightness)))
        # In favorable regime, allow slightly wider executable entry band.
        if (
            snap["outcomes"] >= 12
            and snap["pf"] >= 1.15
            and snap["recent_pf"] >= 1.10
            and snap["expectancy"] >= 0.10
            and snap["avg_slip"] < 180
        ):
            max_entry_hard = min(0.82, max_entry_hard + 0.02)

        return min_payout, min_ev, max_entry_hard

    def _execution_penalties(self, duration: int, score: int, entry: float) -> tuple[float, float, float]:
        """Estimate execution frictions from realized bucket stats.
        Returns (slip_cost, nofill_penalty, fill_ratio)."""
        k = self._bucket_key(duration, score, entry)
        r = self._bucket_stats.rows.get(k)
        if not r:
            # Conservative cold-start defaults.
            return 0.003, 0.006, 0.90
        fills = int(r.get("fills", 0) or 0)
        outcomes = int(r.get("outcomes", 0) or 0)
        avg_slip_bps = float(r.get("slip_bps", 0.0) or 0.0) / max(1, fills)
        # Convert bps into expected probability-cost space.
        slip_cost = max(0.0, min(0.03, (avg_slip_bps / 10000.0) * max(0.05, float(entry or 0.0))))
        fill_ratio = fills / max(1, outcomes) if outcomes > 0 else min(1.0, fills / 12.0)
        # Penalize low historical fill reliability on this bucket.
        nofill_penalty = max(0.0, min(0.02, (1.0 - max(0.0, min(1.0, fill_ratio))) * 0.02))
        return slip_cost, nofill_penalty, max(0.0, min(1.0, fill_ratio))

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
        ev_net = float(sig.get("execution_ev", (float(sig.get("true_prob", 0.5)) / entry) - 1.0 - FEE_RATE_EST))
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

    def _direction_bias(self, asset: str, side: str, duration: int | None = None) -> float:
        """Small additive bias from realized side quality.
        Prefers recent on-chain outcomes over long-lived persisted aggregates."""
        if duration is not None and RECENT_SIDE_PRIOR_ENABLED:
            rs = self._recent_side_profile(asset, int(duration), side)
            rn = int(rs.get("n", 0) or 0)
            if rn >= max(1, RECENT_SIDE_PRIOR_MIN_N):
                # Convert recent posterior prior into a compact additive bias.
                rb = float(rs.get("prob_adj", 0.0) or 0.0) * 0.80
                return max(-0.05, min(0.05, rb))
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

    def _asset_side_quality(self, asset: str, side: str) -> tuple[int, float, float]:
        """Return realized quality for asset+side from settled on-chain outcomes.
        Returns (n, pf, expectancy_usdc)."""
        k = f"{asset}|{side}"
        s = self.side_perf.get(k, {})
        n = int(s.get("n", 0) or 0)
        if n <= 0:
            return 0, 1.0, 0.0
        gw = float(s.get("gross_win", 0.0) or 0.0)
        gl = float(s.get("gross_loss", 0.0) or 0.0)
        pnl = float(s.get("pnl", 0.0) or 0.0)
        pf = (gw / gl) if gl > 0 else (2.0 if gw > 0 else 1.0)
        exp = pnl / max(1, n)
        return n, pf, exp

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
            self._pm_pattern_stats = data.get("pm_pattern_stats", {}) or {}
            total = sum(s.get("total",0) for a in self.stats.values() for s in a.values())
            wins  = sum(s.get("wins",0)  for a in self.stats.values() for s in a.values())
            if total and LOG_STATS_LOCAL:
                print(f"{B}[STATS-LOCAL]{RS} cache={total} wr={wins/total*100:.1f}% (not on-chain)")
        except Exception:
            self.stats        = {}
            self.recent_trades = deque(maxlen=30)
            self.recent_pnl = deque(maxlen=40)
            self.side_perf = {}
            self._pm_pattern_stats = {}

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
                        "pm_pattern_stats": self._pm_pattern_stats,
                    },
                    f,
                )
        except Exception:
            pass

    def _load_settled_outcomes(self):
        """Load settled CID cache to keep settle accounting idempotent across restarts."""
        self._settled_outcomes = {}
        try:
            with open(SETTLED_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                return
            now = _time.time()
            keep = {}
            for cid, row in data.items():
                if not isinstance(row, dict):
                    continue
                ts = float(row.get("ts", 0.0) or 0.0)
                # Keep 36h of settle cache (covers restart/redeploy overlaps).
                if ts > 0 and (now - ts) <= 36 * 3600:
                    keep[str(cid)] = row
            self._settled_outcomes = keep
        except Exception:
            self._settled_outcomes = {}

    def _save_settled_outcomes(self):
        try:
            now = _time.time()
            pruned = {}
            for cid, row in self._settled_outcomes.items():
                ts = float((row or {}).get("ts", 0.0) or 0.0)
                if ts > 0 and (now - ts) <= 36 * 3600:
                    pruned[cid] = row
            self._settled_outcomes = pruned
            with open(SETTLED_FILE, "w", encoding="utf-8") as f:
                json.dump(self._settled_outcomes, f)
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
            self._pause_entries_until = 0.0
        else:
            self.consec_losses += 1
            if (
                LOSS_STREAK_PAUSE_ENABLED
                and self.consec_losses >= max(1, int(LOSS_STREAK_PAUSE_N))
            ):
                self._pause_entries_until = max(
                    float(self._pause_entries_until or 0.0),
                    _time.time() + max(30.0, float(LOSS_STREAK_PAUSE_SEC)),
                )
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
            f"WR={snap['recent_wr']*100:.1f}% LB={snap['recent_wr_lb']*100:.1f}% "
            f"Exp={sum(last5)/max(1,len(last5)):+.2f} MinEdge={me:.2f} "
            f"Streak={self.consec_losses}L DrawdownScale={self._kelly_drawdown_scale():.0%}{wr_str}"
        )
        if self._pause_entries_until > _time.time():
            rem_s = max(0.0, self._pause_entries_until - _time.time())
            print(
                f"{Y}[PAUSE]{RS} entries paused for {rem_s:.0f}s "
                f"(loss_streak={self.consec_losses} >= {LOSS_STREAK_PAUSE_N})"
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
                pending_n = len(self.pending_redeem)
                claimable_n = int(self.onchain_redeemable_count or 0)
                oldest_q_min = 0.0
                if self._redeem_queued_ts:
                    try:
                        now_q = _time.time()
                        oldest_q = min(float(v or now_q) for v in self._redeem_queued_ts.values())
                        oldest_q_min = max(0.0, (now_q - oldest_q) / 60.0)
                    except Exception:
                        oldest_q_min = 0.0
                should_block_settle = True
                if SETTLE_FIRST_REQUIRE_ONCHAIN_CLAIMABLE:
                    should_block_settle = (
                        (claimable_n > 0)
                        or (oldest_q_min >= max(0.0, SETTLE_FIRST_FORCE_BLOCK_AFTER_MIN))
                    )
                if should_block_settle:
                    if self._should_log("settle-first", LOG_SETTLE_FIRST_EVERY_SEC):
                        print(
                            f"{Y}[SETTLE-FIRST]{RS} pending_redeem={pending_n} "
                            f"(claimable={claimable_n}, oldest={oldest_q_min:.1f}m) "
                            f"— skipping new entries until win/loss is finalized on-chain"
                        )
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                if self._should_log("settle-first-soft", LOG_SETTLE_FIRST_EVERY_SEC):
                    print(
                        f"{Y}[SETTLE-FIRST-SOFT]{RS} pending_redeem={pending_n} "
                        f"(claimable={claimable_n}, oldest={oldest_q_min:.1f}m) "
                        f"— continue scanning while waiting on redeem claimability"
                    )
            if self._pause_entries_until > _time.time():
                if self._should_log("pause-entries", LOG_SETTLE_FIRST_EVERY_SEC):
                    rem_s = max(0.0, self._pause_entries_until - _time.time())
                    print(f"{Y}[PAUSE]{RS} skipping new entries for {rem_s:.0f}s (loss streak guard)")
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
                    f"Open(onchain): {self.onchain_open_count} | "
                    f"Settling(onchain): {self.onchain_redeemable_count}"
                )
            self._scan_state_last = scan_state

            # Global WS health is advisory only: per-market scorer enforces freshness-first
            # (strict WS when available, otherwise fresh REST fallback).
            if WS_HEALTH_REQUIRED:
                ws_ok, hs = self._ws_trade_gate_ok()
                if not ws_ok:
                    warmup_left = max(0.0, WS_GATE_WARMUP_SEC - (_time.time() - float(self._boot_ts or 0.0)))
                    if warmup_left > 0:
                        if self._should_log("ws-health-warmup", LOG_HEALTH_EVERY_SEC):
                            at = int(hs.get("active_markets", 0) or 0)
                            fresh = int(hs.get("fresh_markets", 0) or 0)
                            print(
                                f"{Y}[WS-WARMUP]{RS} hold gate for {warmup_left:.0f}s "
                                f"(fresh_books={fresh}/{at})"
                            )
                    else:
                        if self._should_log("ws-health-gate", LOG_HEALTH_EVERY_SEC):
                            at = int(hs.get("active_markets", 0) or 0)
                            fresh = int(hs.get("fresh_markets", 0) or 0)
                            ratio = (fresh / max(1, at)) if at > 0 else 0.0
                            ws_med = float(hs.get("ws_market_med_ms", 9e9) or 9e9)
                            print(
                                f"{Y}[WS-GATE-SOFT]{RS} degraded ws: fresh_books={fresh}/{at} "
                                f"ratio={ratio:.2f} < {WS_HEALTH_MIN_FRESH_RATIO:.2f} "
                                f"or ws_med={ws_med:.0f}ms > {WS_HEALTH_MAX_MED_AGE_MS:.0f}ms "
                                f"— continuing with per-market freshness checks"
                            )

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

            # Prefetch Polymarket open prices in parallel to avoid sequential stalls.
            pm_open_tasks = {}
            for cid, m in markets.items():
                if m.get("start_ts", 0) > now:
                    continue
                if (m.get("end_ts", 0) - now) / 60 < 1:
                    continue
                src_known = self.open_prices_source.get(cid, "?")
                need_pm = (cid not in self.open_prices) or (src_known != "PM")
                if not need_pm:
                    continue
                asset = m.get("asset")
                dur = int(m.get("duration", 0) or 0)
                start_ts = float(m.get("start_ts", now) or now)
                end_ts_m = float(m.get("end_ts", now + max(1, dur) * 60) or (now + max(1, dur) * 60))
                pm_open_tasks[cid] = self._get_polymarket_open_price(asset, start_ts, end_ts_m, dur)
            pm_open_prefetch = {}
            if pm_open_tasks:
                pm_vals = await self._gather_bounded(
                    list(pm_open_tasks.values()),
                    limit=max(2, min(12, len(pm_open_tasks))),
                )
                for cid, v in zip(pm_open_tasks.keys(), pm_vals):
                    pm_open_prefetch[cid] = 0.0 if isinstance(v, Exception) else float(v or 0.0)

            # Evaluate ALL eligible markets in parallel — no more sequential blocking
            candidates = []
            eligible_started = 0
            blocked_seen = 0
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
                eligible_started += 1
                m["mins_left"] = (m["end_ts"] - now) / 60
                # Set open_price from Chainlink at exact market start time.
                # Must match Polymarket's resolution reference ("price to beat").
                asset    = m.get("asset")
                dur      = m.get("duration", 0)
                title_s  = m.get("question", "")[:50]
                if cid not in self.open_prices:
                    start_ts = m.get("start_ts", now)
                    # Authoritative reference: Polymarket's own price API
                    ref = float(pm_open_prefetch.get(cid, 0.0) or 0.0)
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
                        pm_ref = float(pm_open_prefetch.get(cid, 0.0) or 0.0)
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
                else:
                    blocked_seen += 1
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
                    now_log = _time.time()
                    should_emit_best = (
                        (
                            best_sig != self._last_round_best
                            and (now_log - self._last_round_best_ts) >= max(LOG_ROUND_SIGNAL_MIN_SEC, float(LOG_ROUND_SIGNAL_EVERY_SEC))
                        )
                        or self._should_log("round-best-heartbeat", LOG_ROUND_SIGNAL_EVERY_SEC)
                    )
                    if should_emit_best:
                        self._last_round_best = best_sig
                        self._last_round_best_ts = now_log
                        print(f"{B}[ROUND] Best signal: {best['asset']} {best['side']} score={best['score']}{other_strs}{RS}")
                elif self._should_log("round-empty", LOG_ROUND_EMPTY_EVERY_SEC):
                    print(f"{Y}[ROUND]{RS} no executable signal")
                    print(
                        f"{Y}[ROUND-DIAG]{RS} eligible={eligible_started} candidates={len(candidates)} "
                        f"blocked_seen={blocked_seen} pending_redeem={len(self.pending_redeem)} "
                        f"open_onchain={self.onchain_open_count} enable_5m={ENABLE_5M}"
                    )
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
                        now_log = _time.time()
                        should_emit_pick = (
                            (
                                picks != self._last_round_pick
                                and (now_log - self._last_round_pick_ts) >= max(LOG_ROUND_SIGNAL_MIN_SEC, float(LOG_ROUND_SIGNAL_EVERY_SEC))
                            )
                            or self._should_log("round-pick-heartbeat", LOG_ROUND_SIGNAL_EVERY_SEC)
                        )
                        if should_emit_pick:
                            self._last_round_pick = picks
                            self._last_round_pick_ts = now_log
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
        usdc_contract = None
        addr_cs = None
        if self.w3 is not None:
            try:
                usdc_contract = self.w3.eth.contract(
                    address=Web3.to_checksum_address(USDC_E), abi=ERC20_ABI
                )
                addr_cs = Web3.to_checksum_address(ADDRESS)
            except Exception:
                pass

        tick = 0
        while True:
            await asyncio.sleep(max(0.8, ONCHAIN_SYNC_SEC))
            if DRY_RUN or self.w3 is None:
                continue
            tick += 1
            try:
                loop = asyncio.get_event_loop()

                # 1) On-chain wallet USDC + Polymarket positions fetched in parallel.
                usdc_task = (
                    loop.run_in_executor(
                        None, lambda: usdc_contract.functions.balanceOf(addr_cs).call()
                    )
                    if usdc_contract and addr_cs
                    else asyncio.sleep(0, result=0)
                )
                positions_open_task = self._http_get_json(
                    "https://data-api.polymarket.com/positions",
                    params={"user": ADDRESS, "sizeThreshold": "0.01", "redeemable": "false"},
                    timeout=10,
                )
                positions_redeem_task = self._http_get_json(
                    "https://data-api.polymarket.com/positions",
                    params={"user": ADDRESS, "sizeThreshold": "0.01", "redeemable": "true"},
                    timeout=10,
                )
                usdc_raw, positions_open, positions_redeem = await asyncio.gather(
                    usdc_task, positions_open_task, positions_redeem_task
                )
                usdc = float(usdc_raw or 0) / 1e6
                if not isinstance(positions_open, list):
                    positions_open = []
                if not isinstance(positions_redeem, list):
                    positions_redeem = []
                positions = positions_open + positions_redeem
                now_ts = datetime.now(timezone.utc).timestamp()

                # 2) On-chain-backed open/settling valuation from positions feed.
                open_val = 0.0
                open_stake_total = 0.0
                open_count = 0
                settling_claim_val = 0.0
                settling_claim_count = 0
                onchain_open_cids = set()
                onchain_open_usdc_by_cid = {}
                onchain_open_stake_by_cid = {}
                onchain_open_shares_by_cid = {}
                onchain_open_meta_by_cid = {}
                onchain_settling_usdc_by_cid = {}
                for p in positions:
                    cid = str(p.get("conditionId", "") or "")
                    if not cid:
                        continue
                    side = self._normalize_side_label(str(p.get("outcome", "") or ""))
                    val = float(p.get("currentValue", 0) or 0.0)
                    size_tok = self._as_float(p.get("size", 0.0), 0.0)
                    spent_probe = 0.0
                    for k_st in ("initialValue", "costBasis", "totalBought", "amountSpent", "spent"):
                        vv = self._as_float(p.get(k_st, 0.0), 0.0)
                        if vv > 0:
                            spent_probe = vv
                            break
                    # Open position presence must be on-chain truth (shares/spent), not mark-only.
                    has_open_presence = (
                        size_tok > 0
                        or spent_probe >= OPEN_PRESENCE_MIN
                        or val >= OPEN_PRESENCE_MIN
                    )
                    if not side or not has_open_presence:
                        continue
                    title = str(p.get("title", "") or "")
                    redeemable = bool(p.get("redeemable", False))
                    if (not redeemable) and self._is_historical_expired_position(p, now_ts) and val < OPEN_PRESENCE_MIN:
                        continue
                    if redeemable:
                        # Ignore dust/zero redeemables to avoid huge stale settling queues.
                        if val < 0.01:
                            continue
                        settling_claim_val += val
                        settling_claim_count += 1
                        onchain_settling_usdc_by_cid[cid] = round(
                            float(onchain_settling_usdc_by_cid.get(cid, 0.0) or 0.0) + val, 6
                        )
                        # Fast on-chain-first settle queue: don't wait for slower scan loops.
                        if cid not in self.pending_redeem and cid not in self.redeemed_cids:
                            asset = (
                                "BTC" if "Bitcoin" in title else
                                "ETH" if "Ethereum" in title else
                                "SOL" if "Solana" in title else
                                "XRP" if "XRP" in title else "?"
                            )
                            avg_px = self._as_float(p.get("avgPrice", 0.0), 0.0)
                            size_tok = self._as_float(p.get("size", 0.0), 0.0)
                            spent_guess = 0.0
                            for k_st in ("initialValue", "costBasis", "totalBought", "amountSpent", "spent"):
                                vv = self._as_float(p.get(k_st, 0.0), 0.0)
                                if vv > 0:
                                    spent_guess = vv
                                    break
                            if spent_guess <= 0 and size_tok > 0 and avg_px > 0:
                                spent_guess = size_tok * avg_px
                            if spent_guess <= 0:
                                spent_guess = val
                            m_s = {"conditionId": cid, "question": title, "asset": asset}
                            t_s = {
                                "side": side,
                                "asset": asset,
                                "size": spent_guess,
                                "entry": (avg_px if avg_px > 0 else 0.5),
                                "duration": 0,
                                "mkt_price": 0.5,
                                "mins_left": 0,
                                "open_price": 0,
                                "token_id": "",
                                "order_id": "ONCHAIN-REDEEM-QUEUE",
                            }
                            self.pending.pop(cid, None)
                            self.pending_redeem[cid] = (m_s, t_s)
                            self._redeem_queued_ts[cid] = _time.time()
                            print(
                                f"{G}[ONCHAIN-REDEEM-QUEUE]{RS} {title[:45]} {side} "
                                f"~${val:.2f} | cid={self._short_cid(cid)}"
                            )
                        continue
                    size_tok = self._as_float(p.get("size", 0.0), 0.0)
                    avg_px = self._as_float(p.get("avgPrice", 0.0), 0.0)
                    stake_guess = 0.0
                    stake_src = "value_fallback"
                    for k_st in ("initialValue", "costBasis", "totalBought", "amountSpent", "spent"):
                        vv = self._as_float(p.get(k_st, 0.0), 0.0)
                        if vv > 0:
                            stake_guess = vv
                            stake_src = k_st
                            break
                    if stake_guess <= 0 and size_tok > 0 and avg_px > 0:
                        stake_guess = size_tok * avg_px
                        stake_src = "size_x_avgPrice"
                    if stake_guess <= 0:
                        stake_guess = val
                        stake_src = "value_fallback"
                    open_val += val
                    open_count += 1
                    onchain_open_cids.add(cid)
                    onchain_open_usdc_by_cid[cid] = round(
                        float(onchain_open_usdc_by_cid.get(cid, 0.0) or 0.0) + val, 6
                    )
                    if size_tok > 0:
                        onchain_open_shares_by_cid[cid] = round(
                            float(onchain_open_shares_by_cid.get(cid, 0.0) or 0.0) + size_tok, 6
                        )
                    open_stake_total += stake_guess
                    onchain_open_stake_by_cid[cid] = round(
                        float(onchain_open_stake_by_cid.get(cid, 0.0) or 0.0) + stake_guess, 6
                    )
                    prev_meta = onchain_open_meta_by_cid.get(cid) or {}
                    if (not prev_meta) or (val >= float(prev_meta.get("value", 0.0) or 0.0)):
                        asset = (
                            "BTC" if "Bitcoin" in title else
                            "ETH" if "Ethereum" in title else
                            "SOL" if "Solana" in title else
                            "XRP" if "XRP" in title else "?"
                        )
                        start_ts = 0.0
                        end_ts = 0.0
                        for ks in ("eventStartTime", "startDate", "start_date"):
                            s = p.get(ks)
                            if isinstance(s, str) and s:
                                try:
                                    start_ts = datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
                                    break
                                except Exception:
                                    pass
                        for ke in ("endDate", "end_date", "eventEndTime"):
                            s = p.get(ke)
                            if isinstance(s, str) and s:
                                try:
                                    end_ts = datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
                                    break
                                except Exception:
                                    pass
                        q_st, q_et = self._round_bounds_from_question(title)
                        if q_st > 0 and q_et > q_st:
                            start_ts, end_ts = q_st, q_et
                        elif end_ts <= 0 and q_et > 0:
                            end_ts = q_et
                        if start_ts <= 0 and q_st > 0:
                            start_ts = q_st
                        dur_guess = 15
                        if start_ts > 0 and end_ts > start_ts:
                            dur_guess = max(1, int(round((end_ts - start_ts) / 60.0)))
                        # Keep stake stable across refreshes (avoid value-driven oscillation).
                        stable_stake = stake_guess
                        prev_stake = float(prev_meta.get("stake_usdc", 0.0) or 0.0)
                        if prev_stake > 0 and stable_stake <= 0:
                            stable_stake = prev_stake
                        elif prev_stake > 0 and stable_stake > 0:
                            stable_stake = max(prev_stake, stable_stake)
                        onchain_open_meta_by_cid[cid] = {
                            "title": title,
                            "side": side,
                            "asset": asset,
                            "entry": float(p.get("avgPrice", 0.5) or 0.5),
                            "stake_usdc": round(stable_stake, 6),
                            "stake_source": stake_src,
                            "shares": round(size_tok, 6),
                            "duration": dur_guess,
                            "start_ts": float(start_ts if start_ts > 0 else 0.0),
                            "end_ts": float(end_ts if end_ts > 0 else 0.0),
                            "value": val,
                        }

                # On-chain accounting view:
                # - open_positions tracks total stake still at risk (spent, not settled)
                # - open_mark tracks current mark-to-market value of those positions
                total = round(usdc + open_stake_total + settling_claim_val, 2)
                self.onchain_wallet_usdc = round(usdc, 2)
                self.onchain_open_positions = round(open_stake_total, 2)
                self.onchain_usdc_balance = round(usdc, 2)
                self.onchain_open_stake_total = round(open_stake_total, 2)
                self.onchain_redeemable_usdc = round(settling_claim_val, 2)
                self.onchain_open_mark_value = round(open_val, 2)
                self.onchain_open_cids = set(onchain_open_cids)
                self.onchain_open_usdc_by_cid = dict(onchain_open_usdc_by_cid)
                self.onchain_open_stake_by_cid = dict(onchain_open_stake_by_cid)
                self.onchain_open_shares_by_cid = dict(onchain_open_shares_by_cid)
                self.onchain_open_meta_by_cid = dict(onchain_open_meta_by_cid)
                # Prune stale/zero-value pending redeem entries that are not claimable on-chain now.
                stale_redeem = 0
                for cid_q, val_q in list(self.pending_redeem.items()):
                    if cid_q in onchain_settling_usdc_by_cid:
                        continue
                    t_q = val_q[1] if isinstance(val_q, tuple) and len(val_q) > 1 and isinstance(val_q[1], dict) else {}
                    q_size = self._as_float(t_q.get("size", 0.0), 0.0)
                    if q_size > 0.0:
                        continue
                    self.pending_redeem.pop(cid_q, None)
                    stale_redeem += 1
                if stale_redeem:
                    print(f"{Y}[SYNC]{RS} pruned stale pending_redeem: {stale_redeem}")
                # Count unique CIDs for stable on-chain open/settling counters.
                open_count = len(onchain_open_cids)
                settling_claim_count = len(onchain_settling_usdc_by_cid)
                self.onchain_open_count = int(open_count)
                self.onchain_redeemable_count = int(settling_claim_count)
                self.onchain_settling_usdc_by_cid = dict(onchain_settling_usdc_by_cid)
                self.onchain_total_equity = total
                self.onchain_snapshot_ts = _time.time()
                bank_state = (
                    round(usdc, 2),
                    round(open_stake_total, 2),
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
                        f"open_positions(stake)=${open_stake_total:.2f} ({open_count})  "
                        f"open_mark=${open_val:.2f}  "
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
                api_active_cids = set()
                for p in positions:
                    if p.get("redeemable", False):
                        continue
                    cid_p = p.get("conditionId", "")
                    side_p = self._normalize_side_label(p.get("outcome", ""))
                    if not cid_p or not side_p:
                        continue
                    val_p = float(p.get("currentValue", 0) or 0.0)
                    size_p = self._as_float(p.get("size", 0.0), 0.0)
                    spent_p = 0.0
                    for k_st in ("initialValue", "costBasis", "totalBought", "amountSpent", "spent"):
                        vv = self._as_float(p.get(k_st, 0.0), 0.0)
                        if vv > 0:
                            spent_p = vv
                            break
                    if self._is_historical_expired_position(p, now_ts) and val_p < OPEN_PRESENCE_MIN:
                        continue
                    if (size_p > 0) or (spent_p >= OPEN_PRESENCE_MIN) or (val_p >= OPEN_PRESENCE_MIN):
                        api_active_cids.add(cid_p)
                # On-chain-first cleanup: if a local pending CID is neither on-chain nor API-active
                # after a grace window, it is stale and removed from local state.
                prune_n = 0
                for cid, (m_p, t_p) in list(self.pending.items()):
                    if cid in onchain_open_cids or cid in self.pending_redeem or cid in api_active_cids:
                        self._pending_absent_counts.pop(cid, None)
                        continue
                    placed_ts = float(t_p.get("placed_ts", 0) or 0)
                    if placed_ts > 0 and (now_ts - placed_ts) < 90:
                        self._pending_absent_counts.pop(cid, None)
                        continue
                    # Keep very fresh windows briefly even without placed_ts.
                    end_ts = float(m_p.get("end_ts", 0) or 0)
                    if end_ts > 0 and (end_ts - now_ts) > 0 and (end_ts - now_ts) < 90:
                        self._pending_absent_counts.pop(cid, None)
                        continue
                    # On-chain/API indexers can lag; never prune immediately around expiry.
                    # Require the market to be clearly stale in wall-clock terms first.
                    if end_ts > 0 and now_ts < (end_ts + 600):
                        self._pending_absent_counts.pop(cid, None)
                        continue
                    miss_n = int(self._pending_absent_counts.get(cid, 0)) + 1
                    self._pending_absent_counts[cid] = miss_n
                    # Only prune after multiple consecutive absent checks.
                    if miss_n < 3:
                        continue
                    self.pending.pop(cid, None)
                    self._pending_absent_counts.pop(cid, None)
                    prune_n += 1
                if prune_n:
                    self._save_pending()
                    print(f"{Y}[SYNC] pruned stale local pending: {prune_n} (absent on-chain/API){RS}")
                # Recover any on-chain positions not tracked in pending (every tick = 30s)
                for p in positions:
                    cid  = p.get("conditionId", "")
                    rdm  = p.get("redeemable", False)
                    val  = float(p.get("currentValue", 0))
                    side = self._normalize_side_label(p.get("outcome", ""))
                    title = p.get("title", "")
                    rec_size_tok = self._as_float(p.get("size", 0.0), 0.0)
                    rec_spent_probe = 0.0
                    for k_st in ("initialValue", "costBasis", "totalBought", "amountSpent", "spent"):
                        vv = self._as_float(p.get(k_st, 0.0), 0.0)
                        if vv > 0:
                            rec_spent_probe = vv
                            break
                    rec_present = (
                        rec_size_tok > 0
                        or rec_spent_probe >= OPEN_PRESENCE_MIN
                        or val >= OPEN_PRESENCE_MIN
                    )
                    if rdm or not side or not cid or not rec_present:
                        continue
                    if self._is_historical_expired_position(p, now_ts) and val < OPEN_PRESENCE_MIN:
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
                        _, token_up, token_down = self._map_updown_market_fields(mkt)
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
                    rec_avg_px = self._as_float(p.get("avgPrice", 0.0), 0.0)
                    rec_size_tok = self._as_float(p.get("size", 0.0), 0.0)
                    rec_spent = 0.0
                    for k_st in ("initialValue", "costBasis", "totalBought", "amountSpent", "spent"):
                        vv = self._as_float(p.get(k_st, 0.0), 0.0)
                        if vv > 0:
                            rec_spent = vv
                            break
                    if rec_spent <= 0 and rec_size_tok > 0 and rec_avg_px > 0:
                        rec_spent = rec_size_tok * rec_avg_px
                    if rec_spent <= 0:
                        rec_spent = val
                    rec_entry = rec_avg_px if rec_avg_px > 0 else 0.5
                    t_r = {"side": side, "size": rec_spent, "entry": rec_entry,
                           "open_price": open_p, "current_price": 0, "true_prob": 0.5,
                           "mkt_price": 0.5, "edge": 0, "mins_left": mins_left,
                           "end_ts": end_ts, "asset": asset, "duration": duration,
                           "token_id": token_up if side == "Up" else token_down,
                           "order_id": "RECOVERED",
                           "core_position": True,
                           "core_entry_locked": True,
                           "core_entry": rec_entry,
                           "core_size_usdc": rec_spent,
                           "addon_count": 0,
                           "addon_stake_usdc": 0.0}
                    self.pending[cid] = (m_r, t_r)
                    self.seen.add(cid)
                    self._save_pending()
                    print(
                        f"{G}[RECOVER]{RS} Added to pending: {title[:40]} {side} "
                        f"spent=${rec_spent:.2f} mark=${val:.2f} entry={rec_entry:.3f} "
                        f"| open={open_p:.2f} | {mins_left:.1f}min left"
                    )

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
                    params={"user": ADDRESS, "sizeThreshold": "0.01", "redeemable": "true"},
                    timeout=10,
                )
                for pos in positions:
                    cid        = pos.get("conditionId", "")
                    redeemable = pos.get("redeemable", False)
                    val        = float(pos.get("currentValue", 0))
                    outcome    = self._normalize_side_label(pos.get("outcome", ""))
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
                        outcome = self._normalize_side_label(evt.get("outcome") or "")
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
                    params={"user": ADDRESS, "sizeThreshold": "0.01", "redeemable": "false"}, timeout=10
                ).json())
                now = datetime.now(timezone.utc).timestamp()
                added = 0
                for pos in positions:
                    cid  = pos.get("conditionId", "")
                    rdm  = pos.get("redeemable", False)
                    val  = float(pos.get("currentValue", 0))
                    side = self._normalize_side_label(pos.get("outcome", ""))
                    if rdm or not side or not cid:
                        continue
                    self.seen.add(cid)
                    size_tok = self._as_float(pos.get("size", 0.0), 0.0)
                    spent_probe = 0.0
                    for k_st in ("initialValue", "costBasis", "totalBought", "amountSpent", "spent"):
                        vv = self._as_float(pos.get(k_st, 0.0), 0.0)
                        if vv > 0:
                            spent_probe = vv
                            break
                    # Presence is based on open shares/spent, not only mark value.
                    if not ((size_tok > 0) or (spent_probe >= OPEN_PRESENCE_MIN) or (val >= OPEN_PRESENCE_MIN)):
                        continue
                    if self._is_historical_expired_position(pos, now, grace_sec=1800.0) and val < OPEN_PRESENCE_MIN:
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
                        _, token_up, token_down = self._map_updown_market_fields(mkt)
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
                    rec_avg_px = self._as_float(pos.get("avgPrice", 0.0), 0.0)
                    rec_size_tok = self._as_float(pos.get("size", 0.0), 0.0)
                    rec_spent = 0.0
                    for k_st in ("initialValue", "costBasis", "totalBought", "amountSpent", "spent"):
                        vv = self._as_float(pos.get(k_st, 0.0), 0.0)
                        if vv > 0:
                            rec_spent = vv
                            break
                    if rec_spent <= 0 and rec_size_tok > 0 and rec_avg_px > 0:
                        rec_spent = rec_size_tok * rec_avg_px
                    if rec_spent <= 0:
                        rec_spent = val
                    rec_entry = rec_avg_px if rec_avg_px > 0 else 0.5
                    t_r = {"side": side, "size": rec_spent, "entry": rec_entry,
                           "open_price": 0, "current_price": 0, "true_prob": 0.5,
                           "mkt_price": 0.5, "edge": 0, "mins_left": mins_left,
                           "end_ts": end_ts, "asset": asset, "duration": duration,
                           "token_id": token_up if side == "Up" else token_down,
                           "order_id": "SYNC-PERIODIC",
                           "core_position": True,
                           "core_entry_locked": True,
                           "core_entry": rec_entry,
                           "core_size_usdc": rec_spent,
                           "addon_count": 0,
                           "addon_stake_usdc": 0.0}
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
                    if mins_left <= 0:
                        self.pending_redeem[cid] = (m_r, t_r)
                        print(
                            f"{Y}[SYNC]{RS} Recovered->settling: {title[:40]} {side} "
                            f"spent=${rec_spent:.2f} mark=${val:.2f} entry={rec_entry:.3f} "
                            f"| {duration}m ended {abs(mins_left):.1f}min ago"
                        )
                    else:
                        self.pending[cid] = (m_r, t_r)
                        self.seen.add(cid)
                        added += 1
                        print(
                            f"{Y}[SYNC]{RS} Recovered: {title[:40]} {side} "
                            f"spent=${rec_spent:.2f} mark=${val:.2f} entry={rec_entry:.3f} "
                            f"| {duration}m ends in {mins_left:.1f}min"
                        )
                if added:
                    self._save_pending()
                    self._save_seen()
            except Exception as e:
                print(f"{Y}[SYNC] position_sync_loop error: {e}{RS}")

    async def _heartbeat_loop(self):
        """Keep CLOB session warm and open orders alive."""
        if DRY_RUN:
            return
        while True:
            try:
                loop = asyncio.get_event_loop()
                hb_resp = await loop.run_in_executor(
                    None,
                    lambda: self.clob.post_heartbeat(self._heartbeat_id),
                )
                if isinstance(hb_resp, dict):
                    next_id = (hb_resp.get("heartbeat_id") or "").strip()
                    if next_id:
                        self._heartbeat_id = next_id
                self._heartbeat_last_ok = _time.time()
            except Exception as e:
                # Polymarket heartbeat protocol: first call uses "", then reuse returned heartbeat_id.
                # On invalid id error, server may return a fresh heartbeat_id in error payload.
                msg = str(e)
                m = re.search(r"heartbeat_id['\"]?\s*[:=]\s*['\"]([0-9a-fA-F-]{8,})['\"]", msg)
                if m:
                    self._heartbeat_id = m.group(1)
                self._errors.tick("clob_heartbeat", print, err=e, every=10)
                if self._should_log("heartbeat-fail", 30):
                    print(f"{Y}[HEARTBEAT] failed: {e}{RS}")
            await asyncio.sleep(max(2.0, CLOB_HEARTBEAT_SEC))

    async def _user_events_loop(self):
        """Near-realtime order status cache from CLOB notifications."""
        if DRY_RUN or (not USER_EVENTS_ENABLED):
            return
        while True:
            try:
                loop = asyncio.get_event_loop()
                rows = await loop.run_in_executor(None, self.clob.get_notifications)
                if isinstance(rows, dict):
                    # Different wrappers sometimes return {"data":[...]}
                    rows = rows.get("data", []) or rows.get("notifications", [])
                if isinstance(rows, list) and rows:
                    for r in rows:
                        oid, st, fs = self._extract_order_event(r)
                        if oid:
                            self._cache_order_event(oid, st, fs)
                    # Acknowledge consumed notifications to keep payload light.
                    try:
                        await loop.run_in_executor(None, self.clob.drop_notifications)
                    except Exception:
                        pass
                if len(self._order_event_cache) > 2000:
                    self._prune_order_event_cache()
            except Exception as e:
                self._errors.tick("user_events_loop", print, err=e, every=20)
            await asyncio.sleep(max(0.25, USER_EVENTS_POLL_SEC))

    async def _health_check(self):
        now = _time.time()

        # CLOB-WS health: if all active tokens are stale for several checks, force reconnect.
        if CLOB_MARKET_WS_ENABLED and self.active_mkts:
            tids = list(self._trade_focus_token_ids() or self._active_token_ids())
            if tids:
                stale = [tid for tid in tids if self._clob_ws_book_age_ms(tid) > CLOB_MARKET_WS_MAX_AGE_MS]
                if len(stale) == len(tids):
                    self._ws_stale_hits += 1
                    if self._should_log("health-ws-stale", LOG_HEALTH_EVERY_SEC):
                        age_samples = [self._clob_ws_book_age_ms(t) for t in tids[:3]]
                        age_s = ", ".join(f"{a:.0f}ms" for a in age_samples)
                        print(
                            f"{Y}[HEALTH]{RS} CLOB-WS stale {len(stale)}/{len(tids)} tokens "
                            f"(samples={age_s}) hits={self._ws_stale_hits}"
                        )
                    if (
                        self._ws_stale_hits >= max(1, CLOB_WS_STALE_HEAL_HITS)
                        and (now - float(self._ws_last_heal_ts or 0.0)) >= CLOB_WS_STALE_HEAL_COOLDOWN_SEC
                    ):
                        self._ws_last_heal_ts = now
                        self._ws_stale_hits = 0
                        print(f"{Y}[HEALTH]{RS} forcing CLOB-WS reconnect (all books stale)")
                        try:
                            if self._clob_market_ws is not None:
                                await self._clob_market_ws.close(code=1012, reason="stale-books")
                        except Exception:
                            pass
                else:
                    self._ws_stale_hits = 0

        # Leader-flow health: if no fresh leader-flow for active CIDs, force immediate refresh.
        if COPYFLOW_LIVE_ENABLED and self.active_mkts:
            cids = list(self.active_mkts.keys())
            fresh = 0
            for cid in cids:
                row = self._copyflow_live.get(cid, {})
                ts = float(row.get("ts", 0.0) or 0.0)
                n = int(row.get("n", 0) or 0)
                if ts > 0 and (now - ts) <= COPYFLOW_LIVE_MAX_AGE_SEC and n > 0:
                    fresh += 1
            if fresh == 0 and cids:
                if self._should_log("health-leader-empty", LOG_HEALTH_EVERY_SEC):
                    age = (now - self._copyflow_live_last_update_ts) if self._copyflow_live_last_update_ts > 0 else 9e9
                    print(
                        f"{Y}[HEALTH]{RS} no fresh leader-flow (active={len(cids)} "
                        f"last_live={age:.1f}s zero_streak={self._copyflow_live_zero_streak})"
                    )
                if (now - float(self._copyflow_force_refresh_ts or 0.0)) >= COPYFLOW_HEALTH_FORCE_REFRESH_SEC:
                    self._copyflow_force_refresh_ts = now
                    updated = 0
                    try:
                        updated = await self._copyflow_live_refresh_once(reason="health")
                    except Exception:
                        updated = 0
                    if updated <= 0:
                        self._reload_copyflow()

    async def _status_loop(self):
        while True:
            await asyncio.sleep(STATUS_INTERVAL)
            try:
                await self._health_check()
            except Exception as e:
                self._errors.tick("health_check", print, err=e, every=20)
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
            _guard("_stream_clob_market_book", self._stream_clob_market_book),
            _guard("vol_loop",              self.vol_loop),
            _guard("scan_loop",             self.scan_loop),
            _guard("_status_loop",          self._status_loop),
            _guard("_refresh_balance",      self._refresh_balance),
            _guard("_redeem_loop",          self._redeem_loop),
            _guard("_heartbeat_loop",       self._heartbeat_loop),
            _guard("_user_events_loop",     self._user_events_loop),
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
        _install_runtime_json_log()
        asyncio.run(LiveTrader().run())
    except KeyboardInterrupt:
        print(f"\n{Y}[STOP] Log: {LOG_FILE}{RS}")
