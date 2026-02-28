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
try:
    import orjson as _orjson
    class json:  # type: ignore
        loads  = staticmethod(_orjson.loads)
        dumps  = staticmethod(lambda obj, **kw: _orjson.dumps(obj).decode())
        load   = staticmethod(__import__("json").load)
        dump   = staticmethod(__import__("json").dump)
except ImportError:
    import json
import math
import csv
import os
import re
import sqlite3
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
    from prediction_agent import PredictionAgent
except ModuleNotFoundError:
    PredictionAgent = None
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
            self._load()

        def _load(self):
            try:
                with open(BUCKET_STATS_PATH) as f:
                    data = json.load(f)
                for k, v in data.items():
                    self.rows[k].update(v)
            except Exception:
                pass

        def _save(self):
            try:
                with open(BUCKET_STATS_PATH, "w") as f:
                    json.dump(dict(self.rows), f)
            except Exception:
                pass

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
            self._save()

BUCKET_STATS_PATH = "/data/bucket_stats.json"

def _bs_load(bs):
    try:
        with open(BUCKET_STATS_PATH) as f:
            for k, v in json.load(f).items():
                bs.rows[k].update(v)
    except Exception:
        pass

def _bs_save(bs):
    try:
        with open(BUCKET_STATS_PATH, "w") as f:
            json.dump(dict(bs.rows), f)
    except Exception:
        pass

load_dotenv(os.path.expanduser("~/.clawdbot.env"))

from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON, AMOY
from py_clob_client.clob_types import OrderArgs, MarketOrderArgs, OrderType, AssetType, BalanceAllowanceParams, ApiCreds
from py_clob_client.config import get_contract_config

_ALCHEMY_KEY = os.environ.get("ALCHEMY_KEY", "")
_QUICKNODE_WS = os.environ.get("QUICKNODE_WS", "")   # wss://xxx.quiknode.pro/KEY/

POLYGON_RPCS = [
    "https://polygon-mainnet.public.blastapi.io",
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon.drpc.org",
    "https://rpc.ankr.com/polygon",
    *([f"https://polygon-mainnet.g.alchemy.com/v2/{_ALCHEMY_KEY}"] if _ALCHEMY_KEY else []),
]
POLYGON_WS_RPCS = [
    "wss://polygon-mainnet.public.blastapi.io",
    "wss://polygon-bor-rpc.publicnode.com",
    "wss://polygon.drpc.org",
    *([f"wss://polygon-mainnet.g.alchemy.com/v2/{_ALCHEMY_KEY}"] if _ALCHEMY_KEY else []),
    *([_QUICKNODE_WS] if _QUICKNODE_WS else []),
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
PROFIT_PUSH_MODE = os.environ.get("PROFIT_PUSH_MODE", "false").lower() == "true"
PROFIT_PUSH_MULT = float(os.environ.get("PROFIT_PUSH_MULT", "1.0"))
PROFIT_PUSH_ADAPTIVE_MODE = os.environ.get("PROFIT_PUSH_ADAPTIVE_MODE", "true").lower() == "true"
PROFIT_PUSH_PROBE_MULT = float(os.environ.get("PROFIT_PUSH_PROBE_MULT", "0.62"))
PROFIT_PUSH_BASE_MULT = float(os.environ.get("PROFIT_PUSH_BASE_MULT", "1.00"))
PROFIT_PUSH_PUSH_MULT = float(os.environ.get("PROFIT_PUSH_PUSH_MULT", "1.35"))
PROFIT_PUSH_PUSH_MIN_SCORE = int(os.environ.get("PROFIT_PUSH_PUSH_MIN_SCORE", "12"))
PROFIT_PUSH_PUSH_MIN_TRUE_PROB = float(os.environ.get("PROFIT_PUSH_PUSH_MIN_TRUE_PROB", "0.64"))
PROFIT_PUSH_PUSH_MIN_EXEC_EV = float(os.environ.get("PROFIT_PUSH_PUSH_MIN_EXEC_EV", "0.030"))
PROFIT_PUSH_BASE_MIN_SCORE = int(os.environ.get("PROFIT_PUSH_BASE_MIN_SCORE", "9"))
PROFIT_PUSH_BASE_MIN_EXEC_EV = float(os.environ.get("PROFIT_PUSH_BASE_MIN_EXEC_EV", "0.010"))
PROFIT_PUSH_MAX_MULT = float(os.environ.get("PROFIT_PUSH_MAX_MULT", "1.85"))
MIN_EDGE       = float(os.environ.get("MIN_EDGE", "0.20"))     # base edge floor — raised from 0.08 (data: edge>=0.20 → WR=52.7%)
MIN_MOVE       = float(os.environ.get("MIN_MOVE", "0.0003"))   # flat filter threshold
MOMENTUM_WEIGHT = float(os.environ.get("MOMENTUM_WEIGHT", "0.40"))
DUST_BET       = float(os.environ.get("DUST_BET", "5.0"))
MIN_BET_ABS    = float(os.environ.get("MIN_BET_ABS", "2.50"))
MIN_EXEC_NOTIONAL_USDC = float(os.environ.get("MIN_EXEC_NOTIONAL_USDC", "1.0"))
MIN_ORDER_SIZE_SHARES = float(os.environ.get("MIN_ORDER_SIZE_SHARES", "5.0"))
ORDER_SIZE_PAD_SHARES = float(os.environ.get("ORDER_SIZE_PAD_SHARES", "0.02"))
MIN_BET_PCT    = float(os.environ.get("MIN_BET_PCT", "0.022"))
DUST_RECOVER_MIN = float(os.environ.get("DUST_RECOVER_MIN", "0.50"))
MIN_PARTIAL_TRACK_USDC = float(os.environ.get("MIN_PARTIAL_TRACK_USDC", "5.0"))
# Presence threshold for on-chain position visibility/sync.
# Keep this low and independent from recovery sizing thresholds.
OPEN_PRESENCE_MIN = float(os.environ.get("OPEN_PRESENCE_MIN", "0.01"))
MAX_ABS_BET    = float(os.environ.get("MAX_ABS_BET", "14.0"))     # hard ceiling
MAX_BANKROLL_PCT = float(os.environ.get("MAX_BANKROLL_PCT", "0.25"))
MAX_OPEN       = int(os.environ.get("MAX_OPEN", "4"))
MAX_SAME_DIR   = int(os.environ.get("MAX_SAME_DIR", "4"))
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
FORCE_TRADE_EVERY_ROUND = os.environ.get("FORCE_TRADE_EVERY_ROUND", "true").lower() == "true"
ROUND_MAX_TRADES = int(os.environ.get("ROUND_MAX_TRADES", "2"))
ROUND_SECOND_TRADE_PUSH_ONLY = os.environ.get("ROUND_SECOND_TRADE_PUSH_ONLY", "true").lower() == "true"
ROUND_SECOND_TRADE_MIN_SCORE = int(os.environ.get("ROUND_SECOND_TRADE_MIN_SCORE", "12"))
ROUND_SECOND_TRADE_MIN_EXEC_EV = float(os.environ.get("ROUND_SECOND_TRADE_MIN_EXEC_EV", "0.035"))
ROUND_SECOND_TRADE_MIN_TRUE_PROB = float(os.environ.get("ROUND_SECOND_TRADE_MIN_TRUE_PROB", "0.64"))
ROUND_SECOND_TRADE_REQUIRE_CL_AGREE = os.environ.get("ROUND_SECOND_TRADE_REQUIRE_CL_AGREE", "true").lower() == "true"
ROUND_SECOND_TRADE_MAX_ENTRY = float(os.environ.get("ROUND_SECOND_TRADE_MAX_ENTRY", "0.52"))
ROUND_SECOND_TRADE_MAX_GSCORE_GAP = float(os.environ.get("ROUND_SECOND_TRADE_MAX_GSCORE_GAP", "0.030"))
ROUND_FORCE_MAX_BANK_FRAC = float(os.environ.get("ROUND_FORCE_MAX_BANK_FRAC", "0.08"))
ROUND_FORCE_MIN_NOTIONAL_MULT = float(os.environ.get("ROUND_FORCE_MIN_NOTIONAL_MULT", "1.00"))
ROUND_FORCE_PAYOUT_CAP_15M = float(os.environ.get("ROUND_FORCE_PAYOUT_CAP_15M", "1.72"))
ROUND_FORCE_PAYOUT_CAP_5M = float(os.environ.get("ROUND_FORCE_PAYOUT_CAP_5M", "1.68"))
ROUND_CONSENSUS_15M_ENABLED = os.environ.get("ROUND_CONSENSUS_15M_ENABLED", "true").lower() == "true"
ROUND_CONSENSUS_MIN_NET = int(os.environ.get("ROUND_CONSENSUS_MIN_NET", "2"))
ROUND_CONSENSUS_OVERRIDE_SCORE = int(os.environ.get("ROUND_CONSENSUS_OVERRIDE_SCORE", "19"))
ROUND_CONSENSUS_OVERRIDE_EDGE = float(os.environ.get("ROUND_CONSENSUS_OVERRIDE_EDGE", "0.16"))
ROUND_CONSENSUS_STRICT_15M = os.environ.get("ROUND_CONSENSUS_STRICT_15M", "true").lower() == "true"
ROUND_CONSENSUS_MOVE_BPS = float(os.environ.get("ROUND_CONSENSUS_MOVE_BPS", "1.5"))
MIN_SCORE_GATE = int(os.environ.get("MIN_SCORE_GATE", "0"))
MIN_SCORE_GATE_5M = int(os.environ.get("MIN_SCORE_GATE_5M", "0"))
MIN_SCORE_GATE_15M = int(os.environ.get("MIN_SCORE_GATE_15M", "0"))
BLOCK_SCORE_S0_8_15M  = os.environ.get("BLOCK_SCORE_S0_8_15M",  "false").lower() == "true"
BLOCK_SCORE_S9_11_15M = os.environ.get("BLOCK_SCORE_S9_11_15M", "false").lower() == "true"
BLOCK_SCORE_S12P_15M  = os.environ.get("BLOCK_SCORE_S12P_15M",  "false").lower() == "true"
SCORE_BLOCK_SOFT_MODE = os.environ.get("SCORE_BLOCK_SOFT_MODE", "true").lower() == "true"
SCORE_BLOCK_SOFT_EDGE_PEN = float(os.environ.get("SCORE_BLOCK_SOFT_EDGE_PEN", "0.010"))
# Profit guard: low-score 15m performs poorly in 30-50c band; keep only >50c by default.
MIN_ENTRY_PRICE_S0_8_15M = float(os.environ.get("MIN_ENTRY_PRICE_S0_8_15M", "0.50"))  # 0=disabled
BLOCK_ASSET_SOL_15M = os.environ.get("BLOCK_ASSET_SOL_15M", "false").lower() == "true"
BLOCK_ASSET_XRP_15M = os.environ.get("BLOCK_ASSET_XRP_15M", "false").lower() == "true"
ASSET_BLOCK_SOFT_MODE = os.environ.get("ASSET_BLOCK_SOFT_MODE", "true").lower() == "true"
ASSET_BLOCK_SOFT_SCORE_PEN = int(os.environ.get("ASSET_BLOCK_SOFT_SCORE_PEN", "2"))
ASSET_BLOCK_SOFT_EDGE_PEN = float(os.environ.get("ASSET_BLOCK_SOFT_EDGE_PEN", "0.008"))
MIN_TRUE_PROB_GATE_15M = float(os.environ.get("MIN_TRUE_PROB_GATE_15M", "0.50"))
MIN_TRUE_PROB_GATE_5M  = float(os.environ.get("MIN_TRUE_PROB_GATE_5M",  "0.50"))
ROLLING3_WIN_SCORE_PEN = int(os.environ.get("ROLLING3_WIN_SCORE_PEN", "0"))
MAX_ENTRY_PRICE = float(os.environ.get("MAX_ENTRY_PRICE", "0.65"))
MAX_ENTRY_TOL = float(os.environ.get("MAX_ENTRY_TOL", "0.015"))
MIN_ENTRY_PRICE_15M = float(os.environ.get("MIN_ENTRY_PRICE_15M", "0.40"))
MIN_ENTRY_PRICE_5M = float(os.environ.get("MIN_ENTRY_PRICE_5M", "0.35"))
MAX_ENTRY_PRICE_5M = float(os.environ.get("MAX_ENTRY_PRICE_5M", "0.52"))
MIN_PAYOUT_MULT = float(os.environ.get("MIN_PAYOUT_MULT", "1.72"))
# Late-window payout relaxation: aligned entry in last 28% of window → lower payout OK, win rate is higher
LATE_PAYOUT_RELAX_ENABLED   = os.environ.get("LATE_PAYOUT_RELAX_ENABLED", "true").lower() == "true"
LATE_PAYOUT_RELAX_PCT_LEFT  = float(os.environ.get("LATE_PAYOUT_RELAX_PCT_LEFT", "0.45"))   # last 45% of window (was 0.28)
LATE_PAYOUT_RELAX_MIN_MOVE  = float(os.environ.get("LATE_PAYOUT_RELAX_MIN_MOVE", "0.0025")) # 0.25% price move confirming direction
LATE_PAYOUT_RELAX_FLOOR     = float(os.environ.get("LATE_PAYOUT_RELAX_FLOOR", "1.40"))      # accept 1.40x when late+locked (momentum confirmed, WR ~75-80%)
LATE_PAYOUT_PROB_BOOST      = float(os.environ.get("LATE_PAYOUT_PROB_BOOST", "0.04"))       # true_prob boost for locked-direction entries
# Must-fire: guarantee at least 1 trade per 15m window in last N minutes by relaxing score gate
LATE_MUST_FIRE_ENABLED     = os.environ.get("LATE_MUST_FIRE_ENABLED", "false").lower() == "true"  # disabled: early-entry strategy
LATE_MUST_FIRE_MINS_LEFT   = float(os.environ.get("LATE_MUST_FIRE_MINS_LEFT", "10.0"))   # relax in last 10 min (was 7)
LATE_MUST_FIRE_SCORE_RELAX = int(os.environ.get("LATE_MUST_FIRE_SCORE_RELAX", "3"))      # lower gate by 3 pts
LATE_MUST_FIRE_MIN_SCORE   = int(os.environ.get("LATE_MUST_FIRE_MIN_SCORE", "5"))        # absolute floor after relax
LATE_MUST_FIRE_PROB_RELAX  = float(os.environ.get("LATE_MUST_FIRE_PROB_RELAX", "0.05")) # relax true_prob gate by this
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
ENTRY_HARD_CAP_5M = float(os.environ.get("ENTRY_HARD_CAP_5M", "0.72"))
ENTRY_HARD_CAP_15M = float(os.environ.get("ENTRY_HARD_CAP_15M", "0.65"))
ENTRY_NEAR_MISS_TOL = float(os.environ.get("ENTRY_NEAR_MISS_TOL", "0.030"))
PULLBACK_LIMIT_ENABLED = os.environ.get("PULLBACK_LIMIT_ENABLED", "true").lower() == "true"
PULLBACK_LIMIT_MIN_PCT_LEFT = float(os.environ.get("PULLBACK_LIMIT_MIN_PCT_LEFT", "0.10"))
FAST_EXEC_ENABLED = os.environ.get("FAST_EXEC_ENABLED", "false").lower() == "true"
FAST_EXEC_SCORE = int(os.environ.get("FAST_EXEC_SCORE", "6"))
FAST_EXEC_EDGE = float(os.environ.get("FAST_EXEC_EDGE", "0.02"))
QUALITY_MODE = os.environ.get("QUALITY_MODE", "true").lower() == "true"
STRICT_PM_SOURCE = os.environ.get("STRICT_PM_SOURCE", "true").lower() == "true"
MAX_SIGNAL_LATENCY_MS = float(os.environ.get("MAX_SIGNAL_LATENCY_MS", "1200"))
MAX_QUOTE_STALENESS_MS = float(os.environ.get("MAX_QUOTE_STALENESS_MS", "1200"))
MAX_ORDERBOOK_AGE_MS = float(os.environ.get("MAX_ORDERBOOK_AGE_MS", "500"))
MIN_MINS_LEFT_5M = float(os.environ.get("MIN_MINS_LEFT_5M", "0.5"))
MIN_MINS_LEFT_15M = float(os.environ.get("MIN_MINS_LEFT_15M", "4.0"))
LATE_DIR_LOCK_ENABLED = os.environ.get("LATE_DIR_LOCK_ENABLED", "true").lower() == "true"
LATE_DIR_LOCK_MIN_LEFT_5M = float(os.environ.get("LATE_DIR_LOCK_MIN_LEFT_5M", "2.2"))
LATE_DIR_LOCK_MIN_LEFT_15M = float(os.environ.get("LATE_DIR_LOCK_MIN_LEFT_15M", "6.0"))
LATE_DIR_LOCK_MIN_MOVE_PCT = float(os.environ.get("LATE_DIR_LOCK_MIN_MOVE_PCT", "0.0002"))
EVENT_ALIGN_GUARD_ENABLED = os.environ.get("EVENT_ALIGN_GUARD_ENABLED", "true").lower() == "true"
EVENT_ALIGN_MIN_LEFT_15M = float(os.environ.get("EVENT_ALIGN_MIN_LEFT_15M", "10.0"))
EVENT_ALIGN_MIN_MOVE_PCT = float(os.environ.get("EVENT_ALIGN_MIN_MOVE_PCT", "0.0005"))
EVENT_ALIGN_ALLOW_SCORE = int(os.environ.get("EVENT_ALIGN_ALLOW_SCORE", "22"))
EVENT_ALIGN_ALLOW_EV = float(os.environ.get("EVENT_ALIGN_ALLOW_EV", "0.040"))
EVENT_ALIGN_ALLOW_TRUE_PROB = float(os.environ.get("EVENT_ALIGN_ALLOW_TRUE_PROB", "0.84"))
EVENT_ALIGN_CONTRA_SCORE_PEN = int(os.environ.get("EVENT_ALIGN_CONTRA_SCORE_PEN", "3"))
EVENT_ALIGN_CONTRA_EDGE_PEN = float(os.environ.get("EVENT_ALIGN_CONTRA_EDGE_PEN", "0.020"))
EVENT_ALIGN_CONTRA_SIZE_MULT = float(os.environ.get("EVENT_ALIGN_CONTRA_SIZE_MULT", "0.55"))
EVENT_ALIGN_WITH_SCORE_BONUS = int(os.environ.get("EVENT_ALIGN_WITH_SCORE_BONUS", "1"))
EVENT_ALIGN_WITH_EDGE_BONUS = float(os.environ.get("EVENT_ALIGN_WITH_EDGE_BONUS", "0.004"))
CORE_ENTRY_MIN = float(os.environ.get("CORE_ENTRY_MIN", "0.40"))
CORE_ENTRY_MAX = float(os.environ.get("CORE_ENTRY_MAX", "0.60"))
CORE_MIN_SCORE = int(os.environ.get("CORE_MIN_SCORE", "12"))
CORE_MIN_EV = float(os.environ.get("CORE_MIN_EV", "0.020"))
CORE_SIZE_BONUS = float(os.environ.get("CORE_SIZE_BONUS", "1.12"))
CONTRARIAN_ENTRY_MAX = float(os.environ.get("CONTRARIAN_ENTRY_MAX", "0.25"))   # was 0.30; empirical: <20¢ contracts underperform implied odds
CONTRARIAN_SIZE_MULT = float(os.environ.get("CONTRARIAN_SIZE_MULT", "0.35"))   # was 0.55; smaller position for negative-EV-biased entries
CONTRARIAN_STRONG_SCORE = int(os.environ.get("CONTRARIAN_STRONG_SCORE", "22")) # was 20; require very strong signal to counter structural bias
CONTRARIAN_STRONG_EV = float(os.environ.get("CONTRARIAN_STRONG_EV", "0.080"))  # was 0.035; need large edge to overcome empirical underperformance bias
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
    {"inputs":[],"name":"aggregator","outputs":[{"name":"","type":"address"}],
     "stateMutability":"view","type":"function"},
]
# keccak256("AnswerUpdated(int256,uint256,uint256)") — Chainlink aggregator event
CL_ANSWER_UPDATED_TOPIC = "0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f"
SCAN_INTERVAL  = float(os.environ.get("SCAN_INTERVAL", "0.5"))
MARKET_REFRESH_SEC = float(os.environ.get("MARKET_REFRESH_SEC", "30.0"))  # how often to re-fetch Gamma API (was every 0.5s = 600 req/min!)
PING_INTERVAL  = int(os.environ.get("PING_INTERVAL", "5"))
STATUS_INTERVAL= int(os.environ.get("STATUS_INTERVAL", "15"))
ONCHAIN_SYNC_SEC = float(os.environ.get("ONCHAIN_SYNC_SEC", "2.0"))
_DATA_DIR      = os.environ.get("DATA_DIR", os.path.expanduser("~"))
LOG_FILE       = os.path.join(_DATA_DIR, "clawdbot_live_trades.csv")
PENDING_FILE   = os.path.join(_DATA_DIR, "clawdbot_pending.json")
SEEN_FILE      = os.path.join(_DATA_DIR, "clawdbot_seen.json")
STATS_FILE     = os.path.join(_DATA_DIR, "clawdbot_stats.json")
METRICS_FILE   = os.path.join(_DATA_DIR, "clawdbot_onchain_metrics.jsonl")
METRICS_DB_FILE = os.path.join(_DATA_DIR, "clawdbot_metrics.db")
METRICS_DB_IMPORT_MAX_LINES = int(os.environ.get("METRICS_DB_IMPORT_MAX_LINES", "200000"))
RUNTIME_JSON_LOG_FILE = os.path.join(_DATA_DIR, "clawdbot_runtime_logs.jsonl")
PNL_BASELINE_FILE = os.path.join(_DATA_DIR, "clawdbot_pnl_baseline.json")
SETTLED_FILE   = os.path.join(_DATA_DIR, "clawdbot_settled_cids.json")
PRICE_CACHE_FILE = os.path.join(_DATA_DIR, "clawdbot_price_cache.json")
PRICE_CACHE_SAVE_SEC = float(os.environ.get("PRICE_CACHE_SAVE_SEC", "20"))
PRICE_CACHE_POINTS = int(os.environ.get("PRICE_CACHE_POINTS", "300"))
OPEN_STATE_CACHE_FILE = os.path.join(_DATA_DIR, "clawdbot_open_state_cache.json")
OPEN_STATE_CACHE_SAVE_SEC = float(os.environ.get("OPEN_STATE_CACHE_SAVE_SEC", "15"))
DASHBOARD_STRICT_CANONICAL = os.environ.get("DASHBOARD_STRICT_CANONICAL", "true").lower() == "true"
DASHBOARD_FALLBACK_DEBUG = os.environ.get("DASHBOARD_FALLBACK_DEBUG", "false").lower() == "true"
CID_MARKET_CACHE_MAX = int(os.environ.get("CID_MARKET_CACHE_MAX", "1200"))
CID_MARKET_CACHE_TTL_SEC = float(os.environ.get("CID_MARKET_CACHE_TTL_SEC", str(7 * 86400)))
AUTOPILOT_ENABLED = os.environ.get("AUTOPILOT_ENABLED", "true").lower() == "true"
AUTOPILOT_CHECK_SEC = float(os.environ.get("AUTOPILOT_CHECK_SEC", "12"))
AUTOPILOT_POLICY_FILE = os.environ.get("AUTOPILOT_POLICY_FILE", os.path.join(_DATA_DIR, "clawdbot_autopilot_policy.json"))
PRED_AGENT_ENABLED = os.environ.get("PRED_AGENT_ENABLED", "true").lower() == "true"
PRED_AGENT_MIN_SAMPLES = int(os.environ.get("PRED_AGENT_MIN_SAMPLES", "24"))
PNL_BASELINE_RESET_ON_BOOT = os.environ.get("PNL_BASELINE_RESET_ON_BOOT", "false").lower() == "true"
LOSS_STREAK_PAUSE_ENABLED = os.environ.get("LOSS_STREAK_PAUSE_ENABLED", "false").lower() == "true"
LOSS_STREAK_PAUSE_N = int(os.environ.get("LOSS_STREAK_PAUSE_N", "3"))     # tightened 4→3
LOSS_STREAK_PAUSE_SEC = float(os.environ.get("LOSS_STREAK_PAUSE_SEC", "1800"))  # 900→1800s
MACRO_BLACKOUT_ENABLED    = os.environ.get("MACRO_BLACKOUT_ENABLED", "true").lower() == "true"
MACRO_BLACKOUT_FILE       = os.environ.get("MACRO_BLACKOUT_FILE", "/data/macro_events.json")
MACRO_BLACKOUT_MINS_BEFORE = float(os.environ.get("MACRO_BLACKOUT_MINS_BEFORE", "5"))   # skip 5 min before event
MACRO_BLACKOUT_MINS_AFTER  = float(os.environ.get("MACRO_BLACKOUT_MINS_AFTER",  "10"))  # skip 10 min after event
RUNTIME_JSON_LOG_ENABLED = os.environ.get("RUNTIME_JSON_LOG_ENABLED", "true").lower() == "true"
RUNTIME_JSON_LOG_ROTATE_DAILY = os.environ.get("RUNTIME_JSON_LOG_ROTATE_DAILY", "true").lower() == "true"

DRY_RUN   = os.environ.get("DRY_RUN", "true").lower() == "true"
SHOW_DASHBOARD_FALLBACK = os.environ.get("SHOW_DASHBOARD_FALLBACK", "false").lower() == "true"
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
BOOK_CACHE_TTL_MS = float(os.environ.get("BOOK_CACHE_TTL_MS", "450"))  # ~scan_interval to avoid double-fetch
BOOK_CACHE_MAX = int(os.environ.get("BOOK_CACHE_MAX", "256"))
BOOK_FETCH_CONCURRENCY = int(os.environ.get("BOOK_FETCH_CONCURRENCY", "16"))
CLOB_HEARTBEAT_SEC = float(os.environ.get("CLOB_HEARTBEAT_SEC", "6"))
USER_EVENTS_ENABLED = os.environ.get("USER_EVENTS_ENABLED", "true").lower() == "true"
USER_EVENTS_POLL_SEC = float(os.environ.get("USER_EVENTS_POLL_SEC", "0.8"))
USER_EVENTS_CACHE_TTL_SEC = float(os.environ.get("USER_EVENTS_CACHE_TTL_SEC", "180"))
CLOB_MARKET_WS_ENABLED = os.environ.get("CLOB_MARKET_WS_ENABLED", "true").lower() == "true"
CLOB_MARKET_WS_SYNC_SEC = float(os.environ.get("CLOB_MARKET_WS_SYNC_SEC", "2.0"))
CLOB_MARKET_WS_MAX_AGE_MS = float(os.environ.get("CLOB_MARKET_WS_MAX_AGE_MS", "8000"))
CLOB_MARKET_WS_SOFT_AGE_MS = float(os.environ.get("CLOB_MARKET_WS_SOFT_AGE_MS", "20000"))
WS_STRICT_ADAPTIVE_ENABLED = os.environ.get("WS_STRICT_ADAPTIVE_ENABLED", "true").lower() == "true"
WS_STRICT_ADAPTIVE_MULT = float(os.environ.get("WS_STRICT_ADAPTIVE_MULT", "1.35"))
WS_STRICT_ADAPTIVE_MIN_MS = float(os.environ.get("WS_STRICT_ADAPTIVE_MIN_MS", "5000"))
WS_STRICT_ADAPTIVE_MAX_MS = float(os.environ.get("WS_STRICT_ADAPTIVE_MAX_MS", "20000"))
CLOB_REST_FALLBACK_ENABLED = os.environ.get("CLOB_REST_FALLBACK_ENABLED", "false").lower() == "true"
CLOB_REST_FRESH_MAX_AGE_MS = float(os.environ.get("CLOB_REST_FRESH_MAX_AGE_MS", "4000"))  # was 1500; REST scan cycle can be 2-3s
CLOB_WS_STALE_HEAL_HITS = int(os.environ.get("CLOB_WS_STALE_HEAL_HITS", "1"))         # was 3; detect in 0.5s not 1.5s
CLOB_WS_STALE_HEAL_COOLDOWN_SEC = float(os.environ.get("CLOB_WS_STALE_HEAL_COOLDOWN_SEC", "10"))  # was 20
COPYFLOW_FILE = os.environ.get("COPYFLOW_FILE", os.path.join(_DATA_DIR, "clawdbot_copyflow.json"))
COPYFLOW_RELOAD_SEC = int(os.environ.get("COPYFLOW_RELOAD_SEC", "20"))
COPYFLOW_BONUS_MAX = int(os.environ.get("COPYFLOW_BONUS_MAX", "2"))
COPYFLOW_REFRESH_ENABLED = os.environ.get("COPYFLOW_REFRESH_ENABLED", "true").lower() == "true"
COPYFLOW_REFRESH_SEC = int(os.environ.get("COPYFLOW_REFRESH_SEC", "300"))
COPYFLOW_MIN_ROI = float(os.environ.get("COPYFLOW_MIN_ROI", "-0.03"))
COPYFLOW_LIVE_ENABLED = os.environ.get("COPYFLOW_LIVE_ENABLED", "true").lower() == "true"
COPYFLOW_LIVE_REFRESH_SEC = int(os.environ.get("COPYFLOW_LIVE_REFRESH_SEC", "2"))   # was 5
COPYFLOW_LIVE_TRADES_LIMIT = int(os.environ.get("COPYFLOW_LIVE_TRADES_LIMIT", "80"))
COPYFLOW_LIVE_MAX_MARKETS = int(os.environ.get("COPYFLOW_LIVE_MAX_MARKETS", "8"))
COPYFLOW_LIVE_MAX_AGE_SEC = float(os.environ.get("COPYFLOW_LIVE_MAX_AGE_SEC", "180"))
COPYFLOW_FETCH_RETRIES = int(os.environ.get("COPYFLOW_FETCH_RETRIES", "3"))
COPYFLOW_CID_ONDEMAND_ENABLED = os.environ.get("COPYFLOW_CID_ONDEMAND_ENABLED", "true").lower() == "true"
COPYFLOW_CID_ONDEMAND_COOLDOWN_SEC = float(os.environ.get("COPYFLOW_CID_ONDEMAND_COOLDOWN_SEC", "4"))
COPYFLOW_HEALTH_FORCE_REFRESH_SEC = float(os.environ.get("COPYFLOW_HEALTH_FORCE_REFRESH_SEC", "12"))
LOG_HEALTH_EVERY_SEC = int(os.environ.get("LOG_HEALTH_EVERY_SEC", "30"))
COPYFLOW_INTEL_ENABLED = os.environ.get("COPYFLOW_INTEL_ENABLED", "true").lower() == "true"
COPYFLOW_INTEL_REFRESH_SEC = int(os.environ.get("COPYFLOW_INTEL_REFRESH_SEC", "30"))  # was 60
COPYFLOW_INTEL_TRADES_PER_MARKET = int(os.environ.get("COPYFLOW_INTEL_TRADES_PER_MARKET", "120"))
COPYFLOW_INTEL_MAX_WALLETS = int(os.environ.get("COPYFLOW_INTEL_MAX_WALLETS", "40"))
COPYFLOW_INTEL_ACTIVITY_LIMIT = int(os.environ.get("COPYFLOW_INTEL_ACTIVITY_LIMIT", "250"))
COPYFLOW_INTEL_MIN_SETTLED = int(os.environ.get("COPYFLOW_INTEL_MIN_SETTLED", "25"))    # was 12 — need real sample
COPYFLOW_INTEL_MIN_ROI     = float(os.environ.get("COPYFLOW_INTEL_MIN_ROI", "0.10"))    # was 0.02 — require 10% ROI
COPYFLOW_INTEL_MIN_WR      = float(os.environ.get("COPYFLOW_INTEL_MIN_WR", "0.52"))     # require >50% win rate
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
WS_BOOK_FALLBACK_MAX_AGE_MS = float(os.environ.get("WS_BOOK_FALLBACK_MAX_AGE_MS", "6000"))         # was 2500
PM_BOOK_FALLBACK_ENABLED = os.environ.get("PM_BOOK_FALLBACK_ENABLED", "false").lower() == "true"
LEADER_FLOW_FALLBACK_ENABLED = os.environ.get("LEADER_FLOW_FALLBACK_ENABLED", "true").lower() == "true"
LEADER_FLOW_FALLBACK_MAX_AGE_SEC = float(os.environ.get("LEADER_FLOW_FALLBACK_MAX_AGE_SEC", "90"))
REQUIRE_VOLUME_SIGNAL = os.environ.get("REQUIRE_VOLUME_SIGNAL", "true").lower() == "true"
STRICT_REQUIRE_FRESH_LEADER = os.environ.get("STRICT_REQUIRE_FRESH_LEADER", "false").lower() == "true"
STRICT_REQUIRE_FRESH_BOOK_WS = os.environ.get("STRICT_REQUIRE_FRESH_BOOK_WS", "true").lower() == "true"
WS_BOOK_SOFT_MAX_AGE_MS = float(os.environ.get("WS_BOOK_SOFT_MAX_AGE_MS", "12000"))
ANALYSIS_PROB_SCALE_MIN = float(os.environ.get("ANALYSIS_PROB_SCALE_MIN", "0.65"))
ANALYSIS_PROB_SCALE_MAX = float(os.environ.get("ANALYSIS_PROB_SCALE_MAX", "1.20"))
# Small tolerance for payout threshold to avoid dead-zone misses (e.g. 1.98x vs 2.00x).
PAYOUT_NEAR_MISS_TOL = float(os.environ.get("PAYOUT_NEAR_MISS_TOL", "0.03"))
ADAPTIVE_PAYOUT_MAX_UPSHIFT_15M = float(os.environ.get("ADAPTIVE_PAYOUT_MAX_UPSHIFT_15M", "0.02"))
ADAPTIVE_PAYOUT_MAX_UPSHIFT_5M = float(os.environ.get("ADAPTIVE_PAYOUT_MAX_UPSHIFT_5M", "0.05"))
# Mid-round booster (15m only): small additive bet at high payout/high conviction.
MID_BOOSTER_ENABLED = os.environ.get("MID_BOOSTER_ENABLED", "true").lower() == "true"
MID_BOOSTER_ANYTIME_15M = os.environ.get("MID_BOOSTER_ANYTIME_15M", "true").lower() == "true"
MID_BOOSTER_MIN_LEFT_15M = float(os.environ.get("MID_BOOSTER_MIN_LEFT_15M", "6.0"))
MID_BOOSTER_MAX_LEFT_15M = float(os.environ.get("MID_BOOSTER_MAX_LEFT_15M", "10.5"))
MID_BOOSTER_MIN_LEFT_HARD_15M = float(os.environ.get("MID_BOOSTER_MIN_LEFT_HARD_15M", "1.6"))
MID_BOOSTER_MIN_SCORE = int(os.environ.get("MID_BOOSTER_MIN_SCORE", "10"))
MID_BOOSTER_MIN_TRUE_PROB = float(os.environ.get("MID_BOOSTER_MIN_TRUE_PROB", "0.57"))
MID_BOOSTER_MIN_EDGE = float(os.environ.get("MID_BOOSTER_MIN_EDGE", "0.018"))
MID_BOOSTER_MIN_EV_NET = float(os.environ.get("MID_BOOSTER_MIN_EV_NET", "0.028"))
MID_BOOSTER_MIN_PAYOUT = float(os.environ.get("MID_BOOSTER_MIN_PAYOUT", "2.20"))
MID_BOOSTER_MAX_ENTRY = float(os.environ.get("MID_BOOSTER_MAX_ENTRY", "0.50"))
MID_BOOSTER_SIZE_PCT = float(os.environ.get("MID_BOOSTER_SIZE_PCT", "0.012"))
MID_BOOSTER_SIZE_PCT_HIGH = float(os.environ.get("MID_BOOSTER_SIZE_PCT_HIGH", "0.030"))
MID_BOOSTER_MAX_PER_CID = int(os.environ.get("MID_BOOSTER_MAX_PER_CID", "2"))
MID_BOOSTER_LOSS_STREAK_LOCK = int(os.environ.get("MID_BOOSTER_LOSS_STREAK_LOCK", "4"))
MID_BOOSTER_LOCK_HOURS = float(os.environ.get("MID_BOOSTER_LOCK_HOURS", "24"))
CONTINUATION_PRIOR_MAX_BOOST = float(os.environ.get("CONTINUATION_PRIOR_MAX_BOOST", "0.06"))
LOW_CENT_ONLY_ON_EXISTING_POSITION = os.environ.get("LOW_CENT_ONLY_ON_EXISTING_POSITION", "false").lower() == "true"
LOW_CENT_ENTRY_THRESHOLD = float(os.environ.get("LOW_CENT_ENTRY_THRESHOLD", "0.10"))
# Dynamic sizing guardrail for cheap-entry tails:
# - keep default risk around LOW_ENTRY_BASE_SOFT_MAX
# - allow growth toward LOW_ENTRY_HIGH_CONV_SOFT_MAX only on strong conviction
LOW_ENTRY_SOFT_THRESHOLD = float(os.environ.get("LOW_ENTRY_SOFT_THRESHOLD", "0.35"))
LOW_ENTRY_BASE_SOFT_MAX = float(os.environ.get("LOW_ENTRY_BASE_SOFT_MAX", "10.0"))
LOW_ENTRY_HIGH_CONV_SOFT_MAX = float(os.environ.get("LOW_ENTRY_HIGH_CONV_SOFT_MAX", "30.0"))
PREBID_ARM_ENABLED = os.environ.get("PREBID_ARM_ENABLED", "true").lower() == "true"
PREBID_MIN_CONF = float(os.environ.get("PREBID_MIN_CONF", "0.54"))   # was 0.58; allow more pre-bids
PREBID_ARM_WINDOW_SEC = float(os.environ.get("PREBID_ARM_WINDOW_SEC", "30"))
NEXT_MARKET_ANALYSIS_ENABLED = os.environ.get("NEXT_MARKET_ANALYSIS_ENABLED", "true").lower() == "true"
NEXT_MARKET_ANALYSIS_WINDOW_SEC = float(os.environ.get("NEXT_MARKET_ANALYSIS_WINDOW_SEC", "180"))
NEXT_MARKET_ANALYSIS_LOG_EVERY_SEC = float(os.environ.get("NEXT_MARKET_ANALYSIS_LOG_EVERY_SEC", "15"))
NEXT_MARKET_ANALYSIS_MIN_CONF = float(os.environ.get("NEXT_MARKET_ANALYSIS_MIN_CONF", "0.54"))  # was 0.56
# Safety default: keep 5m disabled unless explicitly force-enabled.
FORCE_DISABLE_5M = os.environ.get("FORCE_DISABLE_5M", "true").lower() == "true"
ENABLE_5M = (os.environ.get("ENABLE_5M", "false").lower() == "true") and (not FORCE_DISABLE_5M)
FIVE_M_RUNTIME_GUARD_ENABLED = os.environ.get("FIVE_M_RUNTIME_GUARD_ENABLED", "true").lower() == "true"
FIVE_M_GUARD_WINDOW = int(os.environ.get("FIVE_M_GUARD_WINDOW", "20"))
FIVE_M_GUARD_DISABLE_MIN_OUTCOMES = int(os.environ.get("FIVE_M_GUARD_DISABLE_MIN_OUTCOMES", "12"))
FIVE_M_GUARD_DISABLE_PF = float(os.environ.get("FIVE_M_GUARD_DISABLE_PF", "1.00"))
FIVE_M_GUARD_DISABLE_WR = float(os.environ.get("FIVE_M_GUARD_DISABLE_WR", "0.45"))
FIVE_M_GUARD_REENABLE_MIN_OUTCOMES = int(os.environ.get("FIVE_M_GUARD_REENABLE_MIN_OUTCOMES", "16"))
FIVE_M_GUARD_REENABLE_PF = float(os.environ.get("FIVE_M_GUARD_REENABLE_PF", "1.10"))
FIVE_M_GUARD_REENABLE_WR = float(os.environ.get("FIVE_M_GUARD_REENABLE_WR", "0.52"))
FIVE_M_GUARD_MIN_DISABLE_SEC = float(os.environ.get("FIVE_M_GUARD_MIN_DISABLE_SEC", "900"))
FIVE_M_GUARD_CHECK_EVERY_SEC = float(os.environ.get("FIVE_M_GUARD_CHECK_EVERY_SEC", "20"))
FIVE_M_DYNAMIC_SCORE_ENABLED = os.environ.get("FIVE_M_DYNAMIC_SCORE_ENABLED", "true").lower() == "true"
FIVE_M_DYNAMIC_SCORE_MIN_OUTCOMES = int(os.environ.get("FIVE_M_DYNAMIC_SCORE_MIN_OUTCOMES", "8"))
FIVE_M_DYNAMIC_SCORE_BAD_WR = float(os.environ.get("FIVE_M_DYNAMIC_SCORE_BAD_WR", "0.45"))
FIVE_M_DYNAMIC_SCORE_BAD_PF = float(os.environ.get("FIVE_M_DYNAMIC_SCORE_BAD_PF", "1.00"))
FIVE_M_DYNAMIC_SCORE_WORSE_WR = float(os.environ.get("FIVE_M_DYNAMIC_SCORE_WORSE_WR", "0.40"))
FIVE_M_DYNAMIC_SCORE_WORSE_PF = float(os.environ.get("FIVE_M_DYNAMIC_SCORE_WORSE_PF", "0.85"))
FIVE_M_DYNAMIC_SCORE_ADD_BAD = int(os.environ.get("FIVE_M_DYNAMIC_SCORE_ADD_BAD", "2"))
FIVE_M_DYNAMIC_SCORE_ADD_WORSE = int(os.environ.get("FIVE_M_DYNAMIC_SCORE_ADD_WORSE", "3"))
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
EXEC_SPEED_PRIORITY_ENABLED = os.environ.get("EXEC_SPEED_PRIORITY_ENABLED", "true").lower() == "true"
FAST_TAKER_SPEED_SCORE_5M = int(os.environ.get("FAST_TAKER_SPEED_SCORE_5M", "9"))
FAST_TAKER_SPEED_SCORE_15M = int(os.environ.get("FAST_TAKER_SPEED_SCORE_15M", "10"))
FAST_PATH_MAX_BOOK_AGE_MS = float(os.environ.get("FAST_PATH_MAX_BOOK_AGE_MS", "1300"))
MAX_MAKER_HOLD_5M_SEC = float(os.environ.get("MAX_MAKER_HOLD_5M_SEC", "0.28"))
MAX_MAKER_HOLD_15M_SEC = float(os.environ.get("MAX_MAKER_HOLD_15M_SEC", "0.42"))
ORDER_LATENCY_LOG_ENABLED = os.environ.get("ORDER_LATENCY_LOG_ENABLED", "true").lower() == "true"
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
CONSISTENCY_CORE_ENABLED = os.environ.get("CONSISTENCY_CORE_ENABLED", "false").lower() == "true"
CONSISTENCY_REQUIRE_CL_AGREE_15M = os.environ.get("CONSISTENCY_REQUIRE_CL_AGREE_15M", "false").lower() == "true"
CONSISTENCY_MIN_TRUE_PROB_15M = float(os.environ.get("CONSISTENCY_MIN_TRUE_PROB_15M", "0.54"))
CONSISTENCY_MIN_EXEC_EV_15M = float(os.environ.get("CONSISTENCY_MIN_EXEC_EV_15M", "0.010"))
CONSISTENCY_MAX_ENTRY_15M = float(os.environ.get("CONSISTENCY_MAX_ENTRY_15M", "0.60"))
CONSISTENCY_MIN_PAYOUT_15M = float(os.environ.get("CONSISTENCY_MIN_PAYOUT_15M", "1.72"))
EV_FRONTIER_ENABLED = os.environ.get("EV_FRONTIER_ENABLED", "false").lower() == "true"
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
SUPER_BET_FLOOR_MIN_SCORE = int(os.environ.get("SUPER_BET_FLOOR_MIN_SCORE", "18"))
SUPER_BET_FLOOR_MIN_EV = float(os.environ.get("SUPER_BET_FLOOR_MIN_EV", "0.030"))
SUPER_BET_COOLDOWN_SEC = float(os.environ.get("SUPER_BET_COOLDOWN_SEC", "420"))
SUPER_BET_MAX_SIZE_ENABLED = os.environ.get("SUPER_BET_MAX_SIZE_ENABLED", "true").lower() == "true"
SUPER_BET_MAX_SIZE_USDC = float(os.environ.get("SUPER_BET_MAX_SIZE_USDC", "12.0"))    # raised 10→12
SUPER_BET_MAX_BANKROLL_PCT = float(os.environ.get("SUPER_BET_MAX_BANKROLL_PCT", "0.035"))  # 2%→3.5% (allows $7 on $200)
# High-payout path: 10x+ entries (≤10¢) can bypass the 2/3-rule prob gate
HIGHPAYOUT_MIN_PAYOUT = float(os.environ.get("HIGHPAYOUT_MIN_PAYOUT", "10.0"))  # ≥10x payout
HIGHPAYOUT_MIN_SCORE  = int(os.environ.get("HIGHPAYOUT_MIN_SCORE",  "15"))      # strong signal required
HIGHPAYOUT_MIN_PROB   = float(os.environ.get("HIGHPAYOUT_MIN_PROB",  "0.45"))   # model must say ≥45% (vs market's ≤10%)
HIGHPAYOUT_MIN_EDGE   = float(os.environ.get("HIGHPAYOUT_MIN_EDGE",  "0.06"))   # ≥6% execution edge
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
ASSET_ENTRY_PRIOR_ENABLED = os.environ.get("ASSET_ENTRY_PRIOR_ENABLED", "true").lower() == "true"
ASSET_ENTRY_PRIOR_MIN_N = int(os.environ.get("ASSET_ENTRY_PRIOR_MIN_N", "12"))
ASSET_ENTRY_PRIOR_LOOKBACK_H = float(os.environ.get("ASSET_ENTRY_PRIOR_LOOKBACK_H", "48"))
ASSET_ENTRY_PRIOR_MAX_PROB_ADJ = float(os.environ.get("ASSET_ENTRY_PRIOR_MAX_PROB_ADJ", "0.018"))
ASSET_ENTRY_PRIOR_MAX_EDGE_ADJ = float(os.environ.get("ASSET_ENTRY_PRIOR_MAX_EDGE_ADJ", "0.012"))
ASSET_ENTRY_PRIOR_MAX_SCORE_ADJ = int(os.environ.get("ASSET_ENTRY_PRIOR_MAX_SCORE_ADJ", "3"))
ASSET_ENTRY_PRIOR_MIN_SIZE_MULT = float(os.environ.get("ASSET_ENTRY_PRIOR_MIN_SIZE_MULT", "0.55"))
ASSET_ENTRY_PRIOR_MAX_SIZE_MULT = float(os.environ.get("ASSET_ENTRY_PRIOR_MAX_SIZE_MULT", "1.20"))
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
# Side/alignment and analysis model knobs (all env-tunable to avoid fixed literals in decision path).
LEADER_ENTRY_CAP_HARD_MAX = float(os.environ.get("LEADER_ENTRY_CAP_HARD_MAX", "0.97"))
LEADER_ENTRY_CAP_EXTRA = float(os.environ.get("LEADER_ENTRY_CAP_EXTRA", "0.03"))
COPY_NET_EDGE_MULT = float(os.environ.get("COPY_NET_EDGE_MULT", "0.01"))
LEADER_STYLE_HIGH_SHARE_MIN = float(os.environ.get("LEADER_STYLE_HIGH_SHARE_MIN", "0.60"))
LEADER_STYLE_LOW_SHARE_MIN = float(os.environ.get("LEADER_STYLE_LOW_SHARE_MIN", "0.60"))
LEADER_STYLE_MULTIBET_MIN = float(os.environ.get("LEADER_STYLE_MULTIBET_MIN", "0.30"))
LEADER_STYLE_EXPENSIVE_ENTRY_MIN = float(os.environ.get("LEADER_STYLE_EXPENSIVE_ENTRY_MIN", "0.55"))
LEADER_STYLE_SCORE_BONUS = int(os.environ.get("LEADER_STYLE_SCORE_BONUS", "1"))
LEADER_STYLE_SCORE_PENALTY = int(os.environ.get("LEADER_STYLE_SCORE_PENALTY", "1"))
LEADER_STYLE_EDGE_BONUS = float(os.environ.get("LEADER_STYLE_EDGE_BONUS", "0.005"))
LEADER_STYLE_EDGE_PENALTY = float(os.environ.get("LEADER_STYLE_EDGE_PENALTY", "0.005"))
PM_PUBLIC_OCROWD_AVG_C_MIN = float(os.environ.get("PM_PUBLIC_OCROWD_AVG_C_MIN", "85.0"))
LEADER_NOFLOW_SIZE_SCALE = float(os.environ.get("LEADER_NOFLOW_SIZE_SCALE", "0.90"))
LEADER_FORCE_QUALITY_GUARD_ENABLED = os.environ.get("LEADER_FORCE_QUALITY_GUARD_ENABLED", "true").lower() == "true"
LEADER_FORCE_MIN_WR_LB = float(os.environ.get("LEADER_FORCE_MIN_WR_LB", "0.44"))
LEADER_FORCE_MIN_PF = float(os.environ.get("LEADER_FORCE_MIN_PF", "0.90"))
LEADER_FORCE_MIN_EXP = float(os.environ.get("LEADER_FORCE_MIN_EXP", "-0.10"))
LEADER_FORCE_MIN_N = int(os.environ.get("LEADER_FORCE_MIN_N", "12"))
IMBALANCE_CONFIRM_MIN = float(os.environ.get("IMBALANCE_CONFIRM_MIN", "0.10"))
ANALYSIS_CL_FRESH_MAX_AGE_SEC = float(os.environ.get("ANALYSIS_CL_FRESH_MAX_AGE_SEC", "35.0"))
ANALYSIS_QUOTE_FRESH_MAX_MS = float(os.environ.get("ANALYSIS_QUOTE_FRESH_MAX_MS", "1200.0"))
ANALYSIS_QUALITY_WS_WEIGHT = float(os.environ.get("ANALYSIS_QUALITY_WS_WEIGHT", "0.25"))
ANALYSIS_QUALITY_REST_WEIGHT = float(os.environ.get("ANALYSIS_QUALITY_REST_WEIGHT", "0.25"))  # was 0.22; REST at 0ms age = same quality as WS
ANALYSIS_QUALITY_LEADER_WEIGHT = float(os.environ.get("ANALYSIS_QUALITY_LEADER_WEIGHT", "0.20"))
ANALYSIS_QUALITY_CL_WEIGHT = float(os.environ.get("ANALYSIS_QUALITY_CL_WEIGHT", "0.20"))
ANALYSIS_QUALITY_QUOTE_WEIGHT = float(os.environ.get("ANALYSIS_QUALITY_QUOTE_WEIGHT", "0.15"))
ANALYSIS_QUALITY_VOL_WEIGHT = float(os.environ.get("ANALYSIS_QUALITY_VOL_WEIGHT", "0.20"))
ANALYSIS_OB_OFFSET = float(os.environ.get("ANALYSIS_OB_OFFSET", "0.10"))
ANALYSIS_OB_SCALE = float(os.environ.get("ANALYSIS_OB_SCALE", "0.30"))
ANALYSIS_TAKER_OFFSET = float(os.environ.get("ANALYSIS_TAKER_OFFSET", "0.05"))
ANALYSIS_TAKER_SCALE = float(os.environ.get("ANALYSIS_TAKER_SCALE", "0.18"))
ANALYSIS_TF_BASE = float(os.environ.get("ANALYSIS_TF_BASE", "1.0"))
ANALYSIS_TF_SCALE = float(os.environ.get("ANALYSIS_TF_SCALE", "3.0"))
ANALYSIS_BASIS_OFFSET = float(os.environ.get("ANALYSIS_BASIS_OFFSET", "0.0002"))
ANALYSIS_BASIS_SCALE = float(os.environ.get("ANALYSIS_BASIS_SCALE", "0.0010"))
ANALYSIS_VWAP_OFFSET = float(os.environ.get("ANALYSIS_VWAP_OFFSET", "0.0008"))
ANALYSIS_VWAP_SCALE = float(os.environ.get("ANALYSIS_VWAP_SCALE", "0.0024"))
ANALYSIS_LEADER_BASE = float(os.environ.get("ANALYSIS_LEADER_BASE", "0.5"))
ANALYSIS_LEADER_OFFSET = float(os.environ.get("ANALYSIS_LEADER_OFFSET", "0.12"))
ANALYSIS_LEADER_SCALE = float(os.environ.get("ANALYSIS_LEADER_SCALE", "0.34"))
ANALYSIS_CONV_W_OB     = float(os.environ.get("ANALYSIS_CONV_W_OB",     "0.16"))
ANALYSIS_CONV_W_TK     = float(os.environ.get("ANALYSIS_CONV_W_TK",     "0.16"))
ANALYSIS_CONV_W_TF     = float(os.environ.get("ANALYSIS_CONV_W_TF",     "0.16"))
ANALYSIS_CONV_W_BASIS  = float(os.environ.get("ANALYSIS_CONV_W_BASIS",  "0.05"))
ANALYSIS_CONV_W_VWAP   = float(os.environ.get("ANALYSIS_CONV_W_VWAP",   "0.05"))
ANALYSIS_CONV_W_CL     = float(os.environ.get("ANALYSIS_CONV_W_CL",     "0.20"))  # doubled: momentum is the strongest 15m signal
ANALYSIS_CONV_W_LEADER = float(os.environ.get("ANALYSIS_CONV_W_LEADER", "0.10"))
ANALYSIS_CONV_W_BINARY = float(os.environ.get("ANALYSIS_CONV_W_BINARY", "0.12"))
PROB_CLAMP_MIN = float(os.environ.get("PROB_CLAMP_MIN", "0.05"))
PROB_CLAMP_MAX = float(os.environ.get("PROB_CLAMP_MAX", "0.95"))
PROB_REBALANCE_MIN = float(os.environ.get("PROB_REBALANCE_MIN", "0.01"))
PROB_REBALANCE_MAX = float(os.environ.get("PROB_REBALANCE_MAX", "0.99"))
PREBID_SCORE_BONUS = int(os.environ.get("PREBID_SCORE_BONUS", "2"))
PREBID_EDGE_MULT = float(os.environ.get("PREBID_EDGE_MULT", "0.05"))
LOWCENT_NEW_MIN_SCORE = int(os.environ.get("LOWCENT_NEW_MIN_SCORE", "15"))       # lowered 18→15
LOWCENT_NEW_MIN_TRUE_PROB = float(os.environ.get("LOWCENT_NEW_MIN_TRUE_PROB", "0.60"))  # lowered 0.74→0.60
LOWCENT_NEW_MIN_EXEC_EV = float(os.environ.get("LOWCENT_NEW_MIN_EXEC_EV", "0.05"))     # raised 0.035→0.05
LOWCENT_NEW_MIN_PAYOUT = float(os.environ.get("LOWCENT_NEW_MIN_PAYOUT", "3.0"))
ENTRY_TIER_SCORE_HIGH = int(os.environ.get("ENTRY_TIER_SCORE_HIGH", "12"))
ENTRY_TIER_SCORE_MID = int(os.environ.get("ENTRY_TIER_SCORE_MID", "8"))
ORACLE_SCALE_DISAGREE_FRESH = float(os.environ.get("ORACLE_SCALE_DISAGREE_FRESH", "0.60"))
ORACLE_SCALE_DISAGREE_STALE = float(os.environ.get("ORACLE_SCALE_DISAGREE_STALE", "0.80"))
ENTRY_CENTS_SCALE_3C = float(os.environ.get("ENTRY_CENTS_SCALE_3C", "0.12"))
ENTRY_CENTS_SCALE_5C = float(os.environ.get("ENTRY_CENTS_SCALE_5C", "0.18"))
ENTRY_CENTS_SCALE_10C = float(os.environ.get("ENTRY_CENTS_SCALE_10C", "0.28"))
ENTRY_CENTS_SCALE_20C = float(os.environ.get("ENTRY_CENTS_SCALE_20C", "0.45"))
TIME_SCALE_LATE_2_5 = float(os.environ.get("TIME_SCALE_LATE_2_5", "0.20"))
TIME_SCALE_LATE_3_5 = float(os.environ.get("TIME_SCALE_LATE_3_5", "0.35"))
TIME_SCALE_LATE_5_0 = float(os.environ.get("TIME_SCALE_LATE_5_0", "0.55"))
MAX_SINGLE_ABS_CAP = float(os.environ.get("MAX_SINGLE_ABS_CAP", "100.0"))
MIN_HARD_CAP_USDC = float(os.environ.get("MIN_HARD_CAP_USDC", "0.50"))
TAIL_CAP_ENTRY_1 = float(os.environ.get("TAIL_CAP_ENTRY_1", "0.05"))
TAIL_CAP_ENTRY_2 = float(os.environ.get("TAIL_CAP_ENTRY_2", "0.10"))
TAIL_CAP_BANKROLL_PCT_1 = float(os.environ.get("TAIL_CAP_BANKROLL_PCT_1", "0.015"))
TAIL_CAP_BANKROLL_PCT_2 = float(os.environ.get("TAIL_CAP_BANKROLL_PCT_2", "0.020"))
SOFTCAP_SCORE_BASE = float(os.environ.get("SOFTCAP_SCORE_BASE", "8.0"))
SOFTCAP_SCORE_SCALE = float(os.environ.get("SOFTCAP_SCORE_SCALE", "10.0"))
SOFTCAP_PROB_BASE = float(os.environ.get("SOFTCAP_PROB_BASE", "0.56"))
SOFTCAP_PROB_SCALE = float(os.environ.get("SOFTCAP_PROB_SCALE", "0.20"))
SOFTCAP_EDGE_BASE = float(os.environ.get("SOFTCAP_EDGE_BASE", "0.02"))
SOFTCAP_EDGE_SCALE = float(os.environ.get("SOFTCAP_EDGE_SCALE", "0.12"))
SOFTCAP_LEADER_SCALE = float(os.environ.get("SOFTCAP_LEADER_SCALE", "0.40"))
SOFTCAP_W_SCORE = float(os.environ.get("SOFTCAP_W_SCORE", "0.35"))
SOFTCAP_W_PROB = float(os.environ.get("SOFTCAP_W_PROB", "0.35"))
SOFTCAP_W_EDGE = float(os.environ.get("SOFTCAP_W_EDGE", "0.20"))
SOFTCAP_W_LEADER = float(os.environ.get("SOFTCAP_W_LEADER", "0.10"))
NOLEADER_ENTRY_MAX = float(os.environ.get("NOLEADER_ENTRY_MAX", "0.40"))
NOLEADER_SCORE_BASE = float(os.environ.get("NOLEADER_SCORE_BASE", "9.0"))
NOLEADER_SCORE_SCALE = float(os.environ.get("NOLEADER_SCORE_SCALE", "9.0"))
NOLEADER_PROB_BASE = float(os.environ.get("NOLEADER_PROB_BASE", "0.58"))
NOLEADER_PROB_SCALE = float(os.environ.get("NOLEADER_PROB_SCALE", "0.20"))
NOLEADER_EDGE_BASE = float(os.environ.get("NOLEADER_EDGE_BASE", "0.02"))
NOLEADER_EDGE_SCALE = float(os.environ.get("NOLEADER_EDGE_SCALE", "0.10"))
NOLEADER_W_SCORE = float(os.environ.get("NOLEADER_W_SCORE", "0.45"))
NOLEADER_W_PROB = float(os.environ.get("NOLEADER_W_PROB", "0.35"))
NOLEADER_W_EDGE = float(os.environ.get("NOLEADER_W_EDGE", "0.20"))
NOLEADER_CAP_BASE = float(os.environ.get("NOLEADER_CAP_BASE", "6.0"))
NOLEADER_CAP_RANGE = float(os.environ.get("NOLEADER_CAP_RANGE", "6.0"))
NOLEADER_NEAR_END_MIN_LEFT = float(os.environ.get("NOLEADER_NEAR_END_MIN_LEFT", "3.5"))
NOLEADER_NEAR_END_CAP = float(os.environ.get("NOLEADER_NEAR_END_CAP", "8.0"))
HIGH_EV_ENTRY_MAX = float(os.environ.get("HIGH_EV_ENTRY_MAX", "0.50"))
MID_FLOOR_ENTRY_MIN = float(os.environ.get("MID_FLOOR_ENTRY_MIN", "0.30"))
MID_FLOOR_ENTRY_MAX = float(os.environ.get("MID_FLOOR_ENTRY_MAX", "0.55"))
MID_FLOOR_MIN_SCORE = int(os.environ.get("MID_FLOOR_MIN_SCORE", "12"))
MID_FLOOR_MIN_TRUE_PROB = float(os.environ.get("MID_FLOOR_MIN_TRUE_PROB", "0.70"))
MID_FLOOR_EV_MARGIN = float(os.environ.get("MID_FLOOR_EV_MARGIN", "0.008"))
MID_FLOOR_EV_ABS = float(os.environ.get("MID_FLOOR_EV_ABS", "0.020"))
MID_FLOOR_MIN_USDC = float(os.environ.get("MID_FLOOR_MIN_USDC", "4.0"))
MID_FLOOR_BANKROLL_PCT = float(os.environ.get("MID_FLOOR_BANKROLL_PCT", "0.012"))
FORCE_MIN_ENTRY_TAIL = float(os.environ.get("FORCE_MIN_ENTRY_TAIL", "0.10"))
FORCE_MIN_LEFT_TAIL = float(os.environ.get("FORCE_MIN_LEFT_TAIL", "3.5"))
MODEL_HICONF_MIN_SCORE = int(os.environ.get("MODEL_HICONF_MIN_SCORE", "12"))
MODEL_HICONF_MIN_TRUE_PROB = float(os.environ.get("MODEL_HICONF_MIN_TRUE_PROB", "0.62"))
MODEL_HICONF_MIN_EDGE = float(os.environ.get("MODEL_HICONF_MIN_EDGE", "0.03"))
ABS_MIN_SIZE_USDC = float(os.environ.get("ABS_MIN_SIZE_USDC", "0.50"))
EVENT_ALIGN_SIZE_MIN = float(os.environ.get("EVENT_ALIGN_SIZE_MIN", "0.20"))
EVENT_ALIGN_SIZE_MAX = float(os.environ.get("EVENT_ALIGN_SIZE_MAX", "1.20"))
CONTRARIAN_SIZE_MIN_MULT = float(os.environ.get("CONTRARIAN_SIZE_MIN_MULT", "0.20"))
BOOSTER_TAKER_CONF_UP_MIN = float(os.environ.get("BOOSTER_TAKER_CONF_UP_MIN", "0.54"))
BOOSTER_TAKER_CONF_DN_MAX = float(os.environ.get("BOOSTER_TAKER_CONF_DN_MAX", "0.46"))
BOOSTER_OB_SCALE = float(os.environ.get("BOOSTER_OB_SCALE", "0.35"))
BOOSTER_TF_BASE = float(os.environ.get("BOOSTER_TF_BASE", "1.0"))
BOOSTER_TF_SCALE = float(os.environ.get("BOOSTER_TF_SCALE", "3.0"))
BOOSTER_FLOW_SCALE = float(os.environ.get("BOOSTER_FLOW_SCALE", "2.0"))
BOOSTER_VOL_BASE = float(os.environ.get("BOOSTER_VOL_BASE", "1.0"))
BOOSTER_VOL_SCALE = float(os.environ.get("BOOSTER_VOL_SCALE", "1.5"))
BOOSTER_BASIS_SCALE = float(os.environ.get("BOOSTER_BASIS_SCALE", "0.0008"))
BOOSTER_VWAP_SCALE = float(os.environ.get("BOOSTER_VWAP_SCALE", "0.0012"))
BOOSTER_W_TF = float(os.environ.get("BOOSTER_W_TF", "0.24"))
BOOSTER_W_OB = float(os.environ.get("BOOSTER_W_OB", "0.20"))
BOOSTER_W_FLOW = float(os.environ.get("BOOSTER_W_FLOW", "0.16"))
BOOSTER_W_VOL = float(os.environ.get("BOOSTER_W_VOL", "0.12"))
BOOSTER_W_BASIS = float(os.environ.get("BOOSTER_W_BASIS", "0.10"))
BOOSTER_W_VWAP = float(os.environ.get("BOOSTER_W_VWAP", "0.10"))
BOOSTER_W_ORACLE = float(os.environ.get("BOOSTER_W_ORACLE", "0.08"))
BOOSTER_STRONG_SCORE_DELTA = int(os.environ.get("BOOSTER_STRONG_SCORE_DELTA", "3"))
BOOSTER_STRONG_EDGE_DELTA = float(os.environ.get("BOOSTER_STRONG_EDGE_DELTA", "0.02"))
BOOSTER_MIN_VOL_RATIO = float(os.environ.get("BOOSTER_MIN_VOL_RATIO", "0.90"))
BOOSTER_MIN_CONV = float(os.environ.get("BOOSTER_MIN_CONV", "0.50"))
BOOSTER_OUTSIDE_SCORE_DELTA = int(os.environ.get("BOOSTER_OUTSIDE_SCORE_DELTA", "2"))
BOOSTER_OUTSIDE_PROB_DELTA = float(os.environ.get("BOOSTER_OUTSIDE_PROB_DELTA", "0.02"))
BOOSTER_OUTSIDE_EV_DELTA = float(os.environ.get("BOOSTER_OUTSIDE_EV_DELTA", "0.015"))
BOOSTER_OUTSIDE_MIN_CONV = float(os.environ.get("BOOSTER_OUTSIDE_MIN_CONV", "0.66"))
BOOSTER_STRONG_SIZE_SCORE_DELTA = int(os.environ.get("BOOSTER_STRONG_SIZE_SCORE_DELTA", "3"))
BOOSTER_STRONG_SIZE_PROB_DELTA = float(os.environ.get("BOOSTER_STRONG_SIZE_PROB_DELTA", "0.03"))
BOOSTER_PREV_SIZE_CAP_MULT = float(os.environ.get("BOOSTER_PREV_SIZE_CAP_MULT", "0.35"))
FORCE_TAKER_SCORE = int(os.environ.get("FORCE_TAKER_SCORE", "12"))
FORCE_TAKER_MOVE_MIN = float(os.environ.get("FORCE_TAKER_MOVE_MIN", "0.0015"))
DEFAULT_EPS = float(os.environ.get("DEFAULT_EPS", "1e-9"))
DEFAULT_CMP_EPS = float(os.environ.get("DEFAULT_CMP_EPS", "1e-6"))
# Core score model thresholds (env-tunable; no fixed literals in scoring path).
PCT_REMAINING_MIN = float(os.environ.get("PCT_REMAINING_MIN", "0.02"))  # allow entry until ~18s before close
ORACLE_LATENCY_ONLY_MODE = os.environ.get("ORACLE_LATENCY_ONLY_MODE", "false").lower() == "true"
ORACLE_FRESH_AGE_S   = float(os.environ.get("ORACLE_FRESH_AGE_S",  "35.0"))   # CL updated within N sec (cl_med~33.8s)
ORACLE_MAX_MINS_LEFT = float(os.environ.get("ORACLE_MAX_MINS_LEFT", "2.5"))    # only enter in last 2.5min
ORACLE_MIN_MOVE_PCT  = float(os.environ.get("ORACLE_MIN_MOVE_PCT",  "0.001"))  # 0.1% move from open
ORACLE_SIZE_MULT     = float(os.environ.get("ORACLE_SIZE_MULT",     "4.0"))    # 4x size on near-certain bet
CONTRARIAN_TAIL_ENABLED      = os.environ.get("CONTRARIAN_TAIL_ENABLED", "false").lower() == "true"
CONTRARIAN_TAIL_MAX_ENTRY    = float(os.environ.get("CONTRARIAN_TAIL_MAX_ENTRY",    "0.45"))  # buy when cheap side ≤45¢
CONTRARIAN_TAIL_MIN_MINS_LEFT= float(os.environ.get("CONTRARIAN_TAIL_MIN_MINS_LEFT","7.0"))   # at least 7min remaining
CONTRARIAN_TAIL_MIN_MOVE_PCT = float(os.environ.get("CONTRARIAN_TAIL_MIN_MOVE_PCT", "0.0008"))# 0.08% move triggered (5m moves 0.04-0.12%)
CONTRARIAN_TAIL_SIZE_MULT    = float(os.environ.get("CONTRARIAN_TAIL_SIZE_MULT",    "2.0"))   # 2x size (prediction, not arb)
PREV_WIN_DIR_MOVE_MIN = float(os.environ.get("PREV_WIN_DIR_MOVE_MIN", "0.0002"))
MOM_THRESH_UP = float(os.environ.get("MOM_THRESH_UP", "0.53"))
MOM_THRESH_DN = float(os.environ.get("MOM_THRESH_DN", "0.47"))
CL_DIRECTION_MOVE_MIN = float(os.environ.get("CL_DIRECTION_MOVE_MIN", "0.0"))  # any move picks direction
DIR_MOVE_MIN = float(os.environ.get("DIR_MOVE_MIN", "0.0"))                  # any move picks direction
DIR_CONFLICT_MOVE_MAX = float(os.environ.get("DIR_CONFLICT_MOVE_MAX", "0.0010"))
DIR_CONFLICT_CL_AGE_MAX = float(os.environ.get("DIR_CONFLICT_CL_AGE_MAX", "20.0"))
DIR_CONFLICT_SCORE_PEN = int(os.environ.get("DIR_CONFLICT_SCORE_PEN", "3"))
DIR_CONFLICT_EDGE_PEN = float(os.environ.get("DIR_CONFLICT_EDGE_PEN", "0.020"))
TIMING_SCORE_PCT_2 = float(os.environ.get("TIMING_SCORE_PCT_2", "0.85"))
TIMING_SCORE_PCT_1 = float(os.environ.get("TIMING_SCORE_PCT_1", "0.70"))
MOVE_SCORE_T3 = float(os.environ.get("MOVE_SCORE_T3", "0.0020"))
MOVE_SCORE_T2 = float(os.environ.get("MOVE_SCORE_T2", "0.0012"))
MOVE_SCORE_T1 = float(os.environ.get("MOVE_SCORE_T1", "0.0005"))
CL_AGREE_SCORE_BONUS = int(os.environ.get("CL_AGREE_SCORE_BONUS", "1"))
CL_DISAGREE_SCORE_PEN = int(os.environ.get("CL_DISAGREE_SCORE_PEN", "3"))
DIV_PEN_START = float(os.environ.get("DIV_PEN_START", "0.0008"))      # was 0.0004; CL heartbeat 30-60s causes 0.04-0.10% div — don't penalise normal lag
DIV_PEN_MAX_SCORE = int(os.environ.get("DIV_PEN_MAX_SCORE", "2"))     # was 3; cap at -2 to avoid over-penalising
DIV_PEN_EDGE_CAP = float(os.environ.get("DIV_PEN_EDGE_CAP", "0.03"))
DIV_PEN_EDGE_MULT = float(os.environ.get("DIV_PEN_EDGE_MULT", "3.0"))
CL_AGE_MAX_SKIP = float(os.environ.get("CL_AGE_MAX_SKIP", "90.0"))
CL_AGE_WARN = float(os.environ.get("CL_AGE_WARN", "45.0"))
CL_AGE_WARN_SCORE_PEN = int(os.environ.get("CL_AGE_WARN_SCORE_PEN", "2"))
WS_FALLBACK_SCORE_PEN = int(os.environ.get("WS_FALLBACK_SCORE_PEN", "0"))  # was 1; CLOB REST at age=0ms is real-time — no penalty
WS_SOFT_SCORE_PEN = int(os.environ.get("WS_SOFT_SCORE_PEN", "2"))
CACHE_MIN_KLINES = int(os.environ.get("CACHE_MIN_KLINES", "10"))
RSI_PERIOD = int(os.environ.get("RSI_PERIOD", "14"))
RSI_OB     = float(os.environ.get("RSI_OB",    "65"))   # > RSI_OB → strong upward momentum confirmation
RSI_OS     = float(os.environ.get("RSI_OS",    "35"))   # < RSI_OS → strong downward momentum confirmation
WR_PERIOD  = int(os.environ.get("WR_PERIOD",  "14"))
WR_OB      = float(os.environ.get("WR_OB",    "-20"))   # > WR_OB  → overbought / Up momentum confirmed
WR_OS      = float(os.environ.get("WR_OS",    "-80"))   # < WR_OS  → oversold   / Down momentum confirmed
JUMP_CONFIRM_SCORE = int(os.environ.get("JUMP_CONFIRM_SCORE", "2"))
OB_HARD_BLOCK = float(os.environ.get("OB_HARD_BLOCK", "-0.40"))
OB_SCORE_T3 = float(os.environ.get("OB_SCORE_T3", "0.25"))
OB_SCORE_T2 = float(os.environ.get("OB_SCORE_T2", "0.10"))
OB_SCORE_T1 = float(os.environ.get("OB_SCORE_T1", "-0.10"))
TAKER_T3_UP = float(os.environ.get("TAKER_T3_UP", "0.62"))
TAKER_T3_DN = float(os.environ.get("TAKER_T3_DN", "0.38"))
TAKER_T2_UP = float(os.environ.get("TAKER_T2_UP", "0.55"))
TAKER_T2_DN = float(os.environ.get("TAKER_T2_DN", "0.45"))
TAKER_NEUTRAL = float(os.environ.get("TAKER_NEUTRAL", "0.05"))
VOL_SCORE_T2 = float(os.environ.get("VOL_SCORE_T2", "2.0"))
VOL_SCORE_T1 = float(os.environ.get("VOL_SCORE_T1", "1.3"))
PERP_CONFIRM = float(os.environ.get("PERP_CONFIRM", "0.0002"))
PERP_STRONG = float(os.environ.get("PERP_STRONG", "0.0005"))
FUNDING_POS_STRONG = float(os.environ.get("FUNDING_POS_STRONG", "0.0005"))
FUNDING_NEG_CONFIRM = float(os.environ.get("FUNDING_NEG_CONFIRM", "-0.0002"))
FUNDING_POS_EXTREME = float(os.environ.get("FUNDING_POS_EXTREME", "0.0010"))
FUNDING_NEG_STRONG = float(os.environ.get("FUNDING_NEG_STRONG", "-0.0005"))
# Liquidation signal (free Binance futures WS — !forceOrder@arr)
LIQ_WINDOW_SEC    = float(os.environ.get("LIQ_WINDOW_SEC",  "60"))
LIQ_SCORE_USD_1   = float(os.environ.get("LIQ_SCORE_USD_1", "500000"))    # $500K opp-side liq = +1
LIQ_SCORE_USD_2   = float(os.environ.get("LIQ_SCORE_USD_2", "2000000"))   # $2M  opp-side liq = +2
# Open Interest delta (free Binance REST — fapi/v1/openInterest)
OI_ENABLED        = os.environ.get("OI_ENABLED", "true").lower() == "true"
OI_POLL_SEC       = float(os.environ.get("OI_POLL_SEC", "30"))
OI_DELTA_UP       = float(os.environ.get("OI_DELTA_UP",  "0.003"))   # +0.3% OI confirms direction
OI_DELTA_DN       = float(os.environ.get("OI_DELTA_DN", "-0.003"))   # −0.3% OI confirms direction
# Long/Short ratio (free Binance REST — futures/data/globalLongShortAccountRatio)
LS_LONG_EXT       = float(os.environ.get("LS_LONG_EXT",  "0.60"))    # >60% longs = crowded → contrarian Down signal
LS_SHORT_EXT      = float(os.environ.get("LS_SHORT_EXT", "0.40"))    # <40% longs = crowded → contrarian Up signal
VWAP_SCORE_T2 = float(os.environ.get("VWAP_SCORE_T2", "0.0015"))
VWAP_SCORE_T1 = float(os.environ.get("VWAP_SCORE_T1", "0.0008"))
DISP_SIGMA_STRONG = float(os.environ.get("DISP_SIGMA_STRONG", "1.0"))
DISP_SIGMA_MID = float(os.environ.get("DISP_SIGMA_MID", "0.5"))
BTC_LEAD_T2_UP = float(os.environ.get("BTC_LEAD_T2_UP", "0.60"))
BTC_LEAD_T2_DN = float(os.environ.get("BTC_LEAD_T2_DN", "0.40"))
BTC_LEAD_T1_UP = float(os.environ.get("BTC_LEAD_T1_UP", "0.55"))
BTC_LEAD_T1_DN = float(os.environ.get("BTC_LEAD_T1_DN", "0.45"))
BTC_LEAD_NEG_UP = float(os.environ.get("BTC_LEAD_NEG_UP", "0.40"))
BTC_LEAD_NEG_DN = float(os.environ.get("BTC_LEAD_NEG_DN", "0.60"))
CONT_HIT_TAKER_UP = float(os.environ.get("CONT_HIT_TAKER_UP", "0.53"))
CONT_HIT_TAKER_DN = float(os.environ.get("CONT_HIT_TAKER_DN", "0.47"))
CONT_HIT_OB = float(os.environ.get("CONT_HIT_OB", "0.08"))
CONT_BONUS_EARLY_PCT = float(os.environ.get("CONT_BONUS_EARLY_PCT", "0.80"))
REGIME_VR_TREND = float(os.environ.get("REGIME_VR_TREND", "1.05"))
REGIME_AC_TREND = float(os.environ.get("REGIME_AC_TREND", "0.05"))
REGIME_VR_MR = float(os.environ.get("REGIME_VR_MR", "0.95"))
REGIME_AC_MR = float(os.environ.get("REGIME_AC_MR", "-0.05"))
REGIME_MULT_TREND = float(os.environ.get("REGIME_MULT_TREND", "1.15"))
REGIME_MULT_MR = float(os.environ.get("REGIME_MULT_MR", "0.85"))
LLR_PRICE_MULT = float(os.environ.get("LLR_PRICE_MULT", "1.5"))
LLR_EMA_MULT = float(os.environ.get("LLR_EMA_MULT", "300.0"))
LLR_KALMAN_MULT = float(os.environ.get("LLR_KALMAN_MULT", "0.4"))
LLR_OB_MULT = float(os.environ.get("LLR_OB_MULT", "2.5"))
LLR_TAKER_MULT = float(os.environ.get("LLR_TAKER_MULT", "5.0"))
LLR_PERP_MULT = float(os.environ.get("LLR_PERP_MULT", "1000.0"))
LLR_PERP_CAP = float(os.environ.get("LLR_PERP_CAP", "1.5"))
LLR_CL_AGREE = float(os.environ.get("LLR_CL_AGREE", "0.4"))
LLR_CL_DISAGREE = float(os.environ.get("LLR_CL_DISAGREE", "1.0"))
LLR_BTC_LEAD_MULT = float(os.environ.get("LLR_BTC_LEAD_MULT", "3.0"))
LLR_CLAMP = float(os.environ.get("LLR_CLAMP", "6.0"))
PRIOR_BOOST_TF = float(os.environ.get("PRIOR_BOOST_TF", "0.015"))
PRIOR_BOOST_TAKER = float(os.environ.get("PRIOR_BOOST_TAKER", "0.015"))
PRIOR_BOOST_OB = float(os.environ.get("PRIOR_BOOST_OB", "0.010"))
PRIOR_BOOST_CL = float(os.environ.get("PRIOR_BOOST_CL", "0.010"))
PRIOR_BOOST_TAKER_UP = float(os.environ.get("PRIOR_BOOST_TAKER_UP", "0.54"))
PRIOR_BOOST_TAKER_DN = float(os.environ.get("PRIOR_BOOST_TAKER_DN", "0.46"))
PRIOR_BOOST_OB_MIN = float(os.environ.get("PRIOR_BOOST_OB_MIN", "0.10"))
PREFILTER_MIN = float(os.environ.get("PREFILTER_MIN", "0.02"))
PREFILTER_EDGE_MULT = float(os.environ.get("PREFILTER_EDGE_MULT", "0.25"))
FORCE_DIR_MOVE_MIN = float(os.environ.get("FORCE_DIR_MOVE_MIN", "0.0005"))
FORCE_DIR_EDGE_HARD_BLOCK = float(os.environ.get("FORCE_DIR_EDGE_HARD_BLOCK", "-0.15"))
FORCE_DIR_EDGE_FLOOR = float(os.environ.get("FORCE_DIR_EDGE_FLOOR", "0.01"))
UTIL_EDGE_MULT = float(os.environ.get("UTIL_EDGE_MULT", "0.35"))
PREFETCH_UP_PRICE_MAX = float(os.environ.get("PREFETCH_UP_PRICE_MAX", "0.50"))
CL_FRESH_PRICE_AGE_SEC = float(os.environ.get("CL_FRESH_PRICE_AGE_SEC", "15.0"))
EARLY_CONT_PCT_MIN = float(os.environ.get("EARLY_CONT_PCT_MIN", "0.85"))
TF_VOTES_STRONG = int(os.environ.get("TF_VOTES_STRONG", "3"))
TF_VOTES_MAX = int(os.environ.get("TF_VOTES_MAX", "4"))
TF_VOTES_MID = int(os.environ.get("TF_VOTES_MID", "2"))
TF_SCORE_MAX = int(os.environ.get("TF_SCORE_MAX", "5"))
TF_SCORE_STRONG = int(os.environ.get("TF_SCORE_STRONG", "4"))
TF_SCORE_MID = int(os.environ.get("TF_SCORE_MID", "2"))
CORE_DURATION_MIN = int(os.environ.get("CORE_DURATION_MIN", "15"))
FAIR_SIDE_MISMATCH_SWAP = float(os.environ.get("FAIR_SIDE_MISMATCH_SWAP", "0.18"))
FAIR_SIDE_SWAP_ERR_MARGIN = float(os.environ.get("FAIR_SIDE_SWAP_ERR_MARGIN", "0.03"))
EXEC_EDGE_FLOOR_DEFAULT  = float(os.environ.get("EXEC_EDGE_FLOOR_DEFAULT",  "0.04"))
EXEC_EDGE_FLOOR_NO_CL    = float(os.environ.get("EXEC_EDGE_FLOOR_NO_CL",    "0.02"))
EXEC_EDGE_STRONG_DELTA   = float(os.environ.get("EXEC_EDGE_STRONG_DELTA",   "0.01"))
EXEC_EDGE_EARLY_DELTA    = float(os.environ.get("EXEC_EDGE_EARLY_DELTA",    "0.015"))
# Resolution uses >= so an exact CL tie resolves as Up — add small structural bias to p_up_ll
TIE_BIAS_UP              = float(os.environ.get("TIE_BIAS_UP",              "0.005"))
# Correlated Kelly: scale size by 1/sqrt(N) when N same-direction positions are open
CORR_KELLY_ENABLED       = os.environ.get("CORR_KELLY_ENABLED", "1") not in ("0", "false", "False")
# Real-time OFI: rolling window for aggTrade stream (seconds)
AGG_OFI_WINDOW_SEC       = float(os.environ.get("AGG_OFI_WINDOW_SEC", "30.0"))
AGG_OFI_BLEND_W          = float(os.environ.get("AGG_OFI_BLEND_W", "0.60"))  # weight of aggOFI vs kline taker
# Window-open OFI surge: when round just started and OFI is extreme → bonus score + prob boost
OFI_SURGE_ENABLED        = os.environ.get("OFI_SURGE_ENABLED", "true").lower() == "true"
OFI_SURGE_PCT_MIN        = float(os.environ.get("OFI_SURGE_PCT_MIN", "0.78"))    # ≥78% window remaining (first ~3.5 min of 15m)
OFI_SURGE_WINDOW_SEC     = float(os.environ.get("OFI_SURGE_WINDOW_SEC", "10.0")) # last 10s aggTrade burst window
OFI_SURGE_THRESH_UP      = float(os.environ.get("OFI_SURGE_THRESH_UP", "0.72"))  # ≥72% buy volume = surge Up
OFI_SURGE_THRESH_DN      = float(os.environ.get("OFI_SURGE_THRESH_DN", "0.28"))  # ≤28% buy volume = surge Down
OFI_SURGE_SCORE_BONUS    = int(os.environ.get("OFI_SURGE_SCORE_BONUS", "2"))
OFI_SURGE_PROB_BOOST     = float(os.environ.get("OFI_SURGE_PROB_BOOST", "0.03"))
# Cross-asset 3/4 consensus: relax min_score when all other assets confirm direction
CROSS_CONSENSUS_ENABLED      = os.environ.get("CROSS_CONSENSUS_ENABLED", "true").lower() == "true"
CROSS_CONSENSUS_MIN_COUNT    = int(os.environ.get("CROSS_CONSENSUS_MIN_COUNT", "3"))    # all 3 other assets agree
CROSS_CONSENSUS_SCORE_RELAX  = int(os.environ.get("CROSS_CONSENSUS_SCORE_RELAX", "2")) # reduce required min_score by this
SETUP_SCORE_SLACK_HIGH = int(os.environ.get("SETUP_SCORE_SLACK_HIGH", "12"))
SETUP_SCORE_SLACK_MID = int(os.environ.get("SETUP_SCORE_SLACK_MID", "9"))
SETUP_SLACK_HIGH = float(os.environ.get("SETUP_SLACK_HIGH", "0.02"))
SETUP_SLACK_MID = float(os.environ.get("SETUP_SLACK_MID", "0.01"))
SETUP_Q_HARD_RELAX_SCORE = int(os.environ.get("SETUP_Q_HARD_RELAX_SCORE", "12"))
SETUP_Q_HARD_RELAX_PROB = float(os.environ.get("SETUP_Q_HARD_RELAX_PROB", "0.82"))
SETUP_Q_HARD_RELAX_MIN = float(os.environ.get("SETUP_Q_HARD_RELAX_MIN", "0.70"))
SETUP_Q_HARD_RELAX_MAX = float(os.environ.get("SETUP_Q_HARD_RELAX_MAX", "0.06"))
SETUP_Q_HARD_RELAX_MULT = float(os.environ.get("SETUP_Q_HARD_RELAX_MULT", "0.30"))
ROLL_EXP_GOOD = float(os.environ.get("ROLL_EXP_GOOD", "0.10"))
ROLL_WR_GOOD = float(os.environ.get("ROLL_WR_GOOD", "0.50"))
ROLL_EXP_BAD = float(os.environ.get("ROLL_EXP_BAD", "-0.10"))
ROLL_WR_BAD = float(os.environ.get("ROLL_WR_BAD", "0.46"))
FRESH_RELAX_MIN_LEFT_15M = float(os.environ.get("FRESH_RELAX_MIN_LEFT_15M", "4.0"))
FRESH_RELAX_MIN_LEFT_5M = float(os.environ.get("FRESH_RELAX_MIN_LEFT_5M", "2.0"))
FRESH_RELAX_ENTRY_CAP = float(os.environ.get("FRESH_RELAX_ENTRY_CAP", "0.90"))
FRESH_RELAX_ENTRY_ADD = float(os.environ.get("FRESH_RELAX_ENTRY_ADD", "0.02"))
SETUP_Q_RELAX_MIN = float(os.environ.get("SETUP_Q_RELAX_MIN", "0.72"))
SETUP_Q_RELAX_MAX = float(os.environ.get("SETUP_Q_RELAX_MAX", "0.08"))
SETUP_Q_RELAX_MULT = float(os.environ.get("SETUP_Q_RELAX_MULT", "0.20"))
SETUP_Q_TIGHTEN_MIN = float(os.environ.get("SETUP_Q_TIGHTEN_MIN", "0.58"))
SETUP_Q_TIGHTEN_MAX = float(os.environ.get("SETUP_Q_TIGHTEN_MAX", "0.06"))
SETUP_Q_TIGHTEN_MULT = float(os.environ.get("SETUP_Q_TIGHTEN_MULT", "0.25"))
SETUP_VOL_RELAX_MIN = float(os.environ.get("SETUP_VOL_RELAX_MIN", "1.10"))
ENTRY_TIGHTEN_ADD = float(os.environ.get("ENTRY_TIGHTEN_ADD", "0.01"))
PULLBACK_MIN_ENTRY_FLOOR = float(os.environ.get("PULLBACK_MIN_ENTRY_FLOOR", "0.10"))
FORCE_TAKER_CL_AGE_FRESH = float(os.environ.get("FORCE_TAKER_CL_AGE_FRESH", "45"))
FIVE_MIN_ASSETS = {
    s.strip().upper() for s in os.environ.get("FIVE_MIN_ASSETS", "BTC,ETH").split(",") if s.strip()
}

if PROFIT_PUSH_MODE:
    _pp = max(0.5, min(2.0, PROFIT_PUSH_MULT))
    # Aggressive-but-selective profile: bigger size only on stronger setups.
    MAX_ABS_BET = min(40.0, MAX_ABS_BET * (1.20 + 0.15 * (_pp - 1.0)))
    MAX_BANKROLL_PCT = min(0.45, MAX_BANKROLL_PCT + 0.07 * _pp)
    MAX_OPEN = min(6, max(MAX_OPEN, int(round(4 + _pp))))
    CORE_SIZE_BONUS = max(CORE_SIZE_BONUS, 1.18 + 0.06 * (_pp - 1.0))
    HIGH_EV_SIZE_BOOST = max(HIGH_EV_SIZE_BOOST, 1.40 + 0.10 * (_pp - 1.0))
    HIGH_EV_SIZE_BOOST_MAX = max(HIGH_EV_SIZE_BOOST_MAX, 1.65 + 0.15 * (_pp - 1.0))
    COPYFLOW_BONUS_MAX = max(COPYFLOW_BONUS_MAX, 3)
    MID_BOOSTER_MIN_EV_NET = min(MID_BOOSTER_MIN_EV_NET, 0.024)
    MID_BOOSTER_MIN_PAYOUT = min(MID_BOOSTER_MIN_PAYOUT, 2.00)
    MID_BOOSTER_MAX_ENTRY = max(MID_BOOSTER_MAX_ENTRY, 0.56)
    ENTRY_WAIT_ENABLED = True
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
                "mark": 0.0, "index": 0.0, "funding": 0.0,
                "agg_ofi_buf": deque(maxlen=2000)}  # (ts_float, buy_qty) for rolling OFI
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
        self._markets_cache_ts  = 0.0  # last time Gamma API was actually fetched
        self._markets_cache_raw = {}   # cached raw market dicts (without mins_left — recalculated live)
        self.asset_cur_open     = {}   # asset → current market open price (for inter-market continuity)
        self.asset_prev_open    = {}   # asset → previous market open price
        self.active_mkts = {}
        self.pending         = {}   # cid → (m, trade)
        self.pending_redeem  = {}   # cid → (side, asset)  — waiting on-chain resolution
        self.redeemed_cids   = set()  # cids already processed — prevents _redeemable_scan re-queueing
        self.token_prices    = {}     # token_id → real-time price from RTDS market stream
        self._rtds_asset_ts  = {}     # asset -> last RTDS crypto_prices tick ts
        self._rtds_ws        = None   # live WebSocket handle for dynamic subscriptions
        self._redeem_queued_ts = {}  # cid → timestamp when queued for redeem
        self._redeem_verify_counts = {}  # cid → non-claimable verification cycles before auto-close
        self._pending_absent_counts = {}  # cid -> consecutive sync cycles absent from on-chain/API
        self.seen            = set()
        self._session        = None   # persistent aiohttp session
        self._http_429_backoff: dict = {}  # host -> backoff_until timestamp
        self._book_cache     = {}     # token_id -> {"ts_ms": float, "book": OrderBook}
        self._book_sem       = asyncio.Semaphore(max(1, BOOK_FETCH_CONCURRENCY))
        self._clob_market_ws = None
        self._clob_ws_books  = {}     # token_id -> {"ts_ms": float, "best_bid": float, "best_ask": float, "tick": float, "asks": [(p,s)]}
        self._clob_ws_assets_subscribed = set()
        # Token ids queued for immediate market-WS subscribe (e.g. newly discovered rounds).
        self._clob_ws_pending_subs = set()
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
        self._open_state_cache_last_persist_ts = 0.0
        self.onchain_total_equity = BANKROLL
        self.onchain_snapshot_ts = 0.0
        self.daily_pnl       = 0.0
        self._dash_daily_cache = {
            "ts": 0.0,
            "day": "",
            "pnl": 0.0,
            "outcomes": 0,
            "wins": 0,
        }
        self._metrics_resolve_cache = {"ts": 0.0, "sig": None, "rows": [], "offset": 0, "db_rowid": 0}
        self._metrics_db_ready = False
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
        self._price_cache_last_persist_ts = 0.0
        self.stats           = {}    # {asset: {side: {wins, total}}} — persisted
        self.recent_trades   = deque(maxlen=30)   # rolling window for WR adaptation
        self.recent_pnl      = deque(maxlen=40)   # rolling pnl window for profit-factor/expectancy adaptation
        self._resolved_samples = deque(maxlen=2000)  # rolling settled outcomes for 15m calibration
        self._pm_pattern_stats = {}
        self.side_perf       = {}                 # "ASSET|SIDE" -> {n, gross_win, gross_loss, pnl}
        self._last_eval_time    = {}              # cid → last RTDS-triggered evaluate() timestamp
        self._exec_lock         = asyncio.Lock()
        self._executing_cids    = set()
        self._reserved_bankroll = 0.0             # sum of in-flight trade sizes (race guard)
        self._round_side_attempt_ts = {}          # "round_key|side" → last attempt ts
        self._cid_side_attempt_ts = {}            # "cid|side" → last attempt ts
        self._round_side_block_until = {}         # "round_fingerprint|side" → ttl epoch
        self._booster_used_by_cid = {}            # cid -> count of executed booster add-ons
        self._booster_consec_losses = 0
        self._booster_lock_until = 0.0
        # Free Binance gap-closer: liquidations / OI / long-short ratio
        self._liq_buf  = {a: deque(maxlen=200) for a in ["BTC","ETH","SOL","XRP"]}
        self._oi       = {a: {"cur": 0.0, "prev": 0.0, "ts": 0.0} for a in ["BTC","ETH","SOL","XRP"]}
        self._ls_ratio = {"BTC": 1.0, "ETH": 1.0, "SOL": 1.0, "XRP": 1.0}
        self._last_superbet_ts = 0.0
        self._redeem_tx_lock    = asyncio.Lock()  # serialize redeem txs to avoid nonce clashes
        self._nonce_mgr         = None
        self._errors            = ErrorTracker()
        self._bucket_stats      = BucketStats()
        _bs_load(self._bucket_stats)
        self._enable_5m_runtime = bool(ENABLE_5M)
        self._five_m_runtime_size_mult = 1.0
        self._five_m_disabled_until = 0.0
        self._five_m_guard_last_eval_ts = 0.0
        self._five_m_guard_last_state_log_ts = 0.0
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
        self._cid_market_cache  = {}
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
        self._autopilot_size_mult = 1.0
        self._autopilot_mode = "normal"
        self._autopilot_last_eval_ts = 0.0
        self._autopilot_day_key = ""
        self._autopilot_day_peak = float(BANKROLL)
        self._autopilot_policy = {}
        self._pred_agent = None
        if PRED_AGENT_ENABLED and PredictionAgent is not None:
            try:
                self._pred_agent = PredictionAgent(min_samples=PRED_AGENT_MIN_SAMPLES)
            except Exception:
                self._pred_agent = None
        # ── Mathematical signal state (EMA + Kalman) ──────────────────────────
        _EMA_HLS = (5, 15, 30, 60, 120)
        self.emas    = {a: {hl: 0.0 for hl in _EMA_HLS} for a in ["BTC","ETH","SOL","XRP"]}
        self._ema_ts = {a: 0.0 for a in ["BTC","ETH","SOL","XRP"]}
        # Kalman: constant-velocity model; state = [price, velocity]; P = 2×2 cov
        self.kalman  = {a: {"pos":0.0,"vel":0.0,"p00":1.0,"p01":0.0,"p11":1.0,"rdy":False}
                        for a in ["BTC","ETH","SOL","XRP"]}
        self._init_log()
        self._init_metrics_db()
        self._load_autopilot_policy()
        self._load_price_cache()
        self._load_open_state_cache()
        self._load_pending()
        self._load_stats()
        self._load_pnl_baseline()
        self._load_settled_outcomes()
        self._bootstrap_resolved_samples_from_metrics()
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

    def _ws_strict_age_cap_ms(self) -> float:
        """Adaptive strict WS freshness cap tuned to live network conditions."""
        base = max(float(CLOB_MARKET_WS_MAX_AGE_MS), 5000.0)
        if not WS_STRICT_ADAPTIVE_ENABLED:
            return base
        try:
            hs = self._feed_health_snapshot()
            ws_med = float(hs.get("ws_market_med_ms", 9e9) or 9e9)
            if ws_med >= 9e8:
                return base
            dyn = max(base, ws_med * max(1.0, float(WS_STRICT_ADAPTIVE_MULT)))
            hard_min = max(5000.0, float(WS_STRICT_ADAPTIVE_MIN_MS))
            hard_max = max(hard_min, float(WS_STRICT_ADAPTIVE_MAX_MS))
            health_cap = max(5000.0, float(WS_HEALTH_MAX_MED_AGE_MS))
            dyn = max(hard_min, dyn)
            dyn = min(hard_max, dyn, health_cap)
            return float(max(base, dyn))
        except Exception:
            return base

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

    def _open_price_by_cid(self, cid: str) -> float:
        """Best-effort open price lookup with cid normalization."""
        c = str(cid or "").strip()
        if not c:
            return 0.0
        v = float(self.open_prices.get(c, 0.0) or 0.0)
        if v > 0:
            return v
        c_norm = c.lower().replace("0x", "")
        for k, pv in self.open_prices.items():
            if str(k or "").strip().lower().replace("0x", "") == c_norm:
                vv = float(pv or 0.0)
                if vv > 0:
                    return vv
        return 0.0

    def _midpoint_token_id_from_cid_side(self, cid: str, side: str, m: dict | None = None, t: dict | None = None) -> str:
        """Prefer live market token ids; fall back to stored/derived ids."""
        side_n = self._normalize_side_label(side)
        cid_n = str(cid or "").strip().lower()
        cid_n_hex = cid_n.replace("0x", "")

        # 1) Active market map (authoritative for clob midpoint endpoint)
        try:
            for cid_a, ma in (self.active_mkts or {}).items():
                ca = str(cid_a or "").strip().lower()
                if not ca:
                    continue
                if ca == cid_n or ca.replace("0x", "") == cid_n_hex:
                    tid = str((ma or {}).get("token_up" if side_n == "Up" else "token_down", "") or "").strip()
                    if tid:
                        return tid
        except Exception:
            pass

        # 2) Explicit token ids already attached to local trade/meta.
        for src in (t or {}, m or {}):
            try:
                tid = str((src or {}).get("token_id", "") or "").strip()
                if tid:
                    return tid
            except Exception:
                pass
            try:
                tid = str((src or {}).get("token_up" if side_n == "Up" else "token_down", "") or "").strip()
                if tid:
                    return tid
            except Exception:
                pass

        # 3) Last resort deterministic derivation.
        return self._token_id_from_cid_side(cid, side_n) or ""

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

    def _duration_from_question(self, question: str) -> int:
        """Infer round duration (minutes) from explicit ET window in title."""
        q_st, q_et = self._round_bounds_from_question(question or "")
        if q_st <= 0 or q_et <= q_st:
            return 0
        try:
            d = int(round((q_et - q_st) / 60.0))
        except Exception:
            return 0
        return d if d in (5, 15) else 0

    def _apply_exact_window_from_question(self, m: dict, t: dict | None = None) -> bool:
        """Normalize start/end timestamps from explicit ET window in title when possible."""
        if not isinstance(m, dict):
            return False
        dur = int((t or {}).get("duration") or m.get("duration") or 0)
        q_dur = self._duration_from_question(m.get("question", ""))
        if q_dur in (5, 15):
            dur = q_dur
        if dur <= 0:
            return False
        q_start, q_end = self._round_bounds_from_question(m.get("question", ""))
        if not self._is_exact_round_bounds(q_start, q_end, dur):
            return False
        m["start_ts"] = q_start
        m["end_ts"] = q_end
        m["duration"] = dur
        if t is not None:
            t["end_ts"] = q_end
            t["duration"] = dur
        return True

    def _force_expired_from_question_if_needed(self, m: dict, t: dict | None = None) -> bool:
        """If title window is exact and already expired, force end_ts to that exact end."""
        if not isinstance(m, dict):
            return False
        dur = int((t or {}).get("duration") or m.get("duration") or 0)
        q_dur = self._duration_from_question(m.get("question", ""))
        if q_dur in (5, 15):
            dur = q_dur
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
            q_dur = self._duration_from_question(m.get("question", ""))
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
            q_dur = self._duration_from_question(m.get("question", ""))
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
        # Per-host 429 backoff: skip the request if we're in a backoff window.
        import urllib.parse as _up
        _host = _up.urlparse(url).netloc
        _bt = self._http_429_backoff.get(_host, 0.0)
        if _bt > _time.time():
            raise RuntimeError(f"http 429 backoff active for {_host} ({_bt - _time.time():.0f}s left)")
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
                if r.status == 429:
                    retry_after = float(r.headers.get("Retry-After", "30"))
                    self._http_429_backoff[_host] = _time.time() + max(30.0, retry_after)
                    if self._should_log(f"429:{_host}", 60):
                        print(f"{Y}[HTTP] 429 rate-limit from {_host} — backing off {retry_after:.0f}s{RS}")
                    raise RuntimeError(f"http 429 {url}")
                if r.status >= 400:
                    raise RuntimeError(f"http {r.status} {url}")
                return await r.json()
        except RuntimeError:
            raise
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
        loop = asyncio.get_running_loop()
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
        m = await self._cid_market_cached(cid)
        if m:
            asset = str(m.get("asset", "?") or "?")
            dur = int(m.get("duration", 0) or 0)
            self._cid_family_cache[cid] = (asset, dur)
            return asset, dur
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

    async def _cid_market_cached(self, cid: str) -> dict:
        """ConditionId market metadata cache (persistent) to avoid repeated Gamma calls."""
        cid = str(cid or "").strip()
        if not cid:
            return {}
        cached = self._cid_market_cache.get(cid) or {}
        now_ts = _time.time()
        if cached:
            cts = float(cached.get("_ts", 0.0) or 0.0)
            if cts > 0 and (now_ts - cts) <= max(3600.0, CID_MARKET_CACHE_TTL_SEC):
                return dict(cached)
            if int(cached.get("duration", 0) or 0) in (5, 15) and float(cached.get("end_ts", 0.0) or 0.0) > 0:
                # Round metadata is effectively immutable once known.
                cached["_ts"] = now_ts
                self._cid_market_cache[cid] = dict(cached)
                return dict(cached)
        if cached and cached.get("asset") and cached.get("duration"):
            return dict(cached)
        out = {
            "asset": "?",
            "duration": 0,
            "start_ts": 0.0,
            "end_ts": 0.0,
            "token_up": "",
            "token_down": "",
            "question": "",
            "series_slug": "",
            "_ts": now_ts,
        }
        try:
            rows = await self._http_get_json(
                f"{GAMMA}/markets",
                params={"conditionId": cid},
                timeout=8,
            )
            m = rows[0] if isinstance(rows, list) and rows else (rows if isinstance(rows, dict) else {})
            out["question"] = str(m.get("question", "") or "")
            slug = str(m.get("seriesSlug") or m.get("series_slug", "") or "")
            out["series_slug"] = slug
            if slug in SERIES:
                out["asset"] = str(SERIES[slug]["asset"])
                out["duration"] = int(SERIES[slug]["duration"])
            else:
                q = out["question"].lower()
                if "bitcoin" in q:
                    out["asset"] = "BTC"
                elif "ethereum" in q:
                    out["asset"] = "ETH"
                elif "solana" in q:
                    out["asset"] = "SOL"
                elif "xrp" in q:
                    out["asset"] = "XRP"
                out["duration"] = 5 if "5m" in q else (15 if "15m" in q else 0)
            es = m.get("endDate") or m.get("end_date", "")
            ss = m.get("eventStartTime") or m.get("startDate") or m.get("start_date", "")
            if isinstance(es, str) and es:
                out["end_ts"] = datetime.fromisoformat(es.replace("Z", "+00:00")).timestamp()
            if isinstance(ss, str) and ss:
                out["start_ts"] = datetime.fromisoformat(ss.replace("Z", "+00:00")).timestamp()
            _, token_up, token_down = self._map_updown_market_fields(m)
            out["token_up"] = str(token_up or "")
            out["token_down"] = str(token_down or "")
        except Exception:
            pass
        out["_ts"] = _time.time()
        self._cid_market_cache[cid] = dict(out)
        if int(out.get("duration", 0) or 0) > 0:
            self._cid_family_cache[cid] = (str(out.get("asset", "?") or "?"), int(out.get("duration", 0) or 0))
        return dict(out)

    async def _warmup_active_cid_cache(self):
        """Warmup market metadata cache only for currently active CIDs."""
        try:
            open_rows, red_rows = await asyncio.gather(
                self._http_get_json(
                    "https://data-api.polymarket.com/positions",
                    params={"user": ADDRESS, "sizeThreshold": "0.01", "redeemable": "false"},
                    timeout=10,
                ),
                self._http_get_json(
                    "https://data-api.polymarket.com/positions",
                    params={"user": ADDRESS, "sizeThreshold": "0.01", "redeemable": "true"},
                    timeout=10,
                ),
            )
            rows = []
            if isinstance(open_rows, list):
                rows.extend(open_rows)
            if isinstance(red_rows, list):
                rows.extend(red_rows)
            cids = []
            seen = set()
            for p in rows:
                cid = str(p.get("conditionId", "") or "").strip()
                if not cid or cid in seen:
                    continue
                seen.add(cid)
                cids.append(cid)
            if not cids:
                return
            sem = asyncio.Semaphore(8)

            async def _one(cid: str):
                async with sem:
                    try:
                        await self._cid_market_cached(cid)
                    except Exception:
                        pass

            await asyncio.gather(*[_one(c) for c in cids[:120]])
            self._save_open_state_cache(force=True)
            print(f"{B}[BOOT]{RS} warmed CID market cache for {min(len(cids), 120)} active positions")
        except Exception as e:
            print(f"{Y}[BOOT] cid warmup skipped: {e}{RS}")

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
                    if settled < COPYFLOW_INTEL_MIN_SETTLED or roi < COPYFLOW_INTEL_MIN_ROI or wr < COPYFLOW_INTEL_MIN_WR:
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
            f"5m_guard={FIVE_M_RUNTIME_GUARD_ENABLED} "
            f"5m_guard_window={FIVE_M_GUARD_WINDOW} "
            f"5m_off(wr/pf)<{FIVE_M_GUARD_DISABLE_WR*100:.0f}%/{FIVE_M_GUARD_DISABLE_PF:.2f} "
            f"5m_on(wr/pf)>={FIVE_M_GUARD_REENABLE_WR*100:.0f}%/{FIVE_M_GUARD_REENABLE_PF:.2f} "
            f"score_gate(5m/15m)={MIN_SCORE_GATE_5M}/{MIN_SCORE_GATE_15M} "
            f"max_entry={MAX_ENTRY_PRICE:.2f}+tol{MAX_ENTRY_TOL:.2f} payout>={MIN_PAYOUT_MULT:.2f}x "
            f"near_miss_tol={ENTRY_NEAR_MISS_TOL:.3f} "
            f"ev_net>={MIN_EV_NET:.3f} fee={FEE_RATE_EST:.4f} "
            f"risk(max_open/same_dir/cid)={MAX_OPEN}/{MAX_SAME_DIR}/{MAX_CID_EXPOSURE_PCT:.0%} "
            f"block_opp(cid/round)={BLOCK_OPPOSITE_SIDE_SAME_CID}/{BLOCK_OPPOSITE_SIDE_SAME_ROUND} "
            f"round_coverage={FORCE_TRADE_EVERY_ROUND} round_max_trades={max(1, ROUND_MAX_TRADES)} "
            f"2nd_push_only={ROUND_SECOND_TRADE_PUSH_ONLY}"
        )
        print(
            f"{B}[BOOT]{RS} profit_push_mode={PROFIT_PUSH_MODE} mult={PROFIT_PUSH_MULT:.2f} "
            f"adaptive={PROFIT_PUSH_ADAPTIVE_MODE} "
            f"tier_x(probe/base/push)={PROFIT_PUSH_PROBE_MULT:.2f}/{PROFIT_PUSH_BASE_MULT:.2f}/{PROFIT_PUSH_PUSH_MULT:.2f} "
            f"max_abs_bet={MAX_ABS_BET:.2f} bankroll_cap={MAX_BANKROLL_PCT:.0%} "
            f"high_ev_boost={HIGH_EV_SIZE_BOOST:.2f}/{HIGH_EV_SIZE_BOOST_MAX:.2f} "
            f"booster(ev/payout/max_entry)>={MID_BOOSTER_MIN_EV_NET:.3f}/{MID_BOOSTER_MIN_PAYOUT:.2f}/{MID_BOOSTER_MAX_ENTRY:.2f}"
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

    def _rsi(self, asset: str) -> float:
        """RSI from 1m klines closes. Returns neutral 50.0 if insufficient data."""
        n = RSI_PERIOD
        klines = self.binance_cache.get(asset, {}).get("klines", [])
        closes = [float(k[4]) for k in klines[-(n + 2):] if float(k[4]) > 0]
        if len(closes) < n + 1:
            return 50.0
        gains, losses = [], []
        for i in range(1, len(closes)):
            d = closes[i] - closes[i - 1]
            gains.append(max(d, 0.0))
            losses.append(max(-d, 0.0))
        ag = sum(gains) / len(gains)
        al = sum(losses) / len(losses)
        if al == 0:
            return 100.0 if ag > 0 else 50.0
        rs = ag / al
        return 100.0 - (100.0 / (1.0 + rs))

    def _williams_r(self, asset: str) -> float:
        """Williams %R from 1m klines. Range -100 to 0. Returns -50 if insufficient."""
        n = WR_PERIOD
        klines = self.binance_cache.get(asset, {}).get("klines", [])
        if len(klines) < n:
            return -50.0
        recent = klines[-n:]
        highs = [float(k[2]) for k in recent if float(k[2]) > 0]
        lows  = [float(k[3]) for k in recent if float(k[3]) > 0]
        close = float(klines[-1][4]) if klines else 0.0
        if not highs or not lows or close <= 0:
            return -50.0
        hh = max(highs)
        ll = min(lows)
        if hh <= ll:
            return -50.0
        return -100.0 * (hh - close) / (hh - ll)

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
        # Per-asset empirical BTC→altcoin lag correlation (from 15m co-window analysis)
        _btc_lag_corr = {"ETH": 0.82, "SOL": 0.80, "XRP": 0.78}
        corr = _btc_lag_corr.get(asset, 0.77)
        return float(norm.cdf(btc_lag_move / vol_t * corr))

    def _init_metrics_db(self):
        try:
            conn = sqlite3.connect(METRICS_DB_FILE, timeout=5.0)
            try:
                conn.execute("PRAGMA journal_mode=WAL")  # non-blocking concurrent reads
                conn.execute("PRAGMA synchronous=NORMAL")  # safe + faster than FULL
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS resolve_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts TEXT NOT NULL,
                        event TEXT NOT NULL,
                        condition_id TEXT NOT NULL,
                        asset TEXT,
                        side TEXT,
                        duration INTEGER,
                        score INTEGER,
                        entry_price REAL,
                        pnl REAL,
                        result TEXT,
                        round_key TEXT,
                        open_price_source TEXT,
                        chainlink_age_s REAL
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_resolve_ts ON resolve_metrics(ts)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_resolve_cid ON resolve_metrics(condition_id)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_resolve_event ON resolve_metrics(event)"
                )
                conn.commit()
            finally:
                conn.close()
            self._metrics_db_ready = True
            self._metrics_db_backfill_from_jsonl()
        except Exception:
            self._metrics_db_ready = False

    def _metrics_db_backfill_from_jsonl(self):
        if not self._metrics_db_ready or not os.path.exists(METRICS_FILE):
            return
        try:
            conn = sqlite3.connect(METRICS_DB_FILE, timeout=8.0)
            try:
                cur = conn.cursor()
                n_existing = int(cur.execute("SELECT COUNT(*) FROM resolve_metrics").fetchone()[0] or 0)
                if n_existing > 0:
                    return
                max_lines = max(1000, int(METRICS_DB_IMPORT_MAX_LINES))
                with open(METRICS_FILE, encoding="utf-8") as f:
                    lines = f.readlines()[-max_lines:]
                ins = 0
                for ln in lines:
                    try:
                        row = json.loads(ln)
                    except Exception:
                        continue
                    ev = str(row.get("event", "") or "")
                    if not ev.startswith("RESOLVE"):
                        continue
                    cur.execute(
                        """
                        INSERT INTO resolve_metrics
                        (ts,event,condition_id,asset,side,duration,score,entry_price,pnl,result,round_key,open_price_source,chainlink_age_s)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            str(row.get("ts", "") or datetime.now(timezone.utc).isoformat()),
                            ev,
                            str(row.get("condition_id", "") or ""),
                            str(row.get("asset", "") or ""),
                            str(row.get("side", "") or ""),
                            int(row.get("duration", 0) or 0),
                            int(row.get("score", 0) or 0),
                            float(row.get("entry_price", 0.0) or 0.0),
                            float(row.get("pnl", 0.0) or 0.0),
                            str(row.get("result", "") or ""),
                            str(row.get("round_key", "") or ""),
                            str(row.get("open_price_source", "") or ""),
                            float(row.get("chainlink_age_s", 0.0) or 0.0),
                        ),
                    )
                    ins += 1
                conn.commit()
                if ins > 0:
                    print(f"{B}[BOOT]{RS} imported {ins} RESOLVE rows into metrics sqlite")
            finally:
                conn.close()
        except Exception:
            pass

    def _metrics_db_insert_resolve(self, rec: dict):
        if not self._metrics_db_ready:
            return
        try:
            ev = str(rec.get("event", "") or "")
            if not ev.startswith("RESOLVE"):
                return
            conn = sqlite3.connect(METRICS_DB_FILE, timeout=3.0)
            try:
                conn.execute(
                    """
                    INSERT INTO resolve_metrics
                    (ts,event,condition_id,asset,side,duration,score,entry_price,pnl,result,round_key,open_price_source,chainlink_age_s)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        str(rec.get("ts", "") or datetime.now(timezone.utc).isoformat()),
                        ev,
                        str(rec.get("condition_id", "") or ""),
                        str(rec.get("asset", "") or ""),
                        str(rec.get("side", "") or ""),
                        int(rec.get("duration", 0) or 0),
                        int(rec.get("score", 0) or 0),
                        float(rec.get("entry_price", 0.0) or 0.0),
                        float(rec.get("pnl", 0.0) or 0.0),
                        str(rec.get("result", "") or ""),
                        str(rec.get("round_key", "") or ""),
                        str(rec.get("open_price_source", "") or ""),
                        float(rec.get("chainlink_age_s", 0.0) or 0.0),
                    ),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            pass

    def _save_pending(self):
        try:
            with open(PENDING_FILE, "w") as f:
                json.dump({k: [m, t] for k, (m, t) in self.pending.items()}, f)
        except Exception:
            pass

    def _save_price_cache(self, force: bool = False):
        try:
            now_ts = _time.time()
            if not force and (now_ts - float(self._price_cache_last_persist_ts or 0.0)) < max(5.0, PRICE_CACHE_SAVE_SEC):
                return
            payload = {
                "ts": now_ts,
                "prices": {k: float(v or 0.0) for k, v in (self.prices or {}).items()},
                "cl_prices": {k: float(v or 0.0) for k, v in (self.cl_prices or {}).items()},
                "cl_updated": {k: float(v or 0.0) for k, v in (self.cl_updated or {}).items()},
                "open_prices": {str(k): float(v or 0.0) for k, v in (self.open_prices or {}).items()},
                "price_history": {
                    a: [[float(ts), float(px)] for ts, px in list(ph)[-max(20, PRICE_CACHE_POINTS):] if px > 0]
                    for a, ph in (self.price_history or {}).items()
                },
            }
            with open(PRICE_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(payload, f)
            self._price_cache_last_persist_ts = now_ts
        except Exception:
            pass

    def _load_price_cache(self):
        if not os.path.exists(PRICE_CACHE_FILE):
            return
        try:
            with open(PRICE_CACHE_FILE, encoding="utf-8") as f:
                data = json.load(f)
            prices = data.get("prices", {}) if isinstance(data, dict) else {}
            cl_prices = data.get("cl_prices", {}) if isinstance(data, dict) else {}
            cl_updated = data.get("cl_updated", {}) if isinstance(data, dict) else {}
            open_prices = data.get("open_prices", {}) if isinstance(data, dict) else {}
            ph_all = data.get("price_history", {}) if isinstance(data, dict) else {}
            if isinstance(prices, dict):
                for a, v in prices.items():
                    vv = float(v or 0.0)
                    if vv > 0:
                        self.prices[str(a)] = vv
            if isinstance(cl_prices, dict):
                for a, v in cl_prices.items():
                    vv = float(v or 0.0)
                    if vv > 0:
                        self.cl_prices[str(a)] = vv
            if isinstance(cl_updated, dict):
                for a, v in cl_updated.items():
                    vv = float(v or 0.0)
                    if vv > 0:
                        self.cl_updated[str(a)] = vv
            if isinstance(open_prices, dict):
                for cid, v in open_prices.items():
                    vv = float(v or 0.0)
                    if vv > 0:
                        self.open_prices[str(cid)] = vv
            if isinstance(ph_all, dict):
                for a, pts in ph_all.items():
                    if a not in self.price_history:
                        continue
                    if not isinstance(pts, list):
                        continue
                    q = deque(maxlen=max(20, PRICE_CACHE_POINTS))
                    for p in pts[-max(20, PRICE_CACHE_POINTS):]:
                        if not isinstance(p, (list, tuple)) or len(p) < 2:
                            continue
                        ts = float(p[0] or 0.0)
                        px = float(p[1] or 0.0)
                        if ts > 0 and px > 0:
                            q.append((ts, px))
                    if q:
                        self.price_history[a] = q
            self._price_cache_last_persist_ts = _time.time()
            print(f"{B}[BOOT]{RS} loaded persisted price cache ({len(self.prices)} spot, {len(self.open_prices)} open refs)")
        except Exception:
            pass

    def _save_open_state_cache(self, force: bool = False):
        try:
            now_ts = _time.time()
            if not force and (now_ts - float(self._open_state_cache_last_persist_ts or 0.0)) < max(5.0, OPEN_STATE_CACHE_SAVE_SEC):
                return
            self._prune_cid_market_cache(now_ts=now_ts)
            payload = {
                "ts": now_ts,
                "onchain_snapshot_ts": float(self.onchain_snapshot_ts or 0.0),
                "onchain_open_cids": sorted(list(self.onchain_open_cids or set())),
                "onchain_open_usdc_by_cid": dict(self.onchain_open_usdc_by_cid or {}),
                "onchain_open_stake_by_cid": dict(self.onchain_open_stake_by_cid or {}),
                "onchain_open_shares_by_cid": dict(self.onchain_open_shares_by_cid or {}),
                "onchain_open_meta_by_cid": dict(self.onchain_open_meta_by_cid or {}),
                "onchain_settling_usdc_by_cid": dict(self.onchain_settling_usdc_by_cid or {}),
                "cid_family_cache": {
                    str(k): [str(v[0]), int(v[1])] for k, v in (self._cid_family_cache or {}).items()
                    if isinstance(v, (list, tuple)) and len(v) >= 2
                },
                "cid_market_cache": dict(self._cid_market_cache or {}),
            }
            with open(OPEN_STATE_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(payload, f)
            self._open_state_cache_last_persist_ts = now_ts
        except Exception:
            pass

    def _prune_cid_market_cache(self, now_ts: float | None = None):
        try:
            now_ts = float(now_ts or _time.time())
            keep_keys = set(self.onchain_open_cids or set()) | set(self.pending.keys()) | set(self.pending_redeem.keys())
            pruned = {}
            rows = []
            for cid, meta in (self._cid_market_cache or {}).items():
                if not isinstance(meta, dict):
                    continue
                ts = float(meta.get("_ts", 0.0) or 0.0)
                if ts <= 0:
                    ts = now_ts
                    meta["_ts"] = ts
                fresh = (now_ts - ts) <= max(3600.0, CID_MARKET_CACHE_TTL_SEC)
                if fresh or cid in keep_keys:
                    rows.append((cid, meta, ts))
            rows.sort(key=lambda x: x[2], reverse=True)
            max_keep = max(200, int(CID_MARKET_CACHE_MAX))
            for cid, meta, _ in rows[:max_keep]:
                pruned[cid] = meta
            self._cid_market_cache = pruned
        except Exception:
            pass

    def _load_open_state_cache(self):
        if not os.path.exists(OPEN_STATE_CACHE_FILE):
            return
        try:
            with open(OPEN_STATE_CACHE_FILE, encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                return
            self.onchain_snapshot_ts = float(data.get("onchain_snapshot_ts", 0.0) or 0.0)
            if self.onchain_snapshot_ts > 0 and (_time.time() - self.onchain_snapshot_ts) > 1800:
                self.onchain_snapshot_ts = 0.0
            self.onchain_open_cids = set(str(x) for x in (data.get("onchain_open_cids") or []) if str(x))
            self.onchain_open_usdc_by_cid = {
                str(k): float(v or 0.0) for k, v in (data.get("onchain_open_usdc_by_cid") or {}).items()
            }
            self.onchain_open_stake_by_cid = {
                str(k): float(v or 0.0) for k, v in (data.get("onchain_open_stake_by_cid") or {}).items()
            }
            self.onchain_open_shares_by_cid = {
                str(k): float(v or 0.0) for k, v in (data.get("onchain_open_shares_by_cid") or {}).items()
            }
            self.onchain_open_meta_by_cid = dict(data.get("onchain_open_meta_by_cid") or {})
            self.onchain_settling_usdc_by_cid = {
                str(k): float(v or 0.0) for k, v in (data.get("onchain_settling_usdc_by_cid") or {}).items()
            }
            fam = data.get("cid_family_cache") or {}
            if isinstance(fam, dict):
                for cid, val in fam.items():
                    if isinstance(val, (list, tuple)) and len(val) >= 2:
                        self._cid_family_cache[str(cid)] = (str(val[0]), int(val[1]))
            mk = data.get("cid_market_cache") or {}
            if isinstance(mk, dict):
                self._cid_market_cache = dict(mk)
            self._prune_cid_market_cache(now_ts=_time.time())
            self.onchain_open_count = len(self.onchain_open_cids)
            self.onchain_redeemable_count = len(self.onchain_settling_usdc_by_cid)
            self._open_state_cache_last_persist_ts = _time.time()
            if self.onchain_open_count or self.onchain_redeemable_count:
                print(
                    f"{B}[BOOT]{RS} loaded open-state cache "
                    f"(open={self.onchain_open_count} settling={self.onchain_redeemable_count})"
                )
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
                end_ts = float(m.get("end_ts", 0) or t.get("end_ts", 0) or 0)
                if end_ts <= 0:   # H-5: fallback expiry from placed_ts + duration window
                    placed = float(t.get("placed_ts", 0) or 0)
                    dur = int(t.get("duration", 15) or 15)
                    end_ts = placed + dur * 60 + 300 if placed > 0 else 0
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
        self._metrics_db_insert_resolve(rec)

    # ── STATUS ────────────────────────────────────────────────────────────────
    def _local_position_counts(self):
        """Local counters for logging: only active open positions and redeem queue size."""
        now_ts = _time.time()
        open_local = 0
        for _, (m, t) in list(self.pending.items()):
            end_ts = float(m.get("end_ts", 0) or t.get("end_ts", 0) or 0)
            if end_ts <= 0:   # H-5: fallback expiry from placed_ts + duration window
                placed = float(t.get("placed_ts", 0) or 0)
                dur = int(t.get("duration", 15) or 15)
                end_ts = placed + dur * 60 + 300 if placed > 0 else 0
            if end_ts <= 0 or end_ts > now_ts:
                open_local += 1
        return open_local, len(self.pending_redeem)

    def status(self):
        self._save_price_cache(force=False)
        self._save_open_state_cache(force=False)
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
            # If no open price yet, try Polymarket API inline (best effort)
            if open_p <= 0:
                start_ts_m = m.get("start_ts", 0)
                end_ts_m   = m.get("end_ts", 0)
                dur_m      = int(t.get("duration") or m.get("duration") or 15)
                if start_ts_m > 0 and end_ts_m > 0:
                    # status() is sync: avoid blocking I/O here.
                    # open price will be refreshed by scan/market loops.
                    _ = (start_ts_m, end_ts_m, dur_m)
            # Use most-recent price: CL or RTDS, whichever has the fresher timestamp
            cl_p   = self.cl_prices.get(asset, 0)
            cl_ts  = self.cl_updated.get(asset, 0)
            rtds_p = self.prices.get(asset, 0)
            _ph    = self.price_history.get(asset)
            rtds_ts = _ph[-1][0] if _ph else 0
            if rtds_ts > cl_ts and rtds_p > 0:
                cur_p = rtds_p
                cur_p_src = "RTDS"
            elif cl_p > 0:
                cur_p = cl_p
                cur_p_src = "CL"
            else:
                cur_p = rtds_p
                cur_p_src = "RTDS"
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
                      f"beat={open_p:.6f}[{src}] now={cur_p:.6f}[{cur_p_src}] {move_str} | "
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
            if end_ts <= 0:   # H-5: fallback from start_ts + duration
                start_ts_m = float(meta.get("start_ts", 0) or 0)
                dur_m = int(meta.get("duration", 15) or 15)
                end_ts = start_ts_m + dur_m * 60 if start_ts_m > 0 else 0
            mins_left = max(0.0, (end_ts - now_ts) / 60.0) if end_ts > 0 else 0.0
            if end_ts > 0 and end_ts <= now_ts - 120.0 and cid not in self.pending_redeem:
                continue
            open_p = float(self.open_prices.get(cid, 0.0) or 0.0)
            src = self.open_prices_source.get(cid, "?")
            if open_p <= 0:
                meta_open = float(meta.get("open_price", 0.0) or 0.0)
                if meta_open > 0:
                    open_p = meta_open
                    src = "TRADE"
            cl_p   = float(self.cl_prices.get(asset, 0.0) or 0.0)
            cl_ts  = self.cl_updated.get(asset, 0)
            rtds_p = float(self.prices.get(asset, 0.0) or 0.0)
            _ph2   = self.price_history.get(asset)
            rtds_ts2 = _ph2[-1][0] if _ph2 else 0
            if rtds_ts2 > cl_ts and rtds_p > 0:
                cur_p = rtds_p
                cur_p_src = "RTDS"
            elif cl_p > 0:
                cur_p = cl_p
                cur_p_src = "CL"
            else:
                cur_p = rtds_p
                cur_p_src = "RTDS"
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
                    f"beat={open_p:.6f}[{src}] now={cur_p:.6f} {move_str} | "
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
                # CL-based mark: stable indicator from oracle direction, not volatile token price
                cl_mark = win_payout if (n > 0 and lead >= n) else (0.0 if (n > 0 and lead == 0) else value_now)
                mark_pnl = cl_mark - spent
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
                        f"{B}REAL_MARK{RS}=${cl_mark:.2f} | "
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
                        f"{B}REAL_MARK{RS}=${cl_mark:.2f} | "
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
                    "real_mark": round(cl_mark, 6),
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
            # Only trigger mismatch for CIDs not already handled in pending_redeem / redeemed
            _unaccounted = set(self.onchain_open_cids) - set(self.pending_redeem.keys()) - self.redeemed_cids
            if _unaccounted and (now_fix - float(self._live_rk_repair_ts or 0.0)) >= 20.0:
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
                    RTDS, additional_headers={"Origin": "https://polymarket.com"},
                    compression=None,
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
                            self._rtds_asset_ts[asset] = _now_ts
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
        loop = asyncio.get_running_loop()
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
                        # Dashboard fallback: if RTDS is stale, drive spot/chart from Chainlink.
                        now = _time.time()
                        if (now - float(self._rtds_asset_ts.get(asset, 0.0) or 0.0)) > 3.0:
                            self.prices[asset] = price
                            self.price_history[asset].append((now, price))
                            self._tick_update(asset, price, now)
                except Exception:
                    pass
            await asyncio.sleep(2)   # reduced from 5s — fallback for when WS is active

    async def _cl_ws_one(self, ws_url: str, agg_to_asset: dict, agg_addrs: list):
        """One parallel WS subscriber — first to deliver an event wins (race pattern)."""
        sub_req = json.dumps({
            "jsonrpc": "2.0", "id": 1, "method": "eth_subscribe",
            "params": ["logs", {"address": agg_addrs, "topics": [CL_ANSWER_UPDATED_TOPIC]}],
        })
        refresh_at = _time.time() + 21600
        async with websockets.connect(ws_url, ping_interval=20, open_timeout=10) as ws:
            await ws.send(sub_req)
            resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
            if "error" in resp or "result" not in resp:
                raise RuntimeError(f"sub rejected: {resp}")
            print(f"{G}[CL-WS]{RS} {ws_url.split('//')[1].split('/')[0]} ready")
            async for raw in ws:
                msg = json.loads(raw)
                if msg.get("method") != "eth_subscription":
                    continue
                log = msg["params"]["result"]
                asset = agg_to_asset.get(log.get("address", "").lower())
                if not asset:
                    continue
                try:
                    price_raw = int(log["topics"][1], 16)
                    if price_raw >= 2**255:
                        price_raw -= 2**256
                    price = price_raw / 1e8
                    updated_at = int(log.get("data", "0x0"), 16)
                    now = _time.time()
                    detect_lag = now - updated_at
                    if 0 < price and detect_lag < 60:
                        prev_ts = self.cl_updated.get(asset, 0)
                        if updated_at > prev_ts:   # only update if newer round
                            self.cl_prices[asset]  = price
                            self.cl_updated[asset] = updated_at
                            # Dashboard fallback: if RTDS is stale, drive spot/chart from Chainlink.
                            if (now - float(self._rtds_asset_ts.get(asset, 0.0) or 0.0)) > 3.0:
                                self.prices[asset] = price
                                self.price_history[asset].append((now, price))
                                self._tick_update(asset, price, now)
                            print(f"{G}[CL-WS]{RS} {asset} {price:.4f} detect={detect_lag*1000:.0f}ms "
                                  f"via {ws_url.split('//')[1].split('/')[0]}")
                except Exception:
                    pass
                if _time.time() >= refresh_at:
                    return   # signal parent to refresh aggregator addresses

    async def chainlink_ws_loop(self):
        """Subscribe to Chainlink AnswerUpdated events — ALL WS RPCs in parallel (race pattern)."""
        loop = asyncio.get_running_loop()
        while True:
            try:
                if self.w3 is None:
                    await asyncio.sleep(5)
                    continue
                # Fetch aggregator addresses from proxy contracts
                agg_to_asset: dict[str, str] = {}
                for asset, proxy_addr in CHAINLINK_FEEDS.items():
                    try:
                        proxy = self.w3.eth.contract(
                            address=Web3.to_checksum_address(proxy_addr), abi=CHAINLINK_ABI
                        )
                        agg_addr = await loop.run_in_executor(None, proxy.functions.aggregator().call)
                        agg_to_asset[agg_addr.lower()] = asset
                    except Exception as e:
                        print(f"{Y}[CL-WS] aggregator lookup failed {asset}: {e}{RS}")
                if not agg_to_asset:
                    await asyncio.sleep(10)
                    continue
                agg_addrs = list(agg_to_asset.keys())
                print(f"{G}[CL-WS] racing {len(POLYGON_WS_RPCS)} WS RPCs for {len(agg_to_asset)} aggregators{RS}")
                # Launch all WS connections in parallel — fastest delivery wins each round
                tasks = [
                    asyncio.ensure_future(self._cl_ws_one(url, agg_to_asset, agg_addrs))
                    for url in POLYGON_WS_RPCS
                ]
                done, pending = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
                for t in pending:
                    t.cancel()
                # Check for errors
                errs = [t.exception() for t in done if t.exception()]
                if errs:
                    print(f"{Y}[CL-WS] errors: {[str(e) for e in errs[:2]]}{RS}")
                await asyncio.sleep(1)   # brief pause before refresh
            except Exception as e:
                print(f"{Y}[CL-WS] outer error: {e}{RS}")
                await asyncio.sleep(5)

    async def _rpc_optimizer_loop(self):
        """Continuously measure RPC latency and switch to fastest healthy endpoint."""
        while True:
            await asyncio.sleep(RPC_OPTIMIZE_SEC)
            try:
                loop = asyncio.get_running_loop()
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
                        _ = await asyncio.get_running_loop().run_in_executor(None, lambda: nw3.eth.block_number)
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
        eff_bank = max(1.0, self.bankroll - self._reserved_bankroll)   # H-4: exclude in-flight
        size  = eff_bank * kelly_f * kelly_frac * self._kelly_drawdown_scale()
        cap   = eff_bank * MAX_BANKROLL_PCT
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
        # Only re-fetch Gamma API every MARKET_REFRESH_SEC (default 30s).
        # Previously called every SCAN_INTERVAL (0.5s) = 600 HTTP requests/minute for data that barely changes.
        # mins_left is always recalculated from end_ts so stale cache is safe.
        if now - self._markets_cache_ts >= MARKET_REFRESH_SEC:
            results = await asyncio.gather(
                *[self._fetch_series(slug, info, now) for slug, info in SERIES.items()]
            )
            found = {}
            for r in results:
                found.update(r)
            # Strip mins_left from cache — will be recalculated live on each use
            self._markets_cache_raw = {cid: {k: v for k, v in m.items() if k != "mins_left"}
                                        for cid, m in found.items()}
            self._markets_cache_ts = now
        # Rebuild active_mkts with fresh mins_left from end_ts
        rebuilt = {}
        for cid, m in self._markets_cache_raw.items():
            end_ts = float(m.get("end_ts", 0))
            if end_ts > 0 and end_ts > now:
                rebuilt[cid] = dict(m)
                rebuilt[cid]["mins_left"] = (end_ts - now) / 60.0
        self.active_mkts = rebuilt
        return rebuilt

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
        _conn_start = 0.0
        while True:
            try:
                async with _ws.connect(
                    CLOB_MARKET_WSS,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=2**22,
                    compression=None,
                ) as ws:
                    self._clob_market_ws = ws
                    self._clob_ws_assets_subscribed = set()
                    print(f"{G}[CLOB-WS] market connected{RS}")
                    _conn_start = _time.time()
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
                            desired = (self._trade_focus_token_ids() or self._active_token_ids()) | set(self._clob_ws_pending_subs)
                            add = desired - self._clob_ws_assets_subscribed
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
                                self._clob_ws_pending_subs -= set(add)
                            try:
                                # Keep a short poll interval so new-token subscriptions are flushed
                                # quickly at round rollover (prevents ws_age=9e9 gaps).
                                raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
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
                # Only reset delay if connection lived >= 30s (server-restart loop keeps delay growing)
                if _time.time() - _conn_start >= 30:
                    delay = 2
                print(f"{Y}[CLOB-WS] {e} — reconnect in {delay}s{RS}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)
            finally:
                self._clob_market_ws = None
                self._clob_ws_assets_subscribed = set()
                self._clob_ws_pending_subs = set()

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
        loop = asyncio.get_running_loop()
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

    async def _score_market(self, m: dict, late_relax: bool = False) -> dict | None:
        from clawbot_v2.legacy_core.score import score_market
        return await score_market(self, m, late_relax)
    async def _place_order(self, token_id, side, price, size_usdc, asset, duration, mins_left, true_prob=0.5, cl_agree=True, min_edge_req=None, force_taker=False, score=0, pm_book_data=None, use_limit=False, max_entry_allowed=None, hc15_mode=False, hc15_fallback_cap=0.36, core_position=True, round_force=False):
        from clawbot_v2.legacy_core.execution import place_order
        return await place_order(self, token_id, side, price, size_usdc, asset, duration, mins_left, true_prob, cl_agree, min_edge_req, force_taker, score, pm_book_data, use_limit, max_entry_allowed, hc15_mode, hc15_fallback_cap, core_position, round_force)
    async def _redeem_loop(self):
        from clawbot_v2.legacy_core.settlement import redeem_loop
        return await redeem_loop(self)
    async def _dashboard_loop(self):
        from clawbot_v2.legacy_core.dashboard import dashboard_loop
        return await dashboard_loop(self)
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
        await self._warmup_active_cid_cache()
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
            _guard("_stream_binance_spot",      self._stream_binance_spot),
            _guard("_stream_binance_futures",   self._stream_binance_futures),
            _guard("_stream_binance_aggtrade",  self._stream_binance_aggtrade),
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
            _guard("chainlink_ws_loop",     self.chainlink_ws_loop),
            _guard("_copyflow_refresh_loop", self._copyflow_refresh_loop),
            _guard("_copyflow_live_loop",   self._copyflow_live_loop),
            _guard("_copyflow_intel_loop",  self._copyflow_intel_loop),
            _guard("_rpc_optimizer_loop",   self._rpc_optimizer_loop),
            _guard("_redeemable_scan",      self._redeemable_scan),
            _guard("_position_sync_loop",   self._position_sync_loop),
            _guard("_stream_binance_liquidations", self._stream_binance_liquidations),
            _guard("_oi_ls_loop",           self._oi_ls_loop),
            _guard("_dashboard_loop",       self._dashboard_loop),
        )


if __name__ == "__main__":
    try:
        _install_runtime_json_log()
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            print("[PERF] uvloop enabled — faster async event loop")
        except ImportError:
            pass
        asyncio.run(LiveTrader().run())
    except KeyboardInterrupt:
        print(f"\n{Y}[STOP] Log: {LOG_FILE}{RS}")
