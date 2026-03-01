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
import random
import traceback
import time as _time
from collections import deque, defaultdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from scipy.stats import norm
from dotenv import load_dotenv
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from eth_account import Account
from clawbot_v2.data import HttpService
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
FORCE_TRADE_EVERY_ROUND = os.environ.get("FORCE_TRADE_EVERY_ROUND", "false").lower() == "true"
NO_TRADE_RECOVERY_ENABLED = False
NO_TRADE_RECOVERY_ROUNDS = int(os.environ.get("NO_TRADE_RECOVERY_ROUNDS", "2"))
FORCE_COVERAGE_MIN_PAYOUT_15M = float(os.environ.get("FORCE_COVERAGE_MIN_PAYOUT_15M", "1.40"))
FORCE_COVERAGE_MIN_EV_15M = float(os.environ.get("FORCE_COVERAGE_MIN_EV_15M", "0.000"))
FORCE_COVERAGE_HARD_MIN_EV_15M = float(os.environ.get("FORCE_COVERAGE_HARD_MIN_EV_15M", "-0.030"))
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
ROUND_FORCE_PAYOUT_CAP_15M = float(os.environ.get("ROUND_FORCE_PAYOUT_CAP_15M", "1.58"))
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
MIN_PAYOUT_MULT = float(os.environ.get("MIN_PAYOUT_MULT", "1.58"))
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
STRICT_PM_SOURCE = os.environ.get("STRICT_PM_SOURCE", "false").lower() == "true"
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
SCORE_DEBOUNCE_SEC = float(os.environ.get("SCORE_DEBOUNCE_SEC", "0.9"))
RTDS_EVAL_MIN_INTERVAL_SEC = float(os.environ.get("RTDS_EVAL_MIN_INTERVAL_SEC", "1.0"))
MARKET_REFRESH_SEC = float(os.environ.get("MARKET_REFRESH_SEC", "30.0"))  # how often to re-fetch Gamma API (was every 0.5s = 600 req/min!)
PING_INTERVAL  = int(os.environ.get("PING_INTERVAL", "5"))
RTDS_RECV_TIMEOUT_SEC = float(os.environ.get("RTDS_RECV_TIMEOUT_SEC", "20.0"))
RTDS_IDLE_RECONNECT_SEC = float(os.environ.get("RTDS_IDLE_RECONNECT_SEC", "30.0"))
RTDS_CONNECT_MIN_GAP_SEC = float(os.environ.get("RTDS_CONNECT_MIN_GAP_SEC", "3.0"))
RTDS_429_BASE_BACKOFF_SEC = float(os.environ.get("RTDS_429_BASE_BACKOFF_SEC", "45.0"))
RTDS_429_MAX_BACKOFF_SEC = float(os.environ.get("RTDS_429_MAX_BACKOFF_SEC", "900.0"))
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
HTTP_MIN_GAP_MS = float(os.environ.get("HTTP_MIN_GAP_MS", "120"))
HTTP_429_RETRIES = int(os.environ.get("HTTP_429_RETRIES", "2"))
HTTP_5XX_RETRIES = int(os.environ.get("HTTP_5XX_RETRIES", "1"))
HTTP_CACHE_DEFAULT_TTL_SEC = float(os.environ.get("HTTP_CACHE_DEFAULT_TTL_SEC", "0.8"))
HTTP_CACHE_STALE_TTL_SEC = float(os.environ.get("HTTP_CACHE_STALE_TTL_SEC", "45"))
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
WS_BOOK_FALLBACK_ENABLED = os.environ.get("WS_BOOK_FALLBACK_ENABLED", "true").lower() == "true"
WS_BOOK_FALLBACK_MAX_AGE_MS = float(os.environ.get("WS_BOOK_FALLBACK_MAX_AGE_MS", "6000"))         # was 2500
PM_BOOK_FALLBACK_ENABLED = os.environ.get("PM_BOOK_FALLBACK_ENABLED", "true").lower() == "true"
LEADER_FLOW_FALLBACK_ENABLED = os.environ.get("LEADER_FLOW_FALLBACK_ENABLED", "true").lower() == "true"
LEADER_FLOW_FALLBACK_MAX_AGE_SEC = float(os.environ.get("LEADER_FLOW_FALLBACK_MAX_AGE_SEC", "90"))
REQUIRE_VOLUME_SIGNAL = os.environ.get("REQUIRE_VOLUME_SIGNAL", "true").lower() == "true"
STRICT_REQUIRE_FRESH_LEADER = os.environ.get("STRICT_REQUIRE_FRESH_LEADER", "false").lower() == "true"
STRICT_REQUIRE_FRESH_BOOK_WS = os.environ.get("STRICT_REQUIRE_FRESH_BOOK_WS", "false").lower() == "true"
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
CONSISTENCY_MIN_PAYOUT_15M = float(os.environ.get("CONSISTENCY_MIN_PAYOUT_15M", "1.58"))
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
        self._no_trade_rounds = 0
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
        self._rtds_fails = 0
        self._rtds_cooldown_until = 0.0
        self._rtds_last_msg_ts = 0.0
        self._rtds_last_connect_try_ts = 0.0
        self._rtds_ping_task = None
        self._redeem_queued_ts = {}  # cid → timestamp when queued for redeem
        self._redeem_verify_counts = {}  # cid → non-claimable verification cycles before auto-close
        self._pending_absent_counts = {}  # cid -> consecutive sync cycles absent from on-chain/API
        self.seen            = set()
        self._session        = None   # persistent aiohttp session
        self._http_429_backoff: dict = {}  # legacy field kept for compatibility
        self._http_cache: dict = {}  # legacy field kept for compatibility
        self._http_host_last_ts: dict = {}  # legacy field kept for compatibility
        self._http_host_locks: dict = {}  # legacy field kept for compatibility
        self._http_service = HttpService(
            conn_limit=HTTP_CONN_LIMIT,
            conn_per_host=HTTP_CONN_PER_HOST,
            dns_ttl_sec=HTTP_DNS_TTL_SEC,
            keepalive_sec=HTTP_KEEPALIVE_SEC,
            min_gap_ms=HTTP_MIN_GAP_MS,
            retries_429=HTTP_429_RETRIES,
            retries_5xx=HTTP_5XX_RETRIES,
            default_cache_ttl=HTTP_CACHE_DEFAULT_TTL_SEC,
            default_stale_ttl=HTTP_CACHE_STALE_TTL_SEC,
            should_log=self._should_log,
            error_tick=lambda key, log_fn, err=None, every=20: (
                getattr(self, "_errors", None).tick(key, log_fn, err=err, every=every)
                if getattr(self, "_errors", None) is not None
                else None
            ),
            log_warn=print,
        )
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
        self._score_cache_by_key = {}             # (cid, late_relax) -> {"ts","fp","sig"}
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
        while self._skip_events:
            head = self._skip_events[0]
            if isinstance(head, (tuple, list)):
                ts = float(head[0] or 0.0) if len(head) >= 1 else 0.0
            elif isinstance(head, (int, float)):
                ts = float(head)
            else:
                ts = 0.0
            if (now - ts) > max(60, int(window_sec)):
                self._skip_events.popleft()
                continue
            break
        counts = defaultdict(int)
        for ev in self._skip_events:
            if isinstance(ev, (tuple, list)) and len(ev) >= 2:
                reason = str(ev[1] or "unknown")
            else:
                reason = "unknown"
            counts[reason] += 1
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
        if cid is None:
            return "n/a"
        s = str(cid)
        if not s:
            return "n/a"
        return f"{s[:10]}..."

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

    async def _http_get_json(
        self,
        url: str,
        params: dict | None = None,
        timeout: float = 8.0,
        cache_ttl: float = HTTP_CACHE_DEFAULT_TTL_SEC,
        stale_ttl: float = HTTP_CACHE_STALE_TTL_SEC,
    ):
        return await self._http_service.get_json(
            url,
            params=params,
            timeout=timeout,
            cache_ttl=cache_ttl,
            stale_ttl=stale_ttl,
        )

    async def _gather_bounded(self, coros, limit: int):
        """Run awaitables with bounded concurrency to avoid network bursts."""
        return await self._http_service.gather_bounded(list(coros), limit=limit)

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
                        top_s = ", ".join(f"{str(w)[:6]}..={s:.2f}" for w, s in top)
                        print(f"{B}[COPY-INTEL]{RS} leaders24h={len(live_scores)} top={top_s}")
                self._copyflow_leaders_family = family_scores
                if self._should_log("copyflow-intel-family", 150):
                    fam_parts = []
                    for fam_key, mp in sorted(family_scores.items()):
                        if not mp:
                            continue
                        topf = sorted(mp.items(), key=lambda kv: kv[1], reverse=True)[:1]
                        fam_wallet = str(topf[0][0]) if topf else "n/a"
                        fam_score = float(topf[0][1]) if topf else 0.0
                        fam_parts.append(f"{fam_key}:{fam_wallet[:6]}..={fam_score:.2f}")
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

    def _price_hist_last_ts(self, asset: str) -> float:
        """Safe last timestamp from price_history across legacy/new entry formats."""
        ph = self.price_history.get(asset)
        if not ph:
            return 0.0
        last = ph[-1]
        if isinstance(last, (tuple, list)):
            return float(last[0] or 0.0) if len(last) >= 1 else 0.0
        if isinstance(last, (int, float)):
            return float(last)
        return 0.0

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
            rtds_ts = self._price_hist_last_ts(asset)
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
            rtds_ts2 = self._price_hist_last_ts(asset)
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
            now0 = _time.time()
            wait_until = max(
                float(self._rtds_cooldown_until or 0.0),
                float(self._rtds_last_connect_try_ts or 0.0) + max(0.2, RTDS_CONNECT_MIN_GAP_SEC),
            )
            if wait_until > now0:
                await asyncio.sleep(wait_until - now0)
            self._rtds_last_connect_try_ts = _time.time()
            pinger_task = None
            try:
                async with websockets.connect(
                    RTDS, additional_headers={"Origin": "https://polymarket.com"},
                    ping_interval=None,
                    compression=None,
                ) as ws:
                    await ws.send(json.dumps({
                        "action": "subscribe",
                        "subscriptions": [{"topic": "crypto_prices", "type": "update"}]
                    }))
                    self.rtds_ok = True
                    self._rtds_ws = ws   # expose for dynamic market subscriptions
                    self._rtds_fails = 0
                    self._rtds_cooldown_until = 0.0
                    self._rtds_last_msg_ts = _time.time()
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
                            try:
                                await ws.send(json.dumps({"action": "ping"}))
                            except Exception:
                                try:
                                    await ws.send("PING")
                                except Exception:
                                    break
                            # If server is silent for too long, force reconnect.
                            if (_time.time() - float(self._rtds_last_msg_ts or 0.0)) > max(8.0, RTDS_IDLE_RECONNECT_SEC):
                                try:
                                    await ws.close(code=1012, reason="rtds-idle-timeout")
                                except Exception:
                                    pass
                                break
                    pinger_task = asyncio.create_task(pinger())
                    self._rtds_ping_task = pinger_task

                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=max(5.0, RTDS_RECV_TIMEOUT_SEC))
                        except asyncio.TimeoutError:
                            raise TimeoutError("RTDS recv timeout")
                        if not raw: continue
                        self._rtds_last_msg_ts = _time.time()
                        try:
                            if isinstance(raw, (bytes, bytearray)):
                                raw = raw.decode("utf-8", "ignore")
                            msg = json.loads(raw)
                        except: continue
                        events = msg if isinstance(msg, list) else [msg]

                        # ── Market token price update (instant up_price refresh) ──
                        for ev in events:
                            if not isinstance(ev, dict):
                                continue
                            if ev.get("event_type") == "price_change" or ev.get("topic") == "market":
                                pl = ev.get("payload", {}) or ev
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

                            topic = str(ev.get("topic", "") or "").lower()
                            if topic not in ("crypto_prices", "crypto_prices_update", "crypto_prices_v2"):
                                continue
                            payload = ev.get("payload", {}) or {}
                            payloads = payload if isinstance(payload, list) else [payload]
                            for p in payloads:
                                if not isinstance(p, dict):
                                    continue
                                sym = str(p.get("symbol", "") or p.get("pair", "") or p.get("s", "")).lower()
                                val = float(p.get("value") or p.get("price") or p.get("v") or 0)
                                if val <= 0:
                                    continue
                                MAP = {"btcusdt":"BTC","ethusdt":"ETH","solusdt":"SOL","xrpusdt":"XRP"}
                                asset = MAP.get(sym)
                                if not asset:
                                    continue
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
                                    if now_t - self._last_eval_time.get(cid, 0) < RTDS_EVAL_MIN_INTERVAL_SEC: continue
                                    mins = (m["end_ts"] - now_t) / 60
                                    if mins < 1: continue
                                    self._last_eval_time[cid] = now_t
                                    m_rt = dict(m); m_rt["mins_left"] = mins
                                    asyncio.create_task(self.evaluate(m_rt))
            except Exception as e:
                self.rtds_ok = False
                self._rtds_ws = None
                print(f"{R}[RTDS] Reconnect: {e}{RS}")
                err_s = str(e).lower()
                fails = int(getattr(self, "_rtds_fails", 0) or 0) + 1
                self._rtds_fails = fails
                if "429" in err_s or "too many requests" in err_s:
                    # Upstream throttling: back off harder to recover stable freshness.
                    wait_s = min(
                        max(10.0, RTDS_429_MAX_BACKOFF_SEC),
                        max(5.0, RTDS_429_BASE_BACKOFF_SEC) * (2.0 ** min(8, fails - 1)),
                    )
                else:
                    wait_s = min(60.0, 2.0 * (2 ** min(5, fails - 1)))
                wait_s = wait_s * random.uniform(0.90, 1.25)
                self._rtds_cooldown_until = max(float(self._rtds_cooldown_until or 0.0), _time.time() + wait_s)
                if self._noisy_log_enabled("rtds-retry", 5.0):
                    print(f"{Y}[RTDS]{RS} retry in {wait_s:.1f}s (fails={fails})")
                await asyncio.sleep(wait_s)
            else:
                self._rtds_fails = 0
                self._rtds_cooldown_until = 0.0
            finally:
                self.rtds_ok = False
                self._rtds_ws = None
                if pinger_task is not None:
                    try:
                        pinger_task.cancel()
                    except Exception:
                        pass
                if self._rtds_ping_task is pinger_task:
                    self._rtds_ping_task = None

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
            data = await self._http_get_json(
                f"{GAMMA}/events",
                params={
                    "series_id": info["id"],
                    "active": "true",
                    "closed": "false",
                    "order": "startDate",
                    "ascending": "true",
                    "limit": "20",
                },
                timeout=5,
                cache_ttl=max(0.8, MARKET_REFRESH_SEC / 3.0),
                stale_ttl=max(12.0, MARKET_REFRESH_SEC * 2.0),
            )
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
            if self._should_log(f"fetch-series:{slug}", 20):
                print(f"{Y}[FETCH]{RS} {slug} degraded: {e}")
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
        from clawbot_v2.strategy.core import _score_market
        cid = str(m.get("conditionId", "") or "")
        if not cid:
            return await _score_market(self, m, late_relax)

        key = (cid, bool(late_relax))
        now = _time.time()
        asset = str(m.get("asset", "") or "")
        up_price = float(m.get("up_price", 0.0) or 0.0)
        mins_left = float(m.get("mins_left", 0.0) or 0.0)
        cl_p = float(self.cl_prices.get(asset, 0.0) or 0.0) if asset else 0.0
        rtds_p = float(self.prices.get(asset, 0.0) or 0.0) if asset else 0.0
        fp = (
            round(up_price, 4),
            round(mins_left, 1),
            round(cl_p, 4),
            round(rtds_p, 4),
            int(m.get("start_ts", 0) or 0),
            int(m.get("end_ts", 0) or 0),
        )
        cached = self._score_cache_by_key.get(key)
        if cached:
            age = now - float(cached.get("ts", 0.0) or 0.0)
            if age < SCORE_DEBOUNCE_SEC and cached.get("fp") == fp:
                sig_cached = cached.get("sig")
                return dict(sig_cached) if isinstance(sig_cached, dict) else None
        if len(self._score_cache_by_key) > 5000:
            cutoff = now - 180.0
            self._score_cache_by_key = {
                k: v for k, v in self._score_cache_by_key.items()
                if float((v or {}).get("ts", 0.0) or 0.0) >= cutoff
            }

        sig = await _score_market(self, m, late_relax)
        self._score_cache_by_key[key] = {
            "ts": now,
            "fp": fp,
            "sig": dict(sig) if isinstance(sig, dict) else None,
        }
        return sig

    def _build_forced_round_signal(self, m: dict) -> dict | None:
        """Build a minimally-complete executable signal when score path yields no candidates.
        This is used only for force-coverage recovery to avoid prolonged zero-trade stalls.
        """
        try:
            cid = str(m.get("conditionId", "") or "")
            if not cid:
                return None
            asset = str(m.get("asset", "") or "")
            duration = int(m.get("duration", 0) or 0)
            mins_left = float(m.get("mins_left", 0.0) or 0.0)
            if mins_left <= 0.5:
                return None
            open_price = float(self.open_prices.get(cid, 0.0) or 0.0)
            if open_price <= 0:
                print(f"{Y}[FORCE-SYNTH-SKIP]{RS} {asset} {duration}m no open_price (cid={self._short_cid(cid)})")
                return None
            current = float(self._current_price(asset) or 0.0)
            if current <= 0:
                print(f"{Y}[FORCE-SYNTH-SKIP]{RS} {asset} {duration}m no current price (RTDS+CL both 0)")
                return None
            side = "Up" if current >= open_price else "Down"
            up_price = float(m.get("up_price", 0.5) or 0.5)
            up_price = max(0.01, min(0.99, up_price))
            entry = up_price if side == "Up" else (1.0 - up_price)
            max_entry_allowed = min(0.95, MAX_ENTRY_PRICE + MAX_ENTRY_TOL)
            entry = max(0.01, min(max_entry_allowed, entry))
            if entry <= 0.0 or entry >= 1.0:
                return None
            token_id = str(m.get("token_up", "") if side == "Up" else m.get("token_down", "")).strip()
            if not token_id:
                return None
            src_tag = f"[{self.open_prices_source.get(cid, '?')}]"
            total_life = float((m.get("end_ts", 0.0) or 0.0) - (m.get("start_ts", 0.0) or 0.0))
            pct_remaining = ((mins_left * 60.0) / total_life) if total_life > 0 else 0.0
            move_rel = (current - open_price) / max(open_price, 1e-9)
            move_str = f"{move_rel:+.3%}"
            true_prob = max(0.50, min(0.62, 0.50 + min(0.12, abs(move_rel) * 3.0)))
            edge = true_prob - entry
            size = float(self._kelly_size(true_prob=true_prob, entry=entry, kelly_frac=0.10))
            size = max(max(MIN_BET_ABS, MIN_EXEC_NOTIONAL_USDC), round(size, 2))
            if size > self.bankroll * MAX_BANKROLL_PCT:
                size = round(self.bankroll * MAX_BANKROLL_PCT, 2)
            # Guarantee size is always above execution minimum after bankroll cap.
            size = max(size, MIN_EXEC_NOTIONAL_USDC, MIN_BET_ABS)
            if size <= 0:
                return None
            cl_now = float(self.cl_prices.get(asset, 0.0) or 0.0)
            cl_agree = True
            if cl_now > 0:
                cl_agree = (cl_now >= open_price) if side == "Up" else (cl_now < open_price)
            return {
                "cid": cid,
                "m": m,
                "asset": asset,
                "duration": duration,
                "mins_left": mins_left,
                "side": side,
                "score": max(MIN_SCORE_GATE, 9),
                "label": f"{asset} {duration}m | forced-coverage",
                "open_price": open_price,
                "src_tag": src_tag,
                "current": current,
                "move_str": move_str,
                "pct_remaining": pct_remaining,
                "bs_prob": true_prob,
                "mom_prob": true_prob,
                "true_prob": true_prob,
                "up_price": up_price,
                "edge": edge,
                "entry": entry,
                "size": size,
                "token_id": token_id,
                "cl_agree": cl_agree,
                "ob_imbalance": 0.0,
                "imbalance_confirms": False,
                "tf_votes": 0,
                "very_strong_mom": False,
                "perp_basis": 0.0,
                "vwap_dev": 0.0,
                "cross_count": 0,
                "chainlink_age_s": float((_time.time() - float(self.cl_updated.get(asset, 0.0) or 0.0)) if self.cl_updated.get(asset, 0.0) else -1.0),
                "open_price_source": self.open_prices_source.get(cid, "?"),
                "min_edge": float(self._adaptive_min_edge()),
                "force_taker": False,
                "pm_book_data": None,
                "use_limit": False,
                "max_entry_allowed": max_entry_allowed,
                "analysis_quality": 0.50,
                "analysis_conviction": 0.50,
                "taker_ratio": 0.0,
                "vol_ratio": 0.0,
                "snap_token_up": str(m.get("token_up", "") or ""),
                "snap_token_down": str(m.get("token_down", "") or ""),
                "round_force_coverage": True,
                "round_force_execute": True,
            }
        except Exception:
            return None

    async def _execute_trade(self, sig: dict):
        from clawbot_v2.execution.core import _execute_trade
        return await _execute_trade(self, sig)

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
            # M-4: block copyflow on-demand refreshes during the wait window — data was just fetched
            _cf_block_until = deadline + 1.0
            self._copyflow_cid_on_demand_ts[sig["cid"]] = _cf_block_until
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
        from clawbot_v2.execution.core import evaluate
        return await evaluate(self, m)

    async def _place_order(self, token_id, side, price, size_usdc, asset, duration, mins_left, true_prob=0.5, cl_agree=True, min_edge_req=None, force_taker=False, score=0, pm_book_data=None, use_limit=False, max_entry_allowed=None, hc15_mode=False, hc15_fallback_cap=0.36, core_position=True, round_force=False):
        from clawbot_v2.execution.core import _place_order
        return await _place_order(self, token_id, side, price, size_usdc, asset, duration, mins_left, true_prob, cl_agree, min_edge_req, force_taker, score, pm_book_data, use_limit, max_entry_allowed, hc15_mode, hc15_fallback_cap, core_position, round_force)

    # ── RESOLVE ───────────────────────────────────────────────────────────────
    async def _resolve(self):
        from clawbot_v2.settlement.core import _resolve
        return await _resolve(self)

    # ── REDEEM LOOP — polls every 30s, determines win/loss on-chain ───────────
    async def _redeem_loop(self):
        from clawbot_v2.settlement.core import _redeem_loop
        return await _redeem_loop(self)

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
        kline_ratio = rec_taker / rec_vol if rec_vol > 0 else 0.5
        vol_ratio   = (rec_vol / 3) / hist_avg if hist_avg > 0 else 1.0
        # Blend with real-time aggTrade OFI when available (higher weight = more responsive)
        agg_ratio = self._binance_agg_ofi(asset)
        if agg_ratio != 0.5:
            taker_ratio = AGG_OFI_BLEND_W * agg_ratio + (1.0 - AGG_OFI_BLEND_W) * kline_ratio
        else:
            taker_ratio = kline_ratio
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
        loop = asyncio.get_running_loop()
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
                async with _ws.connect(url, ping_interval=20, ping_timeout=30, compression=None) as ws:
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
                async with _ws.connect(url, ping_interval=20, ping_timeout=30, compression=None) as ws:
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

    async def _stream_binance_aggtrade(self):
        """Persistent WS: aggTrade stream for all assets → real-time OFI in binance_cache."""
        import websockets as _ws, json as _j, time as _t
        sym_map = {v: k for k, v in BNB_SYM.items()}
        streams = [f"{s}@aggTrade" for s in BNB_SYM.values()]
        url = "wss://stream.binance.com/stream?streams=" + "/".join(streams)
        delay = 5
        while True:
            try:
                async with _ws.connect(url, ping_interval=20, ping_timeout=30, compression=None) as ws:
                    print(f"{G}[BNB-AGG] aggTrade OFI stream connected{RS}")
                    delay = 5
                    async for raw in ws:
                        msg    = _j.loads(raw)
                        stream = msg.get("stream", "")
                        data   = msg.get("data", {})
                        sym    = stream.split("@")[0]
                        asset  = sym_map.get(sym)
                        if not asset:
                            continue
                        # m=True means maker was buyer → taker is SELLER; m=False → taker is BUYER
                        is_buy = not bool(data.get("m", False))
                        qty    = float(data.get("q", 0) or 0)
                        ts     = _t.time()
                        self.binance_cache[asset]["agg_ofi_buf"].append((ts, is_buy, qty))
            except Exception as e:
                print(f"{Y}[BNB-AGG] {e} — reconnect in {delay}s{RS}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)

    def _binance_agg_ofi(self, asset: str, window_sec: float | None = None) -> float:
        """Rolling OFI ratio from aggTrade buffer. Returns 0.5 if no data."""
        import time as _t
        win = window_sec if window_sec is not None else AGG_OFI_WINDOW_SEC
        buf = self.binance_cache.get(asset, {}).get("agg_ofi_buf")
        if not buf:
            return 0.5
        cutoff = _t.time() - win
        buy_q = sell_q = 0.0
        for (ts, is_buy, qty) in reversed(buf):   # newest-first; break on first old entry
            if ts < cutoff:
                break
            if is_buy:
                buy_q += qty
            else:
                sell_q += qty
        total = buy_q + sell_q
        return round(buy_q / total, 4) if total > 0 else 0.5

    # ── Free Binance gap-closer streams ──────────────────────────────────────

    async def _stream_binance_liquidations(self):
        """Subscribe to all-market forced-liquidation stream (free, no auth).
        Opposing-side liquidations confirm our direction signal."""
        import time as _t, json as _json
        url  = "wss://fstream.binance.com/ws/!forceOrder@arr"
        SYMS = {"BTCUSDT": "BTC", "ETHUSDT": "ETH", "SOLUSDT": "SOL", "XRPUSDT": "XRP"}
        delay = 5
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    delay = 5
                    async for raw in ws:
                        data  = _json.loads(raw)
                        order = data.get("o", {})
                        sym   = order.get("s", "")
                        asset = SYMS.get(sym)
                        if asset is None:
                            continue
                        side  = order.get("S", "")   # "BUY"=liquidated long, "SELL"=liquidated short
                        qty   = float(order.get("q", 0) or 0)
                        price = float(order.get("p", 0) or 0)
                        usd   = qty * price
                        self._liq_buf[asset].append((_t.time(), side, usd))
            except Exception as e:
                print(f"{Y}[BNB-LIQ] {e} — reconnect in {delay}s{RS}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)

    def _liq_signal(self, asset: str, direction: str) -> int:
        """Score from liquidation stream: opposing-side liquidations confirm our direction.
        BUY-side liquidated = leveraged longs blown out = bearish pressure (Down confirms).
        SELL-side liquidated = leveraged shorts blown out = bullish pressure (Up confirms).
        Returns 0..+2."""
        import time as _t
        buf = self._liq_buf.get(asset)
        if not buf:
            return 0
        cutoff = _t.time() - LIQ_WINDOW_SEC
        is_up  = direction == "Up"
        # Opposing-side liq = confirms our direction
        opp_usd = sum(usd for ts, side, usd in buf
                      if ts >= cutoff and ((is_up and side == "SELL") or (not is_up and side == "BUY")))
        if   opp_usd >= LIQ_SCORE_USD_2: return 2
        elif opp_usd >= LIQ_SCORE_USD_1: return 1
        return 0

    async def _oi_ls_loop(self):
        """Poll Open Interest and global Long/Short ratio every OI_POLL_SEC (free Binance REST)."""
        import aiohttp, time as _t
        SYMS = {"BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT", "XRP": "XRPUSDT"}
        while True:
            await asyncio.sleep(OI_POLL_SEC)
            if not OI_ENABLED:
                continue
            async with aiohttp.ClientSession() as sess:
                for asset, sym in SYMS.items():
                    # Open Interest
                    try:
                        async with sess.get(
                            f"https://fapi.binance.com/fapi/v1/openInterest?symbol={sym}",
                            timeout=aiohttp.ClientTimeout(total=5)
                        ) as r:
                            d = await r.json()
                            oi_now = float(d.get("openInterest", 0) or 0)
                            if oi_now > 0:
                                prev = self._oi[asset]["cur"]
                                self._oi[asset] = {"cur": oi_now, "prev": prev, "ts": _t.time()}
                    except Exception:
                        pass
                    # Global Long/Short ratio
                    try:
                        async with sess.get(
                            f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
                            f"?symbol={sym}&period=5m&limit=1",
                            timeout=aiohttp.ClientTimeout(total=5)
                        ) as r:
                            rows = await r.json()
                            if rows and isinstance(rows, list):
                                long_pct = float(rows[0].get("longAccount", 0.5) or 0.5)
                                self._ls_ratio[asset] = long_pct
                    except Exception:
                        pass

    def _cross_asset_direction(self, asset: str, direction: str) -> int:
        """Count how many OTHER assets are trending in the same direction right now.
        Uses Binance RTDS price vs window open_price as primary signal; falls back to
        Kalman velocity when the price move is too small (<0.03%) to be directionally
        reliable (first ~60s of a new window). Returns 0-3.
        Each distinct asset is counted at most once (uses freshest window open_price)."""
        is_up = (direction == "Up")
        count = 0
        now_ts = _time.time()
        seen_assets: set[str] = set()
        for cid, m in self.active_mkts.items():
            a = m.get("asset", "")
            if a == asset or a in seen_assets:
                continue
            op  = self.open_prices.get(cid, 0)
            if op <= 0:
                continue
            # Prefer RTDS price; skip if stale (> 30s) to avoid false consensus from stale feed
            cur = self.prices.get(a, 0)
            ts_cur = self._price_hist_last_ts(a)
            if cur <= 0 or (ts_cur > 0 and now_ts - ts_cur > 30.0):
                cur = self.cl_prices.get(a, 0)
            if cur <= 0:
                continue
            seen_assets.add(a)
            move_pct = abs(cur - op) / max(op, 1e-8)
            if move_pct >= 0.0003:
                # Price has moved enough from open → trust direct price direction
                dir_up = (cur > op)
            else:
                # Near window open: use Kalman velocity for instantaneous direction
                kp = self._kalman_vel_prob(a)
                if kp < 0.45 or kp > 0.55:
                    dir_up = (kp > 0.50)
                else:
                    continue  # Kalman also undecided → skip this asset
            if dir_up == is_up:
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

    def _bootstrap_resolved_samples_from_metrics(self):
        """Warm-start resolved sample buffer from persisted on-chain metrics file.

        This avoids a blind period after bot restart where 5m runtime guard has
        no settled history and keeps trading low-quality 5m rounds.
        """
        try:
            loaded = 0
            rows = []
            if self._metrics_db_ready:
                try:
                    conn = sqlite3.connect(METRICS_DB_FILE, timeout=3.0)
                    try:
                        cur = conn.cursor()
                        rows = cur.execute(
                            """
                            SELECT asset, side, duration, entry_price, open_price_source, chainlink_age_s, pnl, result, round_key
                            FROM resolve_metrics
                            WHERE event='RESOLVE'
                            ORDER BY id DESC
                            LIMIT 5000
                            """
                        ).fetchall()
                    finally:
                        conn.close()
                    rows = list(reversed(rows))
                except Exception:
                    rows = []
            if rows:
                for asset, side, dur, entry_price, open_src, cl_age_s, pnl, result, rk in rows:
                    d = int(dur or 0)
                    if d <= 0:
                        rks = str(rk or "")
                        d = 5 if "-5m-" in rks else (15 if "-15m-" in rks else 0)
                    if d <= 0:
                        continue
                    self._resolved_samples.append({
                        "ts": _time.time(),
                        "asset": str(asset or ""),
                        "side": str(side or ""),
                        "duration": d,
                        "entry": float(entry_price or 0.0),
                        "source": str(open_src or "?"),
                        "cl_age_s": cl_age_s,
                        "pnl": float(pnl or 0.0),
                        "won": str(result or "").upper() == "WIN",
                    })
                    loaded += 1
            elif os.path.exists(METRICS_FILE):
                # Fallback path for first migration / db unavailable.
                with open(METRICS_FILE, encoding="utf-8") as f:
                    lines = f.readlines()[-5000:]
                for ln in lines:
                    try:
                        row = json.loads(ln)
                    except Exception:
                        continue
                    if str(row.get("event", "")) != "RESOLVE":
                        continue
                    dur = int(row.get("duration", 0) or 0)
                    if dur <= 0:
                        rk = str(row.get("round_key", "") or "")
                        if "-5m-" in rk:
                            dur = 5
                        elif "-15m-" in rk:
                            dur = 15
                    if dur <= 0:
                        continue
                    pnl = float(row.get("pnl", 0.0) or 0.0)
                    won = str(row.get("result", "")).upper() == "WIN"
                    self._resolved_samples.append({
                        "ts": _time.time(),
                        "asset": str(row.get("asset", "") or ""),
                        "side": str(row.get("side", "") or ""),
                        "duration": dur,
                        "entry": float(row.get("entry_price", 0.0) or 0.0),
                        "source": str(row.get("open_price_source", "?") or "?"),
                        "cl_age_s": row.get("chainlink_age_s", None),
                        "pnl": pnl,
                        "won": bool(won),
                    })
                    loaded += 1
            if loaded > 0:
                print(f"{B}[BOOT]{RS} loaded {loaded} resolved samples from metrics for runtime guards")
        except Exception as e:
            print(f"{Y}[BOOT] resolved-sample bootstrap failed: {e}{RS}")

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

    def _asset_entry_profile(self, asset: str, duration: int, side: str, entry: float, open_src: str, cl_age_s) -> dict:
        """Data-driven quality prior from settled on-chain outcomes for (asset,duration,side,entry-band).
        Non-blocking: adjusts score/edge/prob/size continuously."""
        if not ASSET_ENTRY_PRIOR_ENABLED:
            return {"score_adj": 0, "edge_adj": 0.0, "prob_adj": 0.0, "size_mult": 1.0, "n": 0, "exp": 0.0, "wr_lb": 0.5, "pf": 1.0, "band": "na"}
        try:
            band = self._entry_band(float(entry or 0.0))
            now_ts = _time.time()
            lookback_s = max(0.0, float(ASSET_ENTRY_PRIOR_LOOKBACK_H) * 3600.0)
            src_ok = (open_src == "PM") and (cl_age_s is not None) and (float(cl_age_s) <= 45.0)
            vals = []
            for s in list(self._resolved_samples)[-1200:]:
                if str(s.get("asset", "") or "") != str(asset):
                    continue
                if int(s.get("duration", 0) or 0) != int(duration):
                    continue
                if str(s.get("side", "") or "") != str(side):
                    continue
                if self._entry_band(float(s.get("entry", 0.0) or 0.0)) != band:
                    continue
                ts = float(s.get("ts", 0.0) or 0.0)
                if ts <= 0:
                    continue
                if lookback_s > 0 and (now_ts - ts) > lookback_s:
                    continue
                s_src = str(s.get("source", "?") or "?")
                s_age = s.get("cl_age_s", None)
                s_ok = (s_src == "PM") and (s_age is not None) and (float(s_age) <= 45.0)
                # Prefer same source-quality bucket; if sparse, fall back to full bucket.
                if s_ok == src_ok:
                    vals.append(s)
            if len(vals) < max(1, ASSET_ENTRY_PRIOR_MIN_N // 2):
                vals = []
                for s in list(self._resolved_samples)[-1200:]:
                    if str(s.get("asset", "") or "") != str(asset):
                        continue
                    if int(s.get("duration", 0) or 0) != int(duration):
                        continue
                    if str(s.get("side", "") or "") != str(side):
                        continue
                    if self._entry_band(float(s.get("entry", 0.0) or 0.0)) != band:
                        continue
                    ts = float(s.get("ts", 0.0) or 0.0)
                    if ts <= 0:
                        continue
                    if lookback_s > 0 and (now_ts - ts) > lookback_s:
                        continue
                    vals.append(s)
            n = len(vals)
            if n < max(1, ASSET_ENTRY_PRIOR_MIN_N):
                return {"score_adj": 0, "edge_adj": 0.0, "prob_adj": 0.0, "size_mult": 1.0, "n": n, "exp": 0.0, "wr_lb": 0.5, "pf": 1.0, "band": band}
            wins = sum(1 for x in vals if bool(x.get("won", False)))
            pnl = sum(float(x.get("pnl", 0.0) or 0.0) for x in vals)
            exp = pnl / max(1, n)
            wr_lb = self._wilson_lower_bound(wins, n)
            gross_win = sum(float(x.get("pnl", 0.0) or 0.0) for x in vals if float(x.get("pnl", 0.0) or 0.0) > 0)
            gross_loss = -sum(float(x.get("pnl", 0.0) or 0.0) for x in vals if float(x.get("pnl", 0.0) or 0.0) < 0)
            pf = (gross_win / gross_loss) if gross_loss > 0 else (2.0 if gross_win > 0 else 1.0)

            score_adj = 0
            edge_adj = 0.0
            prob_adj = 0.0
            size_mult = 1.0
            # Poor cluster => reduce confidence and size, but never block.
            if (wr_lb < 0.44 and pf < 0.90) or exp <= -0.70:
                score_adj = -3 if (n >= 24 and wr_lb < 0.42 and exp <= -1.00) else -2
                edge_adj = -(0.005 + min(0.007, abs(exp) / 120.0))
                prob_adj = -(0.008 + min(0.010, abs(exp) / 90.0))
                size_mult = 0.55 if score_adj <= -3 else 0.70
            elif (wr_lb < 0.49 and pf < 0.98) or exp < 0.0:
                score_adj = -1
                edge_adj = -0.003
                prob_adj = -0.005
                size_mult = 0.85
            # Strong cluster => boost confidence and allow modest size increase.
            elif (wr_lb >= 0.58 and pf >= 1.25 and exp >= 0.90 and n >= 24):
                score_adj = 2
                edge_adj = 0.005
                prob_adj = 0.008
                size_mult = 1.15
            elif (wr_lb >= 0.54 and pf >= 1.10 and exp >= 0.30):
                score_adj = 1
                edge_adj = 0.003
                prob_adj = 0.005
                size_mult = 1.06

            score_adj = int(max(-ASSET_ENTRY_PRIOR_MAX_SCORE_ADJ, min(ASSET_ENTRY_PRIOR_MAX_SCORE_ADJ, score_adj)))
            edge_adj = float(max(-ASSET_ENTRY_PRIOR_MAX_EDGE_ADJ, min(ASSET_ENTRY_PRIOR_MAX_EDGE_ADJ, edge_adj)))
            prob_adj = float(max(-ASSET_ENTRY_PRIOR_MAX_PROB_ADJ, min(ASSET_ENTRY_PRIOR_MAX_PROB_ADJ, prob_adj)))
            size_mult = float(max(ASSET_ENTRY_PRIOR_MIN_SIZE_MULT, min(ASSET_ENTRY_PRIOR_MAX_SIZE_MULT, size_mult)))
            return {
                "score_adj": score_adj,
                "edge_adj": edge_adj,
                "prob_adj": prob_adj,
                "size_mult": size_mult,
                "n": int(n),
                "exp": float(exp),
                "wr_lb": float(wr_lb),
                "pf": float(pf),
                "band": band,
            }
        except Exception:
            return {"score_adj": 0, "edge_adj": 0.0, "prob_adj": 0.0, "size_mult": 1.0, "n": 0, "exp": 0.0, "wr_lb": 0.5, "pf": 1.0, "band": "na"}

    def _macro_blackout_active(self) -> str:
        """Return event name if current time is within macro blackout window, else empty string."""
        if not MACRO_BLACKOUT_ENABLED:
            return ""
        import time as _t
        try:
            with open(MACRO_BLACKOUT_FILE, "r") as f:
                events = json.load(f)
        except Exception:
            return ""
        now = _t.time()
        before_s = MACRO_BLACKOUT_MINS_BEFORE * 60
        after_s  = MACRO_BLACKOUT_MINS_AFTER  * 60
        for ev in events:
            ev_ts = float(ev.get("ts", 0))
            if (now >= ev_ts - before_s) and (now <= ev_ts + after_s):
                return str(ev.get("name", "MACRO"))
        return ""

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

    def _load_autopilot_policy(self):
        # Profit-oriented defaults with safety rails; never hard-stop trading by default.
        self._autopilot_policy = {
            "enabled": True,
            "target_daily_profit_usdc": 100.0,
            "soft_daily_loss_usdc": 25.0,
            "hard_daily_loss_usdc": 45.0,
            "soft_intraday_drawdown_usdc": 20.0,
            "hard_intraday_drawdown_usdc": 35.0,
            "size_mult_boost": 1.12,
            "size_mult_normal": 1.00,
            "size_mult_soft": 0.82,
            "size_mult_hard": 0.62,
            "size_mult_floor": 0.55,   # never 0: avoids false no-trade days
            "disable_5m_on_hard": False,  # keep trading unless explicitly enabled
        }
        try:
            if os.path.exists(AUTOPILOT_POLICY_FILE):
                with open(AUTOPILOT_POLICY_FILE, encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    self._autopilot_policy.update(data)
        except Exception:
            pass

    def _safe_daily_pnl(self) -> float:
        # Use cached UTC-day reconciliation, not historical start-bank deltas.
        try:
            now_ts = _time.time()
            day_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            dc = self._dash_daily_cache or {}
            if dc.get("day") == day_utc and (now_ts - float(dc.get("ts", 0.0) or 0.0)) <= 60.0:
                return float(dc.get("pnl", 0.0) or 0.0)
            pnl = 0.0
            for r in self._metrics_resolve_rows_cached(ttl_sec=15.0):
                if str(r.get("ts", "")).startswith(day_utc):
                    pnl += float(r.get("pnl", 0.0) or 0.0)
            return float(pnl)
        except Exception:
            return float(self.daily_pnl or 0.0)

    def _apply_autopilot_policy(self):
        if not AUTOPILOT_ENABLED:
            self._autopilot_size_mult = 1.0
            self._autopilot_mode = "off"
            return
        now_ts = _time.time()
        if (now_ts - float(self._autopilot_last_eval_ts or 0.0)) < max(3.0, AUTOPILOT_CHECK_SEC):
            return
        self._autopilot_last_eval_ts = now_ts

        pol = dict(self._autopilot_policy or {})
        if not bool(pol.get("enabled", True)):
            self._autopilot_size_mult = 1.0
            self._autopilot_mode = "disabled"
            return

        day_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        eq_now = float(self.onchain_total_equity if self.onchain_snapshot_ts > 0 else self.bankroll)
        if self._autopilot_day_key != day_key:
            self._autopilot_day_key = day_key
            self._autopilot_day_peak = eq_now
        self._autopilot_day_peak = max(float(self._autopilot_day_peak or eq_now), eq_now)

        day_pnl = self._safe_daily_pnl()
        day_dd = max(0.0, float(self._autopilot_day_peak) - eq_now)
        snap = self._growth_snapshot()
        recent_pf = float(snap.get("recent_pf", 1.0) or 1.0)
        recent_wr_lb = float(snap.get("recent_wr_lb", 0.5) or 0.5)

        target = float(pol.get("target_daily_profit_usdc", 100.0) or 100.0)
        base_soft_loss = abs(float(pol.get("soft_daily_loss_usdc", 25.0) or 25.0))
        base_hard_loss = abs(float(pol.get("hard_daily_loss_usdc", 45.0) or 45.0))
        base_soft_dd = abs(float(pol.get("soft_intraday_drawdown_usdc", 20.0) or 20.0))
        base_hard_dd = abs(float(pol.get("hard_intraday_drawdown_usdc", 35.0) or 35.0))
        # Dynamic thresholds from realized PnL volatility (agentic, regime-aware).
        rp = [abs(float(x or 0.0)) for x in list(self.recent_pnl)[-30:]]
        avg_abs = (sum(rp) / len(rp)) if rp else 2.5
        soft_loss = max(base_soft_loss, avg_abs * 5.0)
        hard_loss = max(base_hard_loss, avg_abs * 8.0)
        soft_dd = max(base_soft_dd, avg_abs * 4.0)
        hard_dd = max(base_hard_dd, avg_abs * 7.0)
        m_boost = float(pol.get("size_mult_boost", 1.12) or 1.12)
        m_norm = float(pol.get("size_mult_normal", 1.0) or 1.0)
        m_soft = float(pol.get("size_mult_soft", 0.82) or 0.82)
        m_hard = float(pol.get("size_mult_hard", 0.62) or 0.62)
        m_floor = max(0.25, float(pol.get("size_mult_floor", 0.55) or 0.55))
        disable_5m_on_hard = bool(pol.get("disable_5m_on_hard", False))

        mode = "normal"
        mult = m_norm
        if (day_pnl <= -hard_loss) or (day_dd >= hard_dd):
            mode = "hard-protect"
            mult = m_hard
            if disable_5m_on_hard:
                self._enable_5m_runtime = False
        elif (day_pnl <= -soft_loss) or (day_dd >= soft_dd):
            mode = "soft-protect"
            mult = m_soft
        elif (day_pnl >= target) and (recent_pf >= 1.10) and (recent_wr_lb >= 0.50):
            mode = "boost"
            mult = m_boost

        # Never drop to no-trade due to policy noise/mismeasurement.
        mult = max(m_floor, min(1.35, mult))
        changed = (abs(mult - float(self._autopilot_size_mult or 1.0)) >= 0.03) or (mode != self._autopilot_mode)
        self._autopilot_size_mult = mult
        self._autopilot_mode = mode
        if changed and self._should_log("autopilot-mode", 30):
            print(
                f"{B}[AUTOPILOT]{RS} mode={mode} size_x={mult:.2f} "
                f"day_pnl={day_pnl:+.2f} day_dd={day_dd:.2f} "
                f"pf20={recent_pf:.2f} wr_lb20={recent_wr_lb:.2f}"
            )

    def _late_lock_thresholds(self, duration: int) -> tuple[float, float]:
        """Adaptive late-lock thresholds; avoids rigid hardcoded behavior."""
        lock_mins = LATE_DIR_LOCK_MIN_LEFT_5M if int(duration or 0) <= 5 else LATE_DIR_LOCK_MIN_LEFT_15M
        move_min = float(LATE_DIR_LOCK_MIN_MOVE_PCT)
        snap = self._growth_snapshot()
        pf = float(snap.get("recent_pf", 1.0) or 1.0)
        wr_lb = float(snap.get("recent_wr_lb", 0.5) or 0.5)
        mode = str(self._autopilot_mode or "normal")

        if mode == "boost" and pf >= 1.10 and wr_lb >= 0.50:
            lock_mins = max(0.8, lock_mins - (0.5 if int(duration or 0) <= 5 else 1.0))
            move_min = min(0.0030, move_min + 0.00008)
        elif mode in ("soft-protect", "hard-protect"):
            lock_mins = min(lock_mins + (0.4 if mode == "soft-protect" else 0.8), 3.5 if int(duration or 0) <= 5 else 8.5)
            move_min = max(0.00012, move_min - (0.00003 if mode == "soft-protect" else 0.00006))
        # Global clamps to prevent pathological no-trade behavior.
        lock_mins = max(0.8, min(3.5 if int(duration or 0) <= 5 else 8.5, lock_mins))
        move_min = max(0.00010, min(0.0035, move_min))
        return lock_mins, move_min

    def _five_m_quality_snapshot(self, window: int | None = None) -> dict:
        """Rolling settled quality snapshot for 5m only (from on-chain resolved samples)."""
        w = int(window or FIVE_M_GUARD_WINDOW)
        vals = []
        for s in reversed(self._resolved_samples):
            if int(s.get("duration", 0) or 0) > 5:
                continue
            vals.append(s)
            if len(vals) >= max(1, w):
                break
        vals.reverse()
        n = len(vals)
        wins = sum(1 for x in vals if bool(x.get("won", False)))
        wr = (wins / n) if n > 0 else 0.0
        pnl = sum(float(x.get("pnl", 0.0) or 0.0) for x in vals)
        gross_win = sum(float(x.get("pnl", 0.0) or 0.0) for x in vals if float(x.get("pnl", 0.0) or 0.0) > 0.0)
        gross_loss = -sum(float(x.get("pnl", 0.0) or 0.0) for x in vals if float(x.get("pnl", 0.0) or 0.0) < 0.0)
        pf = (gross_win / gross_loss) if gross_loss > 0 else (2.0 if gross_win > 0 else 1.0)
        return {
            "n": n,
            "wins": wins,
            "wr": wr,
            "pnl": pnl,
            "pf": pf,
        }

    def _update_5m_runtime_guard(self):
        """Auto-disable/enable 5m based on rolling settled quality."""
        if not ENABLE_5M:
            self._enable_5m_runtime = False
            self._five_m_runtime_size_mult = 0.0
            return
        if not FIVE_M_RUNTIME_GUARD_ENABLED:
            self._enable_5m_runtime = True
            self._five_m_runtime_size_mult = 1.0
            return
        now = _time.time()
        if (now - self._five_m_guard_last_eval_ts) < max(5.0, FIVE_M_GUARD_CHECK_EVERY_SEC):
            return
        self._five_m_guard_last_eval_ts = now

        snap = self._five_m_quality_snapshot(max(FIVE_M_GUARD_WINDOW, FIVE_M_GUARD_REENABLE_MIN_OUTCOMES))
        n = int(snap.get("n", 0) or 0)
        wr = float(snap.get("wr", 0.0) or 0.0)
        pf = float(snap.get("pf", 1.0) or 1.0)
        pnl = float(snap.get("pnl", 0.0) or 0.0)

        # Profit-push adaptive path:
        # keep 5m active but resize exposure by rolling realized quality.
        if PROFIT_PUSH_MODE and PROFIT_PUSH_ADAPTIVE_MODE:
            self._enable_5m_runtime = True
            prev_mult = float(self._five_m_runtime_size_mult or 1.0)
            mult = 1.0
            if n < max(1, FIVE_M_DYNAMIC_SCORE_MIN_OUTCOMES):
                mult = 1.0
            elif pf < 0.85 or wr < 0.40:
                mult = 0.22
            elif pf < 1.00 or wr < 0.45:
                mult = 0.35
            elif pf < 1.05 or wr < 0.50:
                mult = 0.60
            elif pf >= 1.15 and wr >= 0.55:
                mult = 1.15
            self._five_m_runtime_size_mult = max(0.20, min(1.25, mult))
            if (
                abs(self._five_m_runtime_size_mult - prev_mult) >= 0.05
                or (now - self._five_m_guard_last_state_log_ts) >= 60.0
            ):
                self._five_m_guard_last_state_log_ts = now
                print(
                    f"{B}[5M-ADAPT]{RS} size_x={self._five_m_runtime_size_mult:.2f} "
                    f"(n={n} wr={wr*100.0:.1f}% pf={pf:.2f} pnl={pnl:+.2f})"
                )
            return

        if self._enable_5m_runtime:
            bad_quality = (
                n >= max(1, FIVE_M_GUARD_DISABLE_MIN_OUTCOMES)
                and (pf < FIVE_M_GUARD_DISABLE_PF or wr < FIVE_M_GUARD_DISABLE_WR)
            )
            if bad_quality:
                self._enable_5m_runtime = False
                self._five_m_runtime_size_mult = 0.0
                self._five_m_disabled_until = now + max(60.0, FIVE_M_GUARD_MIN_DISABLE_SEC)
                self._five_m_guard_last_state_log_ts = now
                print(
                    f"{Y}[5M-GUARD]{RS} disabled for {max(1.0, FIVE_M_GUARD_MIN_DISABLE_SEC)/60.0:.1f}m "
                    f"(n={n} wr={wr*100.0:.1f}% pf={pf:.2f} pnl={pnl:+.2f})"
                )
            return

        # Currently disabled.
        if now < float(self._five_m_disabled_until or 0.0):
            if (now - self._five_m_guard_last_state_log_ts) >= 60.0:
                mins_left = (float(self._five_m_disabled_until) - now) / 60.0
                self._five_m_guard_last_state_log_ts = now
                print(
                    f"{Y}[5M-GUARD]{RS} cooling-down {max(0.0, mins_left):.1f}m "
                    f"(n={n} wr={wr*100.0:.1f}% pf={pf:.2f} pnl={pnl:+.2f})"
                )
            return

        good_quality = (
            n >= max(1, FIVE_M_GUARD_REENABLE_MIN_OUTCOMES)
            and pf >= FIVE_M_GUARD_REENABLE_PF
            and wr >= FIVE_M_GUARD_REENABLE_WR
        )
        if good_quality:
            self._enable_5m_runtime = True
            self._five_m_runtime_size_mult = 1.0
            self._five_m_guard_last_state_log_ts = now
            print(
                f"{G}[5M-GUARD]{RS} re-enabled "
                f"(n={n} wr={wr*100.0:.1f}% pf={pf:.2f} pnl={pnl:+.2f})"
            )

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
            tightness += min(0.45, (0.50 - snap["recent_wr_lb"]) * 2.0)
        if snap["avg_slip"] > 220:
            tightness += min(0.6, (snap["avg_slip"] - 220.0) / 600.0)
        if self.consec_losses >= 2:
            tightness += min(0.45, (self.consec_losses - 1) * 0.15)
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
            min_payout = max(base_payout, min(base_payout + min(payout_upshift_cap, 0.18), min_payout_raw))
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
            return 0.78  # was 0.70: less aggressive cold-start suppression
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
        # Anchor to realized win rate: if recent WR is below 50%, model is overconfident
        if snap["recent_n"] >= 10:
            rwr = snap["recent_wr_lb"]  # Wilson lower bound on last 20 trades
            if rwr < 0.50:
                shrink -= min(0.20, (0.50 - rwr) * 2.5)
            elif rwr > 0.55:
                shrink += min(0.05, (rwr - 0.55) * 0.50)
        return max(0.35, min(0.92, shrink))

    def _signal_growth_score(self, sig: dict) -> float:
        """Rank candidates by growth quality (higher is better)."""
        entry = max(float(sig.get("entry", 0.5)), 1e-6)
        score = float(sig.get("score", 0))
        edge = float(sig.get("edge", 0.0))
        cl_bonus = 0.02 if sig.get("cl_agree", True) else -0.03
        payout = (1.0 / entry) - 1.0
        _fee_dyn_sg = max(0.001, entry * (1.0 - entry) * 0.0624)  # parabolic fee model (peaks at 0.50)
        ev_net = float(sig.get("execution_ev", (float(sig.get("true_prob", 0.5)) / entry) - 1.0 - _fee_dyn_sg))
        q_age = float(sig.get("quote_age_ms", 0.0) or 0.0)
        s_lat = float(sig.get("signal_latency_ms", 0.0) or 0.0)
        lag_penalty = 0.0
        if q_age > 900:
            lag_penalty -= 0.03
        if s_lat > 550:
            lag_penalty -= 0.03
        wr_lb = float(sig.get("recent_side_wr_lb", 0.5) or 0.5)
        wr_pen = 0.0
        if wr_lb < 0.49:
            wr_pen -= min(0.06, (0.49 - wr_lb) * 0.30)
        elif wr_lb >= 0.55:
            wr_pen += min(0.03, (wr_lb - 0.55) * 0.20)
        # Portfolio-aware utility:
        # keep contrarian opportunities, but avoid over-prioritizing low-cent tails.
        low_cent_pen = 0.0
        if entry <= CONTRARIAN_ENTRY_MAX and score < CONTRARIAN_STRONG_SCORE:
            low_cent_pen -= min(0.08, (CONTRARIAN_ENTRY_MAX - entry) * 0.20)
        core_bonus = 0.0
        if (
            CORE_ENTRY_MIN <= entry <= CORE_ENTRY_MAX
            and score >= CORE_MIN_SCORE
            and ev_net >= CORE_MIN_EV
        ):
            core_bonus += 0.03
        # Wallet-alpha (Polymarket copyflow) bonus: reward fresh, coherent leader flow,
        # but keep bounded to avoid overfitting crowd signals.
        wallet_alpha = self._wallet_alpha_bonus(sig)
        # Horizon optimizer (Polymarket 5m/15m round outcomes): reward duration
        # with stronger recent win-quality / expectancy on resolved markets.
        horizon_bonus = self._horizon_quality_bonus(int(sig.get("duration", 0) or 0))
        return (
            ev_net
            + edge * 0.38
            + payout * 0.03
            + score * 0.005
            + cl_bonus
            + core_bonus
            + wallet_alpha
            + horizon_bonus
            + low_cent_pen
            + wr_pen
            + lag_penalty
        )

    def _wallet_alpha_bonus(self, sig: dict) -> float:
        """Bounded alpha bonus from Polymarket leader-flow quality."""
        try:
            copy_net = float(sig.get("copy_net", 0.0) or 0.0)
            src = str(sig.get("signal_source", "") or "")
            tier = str(sig.get("signal_tier", "") or "")
            aq = float(sig.get("analysis_quality", 0.0) or 0.0)
            b = 0.0
            if src.startswith("leader"):
                b += 0.010
            if tier == "TIER-A":
                b += 0.008
            b += max(-0.010, min(0.010, copy_net * 0.025))
            # Only trust copyflow more when full signal stack is fresh/coherent.
            b *= max(0.55, min(1.10, 0.65 + (aq * 0.55)))
            return max(-0.018, min(0.028, b))
        except Exception:
            return 0.0

    def _horizon_quality_bonus(self, duration: int) -> float:
        """Duration-level adaptive bonus from resolved Polymarket round outcomes."""
        d = int(duration or 0)
        if d not in (5, 15):
            return 0.0
        vals = []
        for s in reversed(self._resolved_samples):
            if int(s.get("duration", 0) or 0) != d:
                continue
            vals.append(s)
            if len(vals) >= 160:
                break
        n = len(vals)
        if n < 12:
            return 0.0
        wins = sum(1 for x in vals if bool(x.get("won", False)))
        pnl = sum(float(x.get("pnl", 0.0) or 0.0) for x in vals)
        wr_lb = self._wilson_lower_bound(int(wins), int(n), z=1.0)
        exp = pnl / max(1, n)
        b = 0.0
        if wr_lb >= 0.52:
            b += min(0.012, (wr_lb - 0.52) * 0.10)
        elif wr_lb <= 0.47:
            b -= min(0.012, (0.47 - wr_lb) * 0.10)
        if exp > 0:
            b += min(0.010, exp / 60.0)
        elif exp < 0:
            b -= min(0.010, abs(exp) / 60.0)
        return max(-0.016, min(0.020, b))

    def _allow_second_round_trade(self, sig: dict, first_sig: dict | None = None) -> bool:
        """Permit second trade in same round only for high-quality push setups."""
        if not ROUND_SECOND_TRADE_PUSH_ONLY:
            return True
        if int(sig.get("score", 0) or 0) < ROUND_SECOND_TRADE_MIN_SCORE:
            return False
        if float(sig.get("execution_ev", 0.0) or 0.0) < ROUND_SECOND_TRADE_MIN_EXEC_EV:
            return False
        if float(sig.get("true_prob", 0.0) or 0.0) < ROUND_SECOND_TRADE_MIN_TRUE_PROB:
            return False
        if ROUND_SECOND_TRADE_REQUIRE_CL_AGREE and (not bool(sig.get("cl_agree", False))):
            return False
        if float(sig.get("entry", 1.0) or 1.0) > ROUND_SECOND_TRADE_MAX_ENTRY:
            return False
        if first_sig is not None:
            g = float(self._signal_growth_score(sig))
            g0 = float(self._signal_growth_score(first_sig))
            if g < (g0 - ROUND_SECOND_TRADE_MAX_GSCORE_GAP):
                return False
        return True

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
        sig_asset = str(sig.get("asset", "") or "").upper()
        sig_dur = int(sig.get("duration", 0) or 0)
        sig_end = float(sig.get("end_ts", sig_m.get("end_ts", 0)) or 0)
        sig_exact = self._is_exact_round_bounds(
            float(sig_m.get("start_ts", 0) or 0),
            sig_end,
            sig_dur,
        )
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
        # Ambiguous-round safeguard:
        # if either side lacks exact round bounds (fallback cid/question identity),
        # never allow opposite-side overlap on same asset+duration.
        if BLOCK_OPPOSITE_SIDE_SAME_ROUND:
            for c, (m, t) in active_pending.items():
                p_asset = str((t or {}).get("asset", (m or {}).get("asset", "") or "")).upper()
                p_dur = int((t or {}).get("duration", (m or {}).get("duration", 0) or 0))
                if p_asset != sig_asset or p_dur != sig_dur:
                    continue
                p_side = str((t or {}).get("side", "") or "")
                if not p_side or p_side == side:
                    continue
                p_exact = self._is_exact_round_bounds(
                    float((m or {}).get("start_ts", 0) or 0),
                    float((t or {}).get("end_ts", (m or {}).get("end_ts", 0)) or 0),
                    p_dur,
                )
                if (not sig_exact) or (not p_exact):
                    if LOG_VERBOSE:
                        print(
                            f"{Y}[RISK] skip {sig.get('asset')} {side} opposite-side with ambiguous round "
                            f"(sig_exact={sig_exact} pending_exact={p_exact}){RS}"
                        )
                    return False
        # Same-side duplicate safeguard across Polymarket short rounds:
        # do not stack same asset/side in the same 5m/15m round due to cid/fallback drift.
        if sig_dur in (5, 15) and sig_end > 0:
            end_tol = 60.0 if sig_dur <= 5 else 120.0
            for c, (m, t) in active_pending.items():
                p_asset = str((t or {}).get("asset", (m or {}).get("asset", "") or "")).upper()
                p_dur = int((t or {}).get("duration", (m or {}).get("duration", 0) or 0))
                p_side = str((t or {}).get("side", "") or "")
                if p_asset != sig_asset or p_dur != sig_dur or p_side != side:
                    continue
                p_end = float((t or {}).get("end_ts", (m or {}).get("end_ts", 0)) or 0)
                if p_end > 0 and abs(p_end - sig_end) <= end_tol:
                    if LOG_VERBOSE:
                        print(
                            f"{Y}[RISK] skip {sig_asset} {sig_dur}m {side} duplicate same-side round "
                            f"(sig_end={sig_end:.0f} pending_end={p_end:.0f} tol={end_tol:.0f}s){RS}"
                        )
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
        loop = asyncio.get_running_loop()
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
            self._update_5m_runtime_guard()
            self._apply_autopilot_policy()
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

            # Subscribe new markets to RTDS token price stream.
            # Also queue token ids for immediate CLOB market-WS subscription.
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
                                try:
                                    self._clob_ws_pending_subs.add(str(tid))
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
                    if NEXT_MARKET_ANALYSIS_ENABLED and sec_to_start <= max(30.0, NEXT_MARKET_ANALYSIS_WINDOW_SEC):
                        flow_pre = self._copyflow_live.get(cid) or self._copyflow_map.get(cid, {})
                        upc = dnc = 0.0
                        if isinstance(flow_pre, dict):
                            upc = float(flow_pre.get("Up", 0.0) or 0.0)
                            dnc = float(flow_pre.get("Down", 0.0) or 0.0)
                        flow_dom = (upc - dnc) if (upc + dnc) > 0 else 0.0
                        flow_conf = max(upc, dnc) if (upc + dnc) > 0 else 0.0
                        asset_pre = str(m.get("asset", "") or "")
                        dur_pre = int(m.get("duration", 0) or 0)
                        mom_up = float(self._momentum_prob(asset_pre, seconds=60))
                        ob = float(self._binance_imbalance(asset_pre))
                        tk_ratio, _ = self._binance_taker_flow(asset_pre)
                        perp_basis, _ = self._binance_perp_signals(asset_pre)
                        # Pre-open side prior from live feeds (non-blocking, quickly recomputed each scan).
                        up_score = (
                            (mom_up - 0.5) * 1.20
                            + max(-0.25, min(0.25, ob * 0.35))
                            + max(-0.20, min(0.20, (float(tk_ratio) - 0.5) * 0.90))
                            + max(-0.12, min(0.12, flow_dom * 0.60))
                            + max(-0.08, min(0.08, float(perp_basis) * 30.0))
                        )
                        side_pre = "Up" if up_score >= 0 else "Down"
                        conf_pre = max(0.50, min(0.92, 0.50 + abs(up_score)))
                        # If copyflow is materially strong and disagrees, reduce confidence.
                        if flow_conf >= 0.60:
                            side_flow = "Up" if upc >= dnc else "Down"
                            if side_flow != side_pre:
                                conf_pre = max(0.50, conf_pre - min(0.18, flow_conf - 0.50))
                        if PREBID_ARM_ENABLED and conf_pre >= max(PREBID_MIN_CONF, NEXT_MARKET_ANALYSIS_MIN_CONF):
                            self._prebid_plan[cid] = {
                                "side": side_pre,
                                "conf": conf_pre,
                                "start_ts": float(m.get("start_ts", now)),
                                "armed_at": _time.time(),
                            }
                        if self._should_log(f"next-analysis:{cid}", NEXT_MARKET_ANALYSIS_LOG_EVERY_SEC):
                            print(
                                f"{B}[NEXT-ANALYSIS]{RS} {asset_pre} {dur_pre}m starts in {sec_to_start:.0f}s | "
                                f"side={side_pre} conf={conf_pre:.2f} "
                                f"(mom={mom_up:.2f} ob={ob:+.2f} tk={float(tk_ratio):.2f} flow={flow_dom:+.2f}) | "
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
                            ref_fmt = f"{ref:,.6f}" if ref < 100 else f"{ref:,.2f}"
                            cur_fmt = f"{cur:,.6f}" if cur < 100 else f"{cur:,.2f}"
                            print(f"{B}[MKT] {asset} {dur}m | beat=${ref_fmt} [{src}] | "
                                  f"now=${cur_fmt} move={move:+.3f}% | {m['mins_left']:.1f}min left{RS}")
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
                # Force/synth fallback disabled: only real high-quality signals execute.
                if ENABLE_5M and (not self._enable_5m_runtime) and valid and not (PROFIT_PUSH_MODE and PROFIT_PUSH_ADAPTIVE_MODE):
                    pre_n = len(valid)
                    valid = [s for s in valid if int(s.get("duration", 0) or 0) > 5]
                    if pre_n != len(valid) and self._should_log("5m-guard-filter", 20):
                        print(
                            f"{Y}[5M-GUARD]{RS} filtered {pre_n-len(valid)} x 5m signals "
                            f"(runtime OFF)"
                        )
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
                    sk_total, top_sk = self._skip_top(window_sec=300, top_n=3)
                    top_sk_s = ", ".join([f"{reason}={count}" for reason, count in top_sk]) if top_sk else "none"
                    print(
                        f"{Y}[ROUND-DIAG]{RS} eligible={eligible_started} candidates={len(candidates)} "
                        f"blocked_seen={blocked_seen} pending_redeem={len(self.pending_redeem)} "
                        f"open_onchain={self.onchain_open_count} enable_5m={ENABLE_5M}/{self._enable_5m_runtime} "
                        f"skip_total={sk_total} top_skip[{top_sk_s}]"
                    )
                active_pending = {c: (m2, t) for c, (m2, t) in self.pending.items() if m2.get("end_ts", 0) > now}
                shadow_pending = dict(active_pending)
                slots = max(0, MAX_OPEN - len(active_pending))
                to_exec = []
                selected = sorted(valid, key=self._signal_growth_score, reverse=True)
                if FORCE_TRADE_EVERY_ROUND and selected:
                    cap = max(1, min(6, ROUND_MAX_TRADES))
                    if cap <= 1:
                        selected = selected[:1]
                    else:
                        first = selected[0]
                        filtered = [first]
                        for cand in selected[1:]:
                            if len(filtered) >= cap:
                                break
                            if self._allow_second_round_trade(cand, first):
                                filtered.append(cand)
                        selected = filtered
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
                consensus_side_15m = None
                if ROUND_CONSENSUS_15M_ENABLED:
                    up_votes = 0
                    dn_votes = 0
                    move_sum = 0.0
                    # Use all active 15m markets (not just untraded candidates) for a fuller macro picture.
                    _cons_mkts = list(self.active_mkts.values()) if self.active_mkts else candidates
                    for m in _cons_mkts:
                        if int(m.get("duration", 0) or 0) < 15:
                            continue
                        _cid_c  = m.get("conditionId", "")
                        _asset_c = m.get("asset", "")
                        op = float(self.open_prices.get(_cid_c, 0.0) or 0.0)
                        cur = float(self.prices.get(_asset_c, 0) or 0.0) or float(self.cl_prices.get(_asset_c, 0) or 0.0)
                        if op <= 0 or cur <= 0:
                            continue
                        rel = (cur - op) / max(op, 1e-9)
                        move_sum += rel
                        if cur >= op:
                            up_votes += 1
                        else:
                            dn_votes += 1
                    net = abs(up_votes - dn_votes)
                    if net >= max(1, ROUND_CONSENSUS_MIN_NET):
                        consensus_side_15m = "Up" if up_votes > dn_votes else "Down"
                    else:
                        move_thr = max(0.0, float(ROUND_CONSENSUS_MOVE_BPS or 0.0)) / 10000.0
                        if abs(move_sum) >= move_thr and (up_votes + dn_votes) >= 2:
                            consensus_side_15m = "Up" if move_sum > 0 else "Down"
                    if consensus_side_15m and self._should_log("round-consensus-15m", LOG_ROUND_SIGNAL_EVERY_SEC):
                        print(
                            f"{B}[CONSENSUS]{RS} 15m side={consensus_side_15m} "
                            f"(up={up_votes} down={dn_votes} net={net} move={(move_sum*100):+.3f}%)"
                        )
                blackout_ev = self._macro_blackout_active()
                if blackout_ev:
                    if self._should_log("macro-blackout", 120):
                        print(f"{Y}[MACRO-BLACKOUT]{RS} skipping trades — {blackout_ev} event window active")
                force_round_now = bool(
                    FORCE_TRADE_EVERY_ROUND
                    and slots > 0
                    and selected
                    and not blackout_ev
                )
                force_picked = False
                for sig in selected:
                    if len(to_exec) >= slots:
                        break
                    if blackout_ev:
                        continue
                    bypass_gates = bool(force_round_now and not force_picked)
                    if (
                        consensus_side_15m
                        and int(sig.get("duration", 0) or 0) >= 15
                        and str(sig.get("side", "") or "") != consensus_side_15m
                    ):
                        if bypass_gates:
                            if self._should_log("round-force-bypass-consensus", 10):
                                print(
                                    f"{Y}[ROUND-FORCE-BYPASS]{RS} consensus "
                                    f"{sig.get('asset','?')} {sig.get('duration','?')}m "
                                    f"{sig.get('side','?')} != {consensus_side_15m}"
                                )
                        elif ROUND_CONSENSUS_STRICT_15M:
                            self._skip_tick("consensus_contra")
                            continue
                        else:
                            over = (
                                int(sig.get("score", 0) or 0) >= ROUND_CONSENSUS_OVERRIDE_SCORE
                                and float(sig.get("edge", 0.0) or 0.0) >= ROUND_CONSENSUS_OVERRIDE_EDGE
                            )
                            if not over:
                                self._skip_tick("consensus_contra")
                                continue
                    if (not bypass_gates) and (not self._exposure_ok(sig, shadow_pending)):
                        continue
                    if (not bypass_gates) and (not TRADE_ALL_MARKETS):
                        pending_up = sum(1 for _, t in shadow_pending.values() if t.get("side") == "Up")
                        pending_dn = sum(1 for _, t in shadow_pending.values() if t.get("side") == "Down")
                        if sig["side"] == "Up" and pending_up >= MAX_SAME_DIR:
                            continue
                        if sig["side"] == "Down" and pending_dn >= MAX_SAME_DIR:
                            continue
                        # Unified direction: correlated assets move together — never hold Up + Down simultaneously
                        if pending_dn > 0 and sig["side"] == "Up":   continue
                        if pending_up > 0 and sig["side"] == "Down":  continue
                    # NOTE: correlated Kelly sizing is handled by ROUND_CORR_SAME_SIDE_DECAY in execution path.
                    # Removed scan_loop 1/sqrt(N) division to avoid double-applying the same reduction.
                    picked_sig = dict(sig) if bypass_gates else sig
                    if bypass_gates:
                        picked_sig["round_force_coverage"] = True
                        picked_sig["round_force_execute"] = True
                        force_picked = True
                    to_exec.append(picked_sig)
                    m_sig = sig.get("m", {}) if isinstance(sig.get("m", {}), dict) else {}
                    t_sig = {
                        "side": picked_sig.get("side", ""),
                        "size": float(picked_sig.get("size", 0.0) or 0.0),
                        "asset": picked_sig.get("asset", ""),
                        "duration": int(picked_sig.get("duration", 0) or 0),
                        "end_ts": float(m_sig.get("end_ts", now + 60) or (now + 60)),
                    }
                    shadow_pending[f"__pick__{len(to_exec)}"] = (m_sig, t_sig)
                counted_no_trade = False
                if (
                    NO_TRADE_RECOVERY_ENABLED
                    and (not to_exec)
                    and selected
                    and slots > 0
                    and not blackout_ev
                ):
                    self._no_trade_rounds += 1
                    counted_no_trade = True
                    if self._no_trade_rounds >= max(1, NO_TRADE_RECOVERY_ROUNDS):
                        forced = dict(selected[0])
                        forced["round_force_coverage"] = True
                        forced["round_force_execute"] = True
                        to_exec.append(forced)
                        if self._should_log("round-no-trade-recovery", 10):
                            print(
                                f"{Y}[ROUND-RECOVERY]{RS} forcing execution after "
                                f"{self._no_trade_rounds} empty rounds -> "
                                f"{forced.get('asset','?')} {forced.get('duration','?')}m "
                                f"{forced.get('side','?')} score={forced.get('score','?')}"
                            )
                if to_exec:
                    self._no_trade_rounds = 0
                    await asyncio.gather(*[self._execute_trade(sig) for sig in to_exec])
                elif selected:
                    if not counted_no_trade:
                        self._no_trade_rounds += 1
                else:
                    self._no_trade_rounds = max(0, self._no_trade_rounds - 1)

            await asyncio.sleep(SCAN_INTERVAL)
          except Exception as e:
            print(f"{R}[SCAN] Error (continuing): {e}{RS}")
            if self._noisy_log_enabled("scan-traceback", 5.0):
                print(f"{R}[SCAN-TRACE]{RS} {traceback.format_exc(limit=4).strip()}")
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
            # Skip API calls if data-api is in 429 backoff — don't compound rate limits
            _data_api_backoff = self._http_429_backoff.get("data-api.polymarket.com", 0.0)
            if _data_api_backoff > _time.time():
                continue
            tick += 1
            try:
                loop = asyncio.get_running_loop()

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
                    # Filter expired worthless positions: losing tokens have size_tok>0 but val≈0
                    # and are never marked redeemable by Polymarket — use 90s grace (not 1200s)
                    # to stop them counting as "open" for 20 minutes after round end.
                    if (not redeemable) and val < OPEN_PRESENCE_MIN:
                        if self._is_historical_expired_position(p, now_ts, grace_sec=90.0):
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
                        open_ref = float(self._open_price_by_cid(cid) or 0.0)
                        if open_ref <= 0 and asset in ("BTC", "ETH", "SOL", "XRP") and start_ts > 0 and end_ts > start_ts:
                            try:
                                pm_ref = float(await self._get_polymarket_open_price(asset, start_ts, end_ts, dur_guess) or 0.0)
                            except Exception:
                                pm_ref = 0.0
                            if pm_ref > 0:
                                self.open_prices[cid] = pm_ref
                                self.open_prices_source[cid] = "PM"
                                open_ref = pm_ref
                        onchain_open_meta_by_cid[cid] = {
                            "title": title,
                            "side": side,
                            "asset": asset,
                            "entry": float(p.get("avgPrice", 0.5) or 0.5),
                            "open_price": float(open_ref or 0.0),
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
                self._save_open_state_cache(force=False)
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
                    # M-7: guard against API-lag deflation (open stakes not yet indexed)
                    if (
                        self.bankroll > 0
                        and open_stake_total == 0
                        and total < self.bankroll * 0.85
                    ):
                        pass  # skip — likely a momentary API miss; keep current bankroll
                    else:
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
                    # Resolve round bounds and tokens from cache (Gamma only on first miss).
                    end_ts = 0
                    start_ts = now_ts - 60
                    duration = 15
                    token_up = token_down = ""
                    meta_c = await self._cid_market_cached(cid)
                    if meta_c:
                        duration = int(meta_c.get("duration", duration) or duration)
                        asset = str(meta_c.get("asset", asset) or asset)
                        start_ts = float(meta_c.get("start_ts", start_ts) or start_ts)
                        end_ts = float(meta_c.get("end_ts", end_ts) or end_ts)
                        token_up = str(meta_c.get("token_up", "") or "")
                        token_down = str(meta_c.get("token_down", "") or "")
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
                _es = str(e).lower()
                if "429" in _es or "backoff active" in _es:
                    pass  # 429 backoff is expected when other loops hit rate limit — not an error
                else:
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

            if total_bets > 0 and total_bets >= self.total:  # never go backwards (API can return partial page)
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
                _es = str(e).lower()
                if "429" not in _es and "backoff active" not in _es:
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
        loop = asyncio.get_running_loop()
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
                        self.wins += 1; self.total += 1   # H-6: record silent win in stats
                        self._log_onchain_event("RESOLVE-BACKFILL", cid, {
                            "side": winner, "result": "WIN",
                            "held_shares": round(held, 4),
                            "title": d.get("title", "")[:40],
                        })
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
                loop = asyncio.get_running_loop()
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
                    # Resolve round bounds/tokens from persistent CID cache.
                    end_ts = 0
                    start_ts = now - 60
                    duration = 15
                    token_up = token_down = ""
                    meta_c = await self._cid_market_cached(cid)
                    if meta_c:
                        duration = int(meta_c.get("duration", duration) or duration)
                        asset = str(meta_c.get("asset", asset) or asset)
                        start_ts = float(meta_c.get("start_ts", start_ts) or start_ts)
                        end_ts = float(meta_c.get("end_ts", end_ts) or end_ts)
                        token_up = str(meta_c.get("token_up", "") or "")
                        token_down = str(meta_c.get("token_down", "") or "")
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
                loop = asyncio.get_running_loop()
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
                loop = asyncio.get_running_loop()
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
                ws_age_cap = self._ws_strict_age_cap_ms()
                stale = [tid for tid in tids if self._clob_ws_book_age_ms(tid) > ws_age_cap]
                if len(stale) == len(tids):
                    self._ws_stale_hits += 1
                    if self._should_log("health-ws-stale", LOG_HEALTH_EVERY_SEC):
                        age_samples = [self._clob_ws_book_age_ms(t) for t in tids[:3]]
                        age_s = ", ".join(f"{a:.0f}ms" for a in age_samples)
                        print(
                            f"{Y}[HEALTH]{RS} CLOB-WS stale {len(stale)}/{len(tids)} tokens "
                            f"(samples={age_s} cap={ws_age_cap:.0f}ms) hits={self._ws_stale_hits}"
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

    # ── WEB DASHBOARD ─────────────────────────────────────────────────────────
    def _metrics_resolve_rows_cached(self, ttl_sec: float = 20.0) -> list[dict]:
        """SQLite-first incremental RESOLVE cache with JSONL fallback."""
        import os as _os
        import time as _time

        now = _time.time()
        cache = self._metrics_resolve_cache
        db_rowid = int(cache.get("db_rowid", 0) or 0)

        if self._metrics_db_ready:
            try:
                conn = sqlite3.connect(METRICS_DB_FILE, timeout=3.0)
                try:
                    cur = conn.cursor()
                    max_id_row = cur.execute("SELECT COALESCE(MAX(id),0) FROM resolve_metrics").fetchone()
                    max_id = int((max_id_row[0] if max_id_row else 0) or 0)
                    if max_id == db_rowid and cache.get("rows") and (now - float(cache.get("ts", 0.0) or 0.0)) < max(1.0, float(ttl_sec)):
                        return cache.get("rows", [])
                    if max_id > db_rowid and cache.get("rows"):
                        q = cur.execute(
                            """
                            SELECT ts,duration,score,entry_price,pnl,result,asset,id
                            FROM resolve_metrics
                            WHERE id > ?
                            ORDER BY id ASC
                            """,
                            (db_rowid,),
                        ).fetchall()
                        rows = list(cache.get("rows", []) or [])
                        for ts, duration, score, entry_price, pnl, result, asset, rid in q:
                            rows.append(
                                {
                                    "ts": str(ts or ""),
                                    "duration": int(duration or 15),
                                    "score": int(score or 0),
                                    "entry_price": float(entry_price or 0.0),
                                    "pnl": float(pnl or 0.0),
                                    "result": str(result or ""),
                                    "asset": str(asset or "?"),
                                }
                            )
                            db_rowid = int(rid or db_rowid)
                        self._metrics_resolve_cache = {"ts": now, "sig": "sqlite", "rows": rows, "offset": 0, "db_rowid": max_id}
                        return rows
                    # cold path from sqlite
                    q = cur.execute(
                        """
                        SELECT ts,duration,score,entry_price,pnl,result,asset,id
                        FROM resolve_metrics
                        ORDER BY id ASC
                        """
                    ).fetchall()
                    rows: list[dict] = []
                    for ts, duration, score, entry_price, pnl, result, asset, rid in q:
                        rows.append(
                            {
                                "ts": str(ts or ""),
                                "duration": int(duration or 15),
                                "score": int(score or 0),
                                "entry_price": float(entry_price or 0.0),
                                "pnl": float(pnl or 0.0),
                                "result": str(result or ""),
                                "asset": str(asset or "?"),
                            }
                        )
                        db_rowid = int(rid or db_rowid)
                    self._metrics_resolve_cache = {"ts": now, "sig": "sqlite", "rows": rows, "offset": 0, "db_rowid": max_id}
                    return rows
                finally:
                    conn.close()
            except Exception:
                pass

        # JSONL fallback (legacy path)
        try:
            st = _os.stat(METRICS_FILE)
            sig = (int(getattr(st, "st_ino", 0)), int(st.st_size), int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9))))
        except Exception:
            sig = None
        if cache.get("rows") and cache.get("sig") == sig and (now - float(cache.get("ts", 0.0) or 0.0)) < max(1.0, float(ttl_sec)):
            return cache.get("rows", [])
        rows: list[dict] = list(cache.get("rows", []) or [])
        prev_sig = cache.get("sig")
        prev_off = int(cache.get("offset", 0) or 0)
        full_reload = True
        if prev_sig and sig and isinstance(prev_sig, tuple) and len(prev_sig) >= 3:
            same_inode = int(prev_sig[0]) == int(sig[0])
            size_grew = int(sig[1]) >= int(prev_sig[1])
            if same_inode and size_grew:
                full_reload = False
        if prev_off < 0:
            prev_off = 0
        if full_reload:
            rows = []
            prev_off = 0
        try:
            with open(METRICS_FILE, encoding="utf-8") as f:
                try:
                    f.seek(prev_off)
                except Exception:
                    prev_off = 0
                    f.seek(0)
                for line in f:
                    try:
                        r = json.loads(line)
                    except Exception:
                        continue
                    if r.get("event") != "RESOLVE":
                        continue
                    rows.append(
                        {
                            "ts": str(r.get("ts", "") or ""),
                            "duration": int(r.get("duration") or 15),
                            "score": int(r.get("score") or 0),
                            "entry_price": float(r.get("entry_price") or 0.0),
                            "pnl": float(r.get("pnl") or 0.0),
                            "result": str(r.get("result", "") or ""),
                            "asset": str(r.get("asset", "?") or "?"),
                        }
                    )
                end_off = int(f.tell())
        except Exception:
            rows = list(cache.get("rows", []) or [])
            end_off = int(cache.get("offset", 0) or 0)
        self._metrics_resolve_cache = {"ts": now, "sig": sig, "rows": rows, "offset": end_off, "db_rowid": db_rowid}
        return rows

    def _dashboard_data(self) -> dict:
        """Collect current bot state as a JSON-serialisable dict for the web dashboard."""
        import time as _t
        now_ts = _t.time()
        el = datetime.now(timezone.utc) - self.start_time
        h, m = int(el.total_seconds() // 3600), int(el.total_seconds() % 3600 // 60)
        display_bank = self.onchain_total_equity if self.onchain_snapshot_ts > 0 else self.bankroll
        pnl = display_bank - self.start_bank
        roi = pnl / self.start_bank * 100 if self.start_bank > 0 else 0
        wr = round(self.wins / self.total * 100, 1) if self.total else 0

        def _norm_round_bounds(start_ts: float, end_ts: float, duration: int) -> tuple[float, float]:
            """Normalize dashboard round bounds without forcing invalid future windows."""
            d = int(duration or 0)
            if d not in (5, 15):
                return float(start_ts or 0.0), float(end_ts or 0.0)
            step = float(d * 60)
            st = float(start_ts or 0.0)
            et = float(end_ts or 0.0)
            # Keep explicit window if it is plausible for the duration.
            if st > 0 and et > st:
                w = et - st
                if abs(w - step) <= max(2.0, step * 0.25):
                    return st, et
            # If one bound is missing, derive the other from duration.
            if et > 0 and st <= 0:
                return et - step, et
            if st > 0 and et <= 0:
                return st, st + step
            # Last fallback: infer current round boundary from wall clock.
            et_n = math.ceil(now_ts / step) * step
            st_n = et_n - step
            return st_n, et_n

        # Price history for charts (last 20 min, ~4 pts/s max → cap at 600 pts)
        charts = {}
        for asset, ph in self.price_history.items():
            cutoff = now_ts - 1200
            pts = [{"t": round(ts, 1), "p": round(px, 6)}
                   for ts, px in ph if ts >= cutoff and px > 0]
            if pts:
                charts[asset] = pts

        # Open positions
        positions = []
        onchain_open_cids_norm = {
            str(c or "").strip().lower() for c in (self.onchain_open_cids or set())
            if str(c or "").strip()
        }
        seen_cids_norm = set()
        for cid, (mkt, trade) in list(self.pending.items()):
            cid_norm = str(cid or "").strip().lower()
            if not cid_norm or cid_norm in seen_cids_norm:
                continue
            seen_cids_norm.add(cid_norm)
            if (
                self.onchain_snapshot_ts > 0
                and cid not in self.onchain_open_cids
                and cid_norm not in onchain_open_cids_norm
            ):
                continue
            asset = trade.get("asset", "?")
            side  = trade.get("side", "?")
            stake = float(self.onchain_open_stake_by_cid.get(cid, 0.0) or 0.0)
            if stake <= 0:
                stake = float(self.onchain_open_usdc_by_cid.get(cid, 0.0) or 0.0)
            if stake <= 0:
                continue
            entry     = float(trade.get("fill_price", trade.get("entry", 0.5)) or 0.5)
            open_p    = float(self._open_price_by_cid(cid) or 0.0)
            cl_p      = self.cl_prices.get(asset, 0)
            cl_ts     = self.cl_updated.get(asset, 0)
            rtds_p    = self.prices.get(asset, 0)
            rtds_ts   = self._price_hist_last_ts(asset)
            cur_p     = rtds_p if (rtds_ts > cl_ts and rtds_p > 0) else (cl_p if cl_p > 0 else rtds_p)
            duration  = int(trade.get("duration", mkt.get("duration", 15)) or 15)
            end_ts    = float(trade.get("end_ts", mkt.get("end_ts", 0)) or 0)
            start_ts  = float(mkt.get("start_ts", 0) or 0)
            # Round timing must follow "price to beat" lock window, not entry timestamp.
            if end_ts > 0 and duration > 0:
                start_ts = end_ts - duration * 60.0
            start_ts, end_ts = _norm_round_bounds(start_ts, end_ts, duration)
            mins_left = max(0.0, (end_ts - now_ts) / 60)
            if open_p > 0 and cur_p > 0:
                pred_winner = "Up" if cur_p >= open_p else "Down"
                lead = side == pred_winner
                move_pct = (cur_p - open_p) / open_p * 100
            else:
                lead, move_pct = None, 0.0
            token_id = self._midpoint_token_id_from_cid_side(cid, side, mkt, trade)
            positions.append({
                "asset": asset, "side": side, "entry": round(entry, 3),
                "stake": round(stake, 2), "cur_p": round(cur_p, 2),
                "open_p": round(open_p, 6), "move_pct": round(move_pct, 3),
                "lead": lead, "mins_left": round(mins_left, 1),
                "src": "pending",
                "cid_full": cid_norm,
                "cid": cid_norm[:12], "start_ts": start_ts, "end_ts": end_ts,
                "duration": duration, "token_id": token_id,
                "rk": self._round_key(cid=cid_norm, m=mkt, t=trade),
            })

        # Optional fallback rows for on-chain positions not present in local pending state.
        if SHOW_DASHBOARD_FALLBACK and DASHBOARD_FALLBACK_DEBUG:
            for cid, meta in list((self.onchain_open_meta_by_cid or {}).items()):
                cid_norm = str(cid or "").strip().lower()
                if not cid_norm or cid_norm in seen_cids_norm:
                    continue
                seen_cids_norm.add(cid_norm)
                stake = float((self.onchain_open_stake_by_cid or {}).get(cid, 0.0) or 0.0)
                if stake <= 0:
                    stake = float((self.onchain_open_usdc_by_cid or {}).get(cid, 0.0) or 0.0)
                if stake <= 0:
                    continue
                asset = str(meta.get("asset", "?") or "?")
                side = str(meta.get("side", "?") or "?")
                entry = float(meta.get("entry", 0.5) or 0.5)
                open_p = float(self._open_price_by_cid(cid) or 0.0)
                if open_p <= 0:
                    open_p = float(meta.get("open_price", 0.0) or 0.0)
                cl_p = self.cl_prices.get(asset, 0)
                cl_ts = self.cl_updated.get(asset, 0)
                rtds_p = self.prices.get(asset, 0)
                rtds_ts = self._price_hist_last_ts(asset)
                cur_p = rtds_p if (rtds_ts > cl_ts and rtds_p > 0) else (cl_p if cl_p > 0 else rtds_p)
                duration = int(meta.get("duration", 15) or 15)
                end_ts = float(meta.get("end_ts", 0) or 0)
                start_ts = float(meta.get("start_ts", 0) or 0)
                if end_ts > 0 and duration > 0:
                    start_ts = end_ts - duration * 60.0
                start_ts, end_ts = _norm_round_bounds(start_ts, end_ts, duration)
                mins_left = max(0.0, (end_ts - now_ts) / 60.0) if end_ts > 0 else 0.0
                if open_p > 0 and cur_p > 0:
                    pred_winner = "Up" if cur_p >= open_p else "Down"
                    lead = side == pred_winner
                    move_pct = (cur_p - open_p) / open_p * 100
                else:
                    lead, move_pct = None, 0.0
                token_id = self._midpoint_token_id_from_cid_side(cid, side, meta, None)
                positions.append({
                    "asset": asset, "side": side, "entry": round(entry, 3),
                    "stake": round(stake, 2), "cur_p": round(cur_p, 2),
                    "open_p": round(open_p, 6), "move_pct": round(move_pct, 3),
                    "lead": lead, "mins_left": round(mins_left, 1),
                    "src": "fallback",
                    "cid_full": cid_norm,
                    "cid": cid_norm[:12], "start_ts": start_ts, "end_ts": end_ts,
                    "duration": duration, "token_id": token_id,
                    "rk": self._round_key(
                        cid=cid_norm,
                        m={"asset": asset, "duration": duration, "start_ts": start_ts, "end_ts": end_ts},
                        t={"asset": asset, "side": side, "duration": duration, "end_ts": end_ts},
                    ),
                })

        # If a row has cid-fallback rk (missing exact round bounds), align it to
        # the nearest exact round for the same asset+duration.
        if positions:
            exact_ref = {}
            for p in positions:
                rk = str(p.get("rk", "") or "")
                if "-cid" in rk:
                    continue
                k = (
                    str(p.get("asset", "?")),
                    int(p.get("duration", 0) or 0),
                    str(p.get("side", "?")),
                )
                prev = exact_ref.get(k)
                cand = (float(p.get("start_ts", 0.0) or 0.0), float(p.get("end_ts", 0.0) or 0.0))
                if cand[1] <= 0:
                    continue
                if prev is None:
                    exact_ref[k] = cand
                    continue
                # keep the round whose end_ts is closest to "now"
                if abs(cand[1] - now_ts) < abs(prev[1] - now_ts):
                    exact_ref[k] = cand
            for p in positions:
                rk = str(p.get("rk", "") or "")
                if "-cid" not in rk:
                    continue
                k = (
                    str(p.get("asset", "?")),
                    int(p.get("duration", 0) or 0),
                    str(p.get("side", "?")),
                )
                ref = exact_ref.get(k)
                if not ref:
                    continue
                st_ref, et_ref = ref
                if et_ref > 0:
                    p["start_ts"] = float(st_ref)
                    p["end_ts"] = float(et_ref)
                    p["mins_left"] = round(max(0.0, (et_ref - now_ts) / 60.0), 1)

        # Drop cid-fallback duplicates when an exact row exists for same asset/duration/side.
        if positions:
            exact_slots = set()
            for p in positions:
                rk = str(p.get("rk", "") or "")
                if "-cid" in rk:
                    continue
                exact_slots.add(
                    (
                        str(p.get("asset", "?")),
                        int(p.get("duration", 0) or 0),
                        str(p.get("side", "?")),
                        int(round(float(p.get("end_ts", 0.0) or 0.0))),
                    )
                )
            if exact_slots:
                keep = []
                for p in positions:
                    rk = str(p.get("rk", "") or "")
                    if "-cid" not in rk:
                        keep.append(p)
                        continue
                    key = (
                        str(p.get("asset", "?")),
                        int(p.get("duration", 0) or 0),
                        str(p.get("side", "?")),
                        int(round(float(p.get("end_ts", 0.0) or 0.0))),
                    )
                    if key in exact_slots:
                        continue
                    keep.append(p)
                positions = keep

        # Avoid contradictory duplicate cards for the same asset and round slot.
        # Keep the strongest row (higher stake, valid open price preferred).
        if positions:
            by_slot = {}
            for p in positions:
                d = int(p.get("duration", 0) or 0)
                et = float(p.get("end_ts", 0.0) or 0.0)
                if d in (5, 15) and et > 0:
                    slot = round(et / (d * 60.0))
                else:
                    slot = int(et)
                k = (str(p.get("asset", "?")), d, slot)
                by_slot.setdefault(k, []).append(p)
            filtered = []
            for _, rows_k in by_slot.items():
                rows_k.sort(
                    key=lambda r: (
                        1 if float(r.get("open_p", 0.0) or 0.0) > 0 else 0,
                        float(r.get("stake", 0.0) or 0.0),
                        float(r.get("end_ts", 0.0) or 0.0),
                    ),
                    reverse=True,
                )
                filtered.append(rows_k[0])
            positions = filtered

        # Strict source-of-truth mode for dashboard: keep only canonical round windows.
        if DASHBOARD_STRICT_CANONICAL and positions:
            strict_rows = []
            for p in positions:
                d = int(p.get("duration", 0) or 0)
                st = float(p.get("start_ts", 0.0) or 0.0)
                et = float(p.get("end_ts", 0.0) or 0.0)
                if d in (5, 15):
                    step = float(d * 60)
                    if st <= 0 or et <= st:
                        continue
                    w = et - st
                    if abs(w - step) > max(2.0, step * 0.25):
                        continue
                strict_rows.append(p)
            positions = strict_rows

        # Keep one row per CID (already ensured by seen_cids_norm above).
        # Do not collapse by asset/duration/side: multiple real opens can share
        # the same triplet and must stay visible to match on-chain open_count.

        # Skip top reasons (last 15m)
        skip_top = []
        try:
            _, top = self._skip_top(900, 6)
            skip_top = [{"reason": r, "count": c} for r, c in top]
        except Exception:
            pass

        # ExecQ all buckets
        execq_all = []
        execq = {}
        try:
            if self._bucket_stats.rows:
                sorted_rows = sorted(
                    self._bucket_stats.rows.items(),
                    key=lambda kv: kv[1].get("fills", 0), reverse=True
                )
                for k, v in sorted_rows:
                    outcomes = int(v.get("outcomes", v.get("wins", 0) + v.get("losses", 0)))
                    fills    = int(v.get("fills", 0) or 0)
                    if fills == 0 and outcomes == 0:
                        continue
                    wr_b = round(v["wins"] / outcomes * 100, 1) if outcomes > 0 else None
                    pf_b = (float(v.get("gross_win", 0)) / float(v.get("gross_loss", 1e-9))
                            if float(v.get("gross_loss", 0)) > 0 else None)
                    execq_all.append({
                        "bucket": k, "fills": fills, "outcomes": outcomes,
                        "wr": wr_b, "pf": round(pf_b, 2) if pf_b else None,
                        "pnl": round(float(v.get("pnl", 0)), 2),
                    })
                if execq_all:
                    execq = execq_all[0]
        except Exception:
            pass

        # Filter to active (non-gated) buckets only
        def _buck_active(k):
            pts = k.split("|")
            if len(pts) < 2: return True
            d_p, t_p = pts[0], pts[1]
            if d_p == "5m" and not ENABLE_5M: return False
            if d_p == "15m":
                if BLOCK_SCORE_S9_11_15M and t_p == "s9-11": return False
                if BLOCK_SCORE_S0_8_15M  and t_p == "s0-8":  return False
                if BLOCK_SCORE_S12P_15M  and t_p == "s12+":  return False
            return True
        execq_all = [e for e in execq_all if _buck_active(e["bucket"])]
        active_gates: list = []
        if BLOCK_SCORE_S9_11_15M: active_gates.append("soft s9-11" if SCORE_BLOCK_SOFT_MODE else "no s9-11")
        if BLOCK_SCORE_S0_8_15M:  active_gates.append("soft s0-8" if SCORE_BLOCK_SOFT_MODE else "no s0-8")
        if BLOCK_SCORE_S12P_15M:  active_gates.append("soft s12+" if SCORE_BLOCK_SOFT_MODE else "no s12+")
        if BLOCK_ASSET_SOL_15M:   active_gates.append("soft SOL" if ASSET_BLOCK_SOFT_MODE else "no SOL")
        if BLOCK_ASSET_XRP_15M:   active_gates.append("soft XRP" if ASSET_BLOCK_SOFT_MODE else "no XRP")
        if not ENABLE_5M:         active_gates.append("5m off")
        if MIN_ENTRY_PRICE_S0_8_15M > 0:
            active_gates.append(f"s0-8\u2265{MIN_ENTRY_PRICE_S0_8_15M:.2f}")

        # Daily totals (UTC day) from metrics log, cached to keep dashboard fast.
        day_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        dc = self._dash_daily_cache
        if (dc.get("day") != day_utc) or (now_ts - float(dc.get("ts", 0.0) or 0.0) >= 30.0):
            d_pnl = 0.0
            d_outcomes = 0
            d_wins = 0
            for r in self._metrics_resolve_rows_cached(ttl_sec=20.0):
                if not str(r.get("ts", "")).startswith(day_utc):
                    continue
                d_pnl += float(r.get("pnl") or 0.0)
                d_outcomes += 1
                if r.get("result") == "WIN":
                    d_wins += 1
            self._dash_daily_cache = {
                "ts": now_ts,
                "day": day_utc,
                "pnl": round(d_pnl, 2),
                "outcomes": int(d_outcomes),
                "wins": int(d_wins),
            }
            dc = self._dash_daily_cache
        d_out = int(dc.get("outcomes", 0) or 0)
        d_wins = int(dc.get("wins", 0) or 0)
        d_wr = round(d_wins / d_out * 100, 1) if d_out > 0 else 0.0

        # Health
        cl_ages = {a: round(now_ts - float(self.cl_updated.get(a, 0) or 0), 1)
                   for a in ("BTC", "ETH", "SOL", "XRP")}

        return {
            "ts": datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
            "now_ts": round(now_ts, 1),
            "session": f"{h}h{m:02d}m",
            "network": NETWORK,
            "rtds_ok": self.rtds_ok,
            "trades": self.total, "wins": self.wins, "wr": wr,
            "pnl": round(pnl, 2), "roi": round(roi, 1),
            "usdc": round(self.onchain_usdc_balance, 2),
            "open_stake": round(self.onchain_open_stake_total, 2),
            "open_count": len(positions),
            "open_mark": round(self.onchain_open_mark_value, 2),
            "total_equity": round(display_bank, 2),
            "prices": {a: round(p, 2) for a, p in self.prices.items() if p > 0},
            "cl_ages": cl_ages,
            "charts": charts,
            "positions": positions,
            "skip_top": skip_top,
            "execq": execq,
            "execq_all": execq_all,
            "active_gates": active_gates,
            "daily_day": day_utc,
            "daily_pnl_total": round(float(dc.get("pnl", 0.0) or 0.0), 2),
            "daily_outcomes": d_out,
            "daily_wins": d_wins,
            "daily_wr": d_wr,
            "dry_run": DRY_RUN,
        }

    async def _dashboard_loop(self):
        """Serve a live web dashboard on DASHBOARD_PORT (default 8080)."""
        port = int(os.environ.get("DASHBOARD_PORT", "8080"))
        from aiohttp import web

        HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ClawdBot</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Schibsted+Grotesk:wght@400;500;600;700;800&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<style>
:root{
  --bg:#06070a;--s1:#0b0d12;--s2:#10131a;--s3:#151a24;
  --b1:#1b2230;--b2:#273248;--b3:#34435f;
  --t1:#edf2ff;--t2:#97a5c2;--t3:#4f607d;
  --g:#00cc78;--gb:rgba(0,204,120,.09);--gbd:rgba(0,204,120,.22);
  --r:#ff3d3d;--rb:rgba(255,61,61,.09);--rbd:rgba(255,61,61,.22);
  --bl:#5f8dff;--blb:rgba(95,141,255,.10);--blbd:rgba(95,141,255,.24);
  --y:#f5a623;--yb:rgba(245,166,35,.09);
  --rd:12px;--rdl:16px;
}
*{box-sizing:border-box;margin:0;padding:0}
html{font-size:16px;-webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale}
body{
  font-family:'Plus Jakarta Sans','Schibsted Grotesk',system-ui,-apple-system,sans-serif;
  font-variant-numeric:tabular-nums;
  background:var(--bg);color:var(--t1);min-height:100vh;
  background-image:
    radial-gradient(ellipse 88% 52% at 50% -5%,rgba(95,141,255,.09) 0%,transparent 62%),
    radial-gradient(ellipse 40% 30% at 88% 14%,rgba(0,204,120,.05) 0%,transparent 55%);
}
body::before{
  content:"";position:fixed;inset:0;pointer-events:none;z-index:-1;
  background:
    radial-gradient(900px 420px at 10% -5%, rgba(0,204,120,.06), transparent 62%),
    radial-gradient(860px 420px at 92% -8%, rgba(95,141,255,.08), transparent 66%);
}
::-webkit-scrollbar{width:4px;height:4px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--b2);border-radius:2px}

/* HEADER */
.H{
  position:sticky;top:0;z-index:50;height:48px;
  display:flex;align-items:center;justify-content:space-between;padding:0 28px;
  background:rgba(6,7,10,.82);backdrop-filter:blur(24px);
  border-bottom:1px solid rgba(79,96,125,.22);
}
.Hl{display:flex;align-items:center;gap:12px}
.logo{display:flex;align-items:center;gap:7px;font-size:.86rem;font-weight:700;letter-spacing:-.025em}
.ld{width:5px;height:5px;border-radius:50%;background:var(--g);flex-shrink:0;animation:lp 2.8s ease-in-out infinite}
@keyframes lp{0%,100%{opacity:1;box-shadow:0 0 0 0 rgba(0,204,120,.6)}55%{opacity:.85;box-shadow:0 0 0 5px rgba(0,204,120,0)}}
.logo b{color:var(--g)}
.tbadge{font-size:.7rem;color:var(--t3);padding:2px 7px;background:var(--s1);border:1px solid var(--b1);border-radius:3px}
.drybadge{font-size:.58rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;padding:2px 8px;border-radius:4px;background:var(--yb);color:var(--y);border:1px solid rgba(245,166,35,.3);display:none}
.Hr{display:flex;align-items:center}
.hs{display:flex;flex-direction:column;align-items:flex-end;gap:1px;padding:5px 14px;border-right:1px solid var(--b1)}
.hs:first-child{border-left:1px solid var(--b1)}
.hsl{font-size:.52rem;font-weight:700;color:var(--t3);text-transform:uppercase;letter-spacing:.1em}
.hsv{font-size:.95rem;font-weight:600}

/* WRAP */
.W{padding:22px 28px;max-width:1920px;margin:0 auto;display:flex;flex-direction:column;gap:20px}

/* PRICES */
.pc-row{display:flex;gap:4px;flex-wrap:wrap}
.pc{display:flex;align-items:center;gap:6px;background:var(--s1);border:1px solid var(--b1);border-radius:5px;padding:5px 10px;cursor:default;transition:border-color .15s}
.pc:hover{border-color:var(--b2)}
.pc .a{font-size:.61rem;font-weight:700;color:var(--t3);letter-spacing:.07em;text-transform:uppercase}
.pc .v{font-size:.86rem;font-weight:600}

/* METRICS BAR */
.mbar{display:grid;grid-template-columns:repeat(3,minmax(220px,1fr));gap:10px}
.mi{
  padding:16px 18px;border:1px solid var(--b1);border-radius:var(--rdl);
  display:flex;flex-direction:column;gap:6px;transition:all .18s;cursor:default;
  background:linear-gradient(180deg,rgba(16,19,26,.95) 0%, rgba(11,13,18,.95) 100%);
}
.mi:hover{border-color:var(--b2);transform:translateY(-1px);box-shadow:0 10px 26px rgba(0,0,0,.28)}
.mi-l{font-size:.52rem;font-weight:700;color:var(--t3);text-transform:uppercase;letter-spacing:.1em}
.mi-v{font-size:1.52rem;font-weight:800;letter-spacing:-.03em;line-height:1}
.mi-s{font-size:.63rem;color:var(--t2);margin-top:1px}
.mi-s b{color:var(--t1);font-weight:500}

/* SECTION */
.sh{display:flex;align-items:baseline;gap:8px;margin-bottom:10px}
.st{font-size:.6rem;font-weight:700;color:var(--t3);text-transform:uppercase;letter-spacing:.1em}

/* POSITIONS */
.pgrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(360px,1fr));gap:14px}
.pcard{background:#0b111b;border:1px solid #1b2a3f;border-radius:18px;overflow:hidden;display:flex;flex-direction:column;transition:border-color .2s,box-shadow .2s}
.pcard:hover{box-shadow:0 10px 34px rgba(0,0,0,.46)}
.pcard{animation:fadeIn .28s ease both}
@keyframes fadeIn{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}
.pcard.lead{border-color:#1f6a4f;box-shadow:inset 0 0 0 1px rgba(0,204,120,.18)}
.pcard.trail{border-color:#7a2c38;box-shadow:inset 0 0 0 1px rgba(255,61,61,.16)}
.pcard.lead .ca{background:linear-gradient(180deg,rgba(9,26,20,.92) 0%,rgba(8,16,13,.98) 100%)}
.pcard.trail .ca{background:linear-gradient(180deg,rgba(30,12,16,.90) 0%,rgba(15,10,11,.98) 100%)}
.bstate{font-size:.62rem;font-weight:800;letter-spacing:.06em;padding:2px 8px;border-radius:4px;text-transform:uppercase}
.bstate.win{background:rgba(0,204,120,.14);border:1px solid rgba(0,204,120,.40);color:#52d69d}
.bstate.lose{background:rgba(255,61,61,.14);border:1px solid rgba(255,61,61,.38);color:#ff7c7c}
.bstate.na{background:rgba(95,141,255,.12);border:1px solid rgba(95,141,255,.30);color:#8eb0ff}
.ph{padding:14px 16px 0;display:flex;align-items:flex-start;justify-content:space-between;gap:8px}
.phl{display:flex;align-items:flex-start;gap:6px;flex-direction:column}
.psym{font-size:1rem;font-weight:800;letter-spacing:-.02em}
.pevt{font-size:.8rem;color:var(--t2)}
.pdur{font-size:.62rem;color:var(--t3);background:var(--s2);border:1px solid var(--b1);padding:2px 5px;border-radius:3px}
.pup{font-size:.62rem;font-weight:600;padding:2px 7px;border-radius:3px;background:var(--blb);color:var(--bl);border:1px solid var(--blbd)}
.pdn{font-size:.62rem;font-weight:600;padding:2px 7px;border-radius:3px;background:var(--rb);color:var(--r);border:1px solid var(--rbd)}
.phr{display:flex;align-items:center;gap:5px}
.blead{font-size:.58rem;font-weight:700;letter-spacing:.05em;padding:2px 7px;border-radius:3px;background:var(--gb);color:var(--g);border:1px solid var(--gbd)}
.btrail{font-size:.58rem;font-weight:700;letter-spacing:.05em;padding:2px 7px;border-radius:3px;background:var(--rb);color:var(--r);border:1px solid var(--rbd)}
.bunk{font-size:.58rem;font-weight:600;padding:2px 6px;border-radius:3px;background:var(--s2);color:var(--t3);border:1px solid var(--b1)}
.stag{font-size:.62rem;color:var(--t3)}
.pdata{display:grid;grid-template-columns:1fr 1fr auto;padding:10px 16px 12px;gap:10px;border-bottom:1px solid #1a2a41}
.di{display:flex;flex-direction:column;gap:3px}
.dl{font-size:.56rem;font-weight:700;color:#6f85a9;text-transform:uppercase;letter-spacing:.08em}
.dv{font-size:1.05rem;font-weight:700}
.pcount{display:flex;flex-direction:column;align-items:flex-end;justify-content:center}
.pcount .dl{font-size:.52rem}
.pcv{font-size:1.8rem;font-weight:800;line-height:1;color:#ff6b6b}
.pcv small{font-size:.65rem;color:#7f90ad;margin-left:2px}
.ca{height:156px;border-top:1px solid var(--b1);border-bottom:1px solid var(--b1);background:linear-gradient(180deg,rgba(18,22,30,.95) 0%,rgba(9,11,16,.98) 100%);position:relative}
.ca::after{
  content:"";position:absolute;inset:0;pointer-events:none;
  background:linear-gradient(180deg,rgba(95,141,255,.04) 0%,rgba(0,0,0,0) 65%);
}
canvas{display:block;width:100%!important}
.pf{padding:9px 16px;display:flex;justify-content:space-between;align-items:center;font-size:.7rem}
.pf .lbl{color:var(--t2)}
.pf b{color:var(--t1);font-size:.75rem}
.pf .rk{color:var(--t3);font-size:.64rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:150px}
.tb{height:2px;background:var(--b1)}
.tbf{height:100%;transition:width .5s linear}

/* BOTTOM */
.bot{display:grid;grid-template-columns:300px 1fr;gap:16px;align-items:start}
@media(max-width:780px){.bot{grid-template-columns:1fr}}

/* SIDE PANEL */
.lpanel{display:flex;flex-direction:column;gap:9px}
.card{background:var(--s1);border:1px solid var(--b1);border-radius:var(--rdl);overflow:hidden}
.ch{padding:10px 14px;border-bottom:1px solid var(--b1);font-size:.58rem;font-weight:700;color:var(--t3);text-transform:uppercase;letter-spacing:.1em}
.ce{padding:13px;text-align:center;font-size:.7rem;color:var(--t3)}
.gates{padding:8px 10px;display:flex;flex-wrap:wrap;gap:4px}
.gtag{font-size:.58rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;padding:3px 7px;border-radius:4px;background:var(--blb);color:var(--bl);border:1px solid var(--blbd)}
.skrow{display:flex;justify-content:space-between;align-items:center;padding:6px 12px;border-bottom:1px solid rgba(20,20,40,.8)}
.skrow:last-child{border-bottom:none}
.skrow:hover{background:var(--s2)}
.skr{font-size:.64rem;color:var(--t2);line-height:1.4}
.skc{font-size:.76rem;font-weight:600;color:var(--t1)}

/* EXECQ TABLE */
.etw{background:var(--s1);border:1px solid var(--b1);border-radius:var(--rdl);overflow:hidden}
.eth{padding:11px 16px;border-bottom:1px solid var(--b1);display:flex;align-items:center;justify-content:space-between}
.etl{font-size:.58rem;font-weight:700;color:var(--t3);text-transform:uppercase;letter-spacing:.1em}
.ett{font-size:.82rem;font-weight:700}
.et{width:100%;border-collapse:collapse}
.et th{font-size:.56rem;font-weight:700;color:var(--t3);text-transform:uppercase;letter-spacing:.09em;padding:9px 14px;border-bottom:1px solid var(--b1);text-align:left}
.et th:last-child{text-align:right}
.et td{padding:9px 14px;border-bottom:1px solid var(--b1);vertical-align:middle}
.et tr:last-child td{border-bottom:none}
.et tr:hover td{background:var(--s2)}
.etbk{font-size:.8rem;color:var(--t1);font-weight:600}
.etfo{font-size:.78rem;color:var(--t2)}
.wrc{display:flex;align-items:center;gap:7px}
.wrt{flex:1;height:3px;background:var(--b1);border-radius:2px;overflow:hidden;min-width:55px;position:relative}
.wrt::after{content:'';position:absolute;left:50%;top:0;bottom:0;width:1px;background:var(--b2)}
.wrf{height:100%;border-radius:2px;transition:width .5s ease}
.wrn{font-size:.76rem;font-weight:600;min-width:40px;text-align:right}
.etpf{font-size:.76rem;color:var(--t2)}
.etpnl{font-size:.78rem;font-weight:700;text-align:right}

/* UTILS */
.g{color:var(--g)}.r{color:var(--r)}.y{color:var(--y)}.d{color:var(--t2)}.dm{color:var(--t3)}

@media(max-width:1240px){
  .mbar{grid-template-columns:repeat(2,minmax(220px,1fr))}
  .pgrid{grid-template-columns:repeat(auto-fill,minmax(320px,1fr))}
}
@media(max-width:860px){
  .H{padding:0 14px}
  .W{padding:14px}
  .mbar{grid-template-columns:1fr}
  .pgrid{grid-template-columns:1fr}
}
</style>
</head>
<body>
<div class="H">
  <div class="Hl">
    <div class="logo"><div class="ld"></div>Clawd<b>Bot</b></div>
    <span class="tbadge" id="ts">--:--:--</span>
    <span class="drybadge" id="dry">Dry Run</span>
  </div>
  <div class="Hr" id="hstats"></div>
</div>
<div class="W">
  <div class="pc-row" id="prices"></div>
  <div class="mbar" id="mbar"></div>
  <div>
    <div class="sh"><span class="st" id="pos-title">Open Positions</span></div>
    <div class="pgrid" id="positions"></div>
  </div>
  <div class="bot">
    <div class="lpanel" id="lpanel"></div>
    <div id="execq"></div>
  </div>
</div>

<script>
const ch={},cm={};
const _mid404=new Set();
let _midSeries={},_posMeta={};
function fmt(n,d=2){return Number(n).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d})}
function fmtT(ts){const x=new Date(ts*1e3);return x.toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false})}
function pfx(v){return v>0?'+':''}
function pnl(v){return(v>=0?'+':'-')+'$'+fmt(Math.abs(v))}
function pdec(p){if(p<=0)return 5;if(p<0.1)return 6;if(p<1)return 5;if(p<10)return 4;if(p<100)return 3;return 2}
function fmtETRange(sTs,eTs){
  try{
    const opt={hour:'numeric',minute:'2-digit',hour12:true,timeZone:'America/New_York'};
    const s=new Date((sTs||0)*1e3).toLocaleTimeString('en-US',opt);
    const e=new Date((eTs||0)*1e3).toLocaleTimeString('en-US',opt);
    return `${s}-${e} ET`;
  }catch(e){return '';}
}
function countdown(minsLeft){
  const t=Math.max(0,Math.floor((minsLeft||0)*60));
  const m=Math.floor(t/60), s=t%60;
  return {m:String(m).padStart(2,'0'),s:String(s).padStart(2,'0')};
}
function countdownSec(secLeft){
  const t=Math.max(0,Math.floor(secLeft||0));
  const m=Math.floor(t/60), s=t%60;
  return {m:String(m).padStart(2,'0'),s:String(s).padStart(2,'0')};
}
function tickTime(){
  const now=Math.floor(Date.now()/1000);
  for(const [uid,pm] of Object.entries(_posMeta||{})){
    const durSec=Math.max(60,Math.round((pm.duration||15)*60));
    const secLeft=Math.max(0,Math.round((pm.end_ts||now)-now));
    const pct=Math.min(100,Math.max(0,((durSec-secLeft)/durSec)*100));
    const cd=countdownSec(secLeft);
    const tEl=document.getElementById('tm'+uid);
    const pEl=document.getElementById('pb'+uid);
    if(tEl)tEl.innerHTML=`${cd.m}<small>m</small> ${cd.s}<small>s</small>`;
    if(pEl)pEl.style.width=`${pct}%`;
  }
}

function renderHeader(d){
  document.getElementById('ts').textContent=d.ts;
  if(d.dry_run)document.getElementById('dry').style.display='';
  const p=d.pnl,dp=+(d.daily_pnl_total||0),wr=d.wr;
  document.getElementById('hstats').innerHTML=[
    ['P&L',pnl(p),p>=0?'g':'r'],
    ['Today',pnl(dp),dp>=0?'g':'r'],
    ['WR',wr+'%',wr>=52?'g':wr>=48?'y':'r'],
    ['Trades',''+d.trades,'d'],
    ['Equity','$'+fmt(d.total_equity),''],
    ['RTDS',d.rtds_ok?'●':'○',d.rtds_ok?'g':'r'],
  ].map(([l,v,c])=>
    `<div class="hs"><div class="hsl">${l}</div><div class="hsv ${c}">${v}</div></div>`
  ).join('');
}

function renderPrices(d){
  document.getElementById('prices').innerHTML=
    Object.entries(d.prices).map(([a,p])=>
      `<div class="pc"><span class="a">${a}</span><span class="v">$${fmt(p,2)}</span></div>`
    ).join('');
}

function renderMetrics(d){
  const p=d.pnl,pc=p>=0?'g':'r';
  const dp=+(d.daily_pnl_total||0),dpc=dp>=0?'g':'r';
  const wr=d.wr,wrc=wr>=52?'g':wr>=48?'y':'r';
  const dwr=+(d.daily_wr||0),dwrc=dwr>=52?'g':dwr>=48?'y':'r';
  const l=Math.max(0,(d.trades||0)-(d.wins||0));
  const dl=Math.max(0,(d.daily_outcomes||0)-(d.daily_wins||0));
  document.getElementById('mbar').innerHTML=[
    ['Portfolio','$'+fmt(d.total_equity),'Free <b>$'+fmt(d.usdc)+'</b> · Open <b>'+d.open_count+'</b>'],
    ['Session P&L','<span class="'+pc+'">'+pnl(p)+'</span>','ROI <b class="'+pc+'">'+pfx(d.roi)+d.roi.toFixed(1)+'%</b>'],
    ['Today','<span class="'+dpc+'">'+pnl(dp)+'</span>',(d.daily_wins||0)+'W · '+dl+'L · <b>'+(d.daily_outcomes||0)+'</b>'],
    ['Win Rate','<span class="'+wrc+'">'+wr+'%</span>',d.wins+'W / '+l+'L of <b>'+d.trades+'</b>'],
    ['Today WR','<span class="'+dwrc+'">'+dwr.toFixed(1)+'%</span>','<b>'+(d.daily_day||'')+'</b>'],
    ['Open Stake','$'+fmt(d.open_stake),'Mark <b>$'+fmt(d.open_mark)+'</b>'],
  ].map(([l,v,s])=>
    `<div class="mi"><div class="mi-l">${l}</div><div class="mi-v">${v}</div><div class="mi-s">${s}</div></div>`
  ).join('');
}

function drawSparkline(canvas,wp,openP,lead){
  const rect=canvas.getBoundingClientRect();
  const cssW=Math.max(16,Math.floor(rect.width||canvas.clientWidth||320));
  const cssH=Math.max(16,Math.floor(rect.height||canvas.clientHeight||120));
  const dpr=Math.max(1,window.devicePixelRatio||1);
  canvas.width=Math.floor(cssW*dpr);
  canvas.height=Math.floor(cssH*dpr);
  const g=canvas.getContext('2d');
  if(!g)return;
  g.setTransform(1,0,0,1,0,0);
  g.scale(dpr,dpr);
  g.clearRect(0,0,cssW,cssH);

  const line=lead===null?'#3a3a5a':(lead?'#00cc78':'#ff3d3d');
  const fill=lead===null?'rgba(58,58,90,.07)':(lead?'rgba(0,204,120,.09)':'rgba(255,61,61,.09)');
  const minP=Math.min(...wp.map(x=>x.p));
  const maxP=Math.max(...wp.map(x=>x.p));
  const span=Math.max(1e-6,maxP-minP);
  const padX=6,padY=6;
  const w=Math.max(8,cssW-padX*2),h=Math.max(8,cssH-padY*2);
  const pts=wp.map((x,i)=>{
    const xx=padX+(i/(Math.max(1,wp.length-1)))*w;
    const yy=padY+(1-(x.p-minP)/span)*h;
    return [xx,yy];
  });

  const fg=g.createLinearGradient(0,padY,0,padY+h);
  fg.addColorStop(0,fill);fg.addColorStop(1,'rgba(0,0,0,0)');
  g.beginPath();
  pts.forEach(([x,y],i)=>{if(i===0)g.moveTo(x,y);else g.lineTo(x,y);});
  g.lineTo(padX+w,padY+h);g.lineTo(padX,padY+h);g.closePath();
  g.fillStyle=fg;g.fill();

  if(openP>0){
    const oy=padY+(1-(openP-minP)/span)*h;
    g.setLineDash([3,4]);g.lineWidth=1;g.strokeStyle='rgba(255,255,255,.1)';
    g.beginPath();g.moveTo(padX,oy);g.lineTo(padX+w,oy);g.stroke();g.setLineDash([]);
  }

  g.lineWidth=2.0;g.strokeStyle=line;
  g.beginPath();
  pts.forEach(([x,y],i)=>{if(i===0)g.moveTo(x,y);else g.lineTo(x,y);});
  g.stroke();
  const lp=pts[pts.length-1];
  if(lp){
    g.beginPath();
    g.fillStyle=line;
    g.arc(lp[0],lp[1],2.2,0,Math.PI*2);
    g.fill();
  }
}

function drawChart(id,pts,openP,curP,sTs,eTs,now){
  const ctx=document.getElementById(id);if(!ctx)return;
  const _w=Math.floor(ctx.getBoundingClientRect().width||ctx.clientWidth||0);
  if(_w<=8){requestAnimationFrame(()=>drawChart(id,pts,openP,curP,sTs,eTs,now));return;}
  let wp=(pts||[]).filter(x=>x.t>=sTs-5);
  if(!wp.length){
    const op=(typeof openP==='number'&&openP>0)?openP:0.5;
    const cp=(typeof curP==='number'&&curP>0)?curP:op;
    const t0=Math.max((sTs||now)-4,0);
    const t1=Math.max((sTs||now)-1,0);
    const t2=Math.max(now, t1+1);
    wp=[{t:t0,p:op},{t:t1,p:(op+cp)/2},{t:t2,p:cp}];
  }else if(wp.length===1){
    const p0=wp[0].p;
    wp=[{t:Math.max(wp[0].t-1,0),p:p0},{t:wp[0].t,p:p0}];
  }
  const avg=wp.reduce((s,x)=>s+x.p,0)/wp.length;
  const dec=pdec(avg);
  const labels=wp.map(x=>fmtT(x.t));
  const data=wp.map(x=>x.p);
  const dMin=Math.min(...data);
  const dMax=Math.max(...data);
  const dMid=(dMin+dMax)/2;
  let span=Math.max(1e-9,dMax-dMin);
  // Keep y-range tight to expose micro-movements.
  if(span<Math.max(Math.abs(dMid)*0.000004,1e-6))span=Math.max(Math.abs(dMid)*0.000004,1e-6);
  const pad=span*0.18;
  const yMin=dMid-(span/2)-pad;
  const yMax=dMid+(span/2)+pad;
  const last=data[data.length-1];
  const lead=openP>0?last>=openP:null;
  const lc=lead===null?'#3a3a5a':(lead?'#00cc78':'#ff3d3d');
  const fg=lead===null?'rgba(58,58,90,.07)':(lead?'rgba(0,204,120,.09)':'rgba(255,61,61,.09)');
  if(typeof Chart==='undefined'){
    if(ch[id]){try{ch[id].destroy()}catch(e){}delete ch[id];}
    drawSparkline(ctx,wp,openP,lead);
    return;
  }
  if(ch[id]&&cm[id]&&cm[id].lead===lead){
    try{
      const c=ch[id];
      c.data.labels=labels;c.data.datasets[0].data=data;
      if(c.data.datasets[1])c.data.datasets[1].data=wp.map(()=>openP);
      if(c.options&&c.options.scales&&c.options.scales.y){
        c.options.scales.y.min=yMin;
        c.options.scales.y.max=yMax;
      }
      c.update('none');return;
    }catch(e){
      if(ch[id]){try{ch[id].destroy()}catch(e2){}delete ch[id];}
      drawSparkline(ctx,wp,openP,lead);
      return;
    }
  }
  if(ch[id]){ch[id].destroy();delete ch[id];}
  cm[id]={lead};
  const ds=[{
    data,borderColor:lc,borderWidth:1.8,pointRadius:0,pointHoverRadius:2,
    tension:0.08,fill:'origin',
    backgroundColor(cx){
      const g=cx.chart.ctx.createLinearGradient(0,0,0,120);
      g.addColorStop(0,fg);g.addColorStop(1,'rgba(0,0,0,0)');return g;
    }
  }];
  if(openP>0)ds.push({data:wp.map(()=>openP),borderColor:'rgba(255,255,255,.1)',borderWidth:1,borderDash:[3,4],pointRadius:0,fill:false,tension:0});
  try{
    ch[id]=new Chart(ctx,{type:'line',data:{labels,datasets:ds},options:{
      animation:false,responsive:true,maintainAspectRatio:false,
      interaction:{mode:'nearest',intersect:false},
      plugins:{legend:{display:false},tooltip:{mode:'index',intersect:false,
        displayColors:false,
        backgroundColor:'rgba(12,15,21,.96)',borderColor:'rgba(120,136,168,.28)',borderWidth:1,
        titleColor:'#8e9ab4',bodyColor:'#eef3ff',padding:7,
        callbacks:{label:c=>'$'+fmt(c.raw,dec)}}},
      scales:{
        x:{display:false,grid:{display:false},border:{display:false}},
        y:{display:false,grid:{display:false},border:{display:false},
          min:yMin,max:yMax,
          ticks:{callback:v=>'$'+fmt(v,dec)}}
      }
    }});
  }catch(e){
    if(ch[id]){try{ch[id].destroy()}catch(e2){}delete ch[id];}
    drawSparkline(ctx,wp,openP,lead);
  }
}

function renderPositions(d){
  const now=d.now_ts;
  document.getElementById('pos-title').textContent='Open Positions'+(d.positions.length?' · '+d.positions.length:'');
  const el=document.getElementById('positions');
  // Rebuilding cards replaces canvas nodes: drop stale chart instances first.
  Object.keys(ch).forEach(k=>{try{if(ch[k])ch[k].destroy()}catch(e){} delete ch[k];});
  Object.keys(cm).forEach(k=>{delete cm[k];});
  if(!d.positions.length){el.innerHTML='';return;}
  _posMeta={};
  el.innerHTML=d.positions.map((p,idx)=>{
    const uid=((p.cid_full||p.cid||'x').replace(/[^a-zA-Z0-9]/g,'').slice(-20)||'x')+'_'+idx;
    const cid='c'+uid;
    const durSec=Math.max(60,Math.round((p.duration||15)*60));
    const secLeftRaw=Math.max(0,Math.round((p.end_ts||now)-now));
    const secLeft=Math.min(durSec,secLeftRaw);
    const pct=Math.min(100,Math.max(0,((durSec-secLeft)/durSec)*100));
    const cls=p.lead===null?'':p.lead?' lead':' trail';
    const sideH=p.side==='Up'?'<span class="pup">UP ▲</span>':'<span class="pdn">DOWN ▼</span>';
    const bH=p.lead===null?'<span class="bstate na">Unknown</span>':p.lead?'<span class="bstate win">Winning</span>':'<span class="bstate lose">Losing</span>';
    const srcH=p.src==='fallback'?'<span class="bunk">FALLBACK</span>':'';
    const scoreH=p.score!=null?`<span class="stag">${p.score}</span>`:'';
    const mc=p.move_pct>=0?'g':'r';
    const bc=p.lead===null?'#303050':(p.lead?'var(--g)':'var(--r)');
    const cd=countdownSec(secLeft);
    const hasBeat=(p.open_p||0)>0;
    const delta=hasBeat?((p.cur_p||0)-(p.open_p||0)):0;
    const dc=delta>=0?'g':'r';
    const openTxt=hasBeat?('$'+fmt(p.open_p,pdec(p.open_p))):'N/A';
    const curTxt=(p.cur_p||0)>0?('$'+fmt(p.cur_p,pdec(p.cur_p))):'N/A';
    const deltaTxt=hasBeat?`${pfx(delta)}${fmt(Math.abs(delta),pdec(Math.abs(delta)||0.01))}`:'N/A';
    return `<div class="pcard${cls}"><div class="ph"><div class="phl">
  <span class="psym">${p.asset} Up or Down - ${p.duration||15} Minutes</span>
  <span class="pevt">${fmtETRange(p.start_ts,p.end_ts)}</span>
  <span class="pdur">${p.duration||15}m</span>${sideH}</div>
<div class="phr">${bH}${srcH}${scoreH}</div></div>
<div class="pdata">
<div class="di"><div class="dl">Price to beat</div><div class="dv">${openTxt}</div></div>
<div class="di"><div class="dl">Current price</div><div class="dv">${curTxt} <span class="${hasBeat?dc:'dm'}" style="font-size:.8rem">${deltaTxt}</span></div><div class="dv d" style="font-size:.82rem" id="m${uid}">—</div></div>
<div class="pcount"><div class="dl">Time left</div><div class="pcv" id="tm${uid}">${cd.m}<small>m</small> ${cd.s}<small>s</small></div></div>
</div>
<div class="ca"><canvas id="${cid}" height="120"></canvas></div>
<div class="pf"><span class="lbl">Stake <b>$${fmt(p.stake)}</b> · Move <b class="${mc}">${pfx(p.move_pct)}${p.move_pct.toFixed(2)}%</b></span>
<span class="rk">${p.rk||''}</span></div>
<div class="tb"><div class="tbf" id="pb${uid}" style="width:${pct}%;background:${bc}"></div></div>
</div>`;
  }).join('');
  d.positions.forEach((p,idx)=>{
    const uid=((p.cid_full||p.cid||'x').replace(/[^a-zA-Z0-9]/g,'').slice(-20)||'x')+'_'+idx;
    _posMeta[uid]={open_p:p.open_p,cur_p:p.cur_p,start_ts:p.start_ts,end_ts:p.end_ts,duration:p.duration||15};
    const basePts=d.charts[p.asset]||[];
    drawChart('c'+uid,basePts,p.open_p,p.cur_p,p.start_ts,p.end_ts,now);
  });
  _mt={};d.positions.forEach((p,idx)=>{
    const uid=((p.cid_full||p.cid||'x').replace(/[^a-zA-Z0-9]/g,'').slice(-20)||'x')+'_'+idx;
    const tid=String(p.token_id||'').trim();
    if(tid && !_mid404.has(tid))_mt[uid]=tid;
  });
  tickTime();
  pollMid();
}

function renderLeft(d){
  let h='';
  if(d.active_gates&&d.active_gates.length){
    h+=`<div class="card"><div class="ch">Active Gates</div><div class="gates">`+
      d.active_gates.map(g=>`<span class="gtag">${g}</span>`).join('')+`</div></div>`;
  }
  const sk=d.skip_top||[];
  if(!sk.length){
    h+=`<div class="card"><div class="ch">Skip Reasons</div><div class="ce">None in 15m</div></div>`;
  }else{
    h+=`<div class="card"><div class="ch">Skip Reasons · 15m</div>`+
      sk.map(s=>`<div class="skrow"><span class="skr">${s.reason}</span><span class="skc">${s.count}</span></div>`).join('')+`</div>`;
  }
  document.getElementById('lpanel').innerHTML=h;
}

function renderExecQ(rows){
  const el=document.getElementById('execq');
  if(!rows||!rows.length){el.innerHTML='';return;}
  const tot=rows.reduce((s,r)=>s+r.pnl,0);
  const trs=rows.map(r=>{
    const wr=r.wr;
    const wc=wr===null?'dm':wr>=52?'g':wr>=48?'y':'r';
    const bw=wr===null?0:Math.min(100,wr);
    const bc=wr===null?'#242438':wr>=52?'var(--g)':wr>=48?'var(--y)':'var(--r)';
    return `<tr>
<td class="etbk">${r.bucket}</td>
<td class="etfo">${r.fills}/${r.outcomes}</td>
<td><div class="wrc"><div class="wrt"><div class="wrf" style="width:${bw}%;background:${bc}"></div></div><span class="wrn ${wc}">${wr===null?'—':wr+'%'}</span></div></td>
<td class="etpf">${r.pf===null?'<span class="dm">—</span>':r.pf.toFixed(2)}</td>
<td class="etpnl"><span class="${r.pnl>=0?'g':'r'}">${pnl(r.pnl)}</span></td>
</tr>`;
  }).join('');
  el.innerHTML=`<div class="etw"><div class="eth">
  <span class="etl">ExecQ — Active Buckets</span>
  <span class="ett ${tot>=0?'g':'r'}">${pnl(tot)}</span>
</div>
<table class="et">
<thead><tr><th>Bucket</th><th>F/O</th><th>Win Rate</th><th>PF</th><th>PnL</th></tr></thead>
<tbody>${trs}</tbody>
</table></div>`;
}

async function refresh(){
  try{
    const d=await fetch('/api?t='+Date.now(),{cache:'no-store'}).then(r=>r.json());
    renderHeader(d);renderPrices(d);renderMetrics(d);
    renderPositions(d);renderLeft(d);renderExecQ(d.execq_all);
  }catch(e){console.warn(e);}
}

let _mt={};
async function pollMid(){
  for(const [cid,tid] of Object.entries(_mt)){
    if(!tid)continue;
    try{
      const r=await fetch('https://clob.polymarket.com/midpoint?token_id='+tid);
      const el=document.getElementById('m'+cid);
      if(r.status===404){
        // Invalid or stale token id for this market side; stop polling this row.
        _mid404.add(String(tid));
        if(el){el.textContent='n/a';el.className='dv dm';}
        delete _mt[cid];
        continue;
      }
      if(!r.ok)continue;
      const j=await r.json();
      if(el){
        if(j&&j.mid!=null){
          const v=parseFloat(j.mid);
          if(Number.isFinite(v)){
            el.textContent=v.toFixed(3);
            el.className='dv '+(v>=0.5?'g':'r');
            const pm=_posMeta[cid];
            // Keep midpoint as metric text only; chart uses underlying asset price series.
            if(pm && Number(pm.open_p)<=2){
              const now=Math.floor(Date.now()/1000);
              const s=_midSeries[cid]||[];
              s.push({t:now,p:v});
              if(s.length>320)s.splice(0,s.length-320);
              _midSeries[cid]=s;
            }
          }
        }else{
          el.textContent='—';
          el.className='dv d';
        }
      }
    }catch(e){}
  }
}

refresh();
setInterval(refresh,5000);
setInterval(tickTime,1000);
setInterval(pollMid,2000);
</script>
</body></html>"""

        _NO_CACHE_HEADERS = {
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        }

        async def handle_html(request):
            return web.Response(
                text=HTML,
                content_type="text/html",
                headers=_NO_CACHE_HEADERS,
            )

        async def handle_api(request):
            try:
                data = self._dashboard_data()
            except Exception as e:
                data = {"error": str(e)}
            return web.Response(
                text=json.dumps(data),
                content_type="application/json",
                headers=_NO_CACHE_HEADERS,
            )

        async def handle_reload_buckets(request):
            try:
                self._bucket_stats.rows.clear()
                _bs_load(self._bucket_stats)
                n = sum(v.get("outcomes", 0) for v in self._bucket_stats.rows.values())
                return web.Response(
                    text=json.dumps({"ok": True, "outcomes": n, "buckets": len(self._bucket_stats.rows)}),
                    content_type="application/json",
                    headers=_NO_CACHE_HEADERS,
                )
            except Exception as e:
                return web.Response(text=json.dumps({"ok": False, "error": str(e)}),
                                    content_type="application/json")

        async def handle_analyze(request):
            """Detailed breakdown by score|entry_band|asset for a given date prefix."""
            try:
                date = request.rel_url.query.get("date", "")[:10]
                dur_filter = request.rel_url.query.get("dur", "")
                from collections import defaultdict as _dd
                rows = _dd(lambda: {"wins": 0, "outcomes": 0, "pnl": 0.0,
                                    "gross_win": 0.0, "gross_loss": 0.0})
                for r in self._metrics_resolve_rows_cached(ttl_sec=15.0):
                    if date and not str(r.get("ts", "")).startswith(date):
                        continue
                    if dur_filter and str(r.get("duration", "15")) != dur_filter:
                        continue
                    score = int(r.get("score") or 0)
                    entry = float(r.get("entry_price") or 0)
                    pnl   = float(r.get("pnl") or 0)
                    won   = r.get("result") == "WIN"
                    asset = str(r.get("asset", "?"))
                    sc = "s12+" if score >= 12 else ("s9-11" if score >= 9 else "s0-8")
                    if entry < 0.30:   eb = "<30c"
                    elif entry < 0.51: eb = "30-50c"
                    elif entry < 0.61: eb = "51-60c"
                    elif entry < 0.71: eb = "61-70c"
                    else:              eb = ">70c"
                    by = request.rel_url.query.get("by", "bucket")
                    if by == "asset":
                        k = asset
                    elif by == "asset-score":
                        k = asset + "|" + sc
                    else:
                        k = sc + "|" + eb
                    rows[k]["outcomes"] += 1
                    rows[k]["pnl"] += pnl
                    if won:
                        rows[k]["wins"] += 1
                        rows[k]["gross_win"] += max(0.0, pnl)
                    else:
                        rows[k]["gross_loss"] += max(0.0, -pnl)
                out = []
                for k, v in sorted(rows.items(), key=lambda x: x[1]["pnl"], reverse=True):
                    n = v["outcomes"]
                    wr = round(v["wins"] / n * 100, 1) if n else 0
                    pf = round(v["gross_win"] / v["gross_loss"], 2) if v["gross_loss"] > 0 else None
                    be = round(v["gross_loss"] / (v["gross_win"] + v["gross_loss"]) * 100, 1) if (v["gross_win"] + v["gross_loss"]) > 0 else None
                    out.append({"bucket": k, "n": n, "wr": wr, "pf": pf,
                                "be_wr": be, "pnl": round(v["pnl"], 2)})
                return web.Response(text=json.dumps({"date": date, "rows": out}),
                                    content_type="application/json",
                                    headers=_NO_CACHE_HEADERS)
            except Exception as e:
                return web.Response(text=json.dumps({"error": str(e)}),
                                    content_type="application/json")

        async def handle_daily(request):
            try:
                dur_filter = request.rel_url.query.get("dur", "")
                daily = {}
                for r in self._metrics_resolve_rows_cached(ttl_sec=15.0):
                    if dur_filter and str(r.get("duration", "15")) != dur_filter:
                        continue
                    day = str(r.get("ts", ""))[:10]
                    pnl = float(r.get("pnl") or 0)
                    won = r.get("result") == "WIN"
                    if day not in daily:
                        daily[day] = {"wins": 0, "outcomes": 0, "pnl": 0.0}
                    daily[day]["outcomes"] += 1
                    daily[day]["pnl"] += pnl
                    if won:
                        daily[day]["wins"] += 1
                out = []
                cumul = 0.0
                for day in sorted(daily):
                    v = daily[day]
                    wr = round(v["wins"] / v["outcomes"] * 100, 1) if v["outcomes"] else 0
                    cumul += v["pnl"]
                    out.append({"day": day, "n": v["outcomes"], "wr": wr,
                                "pnl": round(v["pnl"], 2), "cumul": round(cumul, 2)})
                return web.Response(text=json.dumps(out),
                                    content_type="application/json",
                                    headers=_NO_CACHE_HEADERS)
            except Exception as e:
                return web.Response(text=json.dumps({"error": str(e)}),
                                    content_type="application/json")

        async def handle_dur_stats(request):
            """Duration breakdown (5m vs 15m) from metrics file."""
            try:
                from collections import defaultdict as _dd
                rows = _dd(lambda: {"wins": 0, "losses": 0, "pnl": 0.0,
                                    "gross_win": 0.0, "gross_loss": 0.0})
                n5 = n15 = 0
                for r in self._metrics_resolve_rows_cached(ttl_sec=15.0):
                    dur = int(r.get("duration") or 15)
                    score = int(r.get("score") or 0)
                    entry = float(r.get("entry_price") or 0)
                    pnl = float(r.get("pnl") or 0)
                    won = r.get("result") == "WIN"
                    sc = "s12+" if score >= 12 else ("s9-11" if score >= 9 else "s0-8")
                    if entry < 0.30:   eb = "<30c"
                    elif entry < 0.51: eb = "30-50c"
                    elif entry < 0.61: eb = "51-60c"
                    elif entry < 0.71: eb = "61-70c"
                    else:              eb = ">70c"
                    k = f"{dur}m|{sc}|{eb}"
                    rows[k]["wins" if won else "losses"] += 1
                    rows[k]["pnl"] += pnl
                    if won: rows[k]["gross_win"] += max(0.0, pnl)
                    else:   rows[k]["gross_loss"] += max(0.0, -pnl)
                    if dur == 5: n5 += 1
                    else: n15 += 1
                out = []
                for k, v in sorted(rows.items(), key=lambda x: x[1]["pnl"], reverse=True):
                    n = v["wins"] + v["losses"]
                    wr = round(v["wins"] / n * 100, 1) if n else 0
                    denom = v["gross_win"] + v["gross_loss"]
                    be = round(v["gross_loss"] / denom * 100, 1) if denom > 0 else None
                    pf = round(v["gross_win"] / v["gross_loss"], 2) if v["gross_loss"] > 0 else None
                    out.append({"bucket": k, "n": n, "wr": wr, "pf": pf,
                                "be_wr": be, "pnl": round(v["pnl"], 2)})
                return web.Response(
                    text=json.dumps({"n5m": n5, "n15m": n15, "rows": out}),
                    content_type="application/json",
                    headers=_NO_CACHE_HEADERS)
            except Exception as e:
                return web.Response(text=json.dumps({"error": str(e)}),
                                    content_type="application/json")

        _CORR_CACHE_PATH = "/data/corr_cache.json"
        _corr_cache = {"source": "polymarket", "windows": 0, "rows": [], "built_at": ""}

        def _build_corr_cache_sync():
            """Fetch Polymarket historical data (15m + 5m) and compute direction correlation.
            Runs in a worker thread to avoid blocking the main asyncio event loop.
            """
            import urllib.request as _ur
            from collections import defaultdict as _dd
            from itertools import combinations
            # {asset: (series_id, duration_label)}
            _SERIES = {
                "BTC|15m": (10192, "15m"), "ETH|15m": (10191, "15m"),
                "SOL|15m": (10423, "15m"), "XRP|15m": (10422, "15m"),
                "BTC|5m":  (10684, "5m"),  "ETH|5m":  (10683, "5m"),
            }
            _HDRS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
            # windows[slot][asset|dur] = direction
            windows = _dd(dict)
            counts = {}
            for key, (sid, dur) in _SERIES.items():
                asset = key.split("|")[0]
                offset = 0; total = 0
                while True:
                    url = (f"https://gamma-api.polymarket.com/events"
                           f"?series_id={sid}&closed=true&limit=200&offset={offset}")
                    req = _ur.Request(url, headers=_HDRS)
                    try:
                        with _ur.urlopen(req, timeout=15) as resp:
                            events = json.loads(resp.read())
                    except Exception:
                        break
                    if not events:
                        break
                    for ev in events:
                        for mkt in ev.get("markets", []):
                            end = str(ev.get("endDate", ""))[:16]
                            if not end:
                                continue
                            try:
                                prices   = json.loads(mkt["outcomePrices"]) if isinstance(mkt.get("outcomePrices"), str) else mkt.get("outcomePrices", [])
                                outcomes = json.loads(mkt["outcomes"])      if isinstance(mkt.get("outcomes"), str)      else mkt.get("outcomes", [])
                                p0, p1 = float(prices[0]), float(prices[1])
                            except Exception:
                                continue
                            if p0 > 0.95:   winner = outcomes[0]
                            elif p1 > 0.95: winner = outcomes[1]
                            else:           continue
                            slot = end + "|" + dur
                            wkey = asset + "|" + dur
                            if wkey not in windows[slot]:
                                windows[slot][wkey] = winner
                                total += 1
                    offset += 200
                    if len(events) < 200:
                        break
                counts[key] = total
            pairs = _dd(lambda: {"uu": 0, "dd": 0, "sp": 0, "n": 0})
            for slot, assets in windows.items():
                dur = slot.rsplit("|", 1)[-1]
                # only pair assets within same duration
                same_dur = [(k, v) for k, v in assets.items() if k.endswith("|" + dur)]
                for (a1, d1), (a2, d2) in combinations(same_dur, 2):
                    a1n = a1.split("|")[0]; a2n = a2.split("|")[0]
                    key = "|".join(sorted([a1n, a2n])) + "|" + dur
                    pairs[key]["n"] += 1
                    if d1 == "Up"   and d2 == "Up":   pairs[key]["uu"] += 1
                    elif d1 == "Down" and d2 == "Down": pairs[key]["dd"] += 1
                    else:                               pairs[key]["sp"] += 1
            out = []
            for k, v in sorted(pairs.items()):
                n = v["n"]
                if n == 0: continue
                same_pct = round((v["uu"] + v["dd"]) / n * 100, 1)
                out.append({"pair": k, "n": n, "up_up": v["uu"],
                            "down_down": v["dd"], "split": v["sp"],
                            "same_pct": same_pct})
            result = {"source": "polymarket", "windows": len(windows),
                      "rows": out, "built_at": datetime.now(timezone.utc).isoformat()}
            _corr_cache.update(result)
            try:
                with open(_CORR_CACHE_PATH, "w") as _f:
                    json.dump(result, _f)
            except Exception:
                pass

        async def _build_corr_cache_async():
            # Offload blocking urllib/file work so trading loops stay responsive.
            await asyncio.to_thread(_build_corr_cache_sync)

        async def _corr_refresh_loop():
            """Rebuild correlation cache once at startup then every 6 hours."""
            try:
                with open(_CORR_CACHE_PATH) as _f:
                    _corr_cache.update(json.load(_f))
            except Exception:
                pass
            # Let trading loops settle first; correlation cache is non-critical.
            await asyncio.sleep(2)
            while True:
                try:
                    await asyncio.wait_for(_build_corr_cache_async(), timeout=120)
                except Exception:
                    pass
                await asyncio.sleep(6 * 3600)

        async def handle_corr(request):
            """Token direction correlation from Polymarket historical data (cached)."""
            return web.Response(text=json.dumps(_corr_cache),
                                content_type="application/json",
                                headers=_NO_CACHE_HEADERS)

        app = web.Application()
        app.router.add_get("/", handle_html)
        app.router.add_get("/api", handle_api)
        app.router.add_get("/daily", handle_daily)
        app.router.add_get("/analyze", handle_analyze)
        app.router.add_get("/dur-stats", handle_dur_stats)
        app.router.add_get("/corr", handle_corr)
        app.router.add_get("/reload-buckets", handle_reload_buckets)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        print(f"[DASH] Dashboard running on http://0.0.0.0:{port}")
        asyncio.ensure_future(_corr_refresh_loop())
        while True:
            await asyncio.sleep(3600)

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
