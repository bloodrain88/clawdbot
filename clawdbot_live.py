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
from web3 import Web3
from eth_account import Account

load_dotenv(os.path.expanduser("~/.clawdbot.env"))

from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON, AMOY
from py_clob_client.clob_types import OrderType, MarketOrderArgs, AssetType, BalanceAllowanceParams
from py_clob_client.config import get_contract_config

POLYGON_RPCS = [
    "https://polygon.llamarpc.com",
    "https://rpc.ankr.com/polygon",
    "https://polygon.drpc.org",
    "https://polygon-mainnet.public.blastapi.io",
]
ERC20_ABI = [
    {"inputs":[{"name":"spender","type":"address"},{"name":"amount","type":"uint256"}],
     "name":"approve","outputs":[{"name":"","type":"bool"}],
     "stateMutability":"nonpayable","type":"function"},
    {"inputs":[{"name":"owner","type":"address"},{"name":"spender","type":"address"}],
     "name":"allowance","outputs":[{"name":"","type":"uint256"}],
     "stateMutability":"view","type":"function"},
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
MIN_EDGE       = 0.06     # 6% min edge (same as paper bot — proven 60-65% WR)
MIN_MOVE       = 0.002    # 0.2% min actual price move — filters pure noise
MIN_BET        = 5.0      # $5 floor
MAX_BET        = 25.0     # $25 ceiling (Kelly can go higher on strong edges)
KELLY_FRAC     = 0.25     # quarter-Kelly — conservative, avoids ruin
MAX_DAILY_LOSS = 0.05     # 5% hard stop
MAX_OPEN       = 3        # max simultaneous open positions

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
SCAN_INTERVAL  = 5
PING_INTERVAL  = 5
STATUS_INTERVAL= 30
_DATA_DIR      = os.environ.get("DATA_DIR", os.path.expanduser("~"))
LOG_FILE       = os.path.join(_DATA_DIR, "clawdbot_live_trades.csv")
PENDING_FILE   = os.path.join(_DATA_DIR, "clawdbot_pending.json")
SEEN_FILE      = os.path.join(_DATA_DIR, "clawdbot_seen.json")

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
        self._init_log()
        self._load_pending()
        self._init_w3()

    def _init_w3(self):
        for rpc in POLYGON_RPCS:
            try:
                _w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
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
            r = _req.get(
                "https://data-api.polymarket.com/positions",
                params={"user": ADDRESS, "sizeThreshold": "0.01"},
                timeout=8
            )
            for p in r.json():
                self.seen.add(p["conditionId"])
            print(f"{Y}[RESUME] Synced seen from Polymarket positions ({len(self.seen)} total){RS}")
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

        # Approve USDC.e on-chain + sync Polymarket backend
        if not DRY_RUN:
            self._approve_usdc()

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

    def _approve_usdc(self):
        """ERC20 approve USDC.e on-chain for both Polymarket exchange contracts,
        then call update_balance_allowance so the CLOB backend syncs."""
        cfg      = get_contract_config(CHAIN_ID, neg_risk=False)
        cfg_neg  = get_contract_config(CHAIN_ID, neg_risk=True)
        collateral = Web3.to_checksum_address(cfg.collateral)
        spenders   = [
            Web3.to_checksum_address(cfg.exchange),
            Web3.to_checksum_address(cfg_neg.exchange),
        ]
        w3 = None
        for rpc in POLYGON_RPCS:
            try:
                _w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
                _w3.eth.block_number
                w3 = _w3
                print(f"{G}[APPROVE] RPC OK: {rpc}{RS}")
                break
            except Exception:
                continue
        if w3 is None:
            print(f"{R}[APPROVE] No working Polygon RPC — skipping on-chain approve{RS}")
        else:
            acct    = Account.from_key(PRIVATE_KEY)
            usdc    = w3.eth.contract(address=collateral, abi=ERC20_ABI)
            max_int = 2**256 - 1
            for spender in spenders:
                try:
                    current = usdc.functions.allowance(acct.address, spender).call()
                    if current > 10**24:
                        print(f"{G}[APPROVE] {spender[:10]}… already approved{RS}")
                        continue
                    nonce  = w3.eth.get_transaction_count(acct.address)
                    tx     = usdc.functions.approve(spender, max_int).build_transaction({
                        "from": acct.address, "nonce": nonce,
                        "gas": 100_000, "gasPrice": w3.eth.gas_price,
                    })
                    signed  = acct.sign_transaction(tx)
                    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
                    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
                    status  = "OK" if receipt.status == 1 else "FAILED"
                    print(f"{G}[APPROVE] {spender[:10]}… {status} tx={tx_hash.hex()[:16]}{RS}")
                except Exception as e:
                    print(f"{Y}[APPROVE] {spender[:10]}… {e}{RS}")
        # Sync Polymarket backend
        try:
            resp = self.clob.update_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            )
            print(f"{G}[CLOB] Backend allowance synced: {resp or 'OK'}{RS}")
        except Exception as e:
            print(f"{Y}[CLOB] Backend sync: {e}{RS}")

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
                await asyncio.sleep(min(60, 5 * 2 ** getattr(self, "_rtds_fails", 0)))
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
                    import time as _t
                    age     = _t.time() - updated
                    if age < 60:   # only use if fresh (<60s)
                        self.cl_prices[asset]  = price
                        self.cl_updated[asset] = updated
                except Exception:
                    pass
            await asyncio.sleep(5)

    def _current_price(self, asset: str) -> float:
        """Best available price: Chainlink if fresh (<45s), else RTDS fallback."""
        import time as _t
        cl = self.cl_prices.get(asset, 0)
        cl_age = _t.time() - self.cl_updated.get(asset, 0)
        if cl > 0 and cl_age < 45:
            return cl
        return self.prices.get(asset, 0)   # RTDS fallback

    def _kelly_size(self, true_prob: float, entry: float) -> float:
        """Quarter-Kelly bet size. Floor=$5, ceil=$25."""
        if entry <= 0 or entry >= 1:
            return MIN_BET
        b = (1 / entry) - 1          # net odds per dollar staked
        q = 1 - true_prob
        kelly_f = (true_prob * b - q) / b
        kelly_f = max(0.0, kelly_f)
        size = self.bankroll * kelly_f * KELLY_FRAC
        return round(max(MIN_BET, min(MAX_BET, size)), 2)

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
        asset     = m["asset"]
        duration  = m["duration"]
        mins_left = m["mins_left"]
        up_price  = m["up_price"]

        if self.daily_pnl <= -(self.bankroll * MAX_DAILY_LOSS):
            return

        # Max open positions guard
        if len(self.pending) >= MAX_OPEN:
            return

        # Use Chainlink price (resolution source) if fresh, else RTDS fallback
        current = self._current_price(asset)
        if current == 0:
            return

        open_price = self.open_prices.get(cid)
        if open_price is None:
            open_price = current
            self.open_prices[cid] = open_price

        # Minimum actual price move — filter pure noise (no move = no signal)
        if open_price > 0 and abs(current - open_price) / open_price < MIN_MOVE:
            return

        # ── Black-Scholes P(Up) — same model as paper bot (proven 60-65% WR) ─
        vol       = self.vols.get(asset, 0.70)
        true_prob = self._prob_up(current, open_price, mins_left, vol)
        edge_up   = true_prob - up_price

        if abs(edge_up) < MIN_EDGE:
            return

        side  = "Up" if edge_up > 0 else "Down"
        edge  = abs(edge_up)
        entry = up_price if side == "Up" else (1 - up_price)
        size  = self._kelly_size(true_prob, entry)   # quarter-Kelly, $5-$25
        token_id = m["token_up"] if side == "Up" else m["token_down"]

        if not token_id:
            print(f"{Y}[SKIP] No token_id for {asset} {side} — market data incomplete{RS}")
            self.seen.add(cid)
            return

        print(f"{B}[EDGE] {asset} {side} | move={((current-open_price)/open_price):+.3%} "
              f"true={true_prob:.3f} mkt={up_price:.3f} edge={edge:.3f} {mins_left:.1f}min left{RS}")

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
        self._save_seen()
        if order_id:
            self.pending[cid] = (m, trade)
            self._save_pending()
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

        amounts = [round(size_usdc, 2), MIN_BET]   # try full Kelly, then floor $5
        for amount in amounts:
            for attempt in range(2):
                try:
                    loop = asyncio.get_event_loop()
                    mkt_args = MarketOrderArgs(
                        token_id=token_id,
                        amount=amount,
                    )
                    signed = await loop.run_in_executor(
                        None, lambda: self.clob.create_market_order(mkt_args)
                    )
                    resp = await loop.run_in_executor(
                        None, lambda: self.clob.post_order(signed, OrderType.FOK)
                    )
                    status   = resp.get("status", "")
                    order_id = resp.get("orderID") or resp.get("id", "")
                    if status in ("unmatched", "cancelled", "") or not order_id:
                        print(f"{Y}[ORDER] FOK unmatched {asset} {side} ${amount} — no liquidity{RS}")
                        break   # try smaller amount
                    self.bankroll -= amount
                    print(
                        f"{Y}[ORDER]{RS} {side} {asset} {duration}m | "
                        f"${amount:.2f} USDC @ ~{price:.3f} | order={order_id[:16]}... | Bank ${self.bankroll:.2f}"
                    )
                    return order_id
                except Exception as e:
                    err = str(e)
                    if "fully filled" in err or "FOK" in err:
                        print(f"{Y}[ORDER] FOK thin liquidity {asset} {side} ${amount} — retrying ${MIN_BET}{RS}")
                        break   # try smaller amount
                    if "429" in err or "rate limit" in err.lower():
                        wait = 10 * (attempt + 1)
                        print(f"{Y}[ORDER] Rate limited — waiting {wait}s{RS}")
                        await asyncio.sleep(wait)
                        continue
                    print(f"{R}[ORDER FAILED] {asset} {side}: {e}{RS}")
                    return None
        return None

    # ── RESOLVE ───────────────────────────────────────────────────────────────
    async def _resolve(self):
        now     = datetime.now(timezone.utc).timestamp()
        expired = [k for k, (m, t) in self.pending.items() if m["end_ts"] <= now]

        for k in expired:
            m, trade = self.pending.pop(k)
            self._save_pending()
            asset = trade["asset"]

            # SYNCED positions: we don't know open_price → can't use price comparison.
            # Queue for on-chain check; _redeem_loop will handle redemption if won.
            # Bankroll is refreshed by _refresh_balance() from real CLOB balance.
            if trade.get("order_id") == "SYNCED":
                print(f"{B}[RESOLVE] Synced {asset} {trade['side']} {trade['duration']}m expired → queued for on-chain check{RS}")
                if not DRY_RUN:
                    self.pending_redeem[k] = (trade["side"], asset)
                continue

            price  = self._current_price(asset) or self.prices.get(asset, trade["open_price"])
            up_won = price >= trade["open_price"]
            won    = (trade["side"] == "Up" and up_won) or (trade["side"] == "Down" and not up_won)

            # Bet was already deducted from bankroll on order place.
            # On WIN: add back full payout (size/entry) minus fee.
            # On LOSS: bankroll already reduced, nothing to add.
            # net pnl (for reporting) = payout - size = size*(1/entry - 1)
            pnl = trade["size"] * (1/trade["entry"] - 1) if won else -trade["size"]
            fee = trade["size"] * 0.0156 * (1 - abs(trade["mkt_price"] - 0.5) * 2)
            pnl = pnl - fee if won else pnl

            self.daily_pnl += pnl
            if won:
                payout = trade["size"] / trade["entry"] - fee
                self.bankroll += payout   # add back payout (bet was already deducted)
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

            # Queue winning tokens for instant on-chain redemption
            if won and not DRY_RUN:
                self.pending_redeem[k] = (trade["side"], asset)

    # ── REDEEM LOOP — polls every 30s, redeems as soon as on-chain resolved ──────
    async def _redeem_loop(self):
        if DRY_RUN or self.w3 is None:
            return
        cfg       = get_contract_config(CHAIN_ID, neg_risk=False)
        ctf_addr  = Web3.to_checksum_address(cfg.conditional_tokens)
        collat    = Web3.to_checksum_address(cfg.collateral)
        acct      = Account.from_key(PRIVATE_KEY)
        CTF_CHECK = [
            {"inputs":[{"name":"conditionId","type":"bytes32"}],
             "name":"payoutDenominator","outputs":[{"name":"","type":"uint256"}],
             "stateMutability":"view","type":"function"},
        ] + CTF_ABI
        ctf  = self.w3.eth.contract(address=ctf_addr, abi=CTF_CHECK)
        loop = asyncio.get_event_loop()

        while True:
            await asyncio.sleep(30)
            if not self.pending_redeem:
                continue
            done = []
            for cid, (side, asset) in list(self.pending_redeem.items()):
                try:
                    cid_bytes = bytes.fromhex(cid.lstrip("0x").zfill(64))
                    denom = await loop.run_in_executor(
                        None, lambda b=cid_bytes: ctf.functions.payoutDenominator(b).call()
                    )
                    if denom == 0:
                        continue   # not yet resolved on-chain, try again next cycle
                    # Resolved — send redeemPositions tx immediately
                    index_set = 1 if side == "Up" else 2
                    nonce  = await loop.run_in_executor(None, lambda: self.w3.eth.get_transaction_count(acct.address))
                    tx     = ctf.functions.redeemPositions(
                        collat, b'\x00'*32, cid_bytes, [index_set]
                    ).build_transaction({"from": acct.address, "nonce": nonce, "gas": 200_000, "gasPrice": self.w3.eth.gas_price})
                    signed  = acct.sign_transaction(tx)
                    tx_hash = await loop.run_in_executor(None, lambda: self.w3.eth.send_raw_transaction(signed.raw_transaction))
                    receipt = await loop.run_in_executor(None, lambda h=tx_hash: self.w3.eth.wait_for_transaction_receipt(h, timeout=60))
                    if receipt.status == 1:
                        print(f"{G}[REDEEM] {asset} {side} → USDC received | tx={tx_hash.hex()[:16]}{RS}")
                        done.append(cid)
                    else:
                        print(f"{Y}[REDEEM] {asset} {side} tx failed, retrying...{RS}")
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
                    # Derive asset from title
                    asset = "BTC" if "Bitcoin" in title else "ETH" if "Ethereum" in title \
                        else "SOL" if "Solana" in title else "XRP" if "XRP" in title else "?"
                    self.pending_redeem[cid] = (outcome, asset)
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
        )


if __name__ == "__main__":
    try:
        asyncio.run(LiveTrader().run())
    except KeyboardInterrupt:
        print(f"\n{Y}[STOP] Log: {LOG_FILE}{RS}")
