"""
ClawdBot Paper Trading v5 — High Win Rate + Email + Database + Self-Learning
=============================================================================
Strategy (data-driven):
  - ONLY trade extreme market prices (mkt<0.40 or >0.60) → 100% hist WR
  - Max 1 bet per asset, max 2 same direction (anti-correlated losses)
  - Kelly-fraction sizing (bigger bet = higher edge)
  - Entry window: first 5 min of interval only

Self-learning:
  - Every 30 min: analyze own trade DB, auto-tune entry thresholds
  - Block assets with <40% WR on 10+ trades
  - Expand/contract entry zone based on which buckets actually win
  - Adjust Kelly fraction based on recent performance

Persistence:
  - SQLite DB survives restarts (accumulates all-time P&L)

Email:
  - 12h reports to alessandro.grandinetti@gmail.com via formsubmit.co
  - Zero login needed — click verification link ONCE on first run
"""

import asyncio, aiohttp, websockets, json, math, sqlite3, os, time, csv, threading
from datetime import datetime, timezone
from scipy.stats import norm
import requests as req_sync

# ── BASE CONFIG (auto-tuned at runtime by _adapt_strategy) ─────────────────
BANKROLL        = 1_000.0
MIN_EDGE        = 0.15    # 15% — only real mispricings
MAX_ENTRY_DEF   = 0.40    # Default: Up bet only when mkt prices it < 40%
MIN_ENTRY_DEF   = 0.60    # Default: Down bet only when mkt prices it > 60%
KELLY_FRAC_DEF  = 0.25    # 25% fractional Kelly
MAX_BET_PCT     = 0.03    # Max 3% bankroll per bet
MIN_BET         = 5.0     # Min $5
MAX_BETS_ASSET  = 1       # Max 1 open bet per asset
MAX_SAME_DIR    = 2       # Max 2 bets same direction
ENTRY_WINDOW    = 5       # Only enter first 5 min of interval
MAX_DAILY_LOSS  = 0.10    # 10% hard stop
ADAPT_INTERVAL  = 1800    # Re-tune every 30 min
MIN_TRADES_TUNE = 5       # Min trades in a bucket to use its stats
MIN_TRADES_ASSET= 10      # Min trades on an asset before blocking it

SCAN_INTERVAL   = 10
PING_INTERVAL   = 5
STATUS_INTERVAL = 30
EMAIL_INTERVAL  = 43200   # 12 hours

EMAIL_TO  = "alessandro.grandinetti@gmail.com"
DB_FILE   = os.path.expanduser("~/clawdbot.db")
LOG_FILE  = os.path.expanduser("~/clawdbot_paper_trades.csv")
STATS_FILE= os.path.expanduser("~/clawdbot_paper_stats.txt")

SERIES = {
    "btc-up-or-down-5m":  {"asset":"BTC","duration":5, "id":"10684"},
    "btc-up-or-down-15m": {"asset":"BTC","duration":15,"id":"10192"},
    "eth-up-or-down-15m": {"asset":"ETH","duration":15,"id":"10191"},
    "sol-up-or-down-15m": {"asset":"SOL","duration":15,"id":"10423"},
    "xrp-up-or-down-15m": {"asset":"XRP","duration":15,"id":"10422"},
}
GAMMA = "https://gamma-api.polymarket.com"
RTDS  = "wss://ws-live-data.polymarket.com"

G="\033[92m"; R="\033[91m"; Y="\033[93m"; B="\033[94m"; W="\033[97m"; RS="\033[0m"


# ── DATABASE ───────────────────────────────────────────────────────────────
class Database:
    def __init__(self, path):
        self.path  = path
        self._lock = threading.Lock()
        self._init()

    def _conn(self):
        return sqlite3.connect(self.path, check_same_thread=False)

    def _init(self):
        with self._conn() as c:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT, asset TEXT, duration INTEGER, side TEXT,
                    size REAL, entry REAL, open_price REAL,
                    mkt_price REAL, edge REAL,
                    result TEXT, pnl REAL, bankroll REAL
                );
                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT, bankroll REAL, daily_pnl REAL,
                    total INTEGER, wins INTEGER,
                    max_entry REAL, min_entry REAL, kelly_frac REAL
                );
            """)

    def save_trade(self, ts, asset, duration, side, size, entry,
                   open_price, mkt_price, edge, result, pnl, bankroll):
        with self._lock:
            with self._conn() as c:
                c.execute(
                    "INSERT INTO trades (ts,asset,duration,side,size,entry,"
                    "open_price,mkt_price,edge,result,pnl,bankroll) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (ts,asset,duration,side,size,entry,open_price,mkt_price,edge,result,pnl,bankroll)
                )

    def save_snapshot(self, bankroll, daily_pnl, total, wins, max_e, min_e, kf):
        with self._lock:
            with self._conn() as c:
                c.execute(
                    "INSERT INTO snapshots (ts,bankroll,daily_pnl,total,wins,"
                    "max_entry,min_entry,kelly_frac) VALUES (?,?,?,?,?,?,?,?)",
                    (datetime.now(timezone.utc).isoformat(),
                     bankroll,daily_pnl,total,wins,max_e,min_e,kf)
                )

    def load_state(self):
        with self._conn() as c:
            row = c.execute(
                "SELECT COALESCE(SUM(pnl),0), COUNT(*), "
                "COALESCE(SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END),0), "
                "COALESCE(MAX(bankroll),?) "
                "FROM trades WHERE result IN ('WIN','LOSS')", (BANKROLL,)
            ).fetchone()
        return row[0], row[1], row[2], row[3]

    def get_winrate_by_bucket(self, bucket=0.05, min_trades=MIN_TRADES_TUNE):
        """Win rate per entry price bucket. Returns list of (low, high, wr, n, total_pnl)."""
        with self._conn() as c:
            rows = c.execute(
                "SELECT entry, result, pnl FROM trades WHERE result IN ('WIN','LOSS')"
            ).fetchall()
        if not rows:
            return []
        buckets = {}
        for entry, result, pnl in rows:
            b = round(int(entry / bucket) * bucket, 3)
            d = buckets.setdefault(b, {"w":0,"n":0,"pnl":0.0})
            d["n"]   += 1
            d["pnl"] += pnl
            if result == "WIN": d["w"] += 1
        out = []
        for low, d in sorted(buckets.items()):
            if d["n"] >= min_trades:
                out.append((low, round(low+bucket,3), d["w"]/d["n"], d["n"], d["pnl"]))
        return out

    def get_asset_stats(self, min_trades=MIN_TRADES_ASSET):
        with self._conn() as c:
            rows = c.execute(
                "SELECT asset, COUNT(*), "
                "SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END), SUM(pnl) "
                "FROM trades WHERE result IN ('WIN','LOSS') "
                "GROUP BY asset HAVING COUNT(*) >= ?", (min_trades,)
            ).fetchall()
        return [(r[0], r[1], r[2]/r[1], r[3]) for r in rows]

    def get_recent_wr(self, n=20):
        """Win rate on last N resolved trades."""
        with self._conn() as c:
            rows = c.execute(
                "SELECT result FROM trades WHERE result IN ('WIN','LOSS') "
                "ORDER BY id DESC LIMIT ?", (n,)
            ).fetchall()
        if not rows: return None
        return sum(1 for r in rows if r[0]=="WIN") / len(rows)

    def get_recent(self, n=10):
        with self._conn() as c:
            return c.execute(
                "SELECT ts,asset,duration,side,entry,result,pnl "
                "FROM trades WHERE result IN ('WIN','LOSS') "
                "ORDER BY id DESC LIMIT ?", (n,)
            ).fetchall()

    def get_summary(self):
        with self._conn() as c:
            return c.execute(
                "SELECT asset, COUNT(*), "
                "SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END), "
                "SUM(pnl), AVG(entry) "
                "FROM trades WHERE result IN ('WIN','LOSS') GROUP BY asset"
            ).fetchall()


# ── EMAIL ──────────────────────────────────────────────────────────────────
def send_email(subject: str, body: str) -> bool:
    """
    Send via formsubmit.co — no account needed.
    FIRST RUN: a verification email arrives at EMAIL_TO.
    Click 'Confirm' once → all future emails delivered directly.
    """
    try:
        r = req_sync.post(
            f"https://formsubmit.co/ajax/{EMAIL_TO}",
            json={
                "name": "ClawdBot", "_subject": subject,
                "message": body, "_template": "box", "_captcha": "false",
            },
            headers={"Content-Type":"application/json","Accept":"application/json"},
            timeout=20,
        )
        data = r.json()
        if str(data.get("success","")).lower() == "true":
            print(f"{G}[EMAIL] Sent ✓ {subject[:60]}{RS}")
            return True
        print(f"{Y}[EMAIL] {data} — if first run, check inbox to confirm{RS}")
        return False
    except Exception as e:
        print(f"{R}[EMAIL] {e}{RS}")
        return False


# ── TRADER ─────────────────────────────────────────────────────────────────
class PaperTrader:
    def __init__(self):
        self.db          = Database(DB_FILE)
        self.prices      = {}
        self.vols        = {"BTC":0.65,"ETH":0.80,"SOL":1.20,"XRP":0.90}
        self.open_prices = {}
        self.active_mkts = {}
        self.pending     = {}
        self.seen        = set()
        self._last_live  = {}

        # ── Adaptive parameters (tuned by _adapt_strategy) ──────────────
        self.max_entry     = MAX_ENTRY_DEF   # Upper bound for Up bets
        self.min_entry     = MIN_ENTRY_DEF   # Lower bound for Down bets
        self.kelly_frac    = KELLY_FRAC_DEF
        self.blocked_assets= set()           # Assets with poor historical WR
        self.good_buckets  = set()           # Entry buckets confirmed to win

        # Load persisted state
        total_pnl, total_trades, wins, last_bank = self.db.load_state()
        self.bankroll   = last_bank
        self.start_bank = last_bank
        self.daily_pnl  = 0.0
        self.all_pnl    = total_pnl
        self.total      = total_trades
        self.wins       = wins
        self.start_time = datetime.now(timezone.utc)
        self.rtds_ok    = False
        self._init_log()

        print(f"{B}[DB] Resumed: {total_trades} past trades | "
              f"All-time P&L ${total_pnl:+.2f} | Bank ${last_bank:.2f}{RS}")

        # Run initial adaptation if we have enough history
        if total_trades >= MIN_TRADES_TUNE:
            self._adapt_strategy_sync()

    # ── LOG ───────────────────────────────────────────────────────────────
    def _init_log(self):
        if not os.path.exists(LOG_FILE):
            with open(LOG_FILE, "w", newline="") as f:
                csv.writer(f).writerow([
                    "time","asset","duration","side","size","entry",
                    "open_price","mkt_price","edge","result","pnl","bankroll"
                ])

    def _log_csv(self, t, result="PENDING", pnl=0):
        with open(LOG_FILE, "a", newline="") as f:
            csv.writer(f).writerow([
                datetime.now(timezone.utc).strftime("%H:%M:%S"),
                t["asset"], t["duration"], t["side"], f"{t['size']:.2f}",
                f"{t['entry']:.3f}", f"{t['open_price']:.4f}",
                f"{t['mkt_price']:.3f}", f"{t['edge']:+.3f}",
                result, f"{pnl:+.2f}", f"{self.bankroll:.2f}"
            ])

    # ── SELF-LEARNING ─────────────────────────────────────────────────────
    def _adapt_strategy_sync(self):
        """Analyze DB and auto-tune thresholds. Called periodically."""
        buckets     = self.db.get_winrate_by_bucket()
        asset_stats = self.db.get_asset_stats()
        recent_wr   = self.db.get_recent_wr(20)
        changes     = []

        # ── 1. Find best entry threshold from actual win rates ─────────
        if buckets:
            # Collect all buckets below 0.50 with good WR (Up bets)
            good_up   = [high for low,high,wr,n,_ in buckets
                         if high <= 0.50 and wr >= 0.55]
            # Collect all buckets above 0.50 with good WR (Down bets)
            good_down = [low for low,high,wr,n,_ in buckets
                         if low >= 0.50 and wr >= 0.55]

            # Expand zone if new good buckets found
            if good_up:
                new_max = max(good_up)
                if abs(new_max - self.max_entry) > 0.01:
                    changes.append(f"MAX_ENTRY {self.max_entry:.2f}→{new_max:.2f}")
                    self.max_entry = new_max

            if good_down:
                new_min = min(good_down)
                if abs(new_min - self.min_entry) > 0.01:
                    changes.append(f"MIN_ENTRY {self.min_entry:.2f}→{new_min:.2f}")
                    self.min_entry = new_min

            # Build confirmed good-bucket set (for fine-grained filtering)
            self.good_buckets = {
                round(low, 3)
                for low, high, wr, n, _ in buckets
                if wr >= 0.55 and n >= MIN_TRADES_TUNE
            }

        # ── 2. Block/unblock assets based on historical WR ────────────
        for asset, n_trades, wr, total_pnl in asset_stats:
            if wr < 0.40:
                if asset not in self.blocked_assets:
                    self.blocked_assets.add(asset)
                    changes.append(f"BLOCK {asset} (WR={wr:.0%} n={n_trades})")
            elif wr > 0.55 and asset in self.blocked_assets:
                self.blocked_assets.discard(asset)
                changes.append(f"UNBLOCK {asset} (WR recovered to {wr:.0%})")

        # ── 3. Adjust Kelly based on recent 20-trade win rate ─────────
        if recent_wr is not None:
            if recent_wr >= 0.70:
                new_kf = min(KELLY_FRAC_DEF * 1.5, 0.40)  # winning → size up
            elif recent_wr <= 0.40:
                new_kf = max(KELLY_FRAC_DEF * 0.5, 0.10)  # losing → size down
            else:
                new_kf = KELLY_FRAC_DEF
            if abs(new_kf - self.kelly_frac) > 0.02:
                changes.append(f"KELLY {self.kelly_frac:.2f}→{new_kf:.2f} (WR20={recent_wr:.0%})")
                self.kelly_frac = new_kf

        # ── Report ────────────────────────────────────────────────────
        if changes:
            print(f"{Y}[ADAPT] " + " | ".join(changes) + RS)
        else:
            print(f"{B}[ADAPT] Parameters stable | "
                  f"max_entry={self.max_entry:.2f} min_entry={self.min_entry:.2f} "
                  f"kelly={self.kelly_frac:.2f} blocked={self.blocked_assets or 'none'}{RS}")

        # Print bucket summary if data exists
        if buckets:
            print(f"{B}[ADAPT] Bucket WR: " +
                  " ".join(f"{low:.2f}-{high:.2f}:{wr:.0%}({n})"
                           for low,high,wr,n,_ in buckets) + RS)

    async def _adapt_loop(self):
        await asyncio.sleep(ADAPT_INTERVAL)  # first run after initial period
        while True:
            self._adapt_strategy_sync()
            self.db.save_snapshot(
                self.bankroll, self.daily_pnl, self.total, self.wins,
                self.max_entry, self.min_entry, self.kelly_frac
            )
            await asyncio.sleep(ADAPT_INTERVAL)

    # ── RTDS STREAM ───────────────────────────────────────────────────────
    async def stream_rtds(self):
        MAP = {"btcusdt":"BTC","ethusdt":"ETH","solusdt":"SOL","xrpusdt":"XRP"}
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
                    print(f"{G}[RTDS] Live — BTC/ETH/SOL/XRP{RS}")

                    async def _ping():
                        while True:
                            await asyncio.sleep(PING_INTERVAL)
                            try: await ws.send(json.dumps({"action":"ping"}))
                            except: break
                    asyncio.create_task(_ping())

                    async for raw in ws:
                        if not raw: continue
                        try: msg = json.loads(raw)
                        except: continue
                        if msg.get("topic") != "crypto_prices": continue
                        p   = msg.get("payload", {})
                        sym = p.get("symbol","").lower()
                        val = float(p.get("value", 0) or 0)
                        if val == 0: continue
                        asset = MAP.get(sym)
                        if not asset: continue
                        self.prices[asset] = val
                        now = datetime.now(timezone.utc).timestamp()
                        for cid, m in self.active_mkts.items():
                            if m.get("asset") == asset and cid not in self.open_prices:
                                if abs(now - m.get("start_ts", 0)) < 30:
                                    self.open_prices[cid] = val
                        self._print_live(asset, val)
            except Exception as e:
                self.rtds_ok = False
                print(f"{R}[RTDS] Reconnect: {e}{RS}")
                await asyncio.sleep(3)

    # ── LIVE P&L ──────────────────────────────────────────────────────────
    def _print_live(self, asset: str, price: float):
        now_t = time.monotonic()
        if now_t - self._last_live.get(asset, 0) < 1.0: return
        self._last_live[asset] = now_t
        now = datetime.now(timezone.utc).timestamp()
        combined, lines = 0.0, []
        for cid, (m, t) in self.pending.items():
            cur  = self.prices.get(t["asset"], t["open_price"])
            mins = max((m["end_ts"] - now) / 60, 0)
            up   = cur >= t["open_price"]
            won  = (t["side"]=="Up" and up) or (t["side"]=="Down" and not up)
            unr  = t["size"] * (1/t["entry"]-1) if won else -t["size"]
            combined += unr
            c    = G if unr >= 0 else R
            arrow= "↑" if cur >= t["open_price"] else "↓"
            lines.append(
                f"  {B}{t['asset']}{RS} ${cur:,.2f}{arrow}${t['open_price']:,.2f} "
                f"{t['side']} {t['duration']}m {c}${unr:+.2f}{RS} ({mins:.1f}m)"
            )
        if not lines: return
        tc = G if (self.daily_pnl+combined) >= 0 else R
        uc = G if combined >= 0 else R
        print(
            f"\n{B}[LIVE]{RS} {datetime.now(timezone.utc).strftime('%H:%M:%S')} | "
            f"Unreal {uc}${combined:+.2f}{RS} | "
            f"Session {tc}${self.daily_pnl+combined:+.2f}{RS} | {len(self.pending)} bets\n" +
            "\n".join(lines), flush=True
        )

    # ── VOL LOOP ──────────────────────────────────────────────────────────
    async def vol_loop(self):
        while True:
            for sym, asset in [("BTCUSDT","BTC"),("ETHUSDT","ETH"),
                                ("SOLUSDT","SOL"),("XRPUSDT","XRP")]:
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

    # ── MARKET FETCHER ────────────────────────────────────────────────────
    async def fetch_markets(self):
        now, found = datetime.now(timezone.utc).timestamp(), {}
        for slug, info in SERIES.items():
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(
                        f"{GAMMA}/events",
                        params={"series_id":info["id"],"active":"true","closed":"false",
                                "order":"startDate","ascending":"true","limit":"20"},
                        timeout=aiohttp.ClientTimeout(total=8)
                    ) as r:
                        data = await r.json()
                events = data if isinstance(data, list) else data.get("data",[])
                for ev in events:
                    start_str = ev.get("startTime","")
                    end_str   = ev.get("endDate","")
                    q         = ev.get("title","") or ev.get("question","")
                    mkts      = ev.get("markets",[])
                    m_data    = mkts[0] if mkts else ev
                    cid       = m_data.get("conditionId","") or ev.get("conditionId","")
                    prices    = m_data.get("outcomePrices") or ev.get("outcomePrices")
                    if not cid or not end_str or not start_str: continue
                    try:
                        end_ts   = datetime.fromisoformat(end_str.replace("Z","+00:00")).timestamp()
                        start_ts = datetime.fromisoformat(start_str.replace("Z","+00:00")).timestamp()
                    except: continue
                    if end_ts <= now or start_ts > now + 60: continue
                    try:
                        if isinstance(prices, str): prices = json.loads(prices)
                        up_price = float(prices[0]) if prices else 0.5
                    except: up_price = 0.5
                    found[cid] = {
                        "conditionId":cid, "question":q,
                        "asset":info["asset"], "duration":info["duration"],
                        "end_ts":end_ts, "start_ts":start_ts,
                        "up_price":up_price, "mins_left":(end_ts-now)/60,
                    }
            except Exception as e:
                print(f"{R}[FETCH] {slug}: {e}{RS}")
        self.active_mkts = found
        return found

    # ── EVALUATE ──────────────────────────────────────────────────────────
    async def evaluate(self, m: dict):
        cid, asset = m["conditionId"], m["asset"]
        duration, up_price = m["duration"], m["up_price"]
        now = datetime.now(timezone.utc).timestamp()

        if self.daily_pnl <= -(self.bankroll * MAX_DAILY_LOSS):
            return

        # ── Filter 1: extreme prices only (auto-tuned threshold) ──────
        if MAX_ENTRY_DEF < up_price < MIN_ENTRY_DEF:
            # Near 50% — only allow if DB confirms this bucket wins
            bucket = round(int(up_price / 0.05) * 0.05, 3)
            if bucket not in self.good_buckets:
                return  # Not confirmed by data

        # Also apply the adaptive threshold
        if self.max_entry < up_price < self.min_entry:
            return

        # ── Filter 2: entry window ─────────────────────────────────────
        if (now - m["start_ts"]) / 60 > ENTRY_WINDOW:
            return

        # ── Filter 3: blocked assets ───────────────────────────────────
        if asset in self.blocked_assets:
            return

        mins_left = m["mins_left"]
        current   = self.prices.get(asset, 0)
        if current == 0: return

        open_price = self.open_prices.get(cid, current)
        self.open_prices.setdefault(cid, current)

        vol       = self.vols.get(asset, 0.70)
        true_prob = self._prob_up(current, open_price, mins_left, vol)
        edge_up   = true_prob - up_price
        side      = "Up" if edge_up > 0 else "Down"
        edge      = abs(edge_up)
        entry     = up_price if side == "Up" else (1 - up_price)

        if edge < MIN_EDGE: return

        # Max 1 bet per asset
        if sum(1 for _,(_,t) in self.pending.items() if t["asset"]==asset) >= MAX_BETS_ASSET:
            return
        # Max 2 same direction
        if sum(1 for _,(_,t) in self.pending.items() if t["side"]==side) >= MAX_SAME_DIR:
            return

        # Kelly sizing (uses adaptive kelly_frac)
        b     = max(1/entry - 1, 0.01)
        kelly = max((b * true_prob - (1 - true_prob)) / b, 0)
        size  = max(
            min(self.bankroll * kelly * self.kelly_frac, self.bankroll * MAX_BET_PCT),
            MIN_BET
        )

        trade = {
            "side":       side,      "size":       size,
            "entry":      entry,     "open_price": open_price,
            "mkt_price":  up_price,  "true_prob":  true_prob,
            "edge":       round(edge, 4),
            "mins_left":  mins_left, "end_ts":     m["end_ts"],
            "asset":      asset,     "duration":   duration,
        }
        self.seen.add(cid)
        self.pending[cid] = (m, trade)
        self._log_csv(trade)

        ec = G if edge > 0 else R
        print(
            f"{Y}[BET]{RS} {side} {ec}{edge:+.1%}{RS} | "
            f"mkt={up_price:.3f} true={true_prob:.2f} payout={b:.1f}x | "
            f"{asset} ${open_price:,.1f} | "
            f"{duration}m ({mins_left:.1f}m left) | ${size:.1f}"
        )

    # ── SCAN LOOP ─────────────────────────────────────────────────────────
    async def scan_loop(self):
        await asyncio.sleep(6)
        while True:
            markets = await self.fetch_markets()
            now     = datetime.now(timezone.utc).timestamp()
            if not markets:
                async with aiohttp.ClientSession() as s:
                    async with s.get(
                        f"{GAMMA}/events",
                        params={"series_id":"10192","active":"true","closed":"false",
                                "order":"startDate","ascending":"true","limit":"3"},
                        timeout=aiohttp.ClientTimeout(total=8)
                    ) as r:
                        nxt = await r.json()
                for ev in (nxt if isinstance(nxt,list) else []):
                    st = ev.get("startTime","")
                    if st:
                        try:
                            wait = (datetime.fromisoformat(st.replace("Z","+00:00")).timestamp()-now)/60
                            print(f"{Y}[WAIT] Markets closed. Next in "
                                  f"{int(wait//60)}h{int(wait%60)}m{RS}")
                            break
                        except: pass
            else:
                extreme = sum(1 for m in markets.values()
                              if m["up_price"] < self.max_entry or m["up_price"] > self.min_entry)
                print(f"{B}[SCAN] {len(markets)} markets | "
                      f"{extreme} in entry zone | {len(self.pending)} bets | "
                      f"zone=[{self.max_entry:.2f},{self.min_entry:.2f}] "
                      f"kelly={self.kelly_frac:.2f}{RS}")

            for cid, m in markets.items():
                if m["start_ts"] > now: continue
                m["mins_left"] = (m["end_ts"] - now) / 60
                if m["mins_left"] < 1: continue
                if cid not in self.seen:
                    await self.evaluate(m)

            await self._resolve()
            await asyncio.sleep(SCAN_INTERVAL)

    # ── RESOLVE ───────────────────────────────────────────────────────────
    async def _resolve_loop(self):
        while True:
            await self._resolve()
            await asyncio.sleep(2)

    async def _resolve(self):
        now     = datetime.now(timezone.utc).timestamp()
        expired = [k for k,(m,_) in self.pending.items() if m["end_ts"] <= now]
        for k in expired:
            m, t   = self.pending.pop(k)
            price  = self.prices.get(t["asset"], t["open_price"])
            up_won = price >= t["open_price"]
            won    = (t["side"]=="Up" and up_won) or (t["side"]=="Down" and not up_won)
            pnl    = t["size"] * (1/t["entry"]-1) if won else -t["size"]
            if won:
                fee = t["size"] * 0.0156 * (1 - abs(t["mkt_price"]-0.5)*2)
                pnl -= fee

            self.daily_pnl += pnl
            self.all_pnl   += pnl
            self.bankroll  += pnl
            self.total     += 1
            if won: self.wins += 1

            ts = datetime.now(timezone.utc).isoformat()
            self.db.save_trade(
                ts, t["asset"], t["duration"], t["side"], t["size"],
                t["entry"], t["open_price"], t["mkt_price"], t["edge"],
                "WIN" if won else "LOSS", pnl, self.bankroll
            )
            self._log_csv(t, "WIN" if won else "LOSS", pnl)

            wr = f"{self.wins/self.total*100:.0f}%"
            c  = G if won else R
            print(
                f"{c}[{'WIN' if won else 'LOSS'}]{RS} "
                f"{t['asset']} ${t['open_price']:,.2f}→${price:,.2f} | "
                f"{t['side']} {t['duration']}m entry={t['entry']:.3f} | "
                f"{c}${pnl:+.2f}{RS} | Bank ${self.bankroll:.2f} | WR {wr}"
            )

            # Trigger adaptation every 10 trades
            if self.total % 10 == 0 and self.total >= MIN_TRADES_TUNE:
                self._adapt_strategy_sync()

    # ── EMAIL LOOP ────────────────────────────────────────────────────────
    async def email_loop(self):
        await asyncio.sleep(120)   # wait 2 min before first send
        while True:
            await asyncio.sleep(EMAIL_INTERVAL)
            await self._send_report()

    async def _send_report(self):
        wr      = f"{self.wins/self.total*100:.1f}%" if self.total else "N/A"
        roi     = (self.bankroll - BANKROLL) / BANKROLL * 100
        recent  = self.db.get_recent(10)
        summary = self.db.get_summary()
        buckets = self.db.get_winrate_by_bucket()

        recent_lines = "\n".join(
            f"  [{r[5]}] {r[1]} {r[2]}m {r[3]} entry={r[4]:.3f} → "
            f"{'+' if r[6]>0 else ''}{r[6]:.2f}"
            for r in recent
        ) or "  No trades yet"

        asset_lines = "\n".join(
            f"  {r[0]}: {r[1]} trades | WR {r[2]/r[1]*100:.0f}% | "
            f"P&L ${r[3]:+.2f} | avg entry {r[4]:.3f}"
            for r in summary
        ) or "  No data"

        bucket_lines = "\n".join(
            f"  {low:.2f}-{high:.2f}: {wr:.0%} WR ({n} trades, ${pnl:+.1f})"
            for low, high, wr, n, pnl in buckets
        ) or "  Not enough data yet"

        body = f"""
ClawdBot 12-Hour Report
========================
{datetime.now().strftime('%Y-%m-%d %H:%M UTC')}

BANKROLL:     ${self.bankroll:.2f}  (started ${BANKROLL:.2f})
ALL-TIME P&L: ${self.all_pnl:+.2f}  ({roi:+.1f}% ROI)
SESSION P&L:  ${self.daily_pnl:+.2f}
TRADES:       {self.total}  |  WINS: {self.wins}  |  WR: {wr}
OPEN BETS:    {len(self.pending)}

LIVE PRICES:
{chr(10).join(f'  {a}: ${p:,.2f}' for a,p in self.prices.items() if p)}

CURRENT PARAMETERS (auto-tuned):
  Entry zone:  mkt < {self.max_entry:.2f} or > {self.min_entry:.2f}
  Kelly frac:  {self.kelly_frac:.2f}
  Blocked:     {', '.join(self.blocked_assets) or 'none'}

WIN RATE BY ENTRY BUCKET:
{bucket_lines}

PERFORMANCE BY ASSET:
{asset_lines}

LAST 10 RESOLVED TRADES:
{recent_lines}
        """.strip()

        subject = (
            f"ClawdBot | Bank ${self.bankroll:.0f} | "
            f"P&L ${self.all_pnl:+.0f} | WR {wr}"
        )
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, send_email, subject, body)

    # ── STATUS ────────────────────────────────────────────────────────────
    def _status(self):
        el  = datetime.now(timezone.utc) - self.start_time
        h,m = int(el.total_seconds()//3600), int(el.total_seconds()%3600//60)
        wr  = f"{self.wins/self.total*100:.1f}%" if self.total else "–"
        roi = (self.bankroll - self.start_bank) / self.start_bank * 100
        pc  = G if self.daily_pnl >= 0 else R
        rs  = G if self.rtds_ok else R
        prices = "  ".join(f"{B}{a}:{RS} ${p:,.2f}" for a,p in self.prices.items() if p>0)
        print(
            f"\n{W}{'─'*70}{RS}\n"
            f"  {B}Up:{RS} {h}h{m}m  {rs}RTDS{'✓' if self.rtds_ok else '✗'}{RS}  "
            f"{B}Trades:{RS} {self.total}  {B}WR:{RS} {wr}  "
            f"{B}ROI:{RS} {pc}{roi:+.1f}%{RS}\n"
            f"  {B}Bankroll:{RS} ${self.bankroll:.2f}  "
            f"{B}Session:{RS} {pc}${self.daily_pnl:+.2f}{RS}  "
            f"{B}All-time:{RS} ${self.all_pnl:+.2f}  "
            f"{B}Bets:{RS} {len(self.pending)}\n"
            f"  {B}Zone:{RS} mkt<{self.max_entry:.2f}/>  {self.min_entry:.2f}  "
            f"{B}Kelly:{RS} {self.kelly_frac:.2f}  "
            f"{B}Blocked:{RS} {self.blocked_assets or 'none'}\n"
            f"  {prices}\n"
            f"{W}{'─'*70}{RS}"
        )

    async def _status_loop(self):
        while True:
            await asyncio.sleep(STATUS_INTERVAL)
            self._status()

    # ── MATH ──────────────────────────────────────────────────────────────
    def _prob_up(self, current, open_price, mins_left, vol):
        if open_price <= 0 or vol <= 0 or mins_left <= 0: return 0.5
        T = max(mins_left, 0.1) / (252 * 24 * 60)
        d = math.log(current / open_price) / (vol * math.sqrt(T))
        return float(norm.cdf(d))

    # ── STATS ─────────────────────────────────────────────────────────────
    async def stats_loop(self):
        while True:
            await asyncio.sleep(1800)
            wr  = f"{self.wins/self.total*100:.1f}%" if self.total else "0%"
            roi = (self.bankroll - BANKROLL) / BANKROLL * 100
            rep = (
                f"\n=== {datetime.now().strftime('%H:%M')} ===\n"
                f"Bank ${self.bankroll:.2f} | P&L ${self.all_pnl:+.2f} ({roi:+.2f}%)\n"
                f"Trades {self.total} | WR {wr} | "
                f"Zone <{self.max_entry:.2f}/>  {self.min_entry:.2f}\n"
                + "  ".join(f"{a}=${p:,.2f}" for a,p in self.prices.items() if p)
            )
            print(f"{B}{rep}{RS}")
            with open(STATS_FILE,"a") as f: f.write(rep+"\n")

    # ── MAIN ──────────────────────────────────────────────────────────────
    async def run(self):
        print(f"""
{B}╔═══════════════════════════════════════════════════════════════╗
║       ClawdBot v5 — Self-Learning Edition                    ║
║  Entry: extreme prices only | Min edge 15% | Kelly sizing   ║
║  Learns: auto-tunes thresholds every 30 min from own DB     ║
║  DB: {DB_FILE:<52}║
║  Email: 12h reports → {EMAIL_TO[:42]:<42}║
╚═══════════════════════════════════════════════════════════════╝{RS}
{Y}[EMAIL] First run triggers formsubmit verification.
        Check {EMAIL_TO} and click Confirm once.{RS}
""")
        await asyncio.gather(
            self.stream_rtds(),
            self.vol_loop(),
            self.scan_loop(),
            self._resolve_loop(),
            self._status_loop(),
            self.stats_loop(),
            self._adapt_loop(),
            self.email_loop(),
        )


if __name__ == "__main__":
    try:
        asyncio.run(PaperTrader().run())
    except KeyboardInterrupt:
        print(f"\n{Y}[STOP] DB: {DB_FILE} | Log: {LOG_FILE}{RS}")
