from __future__ import annotations

import importlib

_LOADED = False

def _ensure_globals() -> None:
    global _LOADED
    if _LOADED:
        return
    legacy = importlib.import_module("clawbot_v2.engine.live_trader")
    g = globals()
    for k, v in vars(legacy).items():
        if k not in g:
            g[k] = v
    _LOADED = True

async def _score_market(self, m: dict, late_relax: bool = False) -> dict | None:
    _ensure_globals()
    """Score a market opportunity. Returns signal dict or None if hard-blocked.
    Pure analysis — no side effects, no order placement.
    late_relax=True: relax score gate by LATE_MUST_FIRE_SCORE_RELAX (must-fire last N min)."""
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
    prefetch_token = m.get("token_up", "") if up_price <= PREFETCH_UP_PRICE_MAX else m.get("token_down", "")

    # Decision price selection: use freshest reliable source (prefer CL when fresh),
    # then apply soft penalties for source divergence instead of hard blocking.
    rtds_now = float(self.prices.get(asset, 0) or 0.0)
    cl_now = float(self.cl_prices.get(asset, 0) or 0.0)
    last_tick_ts = 0.0
    _ph = self.price_history.get(asset)
    if _ph:
        _last = _ph[-1]
        if isinstance(_last, (tuple, list)):
            last_tick_ts = float(_last[0] or 0.0) if len(_last) >= 1 else 0.0
        elif isinstance(_last, (int, float)):
            # Backward-compat: some persisted caches may store plain timestamps.
            last_tick_ts = float(_last)
    quote_age_ms = (_time.time() - last_tick_ts) * 1000.0 if last_tick_ts else 9e9
    cl_updated = self.cl_updated.get(asset, 0)
    cl_age_s = (_time.time() - cl_updated) if cl_updated else None
    if cl_now > 0 and cl_age_s is not None and cl_age_s <= CL_FRESH_PRICE_AGE_SEC:
        current = cl_now
        px_src = "CL"
        quote_age_ms = 0.0   # CL is the resolution oracle; Binance WS freshness irrelevant
    elif rtds_now > 0 and quote_age_ms <= MAX_QUOTE_STALENESS_MS:
        current = rtds_now
        px_src = "RTDS"
    elif cl_now > 0:
        current = cl_now
        px_src = "CL-stale"
    elif rtds_now > 0:
        current = rtds_now
        px_src = "RTDS-stale"
    else:
        return None

    open_price = self.open_prices.get(cid)
    if not open_price:
        if LOG_VERBOSE or self._should_log(f"wait-open:{cid}", LOG_OPEN_WAIT_EVERY_SEC):
            print(f"{Y}[WAIT] {label} → waiting for first CL round{RS}")
        return None

    src = self.open_prices_source.get(cid, "?")
    src_tag = f"[{src}]"

    # Timing gate
    total_life    = m["end_ts"] - m["start_ts"]
    pct_remaining = (mins_left * 60) / total_life if total_life > 0 else 0
    if pct_remaining < PCT_REMAINING_MIN:
        return None   # too close to close

    # Oracle latency mode: only enter when Chainlink JUST updated in the final window.
    # Also allows contrarian tail entries early in the window (cheap tokens, mean-reversion).
    if ORACLE_LATENCY_ONLY_MODE:
        move_now = abs(current - open_price) / max(open_price, 1e-9)
        oracle_window = (
            cl_age_s is not None
            and cl_age_s <= ORACLE_FRESH_AGE_S
            and mins_left <= ORACLE_MAX_MINS_LEFT
            and move_now >= ORACLE_MIN_MOVE_PCT
        )
        tail_window = (
            CONTRARIAN_TAIL_ENABLED
            and min(up_price, 1.0 - up_price) <= CONTRARIAN_TAIL_MAX_ENTRY
            and mins_left >= CONTRARIAN_TAIL_MIN_MINS_LEFT
            and move_now >= CONTRARIAN_TAIL_MIN_MOVE_PCT
        )
        if not oracle_window and not tail_window:
            return None

    # Previous window direction from CL prices (used as one signal among others).
    prev_open     = self.asset_prev_open.get(asset, 0)
    prev_win_move = abs((open_price - prev_open) / prev_open) if prev_open > 0 and open_price > 0 else 0.0
    prev_win_dir  = None
    if prev_open > 0 and open_price > 0:
        diff = (open_price - prev_open) / prev_open
        if   diff >  PREV_WIN_DIR_MOVE_MIN: prev_win_dir = "Up"
        elif diff < -PREV_WIN_DIR_MOVE_MIN: prev_win_dir = "Down"

    move_pct = abs(current - open_price) / open_price if open_price > 0 else 0
    move_str = f"{(current-open_price)/open_price:+.3%}"

    # ── Multi-signal Score ────────────────────────────────────────────────
    # Resolution rule: ANY price above open = Up wins (even $0.001).
    # Direction from price if moved; from momentum consensus if flat.
    # Max possible: 2+3+4+1+3+3+2+1 = 19 pts
    score = 0
    # Keep initialized for any early quality penalties before final side selection.
    edge = 0.0
    px_align_conflict = False
    data_div_pen_applied = False

    # Compute momentum — EMA-based (O(1) from cache) + Kalman velocity signal
    mom_5s   = self._momentum_prob(asset, seconds=5)
    mom_30s  = self._momentum_prob(asset, seconds=30)
    mom_180s = self._momentum_prob(asset, seconds=180)
    mom_kal  = self._kalman_vel_prob(asset)
    th_up, th_dn = MOM_THRESH_UP, MOM_THRESH_DN
    tf_up_votes = sum([mom_5s > th_up, mom_30s > th_up, mom_180s > th_up, mom_kal > th_up])
    tf_dn_votes = sum([mom_5s < th_dn, mom_30s < th_dn, mom_180s < th_dn, mom_kal < th_dn])

    # Chainlink current — the resolution oracle
    cl_move_pct = abs(cl_now - open_price) / open_price if cl_now > 0 and open_price > 0 else 0
    cl_direction = ("Up" if cl_now > open_price else "Down") if cl_move_pct >= CL_DIRECTION_MOVE_MIN else None

    # Direction: Chainlink is authoritative (it resolves the market)
    # Use Binance RTDS only when it's clearly moving; fall back to CL then momentum
    if move_pct >= DIR_MOVE_MIN:
        direction = "Up" if current > open_price else "Down"
        # Small move + opposite CL direction: keep trading, but align to oracle and penalize.
        if cl_direction and cl_direction != direction and move_pct < DIR_CONFLICT_MOVE_MAX:
            score -= DIR_CONFLICT_SCORE_PEN
            edge -= DIR_CONFLICT_EDGE_PEN
            px_align_conflict = True
            if cl_age_s is not None and cl_age_s <= DIR_CONFLICT_CL_AGE_MAX:
                direction = cl_direction
            if self._noisy_log_enabled(f"px-align:{asset}:{duration}", LOG_FLOW_EVERY_SEC):
                print(
                    f"{Y}[PX-ALIGN]{RS} {asset} {duration}m conflict rtds={rtds_now:.4f} "
                    f"cl={cl_now:.4f} open={open_price:.4f} src={px_src} -> dir={direction}"
                )
    elif cl_direction:
        direction = cl_direction   # Binance flat → trust Chainlink direction
    elif tf_up_votes > tf_dn_votes:
        direction = "Up"
    elif tf_dn_votes > tf_up_votes:
        direction = "Down"
    else:
        direction = "Up" if cl_now >= open_price else "Down"  # always bet — binary resolves either way

    is_up    = (direction == "Up")
    tf_votes = tf_up_votes if is_up else tf_dn_votes
    very_strong_mom = (tf_votes >= TF_VOTES_STRONG)

    is_early_continuation = (prev_win_dir == direction and pct_remaining > EARLY_CONT_PCT_MIN)

    # Entry timing (0-2 pts) — earlier = AMM hasn't repriced yet = better odds
    if   pct_remaining >= TIMING_SCORE_PCT_2: score += 2
    elif pct_remaining >= TIMING_SCORE_PCT_1: score += 1

    # Move size (0-3 pts) — price confirmation bonus; not a gate
    if   move_pct >= MOVE_SCORE_T3: score += 3
    elif move_pct >= MOVE_SCORE_T2: score += 2
    elif move_pct >= MOVE_SCORE_T1: score += 1
    # flat/tiny move → +0 pts (still tradeable if momentum/taker confirm)

    # Multi-TF momentum + Kalman (0-5 pts; signals: 5s/30s/180s EMA + Kalman velocity)
    if   tf_votes == TF_VOTES_MAX: score += TF_SCORE_MAX
    elif tf_votes == TF_VOTES_STRONG: score += TF_SCORE_STRONG
    elif tf_votes == TF_VOTES_MID: score += TF_SCORE_MID

    # Chainlink direction agreement (+1 agree / −3 disagree)
    # CL is the resolution oracle — disagreement is a major red flag
    cl_agree = True
    if cl_now > 0 and open_price > 0:
        if (is_up) != (cl_now >= open_price):   # >= matches resolution rule (tie → Up wins)
            cl_agree = False
    if cl_agree:  score += CL_AGREE_SCORE_BONUS
    else:         score -= CL_DISAGREE_SCORE_PEN

    # Soft source-divergence penalty (no hard skip): discourages entries on feed mismatch.
    if open_price > 0 and cl_now > 0 and rtds_now > 0:
        div = abs(cl_now - rtds_now) / open_price
        if div >= DIV_PEN_START:
            div_pen = min(DIV_PEN_MAX_SCORE, max(1, int(div / DIV_PEN_START)))
            score -= div_pen
            edge -= min(DIV_PEN_EDGE_CAP, div * DIV_PEN_EDGE_MULT)
            data_div_pen_applied = True
            if self._noisy_log_enabled(f"data-div:{asset}:{duration}", LOG_FLOW_EVERY_SEC):
                print(
                    f"{Y}[DATA-DIV]{RS} {asset} {duration}m cl={cl_now:.4f} rtds={rtds_now:.4f} "
                    f"open={open_price:.4f} div={div*100:.3f}% (-{div_pen} score)"
                )

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
    elif cl_age_s > CL_AGE_MAX_SKIP:
        return None
    elif cl_age_s > CL_AGE_WARN:
        onchain_adj -= CL_AGE_WARN_SCORE_PEN
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
    ws_strict_age_cap = self._ws_strict_age_cap_ms()
    _pm_book = await self._fetch_pm_book_safe(prefetch_token)
    ws_book_now = self._get_clob_ws_book(prefetch_token, max_age_ms=ws_strict_age_cap)
    ws_book_soft = self._get_clob_ws_book(prefetch_token, max_age_ms=WS_BOOK_SOFT_MAX_AGE_MS)
    ws_book_strict = ws_book_now
    if ws_book_now is None:
        # Try alternate side token before declaring WS missing.
        tok_u = str(m.get("token_up", "") or "")
        tok_d = str(m.get("token_down", "") or "")
        alt = tok_d if prefetch_token == tok_u else tok_u
        if alt:
            ws_book_now = self._get_clob_ws_book(alt, max_age_ms=ws_strict_age_cap)
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
            score -= WS_FALLBACK_SCORE_PEN
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
                score -= WS_SOFT_SCORE_PEN
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
    volume_ready = bool(cache_a.get("depth_bids")) and bool(cache_a.get("depth_asks")) and (len(cache_a.get("klines", [])) >= CACHE_MIN_KLINES)
    if REQUIRE_VOLUME_SIGNAL and not volume_ready:
        if self._noisy_log_enabled(f"skip-no-vol:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
            print(f"{Y}[SKIP] {asset} {duration}m missing live Binance depth/volume cache{RS}")
        self._skip_tick("volume_missing")
        return None

    # Jump detection: sudden move against our direction = hard abort
    if is_jump and jump_dir is not None and jump_dir != direction:
        return None
    if is_jump and jump_dir == direction:
        score += JUMP_CONFIRM_SCORE

    # Order book imbalance — depth-weighted 1/rank (more reliable than flat sum)
    ob_sig = dw_ob if is_up else -dw_ob    # positive = OB confirms direction
    if ob_sig < OB_HARD_BLOCK:
        return None    # extreme contra OB — hard block
    if   ob_sig > OB_SCORE_T3: score += 3
    elif ob_sig > OB_SCORE_T2: score += 2
    elif ob_sig > OB_SCORE_T1: score += 1
    else:               score -= 1
    imbalance_confirms = ob_sig > IMBALANCE_CONFIRM_MIN

    # Taker buy/sell flow + volume vs 30-min avg (−1 to +5 pts)
    if   (is_up and taker_ratio > TAKER_T3_UP) or (not is_up and taker_ratio < TAKER_T3_DN): score += 3
    elif (is_up and taker_ratio > TAKER_T2_UP) or (not is_up and taker_ratio < TAKER_T2_DN): score += 2
    elif abs(taker_ratio - 0.50) < TAKER_NEUTRAL: score += 1
    else:                                score -= 1
    if   vol_ratio > VOL_SCORE_T2: score += 2
    elif vol_ratio > VOL_SCORE_T1: score += 1

    # Perp futures basis: premium = leveraged longs crowding in = bullish (−1 to +2 pts)
    perp_confirms = (is_up and perp_basis > PERP_CONFIRM) or (not is_up and perp_basis < -PERP_CONFIRM)
    perp_strong   = (is_up and perp_basis > PERP_STRONG) or (not is_up and perp_basis < -PERP_STRONG)
    perp_contra   = (is_up and perp_basis < -PERP_CONFIRM) or (not is_up and perp_basis > PERP_CONFIRM)
    if   perp_strong:   score += 2
    elif perp_confirms: score += 1
    elif perp_contra:   score -= 1

    # Funding rate: extreme = crowded = contrarian (−1 to +1 pts)
    # High positive funding + Down bet = contrarian confirms shorts
    # Very high positive funding + Up bet = overcrowded longs = risky
    if   (not is_up and funding_rate >  FUNDING_POS_STRONG): score += 1
    elif (is_up     and funding_rate <  FUNDING_NEG_CONFIRM): score += 1
    elif (is_up     and funding_rate >  FUNDING_POS_EXTREME): score -= 1
    elif (not is_up and funding_rate <  FUNDING_NEG_STRONG): score -= 1

    # Liquidation signal: opposing-side liq confirms direction (0 to +2 pts)
    score += self._liq_signal(asset, direction)

    # Open Interest delta: growing OI confirms momentum (−1 to +1 pt)
    oi_data = self._oi.get(asset, {})
    if oi_data.get("prev", 0) > 0 and oi_data.get("cur", 0) > 0:
        oi_delta = (oi_data["cur"] - oi_data["prev"]) / oi_data["prev"]
        if   (is_up     and oi_delta >  OI_DELTA_UP):  score += 1
        elif (not is_up and oi_delta <  OI_DELTA_DN):  score += 1
        elif (is_up     and oi_delta <  OI_DELTA_DN):  score -= 1
        elif (not is_up and oi_delta >  OI_DELTA_UP):  score -= 1

    # L/S ratio: extreme crowding = contrarian (0 to −1 pt)
    ls_long = self._ls_ratio.get(asset, 1.0)
    if   (is_up     and ls_long > LS_LONG_EXT):  score -= 1   # too crowded long → risky Up
    elif (not is_up and ls_long < LS_SHORT_EXT): score -= 1   # too crowded short → risky Down

    # VWAP: momentum signal (−2 to +2 pts)
    # Price ABOVE window VWAP in our direction = momentum confirms = good
    # Price BELOW window VWAP in our direction = momentum weak = bad
    vwap_net = vwap_dev if is_up else -vwap_dev   # positive = price above VWAP in bet direction
    if   vwap_net >  VWAP_SCORE_T2: score += 2
    elif vwap_net >  VWAP_SCORE_T1: score += 1
    elif vwap_net < -VWAP_SCORE_T2: score -= 2
    elif vwap_net < -VWAP_SCORE_T1: score -= 1

    # Vol-normalized displacement signal (−2 to +2 pts)
    sigma_15m = self.vols.get(asset, 0.70) * (15 / (252 * 390)) ** 0.5
    open_price_disp = open_price
    if open_price_disp and sigma_15m > 0:
        net_disp = (current - open_price_disp) / open_price_disp * (1 if is_up else -1)
        if   net_disp > sigma_15m * DISP_SIGMA_STRONG: score += 2
        elif net_disp > sigma_15m * DISP_SIGMA_MID: score += 1
        elif net_disp < -sigma_15m * DISP_SIGMA_STRONG: score -= 2
        elif net_disp < -sigma_15m * DISP_SIGMA_MID: score -= 1

    # Cross-asset confirmation: bonus for agreement, penalty for disagreement
    cross_count = self._cross_asset_direction(asset, direction)
    cross_contra = self._cross_asset_direction(asset, "Down" if direction == "Up" else "Up")
    if   cross_count == 3: score += 2   # all other assets confirm → strong macro signal
    elif cross_count >= 2: score += 1   # majority confirm
    elif cross_contra == 3: score -= 2  # all 3 other assets going opposite → macro headwind
    elif cross_contra >= 2: score -= 1  # majority going opposite → mild headwind

    # Window-open OFI surge: fresh round + extreme aggTrade burst → strong directional signal
    # Enter at price ~0.50 (2x payout) with 62-68% win probability when all assets moving together
    ofi_surge_active = False
    if (
        OFI_SURGE_ENABLED
        and pct_remaining >= OFI_SURGE_PCT_MIN
        and duration >= 15
    ):
        surge_ofi = self._binance_agg_ofi(asset, window_sec=OFI_SURGE_WINDOW_SEC)
        surge_confirms = (is_up and surge_ofi >= OFI_SURGE_THRESH_UP) or (not is_up and surge_ofi <= OFI_SURGE_THRESH_DN)
        if surge_confirms:
            score += OFI_SURGE_SCORE_BONUS
            ofi_surge_active = True

    # BTC lead signal for non-BTC assets — BTC lagged move predicts altcoins (0–2 pts)
    if asset != "BTC":
        if   (is_up and btc_lead_p > BTC_LEAD_T2_UP) or (not is_up and btc_lead_p < BTC_LEAD_T2_DN): score += 2
        elif (is_up and btc_lead_p > BTC_LEAD_T1_UP) or (not is_up and btc_lead_p < BTC_LEAD_T1_DN): score += 1
        elif (is_up and btc_lead_p < BTC_LEAD_NEG_UP) or (not is_up and btc_lead_p > BTC_LEAD_NEG_DN): score -= 1

    # Previous-window continuation (data-driven, low weight).
    # Never use a fixed global continuation prior; require realtime corroboration.
    if prev_win_dir is not None:
        if prev_win_dir == direction:
            conf_hits = 0
            conf_hits += 1 if tf_votes >= 3 else 0
            conf_hits += 1 if ((is_up and taker_ratio > CONT_HIT_TAKER_UP) or ((not is_up) and taker_ratio < CONT_HIT_TAKER_DN)) else 0
            conf_hits += 1 if ((is_up and ob_sig > CONT_HIT_OB) or ((not is_up) and ob_sig < -CONT_HIT_OB)) else 0
            if conf_hits >= 2:
                score += 2 if pct_remaining > CONT_BONUS_EARLY_PCT else 1
        else:
            score -= 1

    # Autocorr + Variance Ratio regime: trending boosts momentum confidence
    if vr_ratio > REGIME_VR_TREND and autocorr > REGIME_AC_TREND:
        score += 1       # trending regime — momentum more reliable
        regime_mult = REGIME_MULT_TREND
    elif vr_ratio < REGIME_VR_MR and autocorr < REGIME_AC_MR:
        score -= 1       # mean-reverting — momentum less reliable
        regime_mult = REGIME_MULT_MR
    else:
        regime_mult = 1.0

    # RSI + Williams %R momentum oscillators (0 to +2 pts, purely additive)
    # Both confirm strong momentum = +2; one only = +1; neutral or contra = 0
    _rsi_val = self._rsi(asset)
    _wr_val  = self._williams_r(asset)
    if   (is_up  and _rsi_val >= RSI_OB       and _wr_val >= WR_OB):        score += 2
    elif (not is_up and _rsi_val <= RSI_OS     and _wr_val <= WR_OS):        score += 2
    elif (is_up  and (_rsi_val >= RSI_OB - 5  or  _wr_val >= WR_OB + 5)):   score += 1
    elif (not is_up and (_rsi_val <= RSI_OS + 5 or _wr_val <= WR_OS - 5)):  score += 1

    # Log-likelihood true_prob — Bayesian combination of independent signals
    sigma_15m = self.vols.get(asset, 0.70) * (15 / (252 * 390)) ** 0.5
    llr = 0.0
    # 1. Price displacement z-score
    if open_price > 0 and sigma_15m > 0:
        llr += (current - open_price) / open_price / sigma_15m * LLR_PRICE_MULT
    # 2. Short vs long EMA cross
    ema5  = self.emas.get(asset, {}).get(5, current)
    ema60 = self.emas.get(asset, {}).get(60, current)
    if ema60 > 0:
        llr += (ema5 / ema60 - 1.0) * LLR_EMA_MULT
    # 3. Kalman velocity
    k = self.kalman.get(asset, {})
    if k.get("rdy"):
        per_sec_vol = max(self.vols.get(asset, 0.7) / math.sqrt(252 * 24 * 3600), 1e-8)
        llr += k["vel"] / per_sec_vol * LLR_KALMAN_MULT
    # 4. Depth-weighted OB imbalance
    llr += dw_ob * LLR_OB_MULT
    # 5. Taker buy/sell flow
    llr += (taker_ratio - 0.5) * LLR_TAKER_MULT
    # 6. Perp basis
    if abs(perp_basis) > 1e-7:
        llr += math.copysign(min(abs(perp_basis) * LLR_PERP_MULT, LLR_PERP_CAP), perp_basis)
    # 7. Chainlink oracle agreement
    if cl_agree:  llr += LLR_CL_AGREE
    else:         llr -= LLR_CL_DISAGREE
    # 8. BTC lead for altcoins
    if asset != "BTC":
        llr += (btc_lead_p - 0.5) * LLR_BTC_LEAD_MULT
    # 9. Regime scale
    llr *= regime_mult
    # Sigmoid → prob_up
    p_up_ll   = 1.0 / (1.0 + math.exp(-max(-LLR_CLAMP, min(LLR_CLAMP, llr))))
    # Structural tie bias: resolution uses >= so exact CL tie resolves as Up
    p_up_ll   = min(1.0, p_up_ll + TIE_BIAS_UP)
    bias_up   = self._direction_bias(asset, "Up", duration)
    bias_down = self._direction_bias(asset, "Down", duration)
    # Net bias: bias_up raises P(Up), bias_down lowers P(Up) — normalize so sum=1
    prob_up   = max(0.05, min(0.95, p_up_ll + bias_up - bias_down))
    prob_down = 1.0 - prob_up

    # Early continuation prior boost (bounded, realtime-confirmed).
    if prev_win_dir == direction and pct_remaining > CONT_BONUS_EARLY_PCT:
        prior_boost = 0.0
        if tf_votes >= 3:
            prior_boost += PRIOR_BOOST_TF
        if ((is_up and taker_ratio > PRIOR_BOOST_TAKER_UP) or ((not is_up) and taker_ratio < PRIOR_BOOST_TAKER_DN)):
            prior_boost += PRIOR_BOOST_TAKER
        if ((is_up and ob_sig > PRIOR_BOOST_OB_MIN) or ((not is_up) and ob_sig < -PRIOR_BOOST_OB_MIN)):
            prior_boost += PRIOR_BOOST_OB
        if cl_agree:
            prior_boost += PRIOR_BOOST_CL
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
    if abs(up_adj) > DEFAULT_EPS or abs(dn_adj) > DEFAULT_EPS:
        pu = max(PROB_REBALANCE_MIN, min(PROB_REBALANCE_MAX, prob_up + up_adj))
        pd = max(PROB_REBALANCE_MIN, min(PROB_REBALANCE_MAX, prob_down + dn_adj))
        z = pu + pd
        if z > 0:
            prob_up = max(PROB_CLAMP_MIN, min(PROB_CLAMP_MAX, pu / z))
            prob_down = 1.0 - prob_up

    edge_up   = prob_up   - up_price
    edge_down = prob_down - (1 - up_price)
    min_edge   = self._adaptive_min_edge()
    pre_filter = max(PREFILTER_MIN, min_edge * PREFILTER_EDGE_MULT)

    # Lock side to price direction when move is clear.
    # AMM edge comparison only used when price is flat (no clear directional signal)
    if move_pct >= FORCE_DIR_MOVE_MIN:
        forced_side = "Up" if current > open_price else "Down"
        forced_edge = edge_up if forced_side == "Up" else edge_down
        forced_prob = prob_up if forced_side == "Up" else prob_down
        if forced_edge >= 0:
            side, edge, true_prob = forced_side, forced_edge, forced_prob
        elif forced_edge < FORCE_DIR_EDGE_HARD_BLOCK:
            return None   # AMM has massively overpriced this direction — no edge at all
        else:
            side, edge, true_prob = forced_side, max(FORCE_DIR_EDGE_FLOOR, forced_edge), forced_prob
    else:
        # Flat price: side MUST match direction (CL/momentum consensus).
        # AMM edge only used to reject extreme mispricing (< -15%).
        # Momentum/CL direction wins over weak market-pricing noise.
        dir_edge = edge_up if direction == "Up" else edge_down
        dir_prob = prob_up if direction == "Up" else prob_down
        if dir_edge < FORCE_DIR_EDGE_HARD_BLOCK:
            return None   # AMM massively overpriced our direction — no edge
        side, edge, true_prob = direction, max(FORCE_DIR_EDGE_FLOOR, dir_edge), dir_prob

    # Max-win profile (source fix):
    # choose side by EV utility (not raw probability), with soft event-context prior.
    if MAX_WIN_MODE:
        payout_up = 1.0 / max(up_price, 1e-9)
        payout_dn = 1.0 / max(1.0 - up_price, 1e-9)
        ev_up = (prob_up * payout_up) - 1.0 - max(0.001, up_price * (1.0 - up_price) * 0.0624)
        ev_dn = (prob_down * payout_dn) - 1.0 - max(0.001, (1.0 - up_price) * up_price * 0.0624)
        util_up = ev_up + edge_up * UTIL_EDGE_MULT
        util_dn = ev_dn + edge_down * UTIL_EDGE_MULT
        # Soft contextual prior from current event state (non-blocking).
        if duration >= CORE_DURATION_MIN and open_price > 0 and current > 0 and mins_left <= EVENT_ALIGN_MIN_LEFT_15M:
            mv = (current - open_price) / max(open_price, 1e-9)
            if abs(mv) >= EVENT_ALIGN_MIN_MOVE_PCT:
                # bounded utility shift: follow clear event drift unless EV is decisively opposite
                u_shift = min(SETUP_Q_HARD_RELAX_MAX, max(EVENT_ALIGN_WITH_EDGE_BONUS, abs(mv) * LLR_PERP_MULT / 125.0))
                if mv >= 0:
                    util_up += u_shift
                    util_dn -= u_shift
                else:
                    util_dn += u_shift
                    util_up -= u_shift
        if util_up >= util_dn:
            side, edge, true_prob = "Up", edge_up, prob_up
        else:
            side, edge, true_prob = "Down", edge_down, prob_down

    # Keep signal components aligned to the effective side.
    asset_entry_size_mult = 1.0
    side_up = side == "Up"
    event_align_size_mult = 1.0
    recent_side = up_recent if side_up else dn_recent
    recent_side_n = int(recent_side.get("n", 0) or 0)
    recent_side_exp = float(recent_side.get("exp", 0.0) or 0.0)
    recent_side_wr_lb = float(recent_side.get("wr_lb", 0.5) or 0.5)
    recent_side_score_adj = int(recent_side.get("score_adj", 0) or 0)
    recent_side_edge_adj = float(recent_side.get("edge_adj", 0.0) or 0.0)
    recent_side_prob_adj = float(recent_side.get("prob_adj", 0.0) or 0.0)
    if recent_side_score_adj != 0 or abs(recent_side_edge_adj) > DEFAULT_EPS:
        score += recent_side_score_adj
        edge += recent_side_edge_adj
        if self._noisy_log_enabled(f"recent-side:{asset}:{side}", LOG_FLOW_EVERY_SEC):
            print(
                f"{B}[RECENT-SIDE]{RS} {asset} {duration}m {side} "
                f"adj(score={recent_side_score_adj:+d},edge={recent_side_edge_adj:+.3f},prob={recent_side_prob_adj:+.3f}) "
                f"n={recent_side_n} exp={recent_side_exp:+.2f} wr_lb={recent_side_wr_lb:.2f}"
            )
    # Asset+entry-band profile from settled on-chain outcomes.
    aq_entry = up_price if side_up else (1.0 - up_price)
    aq = self._asset_entry_profile(asset, duration, side, aq_entry, open_src, cl_age_s)
    aq_score = int(aq.get("score_adj", 0) or 0)
    aq_edge = float(aq.get("edge_adj", 0.0) or 0.0)
    aq_prob = float(aq.get("prob_adj", 0.0) or 0.0)
    asset_entry_size_mult = float(aq.get("size_mult", 1.0) or 1.0)
    if aq_score != 0 or abs(aq_edge) > DEFAULT_EPS or abs(aq_prob) > DEFAULT_EPS:
        score += aq_score
        edge += aq_edge
        true_prob = max(0.05, min(0.95, true_prob + aq_prob))
        if self._noisy_log_enabled(f"onchain-q:{asset}:{duration}:{side}", LOG_FLOW_EVERY_SEC):
            print(
                f"{B}[ONCHAIN-Q]{RS} {asset} {duration}m {side} "
                f"band={aq.get('band','?')} n={int(aq.get('n',0) or 0)} "
                f"wr_lb={float(aq.get('wr_lb',0.5) or 0.5):.2f} pf={float(aq.get('pf',1.0) or 1.0):.2f} "
                f"exp={float(aq.get('exp',0.0) or 0.0):+.2f} "
                f"adj(score={aq_score:+d},edge={aq_edge:+.3f},prob={aq_prob:+.3f},size_x={asset_entry_size_mult:.2f})"
            )
    tf_votes = tf_up_votes if side_up else tf_dn_votes
    ob_sig = dw_ob if side_up else -dw_ob
    cl_agree = True
    if cl_now > 0 and open_price > 0:
        cl_agree = (cl_now > open_price) == side_up

    # Soft event-alignment penalty (non-blocking):
    # if signal is contrarian while move is already clear in 15m mid/late window,
    # reduce score/edge and shrink size rather than hard-skipping.
    if (
        EVENT_ALIGN_GUARD_ENABLED
        and duration >= 15
        and mins_left <= EVENT_ALIGN_MIN_LEFT_15M
        and open_price > 0
        and current > 0
    ):
        move_now = (current - open_price) / max(open_price, 1e-9)
        if abs(move_now) >= EVENT_ALIGN_MIN_MOVE_PCT:
            event_dir = "Up" if move_now >= 0 else "Down"
            if side != event_dir:
                strong_exception = (
                    score >= EVENT_ALIGN_ALLOW_SCORE
                    and true_prob >= EVENT_ALIGN_ALLOW_TRUE_PROB
                )
                if not strong_exception:
                    pen_scale = max(0.6, min(1.4, abs(move_now) / max(EVENT_ALIGN_MIN_MOVE_PCT, 1e-9)))
                    sc_pen = max(1, int(round(EVENT_ALIGN_CONTRA_SCORE_PEN * pen_scale)))
                    ed_pen = max(0.004, EVENT_ALIGN_CONTRA_EDGE_PEN * pen_scale)
                    score -= sc_pen
                    edge -= ed_pen
                    true_prob = 0.5 + (true_prob - 0.5) * 0.88
                    event_align_size_mult = max(0.20, min(1.0, EVENT_ALIGN_CONTRA_SIZE_MULT))
                    if self._noisy_log_enabled(f"event-align-pen:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
                        print(
                            f"{Y}[EVENT-ALIGN]{RS} {asset} {duration}m contra {side}!={event_dir} "
                            f"move={move_now*100:+.3f}% sc_pen={sc_pen} ed_pen={ed_pen:+.3f} "
                            f"size_x={event_align_size_mult:.2f}"
                        )
            else:
                score += max(0, EVENT_ALIGN_WITH_SCORE_BONUS)
                edge += max(0.0, EVENT_ALIGN_WITH_EDGE_BONUS)
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


    # Late-window direction lock: avoid betting against the beat direction
    # when the move is already clear near expiry.
    if (
        LATE_DIR_LOCK_ENABLED
        and open_price > 0
        and current > 0
        and duration in (5, 15)
    ):
        lock_mins, lock_move_min = self._late_lock_thresholds(duration)
        move_abs = abs((current - open_price) / max(open_price, 1e-9))
        if mins_left <= lock_mins and move_abs >= lock_move_min:
            beat_dir = "Up" if current >= open_price else "Down"
            if side != beat_dir:
                prev_side = side
                side = beat_dir
                # Soft alignment by default: keep trading flow but reduce confidence.
                # This avoids no-trade stalls while preserving direction discipline near expiry.
                score -= 1
                edge -= 0.004
                true_prob = max(0.05, min(0.95, 0.5 + (true_prob - 0.5) * 0.92))
                if self._noisy_log_enabled(f"late-lock-align:{asset}:{prev_side}", LOG_FLOW_EVERY_SEC):
                    print(
                        f"{Y}[LATE-ALIGN]{RS} {asset} {duration}m {prev_side}->{beat_dir} "
                        f"mins_left={mins_left:.1f} move={move_abs*100:.2f}%"
                    )

    # Late-window locked-direction prob boost:
    # When in the last LATE_PAYOUT_RELAX_PCT_LEFT of the window AND price has already moved
    # LATE_PAYOUT_RELAX_MIN_MOVE in our direction AND taker flow confirms → boost true_prob.
    # This captures high-certainty entries where win rate is 72-78% but payout only 1.65-1.80x.
    late_locked = (
        LATE_PAYOUT_RELAX_ENABLED
        and pct_remaining <= LATE_PAYOUT_RELAX_PCT_LEFT
        and duration >= 15
        and open_price > 0
        and move_pct >= LATE_PAYOUT_RELAX_MIN_MOVE
        and ((side_up and current >= open_price) or (not side_up and current < open_price))
        and ((side_up and taker_ratio >= 0.52) or (not side_up and taker_ratio <= 0.48))
    )
    if late_locked:
        true_prob = max(0.05, min(0.95, true_prob + LATE_PAYOUT_PROB_BOOST))

    # OFI surge prob boost (already added score bonus above; now also boost probability)
    if ofi_surge_active:
        true_prob = max(0.05, min(0.95, true_prob + OFI_SURGE_PROB_BOOST))

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
            leader_entry_cap = min(
                LEADER_ENTRY_CAP_HARD_MAX,
                MAX_ENTRY_PRICE + MAX_ENTRY_TOL + LEADER_ENTRY_CAP_EXTRA,
            )
            leader_quality_ok = True
            if LEADER_FORCE_QUALITY_GUARD_ENABLED:
                try:
                    q = self._asset_entry_profile(
                        asset, duration, leader_side, leader_entry, open_src, cl_age_s
                    )
                    q_n = int(q.get("n", 0) or 0)
                    q_wr = float(q.get("wr_lb", 0.5) or 0.5)
                    q_pf = float(q.get("pf", 1.0) or 1.0)
                    q_exp = float(q.get("exp", 0.0) or 0.0)
                    rs = self._recent_side_profile(asset, duration, leader_side)
                    rs_n = int(rs.get("n", 0) or 0)
                    rs_wr = float(rs.get("wr_lb", 0.5) or 0.5)
                    rs_exp = float(rs.get("exp", 0.0) or 0.0)
                    bad_band = (
                        q_n >= LEADER_FORCE_MIN_N
                        and (q_wr < LEADER_FORCE_MIN_WR_LB or q_pf < LEADER_FORCE_MIN_PF or q_exp < LEADER_FORCE_MIN_EXP)
                    )
                    bad_recent = (
                        rs_n >= LEADER_FORCE_MIN_N
                        and rs_wr < LEADER_FORCE_MIN_WR_LB
                        and rs_exp < LEADER_FORCE_MIN_EXP
                    )
                    if bad_band or bad_recent:
                        leader_quality_ok = False
                        if self._noisy_log_enabled(f"leader-block:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
                            print(
                                f"{Y}[LEADER-BLOCK]{RS} {asset} {duration}m side={leader_side} "
                                f"band(n={q_n} wr_lb={q_wr:.2f} pf={q_pf:.2f} exp={q_exp:+.2f}) "
                                f"recent(n={rs_n} wr_lb={rs_wr:.2f} exp={rs_exp:+.2f})"
                            )
                except Exception:
                    pass
            if leader_entry <= leader_entry_cap and leader_quality_ok:
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
        edge += copy_net * COPY_NET_EDGE_MULT
        # Leader behavior style signal: are winners buying cheap or paying high cents?
        avg_c = float(flow.get("avg_entry_c", 0.0) or 0.0)
        low_share = float(flow.get("low_c_share", 0.0) or 0.0)
        high_share = float(flow.get("high_c_share", 0.0) or 0.0)
        multibet_ratio = float(flow.get("multibet_ratio", 0.0) or 0.0)
        if avg_c > 0 and high_share >= LEADER_STYLE_HIGH_SHARE_MIN and multibet_ratio >= LEADER_STYLE_MULTIBET_MIN:
            score += LEADER_STYLE_SCORE_BONUS
            edge += LEADER_STYLE_EDGE_BONUS
        elif avg_c > 0 and low_share >= LEADER_STYLE_LOW_SHARE_MIN and (up_price if side == "Up" else (1 - up_price)) > LEADER_STYLE_EXPENSIVE_ENTRY_MIN:
            # Winners buying low-cents but market now expensive: fade confidence.
            score -= LEADER_STYLE_SCORE_PENALTY
            edge -= LEADER_STYLE_EDGE_PENALTY
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
                    if float(pub.get("avg_c", 0.0) or 0.0) >= PM_PUBLIC_OCROWD_AVG_C_MIN and abs(float(pub.get("dom", 0.0) or 0.0)) >= PM_PUBLIC_PATTERN_DOM_MED:
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
        leader_size_scale = min(leader_size_scale, LEADER_NOFLOW_SIZE_SCALE)

    # Leader/prebid can change side: refresh side-aligned metrics.
    side_up = side == "Up"
    tf_votes = tf_up_votes if side_up else tf_dn_votes
    ob_sig = dw_ob if side_up else -dw_ob
    cl_agree = True
    if cl_now > 0 and open_price > 0:
        cl_agree = (cl_now > open_price) == side_up
    imbalance_confirms = ob_sig > IMBALANCE_CONFIRM_MIN

    # Source-quality + signal-conviction analysis (real data only).
    # This is the primary anti-random layer: trade only when data is both fresh and coherent.
    cl_fresh = (cl_age_s is not None) and (cl_age_s <= ANALYSIS_CL_FRESH_MAX_AGE_SEC)
    quote_fresh = quote_age_ms <= min(MAX_QUOTE_STALENESS_MS, ANALYSIS_QUOTE_FRESH_MAX_MS)
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
        ((ANALYSIS_QUALITY_WS_WEIGHT if ws_fresh else (ANALYSIS_QUALITY_REST_WEIGHT if rest_fresh else 0.0)))
        + (ANALYSIS_QUALITY_LEADER_WEIGHT if leader_fresh else 0.0)
        + (ANALYSIS_QUALITY_CL_WEIGHT if cl_fresh else 0.0)
        + (ANALYSIS_QUALITY_QUOTE_WEIGHT if quote_fresh else 0.0)
        + (ANALYSIS_QUALITY_VOL_WEIGHT if vol_fresh else 0.0)
    )

    dir_sign = 1.0 if side == "Up" else -1.0
    ob_c = max(0.0, min(1.0, (ob_sig + ANALYSIS_OB_OFFSET) / ANALYSIS_OB_SCALE))
    tk_signed = (taker_ratio - 0.5) * dir_sign
    tk_c = max(0.0, min(1.0, (tk_signed + ANALYSIS_TAKER_OFFSET) / ANALYSIS_TAKER_SCALE))
    tf_c = max(0.0, min(1.0, (float(tf_votes) - ANALYSIS_TF_BASE) / ANALYSIS_TF_SCALE))
    basis_c = max(0.0, min(1.0, ((dir_sign * perp_basis) + ANALYSIS_BASIS_OFFSET) / ANALYSIS_BASIS_SCALE))
    vwap_c = max(0.0, min(1.0, ((dir_sign * vwap_dev) + ANALYSIS_VWAP_OFFSET) / ANALYSIS_VWAP_SCALE))
    cl_c = 1.0 if cl_agree else 0.0
    leader_c = ANALYSIS_LEADER_BASE
    if leader_fresh:
        leader_c = max(0.0, min(1.0, ((copy_net) + ANALYSIS_LEADER_OFFSET) / ANALYSIS_LEADER_SCALE))
    # Binary-option time-lock signal: N(d2) from Black-Scholes, probability
    # that current price ends on our side at window close. Always computable
    # from (current vs open_price, time remaining, realized vol). Strong
    # near end of window when price has clearly moved; neutral (0.5) in flat markets.
    _sigma_ann = self.vols.get(asset, 0.70)
    _T_years   = max(mins_left / 525600.0, 1e-9)   # 365*24*60 = 525600 min/year
    if open_price and open_price > 0 and current > 0 and _sigma_ann > 0:
        _d2  = (math.log(current / open_price) - 0.5 * _sigma_ann ** 2 * _T_years) / (_sigma_ann * math.sqrt(_T_years))
        bin_c = float(norm.cdf(_d2 if side_up else -_d2))
    else:
        bin_c = 0.5
    analysis_conviction = (
        ANALYSIS_CONV_W_OB     * ob_c
        + ANALYSIS_CONV_W_TK     * tk_c
        + ANALYSIS_CONV_W_TF     * tf_c
        + ANALYSIS_CONV_W_BASIS  * basis_c
        + ANALYSIS_CONV_W_VWAP   * vwap_c
        + ANALYSIS_CONV_W_CL     * cl_c
        + ANALYSIS_CONV_W_LEADER * leader_c
        + ANALYSIS_CONV_W_BINARY * bin_c
    )


    # Recalibrate posterior using measured analysis quality.
    # Higher quality allows stronger posterior; weaker quality shrinks toward 50%.
    quality_scale = ANALYSIS_PROB_SCALE_MIN + (
        (ANALYSIS_PROB_SCALE_MAX - ANALYSIS_PROB_SCALE_MIN) * max(0.0, min(1.0, analysis_quality))
    )
    prob_up = 0.5 + (prob_up - 0.5) * quality_scale
    prob_up = max(PROB_CLAMP_MIN, min(PROB_CLAMP_MAX, prob_up))
    prob_down = 1.0 - prob_up
    # Apply recent side prior again after quality rescale (keeps final posterior aligned to live outcomes).
    up_recent = self._recent_side_profile(asset, duration, "Up")
    dn_recent = self._recent_side_profile(asset, duration, "Down")
    up_adj = float(up_recent.get("prob_adj", 0.0) or 0.0)
    dn_adj = float(dn_recent.get("prob_adj", 0.0) or 0.0)
    if abs(up_adj) > DEFAULT_EPS or abs(dn_adj) > DEFAULT_EPS:
        pu = max(PROB_REBALANCE_MIN, min(PROB_REBALANCE_MAX, prob_up + up_adj))
        pd = max(PROB_REBALANCE_MIN, min(PROB_REBALANCE_MAX, prob_down + dn_adj))
        z = pu + pd
        if z > 0:
            prob_up = max(PROB_CLAMP_MIN, min(PROB_CLAMP_MAX, pu / z))
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
            score += PREBID_SCORE_BONUS
            edge += max(0.0, conf - 0.5) * PREBID_EDGE_MULT

    # Pre-bid may override side: refresh aligned metrics for downstream gates/sizing.
    side_up = side == "Up"
    tf_votes = tf_up_votes if side_up else tf_dn_votes
    ob_sig = dw_ob if side_up else -dw_ob
    cl_agree = True
    if cl_now > 0 and open_price > 0:
        cl_agree = (cl_now > open_price) == side_up
    imbalance_confirms = ob_sig > IMBALANCE_CONFIRM_MIN


    # Contrarian tail: buy the cheap/trailing side when market overreacts early in window.
    # Mean-reversion edge: cheap side at 25-40¢ with 7+ min left → EV strongly positive.
    contrarian_tail_active = False
    if CONTRARIAN_TAIL_ENABLED:
        cheap_side = "Up" if up_price <= (1.0 - up_price) else "Down"
        cheap_entry_now = min(up_price, 1.0 - up_price)
        if (
            duration >= 15
            and
            cheap_entry_now <= CONTRARIAN_TAIL_MAX_ENTRY
            and mins_left >= CONTRARIAN_TAIL_MIN_MINS_LEFT
            and move_pct >= CONTRARIAN_TAIL_MIN_MOVE_PCT
            and cheap_side != direction   # contrarian to the current move
        ):
            side = cheap_side
            side_up = (side == "Up")
            contrarian_tail_active = True
            if self._noisy_log_enabled(f"tail-contra:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
                print(
                    f"{B}[TAIL]{RS} {asset} {duration}m contrarian {side} "
                    f"entry={cheap_entry_now:.2f} move={move_pct*100:.2f}% mins_left={mins_left:.1f}"
                )

    # Bet the signal direction — EV_net and execution quality drive growth.
    entry = up_price if side == "Up" else (1 - up_price)
    # Keep edge/prob aligned with final side after pre-bid/copyflow adjustments.
    base_prob = prob_up if side == "Up" else prob_down
    base_edge = edge_up if side == "Up" else edge_down
    # Always take the best estimate: preserves aq/prior adjustments over plain base_prob
    true_prob = max(true_prob, base_prob)
    edge      = max(edge, base_edge)
    pred_variant = ""
    pred_variant_probs = {}
    if self._pred_agent is not None:
        try:
            _entry_hint = up_price if side == "Up" else (1.0 - up_price)
            _buck_key = self._bucket_key(duration, score, _entry_hint)
            pred_ctx = {
                "asset": asset,
                "duration": duration,
                "side": side,
                "current": current,
                "open_price": open_price,
                "mins_left": mins_left,
                "base_prob": true_prob,
                "base_edge": edge,
                "score": score,
                "bucket_key": _buck_key,
                "quote_age_ms": quote_age_ms,
                "analysis_quality": analysis_quality,
            }
            pred = self._pred_agent.predict(
                pred_ctx,
                list(self._resolved_samples),
                self._bucket_stats.rows,
            )
            p_new = float(pred.get("prob", true_prob) or true_prob)
            e_adj = float(pred.get("edge_adj", 0.0) or 0.0)
            s_adj = int(pred.get("score_adj", 0) or 0)
            true_prob = max(PROB_CLAMP_MIN, min(PROB_CLAMP_MAX, p_new))
            edge = edge + e_adj
            score = score + s_adj
            pred_variant = str(pred.get("variant", "") or "")
            pred_variant_probs = dict(pred.get("variant_probs", {}) or {})
            if self._noisy_log_enabled(f"pred-agent:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
                print(
                    f"{B}[PRED-AGENT]{RS} {asset} {duration}m {side} "
                    f"p={true_prob:.3f} ({pred.get('prob_adj',0.0):+.3f}) "
                    f"edge_adj={e_adj:+.3f} score_adj={s_adj:+d} variant={pred_variant or 'na'} "
                    f"samples={int(pred.get('samples_side',0) or 0)}"
                )
        except Exception:
            pass
    if duration <= 5 and px_align_conflict and data_div_pen_applied:
        if self._noisy_log_enabled(f"skip-5m-pxalign-div:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
            print(
                f"{Y}[SKIP]{RS} {asset} 5m hard conflict px-align+data-div "
                f"(src={px_src} cl_age={(-1.0 if cl_age_s is None else cl_age_s):.1f}s)"
            )
        self._skip_tick("5m_pxalign_datadiv_conflict")
        return None

    if MAX_WIN_MODE:
        if WINMODE_REQUIRE_CL_AGREE and not cl_agree:
            return None
        min_prob_req = WINMODE_MIN_TRUE_PROB_5M if duration <= 5 else WINMODE_MIN_TRUE_PROB_15M
        if true_prob < min_prob_req:
            return None
        if edge < WINMODE_MIN_EDGE:
            return None

    # ── Minimum true_prob gate (2/3-rule: need ≥60% confidence) ─────────
    min_tp = MIN_TRUE_PROB_GATE_5M if duration <= 5 else MIN_TRUE_PROB_GATE_15M
    _payout_mult_local = 1.0 / max(entry, 1e-9)
    _highpayout_bypass = (   # 10x+ entries get a lower prob floor — still profitable at 45% win rate
        _payout_mult_local >= HIGHPAYOUT_MIN_PAYOUT
        and score >= HIGHPAYOUT_MIN_SCORE
        and edge >= HIGHPAYOUT_MIN_EDGE
        and true_prob >= HIGHPAYOUT_MIN_PROB
    )
    if true_prob < min_tp and not _highpayout_bypass:
        self._skip_tick("prob_below_gate")
        return None

    # ── Score gate (duration-aware) ─────────────────────────────────────
    min_score_local = MIN_SCORE_GATE
    if duration <= 5:
        min_score_local = max(min_score_local, MIN_SCORE_GATE_5M)
    else:
        min_score_local = max(min_score_local, MIN_SCORE_GATE_15M)
    # Rolling 3-trade gate: if last 3 trades had <2 wins, require higher score
    _r3 = list(self.recent_trades)[-3:]
    if len(_r3) >= 3 and sum(_r3) < 2:
        min_score_local += ROLLING3_WIN_SCORE_PEN
    if arm_active:
        min_score_local = max(0, min_score_local - 2)
    # Cross-asset 3/4 consensus override: all other assets confirm same direction.
    # Macro move confirmed by all assets → relax min_score (but never below 4).
    if (
        CROSS_CONSENSUS_ENABLED
        and cross_count >= CROSS_CONSENSUS_MIN_COUNT
        and duration >= 15
    ):
        min_score_local = max(4, min_score_local - CROSS_CONSENSUS_SCORE_RELAX)
    if duration <= 5 and FIVE_M_DYNAMIC_SCORE_ENABLED:
        snap5 = self._five_m_quality_snapshot(max(FIVE_M_GUARD_WINDOW, FIVE_M_DYNAMIC_SCORE_MIN_OUTCOMES))
        n5 = int(snap5.get("n", 0) or 0)
        wr5 = float(snap5.get("wr", 0.0) or 0.0)
        pf5 = float(snap5.get("pf", 1.0) or 1.0)
        if n5 >= max(1, FIVE_M_DYNAMIC_SCORE_MIN_OUTCOMES):
            add_gate = 0
            if (wr5 < FIVE_M_DYNAMIC_SCORE_BAD_WR) or (pf5 < FIVE_M_DYNAMIC_SCORE_BAD_PF):
                add_gate = max(add_gate, FIVE_M_DYNAMIC_SCORE_ADD_BAD)
            if (wr5 < FIVE_M_DYNAMIC_SCORE_WORSE_WR) or (pf5 < FIVE_M_DYNAMIC_SCORE_WORSE_PF):
                add_gate = max(add_gate, FIVE_M_DYNAMIC_SCORE_ADD_WORSE)
            if add_gate > 0:
                min_score_local += add_gate
                if self._noisy_log_enabled(f"5m-adapt:{asset}", LOG_FLOW_EVERY_SEC):
                    print(
                        f"{B}[5M-ADAPT]{RS} gate+{add_gate} => min_score={min_score_local} "
                        f"(n={n5} wr={wr5*100:.1f}% pf={pf5:.2f})"
                    )
    if score < min_score_local:
        return None
    # Per-asset blocking for 15m
    if duration >= 15:
        asset = m.get("asset", "")
        if (BLOCK_ASSET_SOL_15M and asset == "SOL") or (BLOCK_ASSET_XRP_15M and asset == "XRP"):
            if ASSET_BLOCK_SOFT_MODE:
                score -= max(0, ASSET_BLOCK_SOFT_SCORE_PEN)
                edge -= max(0.0, ASSET_BLOCK_SOFT_EDGE_PEN)
                if self._noisy_log_enabled(f"asset-softgate:{asset}:{side}", LOG_FLOW_EVERY_SEC):
                    print(
                        f"{Y}[ASSET-GATE]{RS} {asset} {duration}m soft "
                        f"(score-={ASSET_BLOCK_SOFT_SCORE_PEN} edge-={ASSET_BLOCK_SOFT_EDGE_PEN:.3f})"
                    )
                if score < max(3, min_score_local - 1):
                    self._skip_tick("asset_soft_blocked_low_score")
                    return None
            else:
                self._skip_tick("asset_blocked_sol" if asset == "SOL" else "asset_blocked_xrp")
                return None
    # Per-tier blocking via env vars (default all off)
    if duration >= 15:
        score_tier = "s12+" if score >= 12 else ("s9-11" if score >= 9 else "s0-8")
        tier_blocked = (
            (score_tier == "s0-8" and BLOCK_SCORE_S0_8_15M)
            or (score_tier == "s9-11" and BLOCK_SCORE_S9_11_15M)
            or (score_tier == "s12+" and BLOCK_SCORE_S12P_15M)
        )
        if tier_blocked:
            if SCORE_BLOCK_SOFT_MODE:
                score -= 2
                edge -= max(0.0, SCORE_BLOCK_SOFT_EDGE_PEN)
                if score < max(3, min_score_local - 1):
                    self._skip_tick(f"score_tier_soft_blocked_{score_tier}")
                    return None
            else:
                self._skip_tick(
                    "score_tier_blocked_s0_8" if score_tier == "s0-8"
                    else ("score_tier_blocked_s9_11" if score_tier == "s9-11" else "score_tier_blocked_s12p")
                )
                return None
        if score_tier == "s0-8" and MIN_ENTRY_PRICE_S0_8_15M > 0 and entry < MIN_ENTRY_PRICE_S0_8_15M:
            self._skip_tick("score_s0_8_entry_too_low")
            return None

    token_id = m["token_up"] if side == "Up" else m["token_down"]
    if not token_id:
        return None
    opp_token_id = m["token_down"] if side == "Up" else m["token_up"]
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
    chosen_ask = _best_ask_from(pm_book_data)
    if opp_token_id:
        opp_book = self._get_clob_ws_book(opp_token_id, max_age_ms=CLOB_MARKET_WS_MAX_AGE_MS)
        if opp_book is None:
            opp_book = await self._fetch_pm_book_safe(opp_token_id)
        opp_ask = _best_ask_from(opp_book)
        # Token-map drift guard:
        # if selected token ask is far from expected side entry while opposite token is close,
        # remap token ids for this decision to avoid inverted execution.
        if chosen_ask > 0 and opp_ask > 0:
            err_keep = abs(chosen_ask - fair_side_entry)
            err_swap = abs(opp_ask - fair_side_entry)
            if err_keep >= 0.18 and (err_swap + 0.05) < err_keep:
                if self._noisy_log_enabled(f"side-remap:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
                    print(
                        f"{Y}[SIDE-REMAP]{RS} {asset} {duration}m {side} "
                        f"token {self._short_cid(token_id)}->{self._short_cid(opp_token_id)} "
                        f"ask={chosen_ask:.3f} opp_ask={opp_ask:.3f} fair={fair_side_entry:.3f}"
                    )
                token_id = opp_token_id
                pm_book_data = opp_book
                chosen_ask = opp_ask

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
    # Defensive initialization: keeps evaluate loop alive even if future
    # branches reference execution_ev before the final EV computation.
    execution_ev = -9.0
    # Trade every eligible market while still preferring higher-payout entries.
    use_limit = False
    # Dynamic max entry: base + tolerance + small conviction slack.
    score_slack = SETUP_SLACK_HIGH if score >= SETUP_SCORE_SLACK_HIGH else (SETUP_SLACK_MID if score >= SETUP_SCORE_SLACK_MID else 0.0)
    max_entry_allowed = min(0.97, MAX_ENTRY_PRICE + MAX_ENTRY_TOL + score_slack)
    min_entry_allowed = 0.01
    base_min_entry_allowed = 0.01
    base_max_entry_allowed = max_entry_allowed
    if duration <= 5:
        # 5m entry gating is market-driven (EV/probability/real execution),
        # not static [min,max] clamps that can block profitable rounds.
        min_entry_allowed = max(min_entry_allowed, 0.01)
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
    model_cap = min(model_cap, MAX_ENTRY_PRICE + MAX_ENTRY_TOL)  # never override hard entry cap
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
        duration >= CORE_DURATION_MIN
        and score >= SETUP_Q_HARD_RELAX_SCORE
        and true_prob >= SETUP_Q_HARD_RELAX_PROB
        and setup_q >= SETUP_Q_HARD_RELAX_MIN
        and cl_agree
    ):
        extra_relax = min(SETUP_Q_HARD_RELAX_MAX, max(0.0, setup_q - SETUP_Q_HARD_RELAX_MIN) * SETUP_Q_HARD_RELAX_MULT)
        min_payout_req = max(1.70, min_payout_req - extra_relax)
    # Core 15m payout floor is data-driven from rolling on-chain profile.
    if duration >= CORE_DURATION_MIN and (not booster_eval):
        roll_n = int(rolling_profile.get("n", 0) or 0)
        roll_exp = float(rolling_profile.get("exp", 0.0) or 0.0)
        roll_wr_lb = float(rolling_profile.get("wr_lb", 0.5) or 0.5)
        if roll_n >= max(8, int(ROLLING_15M_CALIB_MIN_N)):
            if roll_exp >= ROLL_EXP_GOOD and roll_wr_lb >= ROLL_WR_GOOD:
                dyn_floor = 1.50
            elif roll_exp <= ROLL_EXP_BAD or roll_wr_lb < ROLL_WR_BAD:
                dyn_floor = 1.78
            else:
                dyn_floor = 1.62
        else:
            dyn_floor = 1.62
        min_payout_req = max(min_payout_req, dyn_floor)
    # Late-window locked-direction payout relax:
    # Win rate is ~72-78% in last LATE_PAYOUT_RELAX_PCT_LEFT of window → 1.65x payout is +EV.
    # This recovers the 33% skip rate from payout_below on late-window aligned entries.
    if (
        LATE_PAYOUT_RELAX_ENABLED
        and duration >= 15
        and pct_remaining <= LATE_PAYOUT_RELAX_PCT_LEFT
        and move_pct >= LATE_PAYOUT_RELAX_MIN_MOVE
        and open_price > 0 and current > 0
        and ((side == "Up" and current >= open_price) or (side == "Down" and current < open_price))
    ):
        min_payout_req = min(min_payout_req, LATE_PAYOUT_RELAX_FLOOR)
    # Trend-confirmed payout relax: when Chainlink + binary model both confirm direction,
    # cap min_payout_req at 1.72x throughout the window (not just last 45%).
    # Prevents paralysis in trending markets where trend-side tokens cost 55-58¢.
    if (
        duration >= CORE_DURATION_MIN
        and cl_agree
        and bin_c >= 0.54
        and move_pct >= LATE_PAYOUT_RELAX_MIN_MOVE
    ):
        min_payout_req = min(min_payout_req, 1.72)
    # High-quality execution unlock:
    # allow 1.80x floor on strong setups to avoid dead-zones around 1.82-1.98x.
    if (
        duration >= CORE_DURATION_MIN
        and score >= 14
        and true_prob >= 0.66
        and edge >= 0.10
        and setup_q >= 0.60
        and cl_agree
    ):
        min_payout_req = min(min_payout_req, 1.80)
    # Round-force anti-freeze: keep payout floor bounded so adaptive tightening
    # cannot fully block executable markets for long stretches.
    if FORCE_TRADE_EVERY_ROUND:
        force_cap = ROUND_FORCE_PAYOUT_CAP_5M if duration <= 5 else ROUND_FORCE_PAYOUT_CAP_15M
        if duration >= 15:
            force_cap = min(force_cap, 1.72)
        min_payout_req = min(min_payout_req, max(1.30, force_cap))
    min_ev_req = max(0.005, min_ev_req - (0.012 * q_relax))
    if (ws_fresh or rest_fresh) and cl_fresh and vol_fresh and mins_left >= (FRESH_RELAX_MIN_LEFT_15M if duration >= CORE_DURATION_MIN else FRESH_RELAX_MIN_LEFT_5M):
        max_entry_allowed = min(FRESH_RELAX_ENTRY_CAP, max_entry_allowed + FRESH_RELAX_ENTRY_ADD)
    max_entry_allowed = min(max_entry_allowed, adaptive_hard_cap)
    # Hard entry ceiling:
    # - 15m unchanged: strict 54c ceiling.
    # - 5m: use adaptive hard cap (execution/quality aware), avoid static 54c choke.
    if duration >= 15:
        strong_relax = 0.0
        if true_prob >= 0.72 and score >= 14 and edge >= 0.14:
            strong_relax = 0.02
        max_entry_allowed = min(max_entry_allowed, min(0.90, ENTRY_HARD_CAP_15M + strong_relax))
    else:
        max_entry_allowed = min(max_entry_allowed, max(0.60, adaptive_hard_cap))
    # Dynamic min-entry (not fixed hard floor): adapt to setup quality + microstructure.
    # High-quality setup can dip lower for super-payout entries; weaker setup is stricter.
    min_entry_dyn = float(base_min_entry_allowed)
    if setup_q >= SETUP_Q_RELAX_MIN and vol_ratio >= SETUP_VOL_RELAX_MIN and tf_votes >= TF_VOTES_MID:
        relax = min(SETUP_Q_RELAX_MAX, (setup_q - SETUP_Q_RELAX_MIN) * SETUP_Q_RELAX_MULT)
        min_entry_dyn = max(0.01, min_entry_dyn - relax)
    elif setup_q <= SETUP_Q_TIGHTEN_MIN:
        tighten = min(SETUP_Q_TIGHTEN_MAX, (SETUP_Q_TIGHTEN_MIN - setup_q) * SETUP_Q_TIGHTEN_MULT)
        min_entry_dyn = min(0.45, min_entry_dyn + tighten)
    if mins_left <= (3.5 if duration >= CORE_DURATION_MIN else 1.8):
        # Near expiry, avoid ultra-low entries that are mostly noise/fill artifacts.
        min_entry_dyn = max(min_entry_dyn, base_min_entry_allowed + ENTRY_TIGHTEN_ADD)
    if not (ws_fresh or rest_fresh):   # only tighten when both WS and REST are stale
        min_entry_dyn = max(min_entry_dyn, base_min_entry_allowed + ENTRY_TIGHTEN_ADD)
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
            PULLBACK_LIMIT_MIN_PCT_LEFT if duration <= 5 else max(PULLBACK_LIMIT_MIN_PCT_LEFT, PULLBACK_MIN_ENTRY_FLOOR)
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
    # Polymarket fee is price-dependent: p*(1-p)*6.24% (parabolic, peaks at p=0.5).
    # FEE_RATE_EST (flat 1.56%) overcounts fees on high-payout entries (e.g. p=0.20 → actual=1.0%).
    _fee_dyn = max(0.001, entry * (1.0 - entry) * 0.0624)
    ev_net = (true_prob / max(entry, 1e-9)) - 1.0 - _fee_dyn
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
                (entry * (1.0 + _fee_dyn))
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
            (side == "Down" and current < open_price)   # strict: tie resolves as Up
        )
        core_strong = (
            score >= CONSISTENCY_STRONG_MIN_SCORE_15M
            and true_prob >= CONSISTENCY_STRONG_MIN_TRUE_PROB_15M
            and execution_ev >= CONSISTENCY_STRONG_MIN_EXEC_EV_15M
            and tf_votes >= 3
            and cl_agree
        )
        _min_payout_consistency = max(1.30, CONSISTENCY_MIN_PAYOUT_15M)
        if core_payout + 1e-9 < _min_payout_consistency:
            if self._noisy_log_enabled(f"skip-consistency-payout:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
                print(
                    f"{Y}[SKIP] {asset} {duration}m consistency payout="
                    f"{core_payout:.3f}x<{_min_payout_consistency:.3f}x{RS}"
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
        # Propagate strict payout rule to execution layer so maker/fallback cannot overpay.
        core_entry_cap = min(0.99, 1.0 / max(1e-9, _min_payout_consistency))
        if max_entry_allowed > core_entry_cap:
            max_entry_allowed = core_entry_cap
            if min_entry_allowed >= max_entry_allowed:
                min_entry_allowed = max(0.01, max_entry_allowed - 0.01)
        _entry_cap_consistency = CONSISTENCY_MAX_ENTRY_15M if not must_fire_active else max(CONSISTENCY_MAX_ENTRY_15M, 0.70)
        if entry > _entry_cap_consistency and (not core_strong):
            if self._noisy_log_enabled(f"skip-consistency-entry:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
                print(
                    f"{Y}[SKIP] {asset} {duration}m consistency entry={entry:.3f} "
                    f"> cap={_entry_cap_consistency:.3f} (not strong-core){RS}"
                )
            self._skip_tick("consistency_entry_high")
            return None
        if not core_lead_now and not must_fire_active:
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
    elif (not booster_eval) and entry <= LOW_CENT_ENTRY_THRESHOLD:
        # Allow low-cent first entries too, but only for very strong setups.
        lowcent_new_ok = (
            duration >= 15
            and score >= LOWCENT_NEW_MIN_SCORE
            and true_prob >= LOWCENT_NEW_MIN_TRUE_PROB
            and execution_ev >= LOWCENT_NEW_MIN_EXEC_EV
            and payout_mult >= LOWCENT_NEW_MIN_PAYOUT
            and cl_agree
        )
        if not lowcent_new_ok:
            if self._noisy_log_enabled(f"skip-lowcent-weak-new:{asset}:{cid}", LOG_SKIP_EVERY_SEC):
                print(
                    f"{Y}[SKIP]{RS} {asset} {duration}m low-cent new entry weak "
                    f"(score={score} p={true_prob:.3f} ev={execution_ev:.3f})"
                )
            self._skip_tick("lowcent_new_weak")
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
    superbet_floor_applied = False   # H-1: track whether superbet floor was activated
    fresh_cl_disagree = (not cl_agree) and (cl_age_s is not None) and (cl_age_s <= FORCE_TAKER_CL_AGE_FRESH)

    # ── ENTRY PRICE TIERS ─────────────────────────────────────────────────
    # Higher payout (cheaper tokens) gets larger Kelly fraction.
    # Expensive tokens still traded but with smaller exposure.
    if entry <= 0.20:
        if   score >= ENTRY_TIER_SCORE_HIGH: kelly_frac, bankroll_pct = 0.20, 0.10
        elif score >= ENTRY_TIER_SCORE_MID:  kelly_frac, bankroll_pct = 0.16, 0.08
        else:                                kelly_frac, bankroll_pct = 0.12, 0.06
    elif entry <= 0.30:
        if   score >= ENTRY_TIER_SCORE_HIGH: kelly_frac, bankroll_pct = 0.16, 0.08
        elif score >= ENTRY_TIER_SCORE_MID:  kelly_frac, bankroll_pct = 0.12, 0.06
        else:                                kelly_frac, bankroll_pct = 0.10, 0.05
    elif entry <= 0.40:
        if   score >= ENTRY_TIER_SCORE_HIGH: kelly_frac, bankroll_pct = 0.12, 0.06
        elif score >= ENTRY_TIER_SCORE_MID:  kelly_frac, bankroll_pct = 0.10, 0.05
        else:                                kelly_frac, bankroll_pct = 0.08, 0.04
    elif entry <= 0.55:
        if   score >= ENTRY_TIER_SCORE_HIGH: kelly_frac, bankroll_pct = 0.08, 0.04
        elif score >= ENTRY_TIER_SCORE_MID:  kelly_frac, bankroll_pct = 0.06, 0.03
        else:                                kelly_frac, bankroll_pct = 0.05, 0.025
    else:
        if   score >= ENTRY_TIER_SCORE_HIGH: kelly_frac, bankroll_pct = 0.04, 0.02
        elif score >= ENTRY_TIER_SCORE_MID:  kelly_frac, bankroll_pct = 0.03, 0.015
        else:                                kelly_frac, bankroll_pct = 0.02, 0.010

    wr_scale   = self._wr_bet_scale()
    oracle_scale = ORACLE_SCALE_DISAGREE_FRESH if fresh_cl_disagree else (ORACLE_SCALE_DISAGREE_STALE if not cl_agree else 1.0)
    bucket_scale = self._bucket_size_scale(duration, score, entry)
    # Risk-aware size decay for tail-priced entries and near-expiry windows.
    # This preserves signal direction while preventing oversized bets on 2c/7c tails.
    cents_scale = 1.0
    if entry <= 0.03:
        cents_scale = ENTRY_CENTS_SCALE_3C
    elif entry <= 0.05:
        cents_scale = ENTRY_CENTS_SCALE_5C
    elif entry <= 0.10:
        cents_scale = ENTRY_CENTS_SCALE_10C
    elif entry <= 0.20:
        cents_scale = ENTRY_CENTS_SCALE_20C
    time_scale = 1.0
    if duration >= 15:
        if mins_left <= 2.5:
            time_scale = TIME_SCALE_LATE_2_5
        elif mins_left <= 3.5:
            time_scale = TIME_SCALE_LATE_3_5
        elif mins_left <= 5.0:
            time_scale = TIME_SCALE_LATE_5_0
    raw_size   = self._kelly_size(true_prob, entry, kelly_frac)
    max_single = min(MAX_SINGLE_ABS_CAP, self.bankroll * bankroll_pct)
    cid_cap    = max(MIN_HARD_CAP_USDC, self.bankroll * MAX_CID_EXPOSURE_PCT)
    hard_cap   = max(MIN_HARD_CAP_USDC, min(max_single, cid_cap, self.bankroll * MAX_BANKROLL_PCT))
    # Avoid oversized tail bets: these entries can look high-multiple but are low-quality fills.
    if entry <= TAIL_CAP_ENTRY_1:
        hard_cap = min(hard_cap, max(MIN_BET_ABS, self.bankroll * TAIL_CAP_BANKROLL_PCT_1))
    elif entry <= TAIL_CAP_ENTRY_2:
        hard_cap = min(hard_cap, max(MIN_BET_ABS, self.bankroll * TAIL_CAP_BANKROLL_PCT_2))
    # Soft cap curve for cheap entries:
    # default around $10, but scale toward ~$30 only if conviction is truly strong.
    if entry <= LOW_ENTRY_SOFT_THRESHOLD:
        score_n = max(0.0, min(1.0, (float(score) - SOFTCAP_SCORE_BASE) / SOFTCAP_SCORE_SCALE))
        prob_n = max(0.0, min(1.0, (float(true_prob) - SOFTCAP_PROB_BASE) / SOFTCAP_PROB_SCALE))
        edge_n = max(0.0, min(1.0, (float(edge) - SOFTCAP_EDGE_BASE) / SOFTCAP_EDGE_SCALE))
        # copy_net > 0 means leaders agree with chosen side; <0 penalizes conviction.
        leader_n = max(0.0, min(1.0, float(copy_net) / SOFTCAP_LEADER_SCALE))
        conviction = (
            SOFTCAP_W_SCORE * score_n
            + SOFTCAP_W_PROB * prob_n
            + SOFTCAP_W_EDGE * edge_n
            + SOFTCAP_W_LEADER * leader_n
        )
        soft_cap = LOW_ENTRY_BASE_SOFT_MAX + (
            (LOW_ENTRY_HIGH_CONV_SOFT_MAX - LOW_ENTRY_BASE_SOFT_MAX) * conviction
        )
        hard_cap = min(hard_cap, max(MIN_BET_ABS, soft_cap))
    # Risk cap when leader flow is not fresh: avoid oversized bets on cheap entries.
    # Deterministic cap from signal quality (score/prob/edge), no random guard.
    if duration >= 15 and entry <= NOLEADER_ENTRY_MAX and (not leader_ready):
        score_q = max(0.0, min(1.0, (float(score) - NOLEADER_SCORE_BASE) / NOLEADER_SCORE_SCALE))
        prob_q = max(0.0, min(1.0, (float(true_prob) - NOLEADER_PROB_BASE) / NOLEADER_PROB_SCALE))
        edge_q = max(0.0, min(1.0, (float(edge) - NOLEADER_EDGE_BASE) / NOLEADER_EDGE_SCALE))
        qual = NOLEADER_W_SCORE * score_q + NOLEADER_W_PROB * prob_q + NOLEADER_W_EDGE * edge_q
        # Cap range: ~$6 (weak) .. ~$12 (strong but no fresh leaders).
        no_leader_cap = NOLEADER_CAP_BASE + (NOLEADER_CAP_RANGE * qual)
        # Near expiry tighten further.
        if mins_left <= NOLEADER_NEAR_END_MIN_LEFT:
            no_leader_cap = min(no_leader_cap, NOLEADER_NEAR_END_CAP)
        hard_cap = min(hard_cap, max(MIN_BET_ABS, no_leader_cap))
    model_size = round(
        min(
            hard_cap,
            raw_size * vol_mult * wr_scale * oracle_scale * bucket_scale * cents_scale * time_scale * leader_size_scale * asset_entry_size_mult,
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
        and entry <= HIGH_EV_ENTRY_MAX
    ):
        boost = min(HIGH_EV_SIZE_BOOST_MAX, max(1.0, HIGH_EV_SIZE_BOOST))
        model_size = round(min(hard_cap, model_size * boost), 2)
    dyn_floor  = min(hard_cap, max(MIN_BET_ABS, self.bankroll * MIN_BET_PCT))
    # Mid-entry high-conviction floor:
    # avoid dust-size core bets on solid 15m setups around 30c-55c.
    if (
        duration >= 15
        and (not booster_eval)
        and MID_FLOOR_ENTRY_MIN <= entry <= MID_FLOOR_ENTRY_MAX
        and score >= MID_FLOOR_MIN_SCORE
        and true_prob >= MID_FLOOR_MIN_TRUE_PROB
        and execution_ev >= max(min_ev_req + MID_FLOOR_EV_MARGIN, MID_FLOOR_EV_ABS)
        and cl_agree
    ):
        dyn_floor = max(
            dyn_floor,
            min(hard_cap, max(MID_FLOOR_MIN_USDC, self.bankroll * MID_FLOOR_BANKROLL_PCT)),
        )
    # Never force a big floor size on ultra-cheap tails or near-expiry entries.
    if entry <= FORCE_MIN_ENTRY_TAIL or (duration >= 15 and mins_left <= FORCE_MIN_LEFT_TAIL):
        dyn_floor = min(dyn_floor, MIN_BET_ABS)
    size = round(max(model_size, dyn_floor), 2)
    if contrarian_tail_active:
        size = round(min(hard_cap, size * CONTRARIAN_TAIL_SIZE_MULT), 2)
    elif ORACLE_LATENCY_ONLY_MODE:
        size = round(min(hard_cap, size * ORACLE_SIZE_MULT), 2)
    size = max(ABS_MIN_SIZE_USDC, min(hard_cap, size))

    # Super-bet floor:
    # for high-multiple entries (very low price), keep at least a meaningful notional.
    if SUPER_BET_MIN_SIZE_ENABLED:
        payout_mult = (1.0 / max(entry, 1e-9))
        can_superbet = (
            score >= SUPER_BET_FLOOR_MIN_SCORE
            and execution_ev >= SUPER_BET_FLOOR_MIN_EV
            and (_time.time() - float(self._last_superbet_ts or 0.0)) >= max(0.0, SUPER_BET_COOLDOWN_SEC)
        )
        if (
            duration >= 15
            and entry <= SUPER_BET_ENTRY_MAX
            and payout_mult >= SUPER_BET_MIN_PAYOUT
            and size < SUPER_BET_MIN_SIZE_USDC
            and can_superbet
        ):
            old_size = size
            size = min(hard_cap, max(size, SUPER_BET_MIN_SIZE_USDC))
            superbet_floor_applied = True   # H-1: set timestamp after confirmed fill, not here
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
            max_super = max(ABS_MIN_SIZE_USDC, min(SUPER_BET_MAX_SIZE_USDC, self.bankroll * SUPER_BET_MAX_BANKROLL_PCT))
            if size > max_super:
                old_size = size
                size = round(max(ABS_MIN_SIZE_USDC, max_super), 2)
                if self._noisy_log_enabled(f"superbet-cap:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
                    print(
                        f"{Y}[SIZE-TUNE]{RS} {asset} {duration}m {side} superbet cap "
                        f"x={payout_mult:.2f} entry={entry:.3f} "
                        f"${old_size:.2f}->${size:.2f}"
                    )

    if size < MIN_EXEC_NOTIONAL_USDC:
        return None

    # Portfolio sizing mix (non-blocking):
    # - core zone: slightly larger size
    # - contrarian tails: keep smaller unless exceptionally strong
    if duration >= 15 and (not booster_mode):
        if (
            CORE_ENTRY_MIN <= entry <= CORE_ENTRY_MAX
            and score >= CORE_MIN_SCORE
            and execution_ev >= CORE_MIN_EV
        ):
            old_size = float(size)
            size = round(min(hard_cap, size * max(1.0, CORE_SIZE_BONUS)), 2)
            if self._noisy_log_enabled(f"core-size-bonus:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
                print(
                    f"{B}[SIZE-TUNE]{RS} {asset} {duration}m {side} core bonus "
                    f"entry={entry:.3f} ${old_size:.2f}->${size:.2f}"
                )
        elif entry <= CONTRARIAN_ENTRY_MAX:
            # OFI surge confirming contrarian direction lowers strong_contra threshold.
            # Reversal at extreme price + burst of buying = high payout near-certain reversal.
            contra_score_thresh = CONTRARIAN_STRONG_SCORE - (2 if ofi_surge_active else 0)
            strong_contra = (
                score >= contra_score_thresh
                and execution_ev >= CONTRARIAN_STRONG_EV
            )
            # Super-strong contrarian: surge + consensus + very high score → slight oversize
            super_contra = (
                strong_contra
                and ofi_surge_active
                and cross_count >= 2
                and score >= (CONTRARIAN_STRONG_SCORE + 2)
            )
            if super_contra:
                mult = 1.15
            elif strong_contra:
                mult = 1.0
            else:
                mult = max(CONTRARIAN_SIZE_MIN_MULT, min(1.0, CONTRARIAN_SIZE_MULT))
            old_size = float(size)
            size = round(max(float(MIN_EXEC_NOTIONAL_USDC), min(hard_cap, size * mult)), 2)
            if self._noisy_log_enabled(f"contra-size-mult:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
                print(
                    f"{Y}[SIZE-TUNE]{RS} {asset} {duration}m {side} contrarian "
                    f"entry={entry:.3f} strong={strong_contra} super={super_contra} x={mult:.2f} "
                    f"${old_size:.2f}->${size:.2f}"
                )

    # Event-context size modifier from soft alignment model.
    if duration >= 15 and (not booster_mode):
        try:
            e_mult = max(EVENT_ALIGN_SIZE_MIN, min(EVENT_ALIGN_SIZE_MAX, float(event_align_size_mult)))
        except Exception:
            e_mult = 1.0
        if abs(e_mult - 1.0) > 1e-9:
            old_size = float(size)
            size = round(max(float(MIN_EXEC_NOTIONAL_USDC), min(hard_cap, size * e_mult)), 2)
            if self._noisy_log_enabled(f"event-align-size:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
                print(
                    f"{Y}[SIZE-TUNE]{RS} {asset} {duration}m {side} event-align x={e_mult:.2f} "
                    f"${old_size:.2f}->${size:.2f}"
                )

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

        taker_conf = (side_up and taker_ratio > BOOSTER_TAKER_CONF_UP_MIN) or ((not side_up) and taker_ratio < BOOSTER_TAKER_CONF_DN_MAX)
        # Mathematical conviction model (15m intraround):
        # combines microstructure + trend persistence + oracle alignment.
        ob_c = max(-1.0, min(1.0, ob_sig / BOOSTER_OB_SCALE))
        tf_c = max(0.0, min(1.0, (tf_votes - BOOSTER_TF_BASE) / BOOSTER_TF_SCALE))
        flow_c = max(-1.0, min(1.0, ((taker_ratio - 0.5) * BOOSTER_FLOW_SCALE) if side_up else ((0.5 - taker_ratio) * BOOSTER_FLOW_SCALE)))
        vol_c = max(0.0, min(1.0, (vol_ratio - BOOSTER_VOL_BASE) / BOOSTER_VOL_SCALE))
        basis_signed = perp_basis if side_up else -perp_basis
        basis_c = max(-1.0, min(1.0, basis_signed / BOOSTER_BASIS_SCALE))
        vwap_signed = vwap_dev if side_up else -vwap_dev
        vwap_c = max(-1.0, min(1.0, vwap_signed / BOOSTER_VWAP_SCALE))
        oracle_c = 1.0 if cl_agree else -1.0
        booster_conv = (
            BOOSTER_W_TF * tf_c
            + BOOSTER_W_OB * max(0.0, ob_c)
            + BOOSTER_W_FLOW * max(0.0, flow_c)
            + BOOSTER_W_VOL * vol_c
            + BOOSTER_W_BASIS * max(0.0, basis_c)
            + BOOSTER_W_VWAP * max(0.0, vwap_c)
            + BOOSTER_W_ORACLE * max(0.0, oracle_c)
        )
        base_quality = (
            score >= MID_BOOSTER_MIN_SCORE
            and true_prob >= MID_BOOSTER_MIN_TRUE_PROB
            and edge >= MID_BOOSTER_MIN_EDGE
            and execution_ev >= MID_BOOSTER_MIN_EV_NET
            and payout_mult >= MID_BOOSTER_MIN_PAYOUT
            and entry <= MID_BOOSTER_MAX_ENTRY
            and ((cl_agree and imbalance_confirms) or score >= (MID_BOOSTER_MIN_SCORE + BOOSTER_STRONG_SCORE_DELTA))
            and (taker_conf or edge >= (MID_BOOSTER_MIN_EDGE + BOOSTER_STRONG_EDGE_DELTA))
            and vol_ratio >= BOOSTER_MIN_VOL_RATIO
            and booster_conv >= BOOSTER_MIN_CONV
        )
        if not base_quality:
            return None
        # Outside ideal window keep booster much stricter.
        if not in_ideal_window:
            if not (
                score >= (MID_BOOSTER_MIN_SCORE + BOOSTER_OUTSIDE_SCORE_DELTA)
                and true_prob >= (MID_BOOSTER_MIN_TRUE_PROB + BOOSTER_OUTSIDE_PROB_DELTA)
                and execution_ev >= (MID_BOOSTER_MIN_EV_NET + BOOSTER_OUTSIDE_EV_DELTA)
                and booster_conv >= BOOSTER_OUTSIDE_MIN_CONV
            ):
                return None

        prev_size = float((self.pending.get(cid, ({}, {}))[1] or {}).get("size", 0.0) or 0.0)
        b_pct = MID_BOOSTER_SIZE_PCT
        if score >= (MID_BOOSTER_MIN_SCORE + BOOSTER_STRONG_SIZE_SCORE_DELTA) and true_prob >= (MID_BOOSTER_MIN_TRUE_PROB + BOOSTER_STRONG_SIZE_PROB_DELTA):
            b_pct = MID_BOOSTER_SIZE_PCT_HIGH
        b_size = round(max(MIN_BET_ABS, self.bankroll * b_pct), 2)
        # Keep additive booster small vs existing exposure.
        if prev_size > 0:
            b_size = min(b_size, max(MIN_BET_ABS, prev_size * BOOSTER_PREV_SIZE_CAP_MULT))
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
    profit_push_tier = "off"
    profit_push_size_mult = 1.0
    if PROFIT_PUSH_MODE and PROFIT_PUSH_ADAPTIVE_MODE:
        is_push = (
            score >= PROFIT_PUSH_PUSH_MIN_SCORE
            and true_prob >= PROFIT_PUSH_PUSH_MIN_TRUE_PROB
            and execution_ev >= PROFIT_PUSH_PUSH_MIN_EXEC_EV
            and cl_agree
            and entry <= max(0.50, WINMODE_MAX_ENTRY_15M if duration > 5 else WINMODE_MAX_ENTRY_5M)
        )
        is_base = (
            score >= PROFIT_PUSH_BASE_MIN_SCORE
            and execution_ev >= PROFIT_PUSH_BASE_MIN_EXEC_EV
        )
        if is_push:
            profit_push_tier = "push"
            profit_push_size_mult = PROFIT_PUSH_PUSH_MULT
        elif is_base:
            profit_push_tier = "base"
            profit_push_size_mult = PROFIT_PUSH_BASE_MULT
        else:
            profit_push_tier = "probe"
            profit_push_size_mult = PROFIT_PUSH_PROBE_MULT
        if duration <= 5:
            profit_push_size_mult *= max(0.20, min(1.25, float(self._five_m_runtime_size_mult or 1.0)))
        profit_push_size_mult = max(0.20, min(PROFIT_PUSH_MAX_MULT, float(profit_push_size_mult)))
        if abs(profit_push_size_mult - 1.0) > 1e-9:
            old_size = float(size)
            size = max(float(MIN_EXEC_NOTIONAL_USDC), round(min(hard_cap, old_size * profit_push_size_mult), 2))
            if self._noisy_log_enabled(f"profit-push-size:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
                print(
                    f"{B}[SIZE-TUNE]{RS} {asset} {duration}m {side} "
                    f"profit-push {profit_push_tier} x{profit_push_size_mult:.2f} "
                    f"${old_size:.2f}->${size:.2f}"
                )

    payout_mult = (1.0 / max(entry, 1e-9))
    if SUPER_BET_MIN_SIZE_ENABLED:
        can_superbet = (
            score >= SUPER_BET_FLOOR_MIN_SCORE
            and execution_ev >= SUPER_BET_FLOOR_MIN_EV
            and (_time.time() - float(self._last_superbet_ts or 0.0)) >= max(0.0, SUPER_BET_COOLDOWN_SEC)
        )
        if (
            duration >= 15
            and entry <= SUPER_BET_ENTRY_MAX
            and payout_mult >= SUPER_BET_MIN_PAYOUT
            and size < SUPER_BET_MIN_SIZE_USDC
            and can_superbet
        ):
            old_size = float(size)
            size = max(float(MIN_EXEC_NOTIONAL_USDC), min(hard_cap, float(SUPER_BET_MIN_SIZE_USDC)))
            superbet_floor_applied = True   # H-1: set timestamp after confirmed fill, not here
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
    # Global autonomous policy multiplier (never zeroed by policy).
    ap_mult = max(0.25, min(1.35, float(self._autopilot_size_mult or 1.0)))
    if abs(ap_mult - 1.0) > 1e-9:
        old_size = float(size)
        size = max(float(MIN_EXEC_NOTIONAL_USDC), round(min(hard_cap, old_size * ap_mult), 2))
        if self._noisy_log_enabled(f"autopilot-size:{asset}:{cid}", LOG_FLOW_EVERY_SEC):
            print(
                f"{B}[SIZE-TUNE]{RS} {asset} {duration}m {side} autopilot "
                f"{self._autopilot_mode} x{ap_mult:.2f} ${old_size:.2f}->${size:.2f}"
            )
    # Immediate fills: FOK on strong signal, GTC limit otherwise
    # Limit orders (use_limit=True) are always GTC — force_taker stays False
    force_taker = (not use_limit) and (
        (score >= FORCE_TAKER_SCORE and very_strong_mom and imbalance_confirms and move_pct > FORCE_TAKER_MOVE_MIN) or
        (score >= FORCE_TAKER_SCORE and is_early_continuation)
    )
    if FAST_EXEC_ENABLED and (not use_limit):
        if score >= FAST_EXEC_SCORE and edge >= FAST_EXEC_EDGE and entry <= MAX_ENTRY_PRICE:
            force_taker = True
    if arm_active:
        force_taker = True

    return {
        "cid": cid, "m": m, "score": score,
        "snap_token_up": str(m.get("token_up", "") or ""),
        "snap_token_down": str(m.get("token_down", "") or ""),
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
        "pred_variant": pred_variant,
        "pred_variant_probs": pred_variant_probs,
        "rolling_n": int(rolling_profile.get("n", 0) or 0),
        "rolling_exp": float(rolling_profile.get("exp", 0.0) or 0.0),
        "rolling_wr_lb": float(rolling_profile.get("wr_lb", 0.5) or 0.5),
        "rolling_prob_add": float(rolling_profile.get("prob_add", 0.0) or 0.0),
        "rolling_ev_add": float(rolling_profile.get("ev_add", 0.0) or 0.0),
        "rolling_size_mult": float(rolling_profile.get("size_mult", 1.0) or 1.0),
        "autopilot_mode": self._autopilot_mode,
        "autopilot_size_mult": float(self._autopilot_size_mult or 1.0),
        "min_payout_req": min_payout_req,
        "min_ev_req": min_ev_req,
        "analysis_quality": analysis_quality,
        "analysis_conviction": analysis_conviction,
        "bin_c": bin_c,
        "analysis_prob_scale": quality_scale,
        "signal_tier": signal_tier,
        "signal_source": signal_source,
        "leader_size_scale": leader_size_scale,
        "booster_mode": booster_mode,
        "booster_note": booster_note,
        "superbet_floor_applied": superbet_floor_applied,
        "book_age_ms": book_age_ms,
        "quote_age_ms": 0.0 if px_src == "CL" else quote_age_ms,
        "signal_latency_ms": (_time.perf_counter() - score_started) * 1000.0,
        "prebid_arm": arm_active,
        "must_fire": False,
        "profit_push_tier": profit_push_tier,
        "profit_push_size_mult": profit_push_size_mult,
        "runtime_5m_size_mult": float(self._five_m_runtime_size_mult or 1.0),
    }
