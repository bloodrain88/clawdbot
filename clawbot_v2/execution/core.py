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

async def _execute_trade(self, sig: dict):
    _ensure_globals()
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
        self._reserved_bankroll += float(sig.get("size", 0.0) or 0.0)  # H-4: pre-reserve
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
              f"{agree_str}{ob_str}{tf_str} tk={sig.get('taker_ratio', 0.0):.2f} vol={sig.get('vol_ratio', 0.0):.1f}x"
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
            lock_mins, lock_move_min = self._late_lock_thresholds(duration)
            if mins_left <= lock_mins:
                cur_now = self._current_price(sig["asset"]) or sig.get("current", 0)
                open_p = float(sig.get("open_price", 0) or 0)
                if cur_now > 0 and open_p > 0:
                    move_abs = abs((cur_now - open_p) / max(open_p, 1e-9))
                    if move_abs >= lock_move_min:
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
        # Side/token integrity check before execution:
        # ensures we are sending BUY on the token corresponding to the normalized side.
        # Prefer token snapshot from signal creation time (avoids stale-m race condition)
        token_up = str(sig.get("snap_token_up") or (m or {}).get("token_up", "") or "")
        token_down = str(sig.get("snap_token_down") or (m or {}).get("token_down", "") or "")
        expected_token = token_up if sig.get("side") == "Up" else (token_down if sig.get("side") == "Down" else "")
        actual_token = str(sig.get("token_id", "") or "")
        if expected_token and actual_token and expected_token != actual_token:
            print(
                f"{R}[SIDE-CHECK]{RS} BLOCK {sig.get('asset','?')} {sig.get('duration',0)}m "
                f"side={sig.get('side','?')} expected_token={expected_token[:10]}.. "
                f"actual_token={actual_token[:10]}.. cid={self._short_cid(sig.get('cid',''))}"
            )
            return
        if self._noisy_log_enabled(f"side-check:{sig.get('asset','?')}:{sig.get('cid','')}", LOG_FLOW_EVERY_SEC):
            print(
                f"{B}[SIDE-CHECK]{RS} OK {sig.get('asset','?')} {sig.get('duration',0)}m "
                f"side={sig.get('side','?')} token={actual_token[:10]}.. "
                f"cid={self._short_cid(sig.get('cid',''))}"
            )
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
            round_force=bool(sig.get("round_force_coverage", False)),
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
            if sig.get("superbet_floor_applied"):   # H-1: only consume cooldown on actual fill
                self._last_superbet_ts = _time.time()
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
            "pred_variant": sig.get("pred_variant", ""),
            "pred_variant_probs": sig.get("pred_variant_probs", {}),
            "autopilot_mode": sig.get("autopilot_mode", ""),
            "autopilot_size_mult": sig.get("autopilot_size_mult", 1.0),
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
            "pred_variant": sig.get("pred_variant", ""),
            "autopilot_mode": sig.get("autopilot_mode", ""),
            "autopilot_size_mult": sig.get("autopilot_size_mult", 1.0),
            "booster_mode": bool(is_booster),
            "booster_note": sig.get("booster_note", ""),
            "duration": sig.get("duration", 15),
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
            self._reserved_bankroll = max(0.0, self._reserved_bankroll - float(sig.get("size", 0.0) or 0.0))  # H-4: release

async def evaluate(self, m: dict):
    _ensure_globals()
    """RTDS fast-path: score a single market and execute if score gate passes."""
    t0 = _time.perf_counter()
    sig = await self._score_market(m)
    self._perf_update("score_ms", (_time.perf_counter() - t0) * 1000.0)
    if sig and sig["score"] >= MIN_SCORE_GATE:
        await self._execute_trade(sig)

async def _place_order(self, token_id, side, price, size_usdc, asset, duration, mins_left, true_prob=0.5, cl_agree=True, min_edge_req=None, force_taker=False, score=0, pm_book_data=None, use_limit=False, max_entry_allowed=None, hc15_mode=False, hc15_fallback_cap=0.36, core_position=True, round_force=False):
    _ensure_globals()
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
    # Keep execution floor aligned with adaptive sizing logic.
    # Using MIN_BET_ABS here can reject valid autopilot-sized trades.
    hard_min_notional = max(float(MIN_EXEC_NOTIONAL_USDC), 1.0)
    if float(size_usdc or 0.0) < hard_min_notional:
        allow_bump = bool(core_position) and (bool(round_force) or FORCE_TRADE_EVERY_ROUND)
        target_min = hard_min_notional * max(1.0, float(ROUND_FORCE_MIN_NOTIONAL_MULT))
        bank_cap = max(0.0, float(self.bankroll or 0.0)) * max(0.01, float(ROUND_FORCE_MAX_BANK_FRAC))
        if allow_bump and bank_cap >= target_min:
            bumped = round(max(float(size_usdc or 0.0), target_min), 2)
            print(
                f"{Y}[SIZE-BUMP]{RS} {asset} {side} size ${float(size_usdc or 0.0):.2f} -> ${bumped:.2f} "
                f"(hard_min=${hard_min_notional:.2f} bank_cap=${bank_cap:.2f})"
            )
            size_usdc = bumped
        else:
            print(
                f"{Y}[SKIP]{RS} {asset} {side} size=${float(size_usdc or 0.0):.2f} "
                f"< hard_min=${hard_min_notional:.2f} (exec backstop)"
            )
            return None

    for attempt in range(max(1, ORDER_RETRY_MAX)):
        try:
            loop = asyncio.get_running_loop()
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
                t_sign0 = _time.perf_counter()
                signed = await loop.run_in_executor(None, lambda: self.clob.create_market_order(order_args))
                t_sign_ms = (_time.perf_counter() - t_sign0) * 1000.0
                try:
                    t_post0 = _time.perf_counter()
                    resp = await loop.run_in_executor(None, lambda: self.clob.post_order(signed, OrderType.FOK))
                    t_post_ms = (_time.perf_counter() - t_post0) * 1000.0
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
                if ORDER_LATENCY_LOG_ENABLED:
                    print(
                        f"{B}[ORDER-LAT]{RS} {asset} {side} {duration}m "
                        f"fok sign={t_sign_ms:.0f}ms post={t_post_ms:.0f}ms"
                    )
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
            book_age_exec_ms = 9e9
            if isinstance(pm_book_data, dict):
                bts = float(pm_book_data.get("ts", 0.0) or 0.0)
                if bts > 0:
                    book_age_exec_ms = max(0.0, (_time.time() - bts) * 1000.0)

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
            edge_floor = min_edge_req if min_edge_req is not None else EXEC_EDGE_FLOOR_DEFAULT
            if not cl_agree:
                edge_floor += EXEC_EDGE_FLOOR_NO_CL
            if round_force:
                # Forced round coverage: allow execution even with weak edge.
                edge_floor = min(edge_floor, -0.03)
            strong_exec = (
                (score >= 12)
                and cl_agree
                and (taker_edge >= (edge_floor + EXEC_EDGE_STRONG_DELTA))
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
                if maker_edge_est < 0 and not round_force:
                    print(f"{Y}[SKIP] {asset} {side} [high-conv]: maker_edge={maker_edge_est:.3f} < 0 "
                          f"(mid={mid_est:.3f} model={true_prob:.3f}){RS}")
                    return None
                print(f"{B}[EXEC-CHECK]{RS} {asset} {side} score={score} maker_edge={maker_edge_est:.3f} taker_edge={taker_edge:.3f}")
            else:
                # Normal conviction: taker edge gate applies
                if taker_edge < edge_floor and not round_force:
                    kind = "disagree" if not cl_agree else "directional"
                    print(f"{Y}[SKIP] {asset} {side} [{kind}]: taker_edge={taker_edge:.3f} < {edge_floor:.2f} "
                          f"(ask={best_ask:.3f} model={true_prob:.3f}){RS}")
                    return None
                print(f"{B}[EXEC-CHECK]{RS} {asset} {side} edge={taker_edge:.3f} floor={edge_floor:.2f}")
            if round_force and not use_limit and not force_taker:
                force_taker = True

            # High conviction: skip maker, go straight to FOK taker for instant fill
            # FOK = Fill-or-Kill: fills completely at price or cancels instantly — no waiting
            if (not force_taker) and ORDER_FAST_MODE and (not use_limit):
                eff_max_entry = max_entry_allowed if max_entry_allowed is not None else 0.99
                fast_spread_cap = FAST_TAKER_SPREAD_MAX_5M if duration <= 5 else FAST_TAKER_SPREAD_MAX_15M
                score_cap = FAST_TAKER_SCORE_5M if duration <= 5 else FAST_TAKER_SCORE_15M
                speed_score_cap = FAST_TAKER_SPEED_SCORE_5M if duration <= 5 else FAST_TAKER_SPEED_SCORE_15M
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
                    and taker_edge >= (edge_floor + EXEC_EDGE_STRONG_DELTA)
                ):
                    force_taker = True
                elif (
                    secs_elapsed <= early_cut
                    and score >= (score_cap + 1)
                    and spread <= (fast_spread_cap + 0.005)
                    and best_ask <= eff_max_entry
                    and taker_edge >= (edge_floor + EXEC_EDGE_EARLY_DELTA)
                ):
                    force_taker = True
                elif (
                    best_ask <= eff_max_entry
                    and taker_edge >= edge_floor
                    and (maker_edge_est - taker_edge) <= FAST_TAKER_EDGE_DIFF_MAX
                ):
                    force_taker = True
                if (
                    EXEC_SPEED_PRIORITY_ENABLED
                    and (book_age_exec_ms <= FAST_PATH_MAX_BOOK_AGE_MS)
                    and score >= speed_score_cap
                    and spread <= (fast_spread_cap + 0.002)
                    and best_ask <= eff_max_entry
                    and taker_edge >= edge_floor
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
                if strong_exec:
                    max_gap_ticks += (1 if duration <= 5 else 2)
                if round_force:
                    max_gap_ticks += (2 if duration <= 5 else 3)
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
                            bypass_filled = False
                            if not (strong_exec or round_force):
                                print(f"{Y}[EXEC-RESULT]{RS} {asset} {side} no-fill reason=maker_bypass_fok_unfilled")
                                return None
                    if strong_exec or round_force:
                        maker_price = round(min(max(tick, best_bid), eff_max_entry, 0.97), 4)
                        if self._noisy_log_enabled(f"maker-reprice-gap:{asset}:{side}", LOG_EXEC_EVERY_SEC):
                            print(
                                f"{Y}[MAKER-REPRICE]{RS} {asset} {side} gap={gap_ticks:.1f} ticks>{max_gap_ticks} "
                                f"-> maker_price={maker_price:.3f}"
                            )
                    else:
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
            t_sign0 = _time.perf_counter()
            signed  = await loop.run_in_executor(None, lambda: self.clob.create_order(order_args))
            t_sign_ms = (_time.perf_counter() - t_sign0) * 1000.0
            t_post0 = _time.perf_counter()
            resp    = await loop.run_in_executor(None, lambda: self.clob.post_order(signed, OrderType.GTC))
            t_post_ms = (_time.perf_counter() - t_post0) * 1000.0
            if ORDER_LATENCY_LOG_ENABLED:
                print(
                    f"{B}[ORDER-LAT]{RS} {asset} {side} {duration}m "
                    f"maker sign={t_sign_ms:.0f}ms post={t_post_ms:.0f}ms"
                )
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
            if EXEC_SPEED_PRIORITY_ENABLED and not use_limit:
                # Speed-first: keep maker exposure extremely short, then re-price via FOK.
                speed_cap = MAX_MAKER_HOLD_5M_SEC if duration <= 5 else MAX_MAKER_HOLD_15M_SEC
                max_wait = min(max_wait, speed_cap)
            polls     = max(1, int(max_wait / poll_interval))
            print(f"{G}[MAKER] posted {asset} {side} @ {maker_price:.3f} — "
                  f"waiting up to {polls*poll_interval}s for fill...{RS}")

            filled = False
            for i in range(polls):
                await asyncio.sleep(poll_interval)
                ev = dict(self._order_event_cache.get(order_id) or {})
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
                ev = dict(self._order_event_cache.get(order_id) or {})
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
                _must_fire_exec = bool(round_force)
                if _must_fire_exec:
                    # Must-fire: accept any EV-positive payout (>= 1.30x minimum)
                    min_payout_fb = max(1.30, min_payout_fb - 0.35)
                    min_ev_fb     = max(-0.005, min_ev_fb - 0.015)
                elif core_position and duration >= 15 and CONSISTENCY_CORE_ENABLED:
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
                fresh_ev_net = (true_prob / max(fresh_ask, 1e-9)) - 1.0 - max(0.001, fresh_ask * (1.0 - fresh_ask) * 0.0624)
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
                    ev = dict(self._order_event_cache.get(order_id) or {})
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
