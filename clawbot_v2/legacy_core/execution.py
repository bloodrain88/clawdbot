from __future__ import annotations

import importlib

_LOADED = False


def _ensure_legacy_globals() -> None:
    global _LOADED
    if _LOADED:
        return
    legacy = importlib.import_module("clawdbot_live")
    g = globals()
    for k, v in vars(legacy).items():
        if k not in g:
            g[k] = v
    _LOADED = True


async def place_order(self, token_id, side, price, size_usdc, asset, duration, mins_left, true_prob=0.5, cl_agree=True, min_edge_req=None, force_taker=False, score=0, pm_book_data=None, use_limit=False, max_entry_allowed=None, hc15_mode=False, hc15_fallback_cap=0.36, core_position=True, round_force=False):
    _ensure_legacy_globals()
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

    # ── RESOLVE ───────────────────────────────────────────────────────────────
