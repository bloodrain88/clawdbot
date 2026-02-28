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


async def redeem_loop(self):
    _ensure_legacy_globals()
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
    loop = asyncio.get_running_loop()
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
                if self._noisy_log_enabled(f"settle-check:{cid}", LOG_REDEEM_WAIT_EVERY_SEC):
                    print(
                        f"{B}[SETTLE-CHECK]{RS} cid={self._short_cid(cid)} "
                        f"side={side} winner={winner} won={won} "
                        f"num0={int(n0)} num1={int(n1)} src={winner_source}"
                    )
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
                        _bs_save(self._bucket_stats)
                        self._record_result(asset, side, True, trade.get("structural", False), pnl=pnl)
                        self._record_resolved_sample(trade, pnl, True)
                        self._record_pm_pattern_outcome(trade, pnl, True)
                        if self._pred_agent is not None:
                            self._pred_agent.observe_outcome(trade, True, pnl)
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
                        "duration": trade.get("duration", 15),
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
                            _bs_save(self._bucket_stats)
                            self._record_result(asset, side, False, trade.get("structural", False), pnl=pnl)
                            self._record_resolved_sample(trade, pnl, False)
                            self._record_pm_pattern_outcome(trade, pnl, False)
                            if self._pred_agent is not None:
                                self._pred_agent.observe_outcome(trade, False, pnl)
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
                            "duration": trade.get("duration", 15),
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

