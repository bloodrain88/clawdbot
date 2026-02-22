# Deploy Changelog

Questo file e' obbligatorio per il tracking operativo.  
Ogni modifica che impatta trading/runtime deve aggiungere una nuova entry con:

- commit hash
- data UTC
- cosa e' cambiato
- stato feed (`WS`, `CLOB REST`, `HEARTBEAT`)
- cambi logica rischio/entry/size
- rollback note

## 2026-02-22

### Commit `pending`
- Scope: `clawdbot_live.py`
- Summary:
  - Rimosso il floor statico effettivo su `min_entry`: ora e' dinamico per setup quality (`analysis_quality` + `analysis_conviction`), vol_ratio, tf_votes, freshness e mins_left.
  - Il range entry in scoring passa da baseline config a range adattato per trade.
  - Log `[FLOW]` esteso con trasparenza completa `base -> adjusted`:
    - `payout>=base->adj`
    - `ev>=base->adj`
    - `entry=[base_min,base_max]->[adj_min,adj_max]`
- Feed/infra status intent:
  - Nessun cambio protocollo WS/CLOB/heartbeat in questo commit.
- Risk/position:
  - Nessun cambio a core/addon separation; resta attiva la protezione no-average sul core.
- Rollback:
  - `git revert <sha-commit>`

### Commit `5fcf679`
- Scope: `clawdbot_live.py`
- Summary:
  - Booster consentito solo con posizione core esistente on-chain (no booster su posizione non confermata).
  - Rimosso merge locale core+booster: niente averaging locale dell'entry core.
  - Booster tracciato separatamente (`addon_count`, `addon_stake_usdc`).
  - Sync on-chain non riscrive `entry/size` del core quando `core_entry_locked=True`.
  - Gate `analysis_quality`/`analysis_conviction` resi dinamici per stato realtime del trade.
  - Vincoli payout/ev/entry adattati dinamicamente da qualita' setup (meno hardcoded statico).
- Feed/infra status intent:
  - WS: richiesto strict quando disponibile; fallback gestito dal codice esistente.
  - CLOB REST: resta fallback operativo per assenza WS fresh.
  - Heartbeat: attivo via `post_heartbeat` loop con recovery `heartbeat_id`.
- Risk/position:
  - Core position preservata.
  - Addon/booster non deve assimilarsi alla media core lato bot.
- Rollback:
  - `git revert 5fcf679`
