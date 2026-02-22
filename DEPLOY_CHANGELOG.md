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

### Commit `heartbeat-docs-align`
- Scope: `clawdbot_live.py`
- Summary:
  - RTDS heartbeat allineato a docs ufficiali: invio `PING` testuale ogni 5s.
  - Market CLOB WS heartbeat applicativo aggiunto: invio `PING` ogni 10s + gestione `PONG`.
  - Obiettivo: ridurre stale books e disconnect silenziosi lato feed realtime.
- Feed/infra status intent:
  - Nessun cambio strategia trading; solo stabilita' e freshness trasporto dati.
- Rollback:
  - `git revert <sha-commit>`

### Commit `runtime-stability-ws-rtds-seen`
- Scope: `clawdbot_live.py`
- Summary:
  - WS health gate con warmup startup (`WS_GATE_WARMUP_SEC`) per evitare blocchi immediati con feed appena connessi.
  - Backoff RTDS migliorato su errori `HTTP 429` con attesa progressiva piu' robusta.
  - Ridotto rumore operativo: `STATS-LOCAL` stampato solo con `LOG_STATS_LOCAL=true`.
  - `seen` cache ridotta/configurabile (`SEEN_MAX_KEEP`) per limitare `blocked_seen` falsi post-restart.
- Feed/infra status intent:
  - Migliora stabilita' ingressi realtime senza cambiare la strategia EV.
- Rollback:
  - `git revert <sha-commit>`

### Commit `33684bb`
- Scope: `clawdbot_live.py`
- Summary:
  - Fix matematico di allineamento lato finale (`side`) per confluence/quality.
  - Rimossi falsi `low confluence` su setup Down dovuti a segno OB non coerente.
  - Refresh side-aligned metrics dopo `leader-follow` e dopo `prebid`.
  - Booster usa metrica lato finale (niente variabili direzionali stale).
- Feed/infra status intent:
  - Nessun cambio di protocollo feed; solo correzione calcolo decisionale.
- Risk/position:
  - Nessun cambio strategia base/risk profile.
  - Riduzione skip errati da incoerenza matematica.
- Rollback:
  - `git revert 33684bb`

### Commit `manual-deploy-script`
- Scope: `scripts/nf_manual_deploy.sh`, `OPERATIONS_RUNBOOK.md`
- Summary:
  - Aggiunto metodo stabile di deploy manuale senza dipendenza da webhook VCS.
  - Flusso: push -> build per SHA -> deploy per SHA -> verifica `deployedSHA`.
- Feed/infra status intent:
  - Nessun impatto runtime sul bot; solo affidabilita' del processo di rilascio.
- Rollback:
  - rimuovere script e revert entry runbook/changelog.

### Commit `753425f`
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
