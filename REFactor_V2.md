# Clawdbot Refactor V2

## Obiettivo
Eliminare il monolite `clawdbot_live.py` e migrare a moduli separati:
- `config`: settings validati
- `data`: market feeds e cache
- `strategy`: scoring/prediction
- `execution`: order placement e retry policy
- `settlement`: redeem/reconcile
- `dashboard`: processo separato read-only
- `runtime`: orchestrazione e supervisione loop

## Stato attuale
- Introdotto entrypoint v2 `clawbot_v2/main.py`
- Introdotto adapter `legacy_engine` per continuità operativa
- Introdotti moduli base (`config`, `domain`, `infra`, `runtime`)
- Estratti metodi core legacy in moduli dedicati:
  - `clawbot_v2/legacy_core/score.py`
  - `clawbot_v2/legacy_core/execution.py`
  - `clawbot_v2/legacy_core/settlement.py`
  - `clawbot_v2/legacy_core/dashboard.py`
- `clawdbot_live.py` ora usa wrapper sottili per i blocchi core estratti

## Prossimi step
1. Estrarre scoring e signal gating in `strategy/engine.py`
2. Estrarre execution path in `execution/router.py`
3. Estrarre redeem/reconcile in `settlement/redeemer.py`
4. Spostare dashboard HTML/JS in servizio separato
5. Eliminare definitivamente adapter legacy dopo parity test su produzione
