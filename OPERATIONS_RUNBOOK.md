# ClawdBot Operations Runbook

Questo file e' il riferimento unico da usare sempre quando c'e' un problema.

## 1) Regola base
- Prima diagnostica, poi fix.
- Mai cambiare strategia senza evidenza dai dati reali.
- Verificare sempre SHA deploy attivo prima di analizzare i log.
- Ogni update operativo deve essere documentato in Git in `DEPLOY_CHANGELOG.md`.

### 1.1 Regola documentazione Git (obbligatoria)
Per ogni commit che tocca runtime/trading/feed/risk:
1. aggiungere una nuova entry in `DEPLOY_CHANGELOG.md`;
2. includere sempre stato `WS / CLOB / HEARTBEAT`;
3. indicare se sono cambiati `entry/payout/ev/size` e come;
4. includere rollback (`git revert <sha>`).

## 2) Check deploy (sempre per primo)
Comando:
```bash
northflank get service --projectId poly2 --serviceId clawdbot --output json | rg -n "deployedSHA|build\"|status|reason|lastTransitionTime"
```
Obiettivo:
- Confermare che lo SHA in esecuzione sia quello atteso.

## 3) Redeem policy (ordine obbligatorio)
Quando viene richiesto "redeem", l'ordine e' sempre:
1. Eseguire prima `FORCE REDEEM` nel service runtime.
2. Verificare subito dopo con check read-only onchain.

### 3.1 FORCE REDEEM (sempre nel container Northflank)
Comando ufficiale:
```bash
northflank exec service --projectId poly2 --serviceId clawdbot --cmd 'sh -lc "cd /app || true; python3 redeem_wins.py"'
```

Nota:
- Non usare il redeem locale come primario.
- Il runtime service ha dipendenze/env corretti per transazioni onchain.

### 3.2 Verifica post-force (read-only onchain)
Usare sempre questo flusso read-only subito dopo il force.

Comando:
```bash
/bin/zsh -lc 'source ~/.clawdbot.env >/dev/null 2>&1; f=$(mktemp); \
curl -s "https://data-api.polymarket.com/positions?user=${POLY_ADDRESS}&sizeThreshold=0.01" > "$f"; \
python3 - "$f" <<"PY"
import json,sys
with open(sys.argv[1],"r",encoding="utf-8") as fh:
    rows=json.load(fh)
red=[r for r in rows if r.get("redeemable") and float(r.get("currentValue",0) or 0)>=0.01]
out={
  "positions_total":len(rows),
  "redeemable_count":len(red),
  "redeemable_total_usdc":round(sum(float(r.get("currentValue",0) or 0) for r in red),4),
  "top":[{
    "cid":r.get("conditionId"),
    "outcome":r.get("outcome"),
    "value":round(float(r.get("currentValue",0) or 0),4),
    "title":str(r.get("title",""))[:90]
  } for r in red[:10]]
}
print(json.dumps(out,ensure_ascii=False))
PY
rm -f "$f"'
```
Interpretazione:
- `redeemable_count=0` => nessun redeem pendente.
- `redeemable_count>0` => esistono posizioni da riscattare.

### 3.3 Verifica onchain diretta (indipendente da Polymarket API)
Usare quando serve conferma da explorer chain:
```bash
curl -s "https://polygon.blockscout.com/api/v2/addresses/0xd2307Bb0E25012e89dF6744c4D70E75784a76301/token-transfers?type=ERC-20"
```
Controllare:
- `method=matchOrders` => trade verso CTF exchange.
- trasferimenti `IN` USDC.E da contratti Polymarket => redeem/payout ricevuti.

## 4) Fallback redeem locale (solo emergenza)
Script:
- `redeem_wins.py`

Comando:
```bash
cd /Users/alessandro/clawdbot && python3 redeem_wins.py
```

Usare questo fallback solo se `northflank exec` non disponibile.

## 5) Diagnostica realtime market-data
### 5.1 Errori da non ignorare
- `missing fresh CLOB WS book`
- `leader-flow realtime unavailable`

### 5.2 Cosa verificare
- WS feed attivo e aggiornato su token UP e DOWN.
- Fallback CLOB REST fresco usato quando WS e' stale.
- `analysis_quality` e `analysis_conviction` presenti nei log.

## 6) Decision engine (regola operativa)
Un trade e' valido solo se:
- dati realtime sono coerenti (`analysis_quality` >= soglia),
- segnali direzionali concordano (`analysis_conviction` >= soglia),
- EV esecuzione e' positivo (`execution_ev` >= soglia),
- sizing coerente con qualita' del segnale.

## 7) Checklist prima di dire "risolto"
- SHA deploy corretto e confermato.
- Nessun errore critico nei log ultimi 5 minuti.
- Nessun mismatch on-chain su bankroll/open/redeem.
- Se richiesto, report redeem read-only allegato.

## 8) Template risposta rapida
Quando c'e' un problema, rispondere sempre con:
1. SHA attivo.
2. Evidenza dati (2-3 righe log + check on-chain).
3. Causa tecnica precisa.
4. Fix applicato.
5. Verifica post-fix.

## 9) Deploy: ordine obbligatorio (sempre)
Usare sempre questo ordine operativo:

1. `Webhook Git` (prima scelta, no API Northflank locale)
2. `Manual deploy script` (fallback quando webhook non parte)

### 9.1 Webhook Git (prima scelta)
Comandi:
```bash
cd /Users/alessandro/clawdbot
git push origin main
```

Se serve forzare il deploy senza cambiare codice:
```bash
cd /Users/alessandro/clawdbot
git commit --allow-empty -m "trigger deploy"
git push origin main
```

Verifica obbligatoria post-deploy:
- cercare nuovo `[BOOT]` del container recente;
- confermare i flag critici runtime (esempio: `trade_all=True 5m=False` quando 5m deve restare spento).

### 9.2 Deploy manuale stabile (fallback senza webhook)
Quando webhook VCS e' instabile/non disponibile, usare questo metodo.

Script versionato:
- `scripts/nf_manual_deploy.sh`

Comando:
```bash
cd /Users/alessandro/clawdbot
./scripts/nf_manual_deploy.sh
```

Cosa fa:
1. push su `origin/main`
2. trigger build Northflank con SHA esplicita (`start service build --input {"sha":...}`)
3. polling fino a `SUCCESS/FAILED`
4. request deploy su stessa SHA
5. verifica finale `deployedSHA == git rev-parse HEAD`

Regola operativa:
- Se il deploy automatico (webhook) non parte, non usare fix ad-hoc: usare solo questo script.
