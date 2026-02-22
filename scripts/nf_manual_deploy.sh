#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-poly2}"
SERVICE_ID="${SERVICE_ID:-clawdbot}"
REPO_DIR="${REPO_DIR:-/Users/alessandro/clawdbot}"
BUILD_TIMEOUT_SEC="${BUILD_TIMEOUT_SEC:-900}"
DEPLOY_TIMEOUT_SEC="${DEPLOY_TIMEOUT_SEC:-600}"
POLL_SEC="${POLL_SEC:-5}"

cd "${REPO_DIR}"

sha="$(git rev-parse HEAD)"
echo "[NF-MANUAL] sha=${sha}"

echo "[NF-MANUAL] push origin main"
git push origin main

echo "[NF-MANUAL] start build for sha"
build_json="$(northflank start service build \
  --projectId "${PROJECT_ID}" \
  --serviceId "${SERVICE_ID}" \
  --input "{\"sha\":\"${sha}\"}" \
  --output json)"

build_id="$(python3 - <<'PY' "${build_json}"
import json, sys
obj = json.loads(sys.argv[1])
print(obj.get("id", ""))
PY
)"

if [[ -z "${build_id}" ]]; then
  echo "[NF-MANUAL][ERR] build id missing"
  exit 1
fi

echo "[NF-MANUAL] build_id=${build_id}"

start_ts="$(date +%s)"
while true; do
  builds_json="$(northflank get service builds --projectId "${PROJECT_ID}" --serviceId "${SERVICE_ID}" --per_page 40 --output json)"
  status="$(python3 - <<'PY' "${builds_json}" "${build_id}"
import json, sys
obj = json.loads(sys.argv[1])
target = sys.argv[2]
for b in obj.get("builds", []):
    if b.get("id") == target:
        print(b.get("status", ""))
        break
else:
    print("")
PY
)"
  now_ts="$(date +%s)"
  elapsed="$((now_ts - start_ts))"
  if [[ "${status}" == "SUCCESS" ]]; then
    echo "[NF-MANUAL] build success"
    break
  fi
  if [[ "${status}" == "FAILED" || "${status}" == "CANCELLED" ]]; then
    echo "[NF-MANUAL][ERR] build status=${status}"
    exit 1
  fi
  if (( elapsed > BUILD_TIMEOUT_SEC )); then
    echo "[NF-MANUAL][ERR] build timeout after ${elapsed}s"
    exit 1
  fi
  echo "[NF-MANUAL] build status=${status:-PENDING} elapsed=${elapsed}s"
  sleep "${POLL_SEC}"
done

echo "[NF-MANUAL] request deployment for sha"
northflank update service deployment \
  --projectId "${PROJECT_ID}" \
  --serviceId "${SERVICE_ID}" \
  --input "{\"branch\":\"main\",\"buildSHA\":\"${sha}\"}" \
  --output json >/dev/null

start_ts="$(date +%s)"
while true; do
  dep_json="$(northflank get service deployment --projectId "${PROJECT_ID}" --serviceId "${SERVICE_ID}" --output json)"
  deployed_sha="$(python3 - <<'PY' "${dep_json}"
import json, sys
obj = json.loads(sys.argv[1])
print((obj.get("internal") or {}).get("deployedSHA", ""))
PY
)"
  now_ts="$(date +%s)"
  elapsed="$((now_ts - start_ts))"
  if [[ "${deployed_sha}" == "${sha}" ]]; then
    echo "[NF-MANUAL] deployed sha=${deployed_sha}"
    exit 0
  fi
  if (( elapsed > DEPLOY_TIMEOUT_SEC )); then
    echo "[NF-MANUAL][ERR] deploy timeout: deployed=${deployed_sha} expected=${sha}"
    exit 1
  fi
  echo "[NF-MANUAL] waiting deploy deployed=${deployed_sha} elapsed=${elapsed}s"
  sleep "${POLL_SEC}"
done
