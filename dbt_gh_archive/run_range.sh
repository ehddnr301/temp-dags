#!/usr/bin/env bash
set -euo pipefail

# 날짜 범위를 받아 날짜별로 dl_* + dw_activity_feed 적재
# 사용법:
#   ./run_range.sh START_DATE END_DATE [SELECTOR] [DEPS_MODE]
# 예시:
#   ./run_range.sh 2025-08-01 2025-08-05
#   ./run_range.sh 2025-08-01 2025-08-05 "dl_* dw_activity_feed"
#   ./run_range.sh 2025-08-01 2025-08-05 "dw_activity_feed" both   # +model+

if [[ ${#} -lt 2 ]]; then
  echo "Usage: $0 START_DATE END_DATE [SELECTOR] [DEPS_MODE]" >&2
  echo "Example: $0 2025-08-01 2025-08-05 'dl_* dw_activity_feed' both" >&2
  exit 1
fi

START_DATE="$1"  # YYYY-MM-DD
END_DATE="$2"    # YYYY-MM-DD
SELECTOR="${3:-dl_* dw_activity_feed}"
DEPS_MODE="${4:-none}"   # none | parents | children | both

# 날짜 유효성 간단 체크 (YYYY-MM-DD)
if ! [[ "${START_DATE}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "Invalid START_DATE: ${START_DATE}" >&2
  exit 2
fi
if ! [[ "${END_DATE}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "Invalid END_DATE: ${END_DATE}" >&2
  exit 2
fi

# deps 모드 검증
case "${DEPS_MODE}" in
  none|parents|children|both) ;;
  *) echo "Invalid DEPS_MODE: ${DEPS_MODE} (allowed: none|parents|children|both)" >&2; exit 3;;
esac

# 선택자 토큰별로 +를 부착해 의존성 포함
decorate_selector() {
  local sel_str="$1" mode="$2"
  local tokens decorated=()
  # shellcheck disable=SC2206
  tokens=( ${sel_str} )
  for t in "${tokens[@]}"; do
    if [[ "${t}" == *+* ]]; then
      decorated+=("${t}")
    else
      case "${mode}" in
        parents)  decorated+=("+${t}") ;;
        children) decorated+=("${t}+") ;;
        both)     decorated+=("+${t}+") ;;
        none)     decorated+=("${t}") ;;
      esac
    fi
  done
  echo "${decorated[*]}"
}

SELECTOR="$(decorate_selector "${SELECTOR}" "${DEPS_MODE}")"

current="${START_DATE}"

while [[ "${current}" < "${END_DATE}" || "${current}" == "${END_DATE}" ]]; do
  echo "[run_range] Loading date=${current} selector=(${SELECTOR})"
  dbt run --profiles-dir /home/dwlee/de-gh-insights/dags/dbt_gh_archive/profiles -s ${SELECTOR} --vars "load_base_date_kst: '${current}'"

  # 다음 날짜로 증가
  current=$(date -d "${current} + 1 day" +%F)
done

echo "[run_range] Done: ${START_DATE}..${END_DATE}"


