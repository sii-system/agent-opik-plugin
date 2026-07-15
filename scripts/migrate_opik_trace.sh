#!/usr/bin/env bash

# Usage:
#   bash migrate_opik_trace.sh dry-run  # Preview without writing to Opik2
#   bash migrate_opik_trace.sh import   # Import into Opik2
#   TRACE_ID=xxx PROJECT=src TARGET_PROJECT=dest bash migrate_opik_trace.sh import
# Environment overrides: SOURCE_URL, DEST_URL, TRACE_ID, PROJECT, TARGET_PROJECT

set -euo pipefail

MODE="${1:-dry-run}" # dry-run or import
SOURCE_URL="${SOURCE_URL:-http://10.252.252.41:8082/api/}"
DEST_URL="${DEST_URL:-https://opik-pre.sii.edu.cn/api/}"
TRACE_ID="${TRACE_ID:-6d4823c7-0baf-7d48-a3c7-0baff7bece92}"
PROJECT="${PROJECT:-sii-sglang-traces}"
TARGET_PROJECT="${TARGET_PROJECT:-sii-sglang-traces-opik-import}"
EXPORT_DIR="/tmp/opik-migration/${TRACE_ID}"
OPIK=(uvx --python 3.10 --from opik==2.1.27 opik)

[[ "${MODE}" == "dry-run" || "${MODE}" == "import" ]] || {
  echo "Usage: $0 [dry-run|import]" >&2
  exit 1
}

# Export the selected trace and all of its spans from Opik1 without attachments.
env -u OPIK_API_KEY \
  OPIK_URL_OVERRIDE="${SOURCE_URL}" \
  "${OPIK[@]}" export default "${PROJECT}" traces \
  --filter "id = \"${TRACE_ID}\"" --max-results 1 \
  --path "${EXPORT_DIR}" --format json --page-size 1000 --no-attachments

import_trace() {
  env -u OPIK_API_KEY \
    OPIK_URL_OVERRIDE="${DEST_URL}" \
    "${OPIK[@]}" import default "${PROJECT}" traces \
    --path "${EXPORT_DIR}" --to-workspace default \
    --to-project "${TARGET_PROJECT}" --no-attachments "$@"
}

if [[ "${MODE}" == "dry-run" ]]; then
  import_trace --dry-run
else
  import_trace
fi
