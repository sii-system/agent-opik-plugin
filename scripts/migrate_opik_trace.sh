#!/usr/bin/env bash

# Usage:
#   SOURCE_URL=... TRACE_ID=... PROJECT=... EXPORT_DIR=... bash migrate_opik_trace.sh export
#   DEST_URL=... PROJECT=... TARGET_PROJECT=... EXPORT_DIR=... bash migrate_opik_trace.sh import

set -euo pipefail

OPIK=(uvx --python 3.10 --from opik==2.1.27 --with socksio opik)

export_trace() {
  : "${SOURCE_URL:?Set SOURCE_URL}"
  : "${TRACE_ID:?Set TRACE_ID}"
  : "${PROJECT:?Set PROJECT}"
  : "${EXPORT_DIR:?Set EXPORT_DIR}"

  env -u OPIK_API_KEY OPIK_URL_OVERRIDE="${SOURCE_URL}" \
    "${OPIK[@]}" export default "${PROJECT}" traces \
    --filter "id = \"${TRACE_ID}\"" --max-results 1 \
    --path "${EXPORT_DIR}" --format json --page-size 1000 --no-attachments
}

import_trace() {
  : "${DEST_URL:?Set DEST_URL}"
  : "${PROJECT:?Set PROJECT}"
  : "${TARGET_PROJECT:?Set TARGET_PROJECT}"
  : "${EXPORT_DIR:?Set EXPORT_DIR}"

  env -u OPIK_API_KEY OPIK_URL_OVERRIDE="${DEST_URL}" \
    "${OPIK[@]}" import default "${PROJECT}" traces \
    --path "${EXPORT_DIR}" --to-workspace default \
    --to-project "${TARGET_PROJECT}" --no-attachments "$@"
}

case "${1:-}" in
  export) export_trace ;;
  import) shift; import_trace "$@" ;;
  *) echo "Usage: $0 {export|import}" >&2; exit 1 ;;
esac
