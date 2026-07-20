#!/usr/bin/env bash

# Replay OpenAI-compatible requests from an exported Opik trace.
#
# Dependencies: bash, curl, jq. Missing curl/jq are installed automatically
# through Homebrew, apt-get, dnf, or yum. Set AUTO_INSTALL_DEPS=0 to disable.
#
# Dry-run is the default. Requests are sent only when --execute is supplied.

set -o pipefail

usage() {
  cat <<'EOF'
Usage:
  bash replay_opik_trace.sh TRACE_JSON [options]

Options:
  --execute                    Send requests (default is dry-run)
  --base-url URL               Target origin, e.g. https://gateway.example
  --model MODEL                Override the captured model
  --round N                    Select one 1-based round; repeatable
  --from-round N               First round to replay
  --to-round N                 Last round to replay
  --only-errors                Select spans that failed in the original trace
  --repeat N                   Replay the selected sequence N times (default: 1)
  --delay SECONDS              Delay between requests (default: 0)
  --session-id ID              Use ID for X-Session-ID and X-Session-Affinity
  --reuse-original-session     Reuse the captured session ID
  --no-session-headers         Do not send session headers
  --api-key-env ENV            ENV contains the Bearer token (default: OPENAI_API_KEY)
  --header NAME=VALUE          Add a header; repeatable
  --header-env NAME=ENV        Read a header value from ENV; repeatable
  --timeout SECONDS            Per-request maximum time (default: 600)
  --output-dir DIR             New directory for summary and raw responses
  --stop-on-error              Stop after the first replay failure
  --ca-file FILE               Use a custom CA bundle
  --insecure                   Disable TLS verification
  -h, --help                   Show this help

Environment:
  AUTO_INSTALL_DEPS=0          Do not install missing curl/jq automatically

The script honors curl proxy variables such as ALL_PROXY, HTTPS_PROXY, and
NO_PROXY. curl supports SOCKS directly; Python socksio is not needed.
EOF
}

die() {
  printf 'Error: %s\n' "$*" >&2
  exit 2
}

is_positive_integer() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

is_nonnegative_number() {
  [[ "$1" =~ ^[0-9]+([.][0-9]+)?$ ]]
}

need_value() {
  [[ $# -ge 2 ]] || die "$1 requires a value"
}

TRACE_FILE=""
EXECUTE=0
BASE_URL=""
MODEL=""
FROM_ROUND=0
TO_ROUND=0
ONLY_ERRORS=0
REPEAT=1
DELAY=0
SESSION_ID=""
SESSION_MODE="fresh"
API_KEY_ENV="OPENAI_API_KEY"
TIMEOUT=600
OUTPUT_DIR=""
STOP_ON_ERROR=0
CA_FILE=""
INSECURE=0
ROUNDS=()
EXTRA_HEADERS=()
ENV_HEADERS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --execute)
      EXECUTE=1
      shift
      ;;
    --base-url)
      need_value "$@"
      BASE_URL="$2"
      shift 2
      ;;
    --model)
      need_value "$@"
      MODEL="$2"
      shift 2
      ;;
    --round)
      need_value "$@"
      is_positive_integer "$2" || die "--round must be a positive integer"
      ROUNDS+=("$2")
      shift 2
      ;;
    --from-round)
      need_value "$@"
      is_positive_integer "$2" || die "--from-round must be a positive integer"
      FROM_ROUND="$2"
      shift 2
      ;;
    --to-round)
      need_value "$@"
      is_positive_integer "$2" || die "--to-round must be a positive integer"
      TO_ROUND="$2"
      shift 2
      ;;
    --only-errors)
      ONLY_ERRORS=1
      shift
      ;;
    --repeat)
      need_value "$@"
      is_positive_integer "$2" || die "--repeat must be a positive integer"
      REPEAT="$2"
      shift 2
      ;;
    --delay)
      need_value "$@"
      is_nonnegative_number "$2" || die "--delay must be a non-negative number"
      DELAY="$2"
      shift 2
      ;;
    --session-id)
      need_value "$@"
      [[ "$SESSION_MODE" == "fresh" ]] || die "session options are mutually exclusive"
      SESSION_ID="$2"
      SESSION_MODE="explicit"
      shift 2
      ;;
    --reuse-original-session)
      [[ "$SESSION_MODE" == "fresh" ]] || die "session options are mutually exclusive"
      SESSION_MODE="original"
      shift
      ;;
    --no-session-headers)
      [[ "$SESSION_MODE" == "fresh" ]] || die "session options are mutually exclusive"
      SESSION_MODE="none"
      shift
      ;;
    --api-key-env)
      need_value "$@"
      API_KEY_ENV="$2"
      shift 2
      ;;
    --header)
      need_value "$@"
      [[ "$2" == *=* ]] || die "--header must use NAME=VALUE syntax"
      EXTRA_HEADERS+=("$2")
      shift 2
      ;;
    --header-env)
      need_value "$@"
      [[ "$2" == *=* ]] || die "--header-env must use NAME=ENV syntax"
      ENV_HEADERS+=("$2")
      shift 2
      ;;
    --timeout)
      need_value "$@"
      is_nonnegative_number "$2" || die "--timeout must be a positive number"
      [[ "$2" != "0" && "$2" != "0.0" ]] || die "--timeout must be positive"
      TIMEOUT="$2"
      shift 2
      ;;
    --output-dir)
      need_value "$@"
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --stop-on-error)
      STOP_ON_ERROR=1
      shift
      ;;
    --ca-file)
      need_value "$@"
      CA_FILE="$2"
      shift 2
      ;;
    --insecure)
      INSECURE=1
      shift
      ;;
    --*)
      die "unknown option: $1"
      ;;
    *)
      [[ -z "$TRACE_FILE" ]] || die "unexpected positional argument: $1"
      TRACE_FILE="$1"
      shift
      ;;
  esac
done

[[ -n "$TRACE_FILE" ]] || { usage >&2; die "TRACE_JSON is required"; }
[[ -f "$TRACE_FILE" ]] || die "trace file does not exist: $TRACE_FILE"
[[ "$FROM_ROUND" -eq 0 || "$TO_ROUND" -eq 0 || "$FROM_ROUND" -le "$TO_ROUND" ]] \
  || die "--from-round cannot be greater than --to-round"
[[ "$INSECURE" -eq 0 || -z "$CA_FILE" ]] || die "--insecure and --ca-file are mutually exclusive"
[[ -z "$CA_FILE" || -f "$CA_FILE" ]] || die "CA file does not exist: $CA_FILE"

install_dependencies() {
  local missing=()
  command -v curl >/dev/null 2>&1 || missing+=(curl)
  command -v jq >/dev/null 2>&1 || missing+=(jq)
  [[ ${#missing[@]} -gt 0 ]] || return 0

  printf 'Missing dependencies: %s\n' "${missing[*]}" >&2
  [[ "${AUTO_INSTALL_DEPS:-1}" != "0" ]] \
    || die "install ${missing[*]} or run again with AUTO_INSTALL_DEPS=1"

  if command -v brew >/dev/null 2>&1; then
    brew install "${missing[@]}" || die "Homebrew could not install dependencies"
  elif command -v apt-get >/dev/null 2>&1; then
    local prefix=()
    [[ "$EUID" -eq 0 ]] || prefix=(sudo)
    "${prefix[@]}" apt-get update || die "apt-get update failed"
    "${prefix[@]}" apt-get install -y "${missing[@]}" || die "apt-get install failed"
  elif command -v dnf >/dev/null 2>&1; then
    local prefix=()
    [[ "$EUID" -eq 0 ]] || prefix=(sudo)
    "${prefix[@]}" dnf install -y "${missing[@]}" || die "dnf install failed"
  elif command -v yum >/dev/null 2>&1; then
    local prefix=()
    [[ "$EUID" -eq 0 ]] || prefix=(sudo)
    "${prefix[@]}" yum install -y "${missing[@]}" || die "yum install failed"
  else
    die "no supported package manager found; install curl and jq manually"
  fi

  command -v curl >/dev/null 2>&1 || die "curl is still unavailable after installation"
  command -v jq >/dev/null 2>&1 || die "jq is still unavailable after installation"
}

install_dependencies

jq -e 'type == "object" and (.spans | type == "array")' "$TRACE_FILE" >/dev/null \
  || die "file is not an Opik trace export containing a spans array"

TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/opik-trace-replay.XXXXXX") \
  || die "could not create a temporary directory"
trap 'rm -rf "$TMP_DIR"' EXIT INT TERM

ROUNDS_JSON='[]'
if [[ ${#ROUNDS[@]} -gt 0 ]]; then
  ROUNDS_JSON=$(printf '%s\n' "${ROUNDS[@]}" | jq -s 'map(tonumber)') \
    || die "could not parse --round values"
fi

ALL_CALLS_FILE="$TMP_DIR/all_calls.jsonl"
SELECTED_FILE="$TMP_DIR/selected_calls.jsonl"

jq -c '
  [.spans[]
   | select(.type == "llm")
   | select(.input | type == "object")
   | select(.input.body | type == "object")
   | select((.input.method // "POST" | ascii_upcase) == "POST")
   | select(.input.path | type == "string" and startswith("/"))]
  | sort_by(.start_time // "")
  | to_entries[]
  | {round: (.key + 1), span: .value}
' "$TRACE_FILE" > "$ALL_CALLS_FILE" || die "failed to parse replayable spans"

TOTAL_CALLS=$(wc -l < "$ALL_CALLS_FILE" | tr -d ' ')
[[ "$TOTAL_CALLS" -gt 0 ]] || die "no replayable LLM POST spans were found"

for round in "${ROUNDS[@]}"; do
  [[ "$round" -le "$TOTAL_CALLS" ]] || die "--round must be between 1 and $TOTAL_CALLS: $round"
done
[[ "$FROM_ROUND" -eq 0 || "$FROM_ROUND" -le "$TOTAL_CALLS" ]] \
  || die "--from-round must be between 1 and $TOTAL_CALLS"
[[ "$TO_ROUND" -eq 0 || "$TO_ROUND" -le "$TOTAL_CALLS" ]] \
  || die "--to-round must be between 1 and $TOTAL_CALLS"

jq -c \
  --argjson rounds "$ROUNDS_JSON" \
  --argjson from "$FROM_ROUND" \
  --argjson to "$TO_ROUND" \
  --argjson only_errors "$ONLY_ERRORS" '
    .round as $current_round
    | select(($rounds | length) == 0 or ($rounds | index($current_round)) != null)
    | select($from == 0 or .round >= $from)
    | select($to == 0 or .round <= $to)
    | select($only_errors == 0 or (.span.metadata["frontgate.error_message"] // "") != "")
  ' "$ALL_CALLS_FILE" > "$SELECTED_FILE" || die "failed to filter calls"

SELECTED_COUNT=$(wc -l < "$SELECTED_FILE" | tr -d ' ')
[[ "$SELECTED_COUNT" -gt 0 ]] || die "the filters selected zero calls"

printf 'round\tmessages\tstream\tmodel\tcaptured\tspan_id\n'
jq -r --arg override_model "$MODEL" '[
    .round,
    (.span.input.body.messages // [] | length),
    (.span.input.body.stream // false),
    (if $override_model == "" then (.span.input.body.model // "") else $override_model end),
    (if (.span.metadata["frontgate.error_message"] // "") != "" then "ERROR" else "ok" end),
    (.span.id // "-")
  ] | @tsv' "$SELECTED_FILE"

SOURCE_URL=$(jq -r -s '
  .[0].span.input.headers as $h
  | if ($h.host // "") == "" then ""
    else (($h["x-forwarded-proto"] // "https") + "://" + $h.host)
    end
' "$ALL_CALLS_FILE")

printf '\nReplayable calls: %s; selected: %s; repeat: %s\n' "$TOTAL_CALLS" "$SELECTED_COUNT" "$REPEAT"
[[ -z "$SOURCE_URL" ]] || printf 'Captured source (informational only): %s\n' "$SOURCE_URL"

if [[ "$EXECUTE" -eq 0 ]]; then
  printf 'Dry-run only: no requests were sent. Add --execute and --base-url to replay.\n'
  exit 0
fi

[[ -n "$BASE_URL" ]] || die "--base-url is required with --execute"
[[ "$BASE_URL" =~ ^https?://[^/]+(/.*)?$ ]] || die "--base-url must be an absolute HTTP(S) URL"

case "$SESSION_MODE" in
  fresh)
    SESSION_ID="ses_replay_$(date +%s)_$$_${RANDOM}"
    ;;
  original)
    SESSION_ID=$(jq -r -s '
      .[0].span.input.headers["x-session-id"]
      // .[0].span.input.headers["x-session-affinity"]
      // ""
    ' "$ALL_CALLS_FILE")
    [[ -n "$SESSION_ID" ]] || die "captured trace has no session ID"
    ;;
  none)
    SESSION_ID=""
    ;;
esac

CURL_HEADERS=(
  -H 'Accept: text/event-stream, application/json'
  -H 'Content-Type: application/json'
  -H 'User-Agent: opik-trace-replay/1.0'
)

AUTH_CONFIGURED=0
if [[ -n "$API_KEY_ENV" ]]; then
  API_KEY=$(printenv "$API_KEY_ENV" 2>/dev/null || true)
  if [[ -n "$API_KEY" ]]; then
    CURL_HEADERS+=(-H "Authorization: Bearer $API_KEY")
    AUTH_CONFIGURED=1
  fi
fi

for assignment in "${EXTRA_HEADERS[@]}"; do
  name=${assignment%%=*}
  value=${assignment#*=}
  [[ -n "$name" ]] || die "header name cannot be empty"
  CURL_HEADERS+=(-H "$name: $value")
  [[ "$name" != "Authorization" && "$name" != "authorization" ]] || AUTH_CONFIGURED=1
done

for assignment in "${ENV_HEADERS[@]}"; do
  name=${assignment%%=*}
  env_name=${assignment#*=}
  [[ -n "$name" ]] || die "header name cannot be empty"
  [[ "$env_name" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || die "invalid environment variable: $env_name"
  value=$(printenv "$env_name" 2>/dev/null) || die "environment variable is not set: $env_name"
  CURL_HEADERS+=(-H "$name: $value")
  [[ "$name" != "Authorization" && "$name" != "authorization" ]] || AUTH_CONFIGURED=1
done

if [[ -n "$SESSION_ID" ]]; then
  CURL_HEADERS+=(-H "X-Session-ID: $SESSION_ID" -H "X-Session-Affinity: $SESSION_ID")
fi

CURL_TLS=()
[[ "$INSECURE" -eq 0 ]] || CURL_TLS+=(--insecure)
[[ -z "$CA_FILE" ]] || CURL_TLS+=(--cacert "$CA_FILE")

if [[ -z "$OUTPUT_DIR" ]]; then
  trace_name=$(basename "$TRACE_FILE" .json)
  OUTPUT_DIR="replay-results/${trace_name}-$(date +%Y%m%dT%H%M%S)"
fi
[[ ! -e "$OUTPUT_DIR" ]] || die "output directory already exists: $OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR" || die "could not create output directory: $OUTPUT_DIR"
SUMMARY_FILE="$OUTPUT_DIR/summary.jsonl"
: > "$SUMMARY_FILE"

printf 'Target: %s\n' "$BASE_URL"
printf 'Session ID: %s\n' "${SESSION_ID:-none}"
if [[ "$AUTH_CONFIGURED" -eq 1 ]]; then
  printf 'Authorization: configured\n'
else
  printf 'Authorization: not configured\n'
fi
printf 'Results: %s\n' "$OUTPUT_DIR"

extract_server_error() {
  local response_file=$1
  local is_stream=$2
  if [[ "$is_stream" == "true" ]]; then
    jq -Rrs '
      def error_text:
        if type == "object" then (.message // .detail // tostring) else tostring end;
      [split("\n")[]
       | select(test("^data:[[:space:]]*"))
       | sub("^data:[[:space:]]*"; "")
       | fromjson?
       | select(type == "object")
       | if .error != null then (.error | error_text)
         elif .object == "error" then (.message // tostring)
         else empty
         end][0] // ""
    ' "$response_file" 2>/dev/null
  else
    jq -r '
      if type == "object" and .error != null then
        (.error | if type == "object" then (.message // .detail // tostring) else tostring end)
      elif type == "object" and .object == "error" then (.message // tostring)
      else ""
      end
    ' "$response_file" 2>/dev/null || true
  fi
}

FAILURE_COUNT=0
STOP=0
cycle=1
while [[ "$cycle" -le "$REPEAT" ]]; do
  call_position=0
  while IFS= read -r record; do
    call_position=$((call_position + 1))
    if [[ "$call_position" -gt 1 ]]; then
      sleep "$DELAY"
    fi

    round=$(printf '%s' "$record" | jq -r '.round')
    span_id=$(printf '%s' "$record" | jq -r '.span.id // "unknown"')
    path=$(printf '%s' "$record" | jq -r '.span.input.path')
    message_count=$(printf '%s' "$record" | jq -r '.span.input.body.messages // [] | length')
    stream=$(printf '%s' "$record" | jq -r '.span.input.body.stream // false')
    captured_start=$(printf '%s' "$record" | jq -r '.span.start_time // ""')
    captured_error=$(printf '%s' "$record" | jq -r '.span.metadata["frontgate.error_message"] // ""')
    captured_model=$(printf '%s' "$record" | jq -r '.span.input.body.model // ""')
    effective_model=${MODEL:-$captured_model}

    request_file="$TMP_DIR/request.json"
    if [[ -n "$MODEL" ]]; then
      printf '%s' "$record" | jq --arg model "$MODEL" '.span.input.body | .model = $model' > "$request_file"
    else
      printf '%s' "$record" | jq '.span.input.body' > "$request_file"
    fi

    target="${BASE_URL%/}${path}"
    response_name=$(printf 'cycle_%03d_round_%03d_%s.response' "$cycle" "$round" "$span_id")
    response_file="$OUTPUT_DIR/$response_name"
    : > "$response_file"

    printf 'cycle %s/%s, round %s: sending %s messages...\n' \
      "$cycle" "$REPEAT" "$round" "$message_count"

    metrics=""
    if metrics=$(curl \
      --silent --show-error --no-buffer \
      --request POST \
      --max-time "$TIMEOUT" \
      "${CURL_TLS[@]}" \
      "${CURL_HEADERS[@]}" \
      --data-binary "@$request_file" \
      --output "$response_file" \
      --write-out $'%{http_code}\t%{time_starttransfer}\t%{time_total}' \
      "$target"); then
      curl_rc=0
    else
      curl_rc=$?
    fi

    IFS=$'\t' read -r http_status ttft duration <<< "$metrics"
    http_status=${http_status:-000}
    ttft=${ttft:-0}
    duration=${duration:-0}
    server_error=$(extract_server_error "$response_file" "$stream")

    stream_done="null"
    incomplete_stream=0
    if [[ "$stream" == "true" ]]; then
      if grep -Eq '^data:[[:space:]]*\[DONE\][[:space:]]*$' "$response_file"; then
        stream_done="true"
      else
        stream_done="false"
        incomplete_stream=1
      fi
    fi

    failed=0
    failure_detail=""
    if [[ "$curl_rc" -ne 0 ]]; then
      failed=1
      failure_detail="curl exited with code $curl_rc"
    elif [[ "$http_status" =~ ^[45][0-9][0-9]$ ]]; then
      failed=1
      failure_detail="HTTP $http_status"
    elif [[ -n "$server_error" ]]; then
      failed=1
      failure_detail="$server_error"
    elif [[ "$incomplete_stream" -eq 1 ]]; then
      failed=1
      failure_detail="SSE stream closed without data: [DONE]"
    fi

    jq -nc \
      --argjson cycle "$cycle" \
      --argjson round "$round" \
      --arg span_id "$span_id" \
      --arg captured_start_time "$captured_start" \
      --arg captured_error "$captured_error" \
      --arg request_url "$target" \
      --arg model "$effective_model" \
      --argjson message_count "$message_count" \
      --argjson stream "$stream" \
      --arg response_file "$response_name" \
      --arg http_status "$http_status" \
      --arg ttft_seconds "$ttft" \
      --arg duration_seconds "$duration" \
      --argjson stream_done "$stream_done" \
      --argjson curl_exit_code "$curl_rc" \
      --arg server_error "$server_error" \
      --arg failure_detail "$failure_detail" \
      --argjson failed "$failed" '
        {
          cycle: $cycle,
          round: $round,
          span_id: $span_id,
          captured_start_time: $captured_start_time,
          captured_error: (if $captured_error == "" then null else $captured_error end),
          request_url: $request_url,
          model: $model,
          message_count: $message_count,
          stream: $stream,
          response_file: $response_file,
          http_status: ($http_status | tonumber? // 0),
          ttft_seconds: ($ttft_seconds | tonumber? // 0),
          duration_seconds: ($duration_seconds | tonumber? // 0),
          stream_done: $stream_done,
          curl_exit_code: $curl_exit_code,
          server_error: (if $server_error == "" then null else $server_error end),
          failure_detail: (if $failure_detail == "" then null else $failure_detail end),
          failed: ($failed == 1)
        }
      ' >> "$SUMMARY_FILE"

    if [[ "$failed" -eq 1 ]]; then
      FAILURE_COUNT=$((FAILURE_COUNT + 1))
      printf 'cycle %s/%s, round %s: FAILED; HTTP %s; %ss; TTFT %ss\n' \
        "$cycle" "$REPEAT" "$round" "$http_status" "$duration" "$ttft"
      printf '  error: %s\n' "$failure_detail" >&2
      if [[ "$STOP_ON_ERROR" -eq 1 ]]; then
        STOP=1
        break
      fi
    else
      printf 'cycle %s/%s, round %s: ok; HTTP %s; %ss; TTFT %ss\n' \
        "$cycle" "$REPEAT" "$round" "$http_status" "$duration" "$ttft"
    fi
  done < "$SELECTED_FILE"

  [[ "$STOP" -eq 0 ]] || break
  cycle=$((cycle + 1))
done

printf 'Finished: failures=%s; summary=%s\n' "$FAILURE_COUNT" "$SUMMARY_FILE"
[[ "$FAILURE_COUNT" -eq 0 ]] || exit 2
