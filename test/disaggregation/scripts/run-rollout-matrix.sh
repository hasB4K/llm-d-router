#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
NAMESPACE="${NAMESPACE:-llm-d-test-disagg-matrix}"
BACKEND_IMAGE="${BACKEND_IMAGE:-localhost:5000/disagg-rollout-backend:latest}"
REPORT_DIR="$TEST_DIR/reports"
REPORT_FILE="${REPORT_FILE:-$REPORT_DIR/rollout-matrix-$(date +%Y%m%d-%H%M%S).log}"
TABLE_REPORT_FILE="${TABLE_REPORT_FILE:-${REPORT_FILE%.*}.md}"
FORWARD_DIR="$(mktemp -d)"
FORWARD_PIDS=()

cleanup() {
    local pid
    for pid in "${FORWARD_PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    rm -r "$FORWARD_DIR"
}
trap cleanup EXIT INT TERM

for command in curl kubectl python3; do
    command -v "$command" >/dev/null || {
        echo "$command is required" >&2
        exit 1
    }
done

start_forward() {
    local service="$1"
    local local_port="$2"
    kubectl -n "$NAMESPACE" port-forward "service/$service" "$local_port:8081" \
        >"$FORWARD_DIR/$service.log" 2>&1 &
    FORWARD_PIDS+=("$!")
}

start_forward matrix-single-sum-epp 18081
start_forward matrix-single-max-role-epp 18082
start_forward matrix-two-sum-prefill-epp 18083
start_forward matrix-two-sum-decode-epp 18084
start_forward matrix-two-max-role-prefill-epp 18085
start_forward matrix-two-max-role-decode-epp 18086
start_forward matrix-prefer-epp 18087

for port in 18081 18082 18083 18084 18085 18086 18087; do
    for _ in $(seq 1 60); do
        if curl --silent --output /dev/null --max-time 1 \
            "http://127.0.0.1:$port/health" 2>/dev/null; then
            break
        fi
        sleep 1
    done
    if ! curl --silent --output /dev/null --max-time 1 \
        "http://127.0.0.1:$port/health" 2>/dev/null; then
        echo "port-forward on $port did not become ready" >&2
        cat "$FORWARD_DIR"/*.log >&2
        exit 1
    fi
done

mkdir -p "$(dirname "$REPORT_FILE")"
{
    printf 'Command:'
    printf ' %q' "$0" "$@"
    printf '\nNamespace: %s\nStarted: %s\n\n' \
        "$NAMESPACE" "$(date '+%Y-%m-%dT%H:%M:%S%z')"
} | tee "$REPORT_FILE"

set +e
python3 -u "$SCRIPT_DIR/rollout_matrix.py" \
    --namespace "$NAMESPACE" \
    --backend-image "$BACKEND_IMAGE" \
    --table-report "$TABLE_REPORT_FILE" \
    "$@" 2>&1 | tee -a "$REPORT_FILE"
status="${PIPESTATUS[0]}"
set -e

printf '\nExit status: %s\nRaw report: %s\nTable report: %s\n' \
    "$status" "$REPORT_FILE" "$TABLE_REPORT_FILE" | tee -a "$REPORT_FILE"
exit "$status"
