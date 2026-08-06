#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_DIR="$(cd "$TEST_DIR/../.." && pwd)"
NAMESPACE="${NAMESPACE:-llm-d-test-disagg-matrix}"
EPP_IMAGE="${EPP_IMAGE:-localhost:5000/llm-d-epp-native:latest}"
BACKEND_IMAGE="${BACKEND_IMAGE:-localhost:5000/disagg-rollout-backend:latest}"
SKIP_BUILD=false

usage() {
    cat <<EOF
Usage: $0 [--skip-build]

Build and deploy the seven EPPs used by the disaggregation rollout matrix.

Environment:
  NAMESPACE      Kubernetes namespace (default: $NAMESPACE)
  EPP_IMAGE      EPP image to build and deploy (default: $EPP_IMAGE)
  BACKEND_IMAGE  test backend image to build (default: $BACKEND_IMAGE)
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-build) SKIP_BUILD=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done

for command in kubectl python3; do
    command -v "$command" >/dev/null || {
        echo "$command is required" >&2
        exit 1
    }
done

if [[ "$SKIP_BUILD" == false ]] && ! command -v docker >/dev/null; then
    echo "docker is required unless --skip-build is set" >&2
    exit 1
fi

kubectl get crd disaggregatedsets.disaggregatedset.x-k8s.io >/dev/null
kubectl get crd leaderworkersets.leaderworkerset.x-k8s.io >/dev/null

if [[ "$SKIP_BUILD" == false ]]; then
    echo "==> Building and pushing $EPP_IMAGE"
    docker build -f "$REPO_DIR/Dockerfile.epp" -t "$EPP_IMAGE" "$REPO_DIR"
    docker push "$EPP_IMAGE"

    echo "==> Building and pushing $BACKEND_IMAGE"
    docker build -t "$BACKEND_IMAGE" "$TEST_DIR/backend"
    docker push "$BACKEND_IMAGE"
fi

echo "==> Creating namespace $NAMESPACE"
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

render() {
    local template="$1"
    shift
    python3 - "$template" "$@" <<'PY'
import pathlib
import sys

text = pathlib.Path(sys.argv[1]).read_text()
for replacement in sys.argv[2:]:
    key, value = replacement.split("=", 1)
    text = text.replace(key, value)
sys.stdout.write(text)
PY
}

echo "==> Installing shared Envoy configuration"
python3 - "$REPO_DIR/deploy/environments/dev/e2e-infra/envoy.yaml" "$NAMESPACE" <<'PY' \
    | kubectl -n "$NAMESPACE" apply -f -
import pathlib
import sys

manifest = pathlib.Path(sys.argv[1]).read_text().split("\n---\n", 1)[0]
manifest = manifest.replace("  name: envoy\n", "  name: disagg-matrix-envoy\n", 1)
manifest = manifest.replace("e2e-epp.${NAMESPACE}", "127.0.0.1")
manifest = manifest.replace("${NAMESPACE}", sys.argv[2])
sys.stdout.write(manifest)
PY

install_epp() {
    local name="$1"
    local selector="$2"
    local mode="$3"
    local plugins_template="$4"

    render "$plugins_template" \
        "__NAME__=$name" \
        "__NAMESPACE__=$NAMESPACE" \
        "__MODE__=$mode" \
        | kubectl apply -f -
    render "$TEST_DIR/k8s/epp.yaml.tpl" \
        "__NAME__=$name" \
        "__NAMESPACE__=$NAMESPACE" \
        "__EPP_IMAGE__=$EPP_IMAGE" \
        "__ENDPOINT_SELECTOR__=$selector" \
        | kubectl apply -f -
}

all_selector="disaggregatedset.x-k8s.io/name=revision-rollout"
prefill_selector="$all_selector,disaggregatedset.x-k8s.io/role=prefill"
decode_selector="$all_selector,disaggregatedset.x-k8s.io/role=decode"

echo "==> Installing single-EPP cases"
install_epp matrix-single-sum-epp "$all_selector" sum \
    "$TEST_DIR/k8s/plugins-single.yaml.tpl"
install_epp matrix-single-max-role-epp "$all_selector" max-role \
    "$TEST_DIR/k8s/plugins-single.yaml.tpl"

echo "==> Installing two-EPP cases"
install_epp matrix-two-sum-prefill-epp "$prefill_selector" sum \
    "$TEST_DIR/k8s/plugins-two.yaml.tpl"
install_epp matrix-two-sum-decode-epp "$decode_selector" sum \
    "$TEST_DIR/k8s/plugins-two.yaml.tpl"
install_epp matrix-two-max-role-prefill-epp "$prefill_selector" max-role \
    "$TEST_DIR/k8s/plugins-two.yaml.tpl"
install_epp matrix-two-max-role-decode-epp "$decode_selector" max-role \
    "$TEST_DIR/k8s/plugins-two.yaml.tpl"

echo "==> Installing generic preference case"
install_epp matrix-prefer-epp "$all_selector" disabled \
    "$TEST_DIR/k8s/plugins-prefer.yaml.tpl"

echo "==> Waiting for EPP deployments"
for deployment in \
    matrix-single-sum-epp \
    matrix-single-max-role-epp \
    matrix-two-sum-prefill-epp \
    matrix-two-sum-decode-epp \
    matrix-two-max-role-prefill-epp \
    matrix-two-max-role-decode-epp \
    matrix-prefer-epp; do
    kubectl -n "$NAMESPACE" rollout status "deployment/$deployment" --timeout=300s
done

echo "Matrix EPPs are ready in namespace $NAMESPACE"
