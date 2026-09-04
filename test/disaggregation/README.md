# Disaggregation rollout matrix

This optional Kubernetes test drives real `DisaggregatedSet` rolling updates
and checks revision gating through both supported EPP topologies. It also
checks generic header-to-label affinity against the role labels on the same
Pods.

The matrix has 12 cases produced from three real rollouts:

| Dimension | Values |
|---|---|
| Rollout shape | `2p10d`, `10p10d`, `10p2d` |
| EPP topology | single EPP, separate prefill and decode EPPs |
| Gating mode | `sum`, `max-role` |

For every stable transition state, the test sends 1,000 requests per selected
topology and mode. It fails on persistent request errors, cross-revision
prefill/decode pairs, or observed traffic outside 6 percentage points of the
share computed from Ready pods.

Transport errors and HTTP 5xx responses are retried up to three times to avoid
failing a rollout on a transient connection termination. A persistent error or
any cross-revision selection still fails the test.

## Flow

Single EPP:

```text
request -> Envoy -> one EPP -> prefill pod
                       |
                       +------> decode pod -> response
```

Two EPPs:

```text
request -> prefill Envoy/EPP -> prefill pod
                    |
                    +-- x-llm-d-disagg-revision header
                                   |
                                   v
                    decode Envoy/EPP -> decode pod -> response
```

The runner creates revision A at the requested P:D shape, changes the
`DisaggregatedSet` pod template to create revision B, and deliberately promotes
one B pod at a time. At each quiet transition state it compares observed
traffic with these weights:

```text
sum:      prefill Ready pods + decode Ready pods
max-role: Ready pods for the globally most numerous role
```

A revision must have at least one Ready prefill and decode pod before either
mode gives it traffic.

The `disaggregatedset-rollout-screener` is declared in the top-level `plugins`
list but is not referenced by a scheduling profile. It screens the endpoint set
before either topology executes profile filters, scorers, or pickers. The test
EPPs pass `--allow-experimental-plugins` because the new plugins have Alpha
stability.

One additional EPP uses one preferred-role header/label mapping for both soft
affinity and response-header stamping:

```yaml
plugins:
- type: header-label-affinity-scorer
  name: role-affinity
  parameters:
    headerName: x-preferred-role
    labelKey: disaggregatedset.x-k8s.io/role
- type: weighted-random-picker
  name: picker

schedulingProfiles:
- name: default
  plugins:
  - pluginRef: role-affinity
    weight: 10
  - pluginRef: picker
```

Response-header stamping is intentionally omitted from the configuration so
the test exercises its `true` default. At each initial stable state, the
preference check requests `prefill` and then `decode`. It verifies the selected
Pod and the stamped `x-preferred-role` response header match the requested
role. Non-matching endpoints remain eligible because the scorer is a soft
preference.

## Prerequisites

- Docker and a registry reachable from the Kubernetes cluster
- `kubectl`, `curl`, and Python 3.10 or newer
- a cluster with the LeaderWorkerSet and DisaggregatedSet CRDs and controllers
- enough capacity for the largest rollout (10 prefill and 10 decode pods,
  plus seven EPP/Envoy pods)

The scripts use a dedicated `llm-d-test-disagg-matrix` namespace by default.
They delete and recreate the `revision-rollout` DisaggregatedSet in that
namespace.

## Deploy

The default images use a registry at `localhost:5000`:

```bash
./test/disaggregation/scripts/deploy-matrix.sh
```

Override the image names when the cluster uses another registry:

```bash
EPP_IMAGE=registry.example/epp:dev \
BACKEND_IMAGE=registry.example/disagg-rollout-backend:dev \
./test/disaggregation/scripts/deploy-matrix.sh
```

Use `--skip-build` to deploy images that already exist. The deployment is
self-contained in this repository and does not require a Helm chart.

## Run

Run all 12 cases and produce raw and Markdown reports under `reports/`:

```bash
./test/disaggregation/scripts/run-rollout-matrix.sh
```

Select one scenario:

```bash
./test/disaggregation/scripts/run-rollout-matrix.sh \
  --shape 2p10d \
  --topology two-epp \
  --mode max-role
```

Each selector can be repeated. Omitting a selector runs every value in that
dimension. Set `REPORT_FILE` to choose the raw report path. Set `NAMESPACE` and
`BACKEND_IMAGE` to the same values used during deployment when overriding the
defaults.

The scripts print the command, every sampled rollout state, observed and
expected percentages, and the final report paths. Kubernetes operations are
contained in the scripts; no separate inspection or patch commands are needed.
