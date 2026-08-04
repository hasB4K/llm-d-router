# DisaggregatedSet Rollout Screener

**Type:** `disaggregatedset-rollout-screener`
**Interfaces:** `requestcontrol.Screener`, `requestcontrol.ResponseHeaderProcessor`

This plugin prevents incompatible prefill and decode Pods from serving the same
request during a `DisaggregatedSet` rollout. It runs before every scheduling
profile, so the compatibility constraint cannot be undone by a later filter,
scorer, or picker.

## Why Revision Screening Is Needed

A `DisaggregatedSet` creates a new revision when any role changes. Every Pod in
that revision has the same `disaggregatedset.x-k8s.io/revision` label. The label
therefore identifies the prefill and decode Pods that were created from the
same role templates and should be treated as a compatibility boundary.

Pairing Pods from different revisions can be troublesome:

- The revisions can use incompatible KV-cache formats.
- Switching libraries or inference-engine versions can change the KV-cache
  transfer protocol without backward compatibility.
- An unreliable old revision can produce corrupted KV-cache state that should
  not reach the new revision.
- A rollout can move Pods to a different driver through a `nodeSelector` or
  toleration. Old and new drivers can be incompatible with the KV-cache
  transfer mechanism.

Rolling out all roles at exactly the same percentage is not always possible.
Replica counts are integers, and each role is constrained by its own surge and
unavailable limits. For example, consider a deployment whose stable shape is
2 prefill Pods and 10 decode Pods. An intermediate state can be:

```text
                 prefill   decode
old revision A      2         9
new revision B      1         1
```

Selecting a revision from the prefill pool alone would send about 2/3 of
requests to A and 1/3 to B. However, B has only 1 of the 10 decode Pods and can
represent only about 10 percent of decode capacity. That can overload B's
decode side while leaving A's decode capacity unused.

The
[DisaggregatedSet rollout KEP](https://github.com/kubernetes-sigs/lws/tree/main/keps/766-DisaggregatedSet)
explains how the controller keeps role progress as close as integer replica
counts permit. Run its rollout planner from the root of a
[`kubernetes-sigs/lws`](https://github.com/kubernetes-sigs/lws) checkout. Using
`maxUnavailable: 2` for both roles keeps this example short:

```bash
go run ./hack/plan-steps \
  --source '{"prefill": 2, "decode": 10}' \
  --target '{"prefill": 2, "decode": 10}' \
  --surge '{"prefill": 0, "decode": 0}' \
  --unavailable '{"prefill": 2, "decode": 2}'
```

```text
Roles: [decode prefill]
Source: decode=10, prefill=2
Target: decode=10, prefill=2
Config: decode(surge=0, unavailable=2), prefill(surge=0, unavailable=2)

Step  Old decode  Old prefill  New decode  New prefill  Total  Action
----  ----------  -----------  ----------  -----------  -----  -----------------------------
0     10          2            0           0            12     initial
1     8           2            0           0            10     old decode -2
2     8           2            2           0            12     new decode +2
3     6           1            2           0            9      old decode -2, old prefill -1
4     6           1            4           1            12     new decode +2, new prefill +1
5     4           1            4           1            10     old decode -2
6     4           1            6           1            12     new decode +2
7     2           1            6           1            10     old decode -2
8     2           1            8           1            12     new decode +2
9     0           0            8           1            9      old decode -2, old prefill -1
10    0           0            10          2            12     new decode +2, new prefill +1
```

At steps 2 and 3, the new revision has no prefill Pod, so it cannot serve a
request. Once both revisions are covered, decode is the globally largest role.
`max-role` therefore produces old/new shares of 60/40 at step 4, 40/60 at step
6, and 20/80 at step 8.

## Request Lifecycle

For every Pod notification, the plugin caches the Ready Pod count by revision
and role. For a request without a strict revision header, it then:

1. Removes every revision that has no Ready Pod for any required role, because
   a revision missing one role cannot serve the request.
2. Computes a weight for each remaining revision.
3. Randomly chooses one revision using those weights.
4. Exposes only that revision's endpoints to all scheduling profiles.
5. Stamps the selected endpoint's revision into the configured response header.

```text
Ready Pod counts
       |
       v
remove incomplete revisions -> choose one covered revision
       |                               |
       +-------------------------------+
                                       v
                        filters, scorers, and picker
                                       |
                                       v
                         stamp the selected revision
```

When a strict revision header is already present, the plugin does not make a
new weighted choice. It checks that the requested revision has all required
roles and keeps only endpoints with that revision.

### Two EPPs

The prefill EPP chooses a covered revision. When the selected prefill begins
responding, the response-header hook stamps its revision. The coordinator must
forward that header to the decode request, where the decode EPP applies it
strictly:

```text
prefill request -> choose revision A -> stamp revision A
                                             |
                                             v
decode request with revision A -> keep only revision A decodes
```

### One EPP

The Screener runs once before the disaggregated scheduling profiles. Choosing
one revision up front gives the decode and prefill profiles the same restricted
candidate set, so they cannot independently select different revisions.

## Revision Gating Modes

### `max-role`

The plugin first totals each required role across all covered revisions and
selects the globally largest role. It then weights every revision using that
same role:

```text
dominantRole = argmax(role) sum(Ready Pods for role across covered revisions)
weight(revision) = Ready Pods for dominantRole in revision
```

For the 2P:10D example:

```text
global totals: prefill = 3, decode = 10
dominant role: decode
A: 9 decode Pods
B: 1 decode Pod
traffic: A 90 percent, B 10 percent
```

The dominant role is selected once for the whole distribution. It cannot be
prefill for one revision and decode for another. If role totals are equal, the
first role in `revisionGating.requireRoles.values` wins the tie.

This mode is useful for a stable, intentionally asymmetric role ratio such as
2P:10D or 10P:2D. It assumes that the more numerous role is a reasonable proxy
for the deployment's traffic-limiting capacity. It is less reliable when the
deployment changes between prefill-heavy and decode-heavy shapes or when the
role ratios differ substantially between revisions.

### `sum`

The weight of a revision is the total number of Ready Pods across the required
roles:

```text
weight(revision) = sum(Ready Pods for every required role)
```

For the same example:

```text
A: 2 + 9 = 11
B: 1 + 1 = 2
traffic: A 84.6 percent, B 15.4 percent
```

`sum` uses progress from every role and is a more general heuristic when role
ratios can change because of scaling, readiness changes, or a topology change.
When every revision has the same P:D ratio, `sum` and `max-role` produce the
same traffic percentages.

Neither mode is an exact capacity model. Exact weighting would require the
relative request capacity of a prefill Pod and a decode Pod, not only their
counts.

### `disabled`

This mode disables revision coverage checks and weighted revision selection.
It does **not** disable header selectors or response-header stamping:

- With no revision header, all located candidates continue to filters,
  scorers, and the picker.
- The selected endpoint's configured labels are still stamped on the response.
- With a revision header, a `strict` selector still keeps only matching
  endpoints and fails if none match.

This supports a two-EPP flow where normal scheduling chooses the prefill, its
revision is stamped, and the coordinator forwards that revision to the decode
EPP. It is safe only when that header is reliably forwarded. Because coverage
is disabled, selecting a prefill revision with no matching Ready decode causes
the later strict decode request to fail rather than cross revisions.

`disabled` alone does not keep the profiles of a single EPP on one revision.
There is no response-header boundary between its profile executions, so the
profiles need revision gating to receive the same candidate set.

## Header Selectors

| Mode | Behavior |
|---|---|
| `strict` | Keeps only endpoints whose label equals the request header. No match fails closed. |
| `prefer` | Declares a soft affinity consumed by the separate [`disaggregatedset-prefer-scorer`](../../../scheduling/scorer/disaggregatedsetprefer/README.md). Non-matching endpoints remain eligible. |

Every selector also stamps its configured response header from the endpoint
that served the request. The mode identifies whether the Screener applies a
hard constraint or the referenced preference scorer applies soft affinity;
keeping both modes here gives matching and stamping one shared header/label
definition. Stamping is independent of the revision gating mode.

## Configuration

```yaml
apiVersion: llm-d.ai/v1alpha1
kind: EndpointPickerConfig
plugins:
- type: disaggregatedset-rollout-screener
  name: rollout-screener
  parameters:
    scope:
      namespace: llm-d
      labelSelector: "disaggregatedset.x-k8s.io/name=my-set"
    headerSelectors:
    - name: revision
      headerName: x-disagg-revision
      labelKey: disaggregatedset.x-k8s.io/revision
      mode: strict
    revisionGating:
      mode: max-role
      requireRoles:
        values: [prefill, decode]
```

The Screener is discovered from the top-level `plugins` list and runs once per
request. Do not add it to a scheduling profile.

## Parameters

| Name | Type | Required | Default | Description |
|---|---|---|---|---|
| `scope.labelSelector` | string | Yes | | Selects the Pods observed for cross-role revision coverage. |
| `scope.namespace` | string | No | EPP Pod `NAMESPACE` | Namespace containing the disaggregated inference Pods. |
| `headerSelectors` | array | No | `[]` | Header-to-label mappings used for strict screening, preference scoring, and response-header stamping. |
| `revisionGating.mode` | string | Yes when gating is configured | | `sum`, `max-role`, or `disabled`. |
| `revisionGating.requireRoles.values` | array | Yes for `sum` and `max-role` | | Roles that must each have a Ready Pod for a revision to receive traffic. |
| `revisionGating.revisionLabelKey` | string | No | `disaggregatedset.x-k8s.io/revision` | Label identifying a rollout revision. |
| `revisionGating.roleLabelKey` | string | No | `disaggregatedset.x-k8s.io/role` | Label identifying a Pod role. |

Each `headerSelectors` entry has:

| Name | Type | Description |
|---|---|---|
| `name` | string | Stable selector identifier, used as a metric label for strict selectors. |
| `headerName` | string | Request and response header carrying the selected label value. |
| `labelKey` | string | Endpoint label compared with the header. |
| `mode` | string | `strict` screens candidates globally; `prefer` is consumed by the separate preference scorer. |

## Fail-Closed Behavior

With `sum` or `max-role`, a revision is ineligible until every required role
has at least one Ready Pod. This also means requests fail closed while the Pod
notification cache is warming. If no revision survives, request control
returns HTTP 503.

A strict header with no matching endpoint also returns HTTP 503. The plugin
never silently substitutes another revision or crosses revisions.

## Metrics

- `llm_d_epp_disaggregatedset_strict_header_no_match_total`: strict header
  selections that matched no endpoint and failed closed.
- `llm_d_epp_disaggregatedset_revision_gating_share`: current weighted share
  from `0` to `1` for each observed revision. Incomplete revisions report `0`.

The share gauge removes a revision's series when that revision disappears from
the observed Pod set.
