# DisaggregatedSet Rollout Screener

**Type:** `disaggregatedset-rollout-screener`
**Interfaces:** `requestcontrol.Screener`, `requestcontrol.ResponseHeaderProcessor`

This plugin prevents incompatible prefill and decode Pods from serving the same
request during a `DisaggregatedSet` rollout. It runs before every scheduling
profile, so the compatibility constraint cannot be undone by a later filter,
scorer, or picker.

## Why Revision Screening Is Needed

A `DisaggregatedSet` revision represents one complete version of the whole
disaggregated deployment, including every role. If the configuration of any
role changes, the controller creates a new revision for the entire
`DisaggregatedSet`. All prefill and decode Pods created for that version receive
the same `disaggregatedset.x-k8s.io/revision` label, while Pods from the previous
version retain their old revision label.

The router uses this label as a compatibility boundary: a prefill Pod and a
decode Pod may serve the same request only when their revision labels are
equal. For example, it may pair a prefill Pod labeled `rev-A` with a decode Pod
labeled `rev-A`, but never a prefill Pod labeled `rev-A` with a decode Pod
labeled `rev-B`.

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

Selecting a revision from the prefill pool alone would send about 67% of
requests to A and 33% to B. However, B has only 1 of the 10 decode Pods and can
represent only about 10% of decode capacity. That can overload B's decode side
while leaving A's decode capacity unused.

The
[DisaggregatedSet rollout KEP](https://github.com/kubernetes-sigs/lws/tree/main/keps/766-DisaggregatedSet)
explains how the controller keeps role progress as close as integer replica
counts permit. To inspect a concrete rollout, run the planner from the root of a
[`kubernetes-sigs/lws`](https://github.com/kubernetes-sigs/lws) checkout. This
example uses a 2P:10D deployment with `maxUnavailable: 2` for both roles:

```bash
cd "$(go env GOPATH)/src/github.com/kubernetes-sigs/lws"
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
`max-role` therefore produces these old/new revision shares:

- Step 4: 60% old, 40% new.
- Step 6: 40% old, 60% new.
- Step 8: 20% old, 80% new.

## Request Lifecycle

For every Pod notification, the plugin caches the Ready Pod count for each
complete compatibility decision: the revision plus the values of every strict,
non-revision header selector. For a request, it then:

1. Removes every decision missing a Ready Pod for any required role.
2. Applies any supplied strict headers as constraints on the decision.
3. Computes a weight for each remaining decision.
4. Randomly chooses one complete decision using those weights.
5. When the request carries `x-llm-d-revision-decision-id`, or falls back to
   `x-request-id`, atomically stores or reads the complete decision through the
   configured `CrossReplicaSyncer`. Without one, it stores the decision in the
   local EPP process.
6. Exposes only endpoints matching that decision to all scheduling profiles.
7. Stamps configured labels from the selected endpoint into response headers.

```text
Ready Pod counts
       |
       v
remove incomplete decisions -> choose one covered decision
       |                               |
       +-------------------------------+
                                       v
                        filters, scorers, and picker
                                       |
                                       v
                        stamp the selected labels
```

For example, with a strict revision selector and a strict slice selector, the
decision is `(revision, slice)`. A revision header fixes only the revision; the
Screener still selects a covered slice. A slice header fixes only the slice;
the Screener still selects a covered revision. When both are supplied, the
decision is fully constrained. The same mechanism works for any strict label;
there is no slice-specific code path.

### Two EPPs

Separate phase EPPs must make the same compatibility decision even though each one
schedules independently. The coordinator generates one
`x-llm-d-revision-decision-id` for the request and sends it to every encode,
prefill, and decode EPP request.

Each Screener can propose a covered decision, but atomic `GetOrSet` makes the
first stored decision authoritative. In E/P/D, parallel encode requests can
therefore begin together without waiting for one encode response. Prefill and
decode use the same decision ID and receive the same revision and strict-label
values.

With a configured `CrossReplicaSyncer`, this coordination works when requests
are distributed across multiple EPP replicas. Without one, the Screener stores
decisions locally. That fallback is safe only when a single EPP replica handles
the pool.

### One EPP

The Screener runs once before the disaggregated scheduling profiles. Choosing
one compatibility decision up front gives the decode and prefill profiles the
same restricted candidate set, so they cannot independently select different
revisions or strict-label values.

## Revision Gating Modes

The modes weight complete compatibility decisions. When no non-revision strict
selector is configured, a decision is simply one revision, so the examples
below are unchanged.

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
traffic: A 90%, B 10%
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
traffic: A 84.6%, B 15.4%
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

This mode does not make a revision decision. It cannot keep separate EPPs, or
the profiles of a single EPP, on one revision.

## Header Selectors

| Mode | Behavior |
|---|---|
| `strict` | With revision gating, selects this label as part of the complete shared decision when the header is absent; a supplied value constrains it. The label must have the same value on every required role. With gating disabled, it only filters an incoming header. |
| `prefer` | Stamps the selected label without screening candidates. Configure a [`header-label-affinity-scorer`](../../../scheduling/scorer/headerlabelaffinity/README.md) to apply the soft preference. |

Every selector also stamps its configured response header from the endpoint
that served the request. The mode identifies whether the Screener applies a
hard constraint or only stamps the label. Stamping is independent of the
revision gating mode.

The generic scorer repeats the `headerName` and `labelKey` mapping. Keeping its
configuration independent allows each preference to use a different weight.

## Configuration

```yaml
apiVersion: llm-d.ai/v1alpha1
kind: EndpointPickerConfig
plugins:
- type: disaggregatedset-rollout-screener
  name: rollout-screener
  parameters:
    scope:
      labelSelector: "disaggregatedset.x-k8s.io/name=my-set"
    headerSelectors:
    - name: revision
      headerName: x-disagg-revision
      labelKey: disaggregatedset.x-k8s.io/revision
      mode: strict
    - name: slice
      headerName: x-disagg-slice
      labelKey: disaggregatedset.x-k8s.io/slice
      mode: prefer
    revisionGating:
      mode: max-role
      requireRoles:
        values: [prefill, decode]
- type: header-label-affinity-scorer
  name: slice-affinity
  parameters:
    headerName: x-disagg-slice
    labelKey: disaggregatedset.x-k8s.io/slice
- type: weighted-random-picker
  name: picker

schedulingProfiles:
- name: decode
  plugins:
  - pluginRef: slice-affinity
    weight: 3
  - pluginRef: picker
```

The Screener is discovered from the top-level `plugins` list and runs once per
request. Do not add it to a scheduling profile.

## Parameters

| Name | Type | Required | Default | Description |
|---|---|---|---|---|
| `scope.labelSelector` | string | Yes | | Selects the Pods observed for cross-role revision coverage. |
| `headerSelectors` | array | No | `[]` | Header-to-label mappings used for strict compatibility decisions and response-header stamping. |
| `revisionGating` | object | Yes | | Revision screening configuration. Use `mode: disabled` to retain selectors and response-header stamping without revision coverage or weighted selection. |
| `revisionGating.mode` | string | Yes | | `sum`, `max-role`, or `disabled`. |
| `revisionGating.requireRoles.values` | array | Yes for `sum` and `max-role` | | Roles that must each have a Ready Pod for a revision to receive traffic. |
| `revisionGating.revisionLabelKey` | string | No | `disaggregatedset.x-k8s.io/revision` | Label identifying a rollout revision. |
| `revisionGating.roleLabelKey` | string | No | `disaggregatedset.x-k8s.io/role` | Label identifying a Pod role. |

Each `headerSelectors` entry has:

| Name | Type | Description |
|---|---|---|
| `name` | string | Stable selector identifier, used as a metric label for strict selectors. |
| `headerName` | string | Request and response header carrying the selected label value. |
| `labelKey` | string | Kubernetes Pod label whose value is compared with the request header and copied from the selected endpoint into the response header. A strict label is also part of the shared decision when gating is active, so it must have the same value across every required role. |
| `mode` | string | `strict` is a hard shared compatibility dimension when gating is active; `prefer` only stamps the selected value and leaves scoring to a separate plugin. |

## DisaggregatedSet Slice Affinity

A `DisaggregatedSet` slice groups cooperating role replicas through the
`disaggregatedset.x-k8s.io/slice` label. A slice can represent endpoints placed
within one NVL72 NVLink domain. Preferring the same slice can avoid a slower
cross-domain KV-cache transfer while retaining fallback capacity when that
slice is unavailable or overloaded.

The scorer weight represents how preferable the same NVL72 domain is relative
to the other scorers in the profile. Benchmark same-slice and cross-slice KV
transfers with representative traffic. Choose a weight that normally avoids a
cross-domain transfer but still lets health and load scorers select another
endpoint when the same-slice endpoint is overloaded.

A matching endpoint receives the full configured weight and a non-matching
endpoint receives zero. For example, assume a load scorer has weight `5`. If
the same-slice endpoint receives a load score of `0.4` and the cross-slice
endpoint receives `0.9`, their weighted load scores are `2.0` and `4.5`. The
cross-slice endpoint has a `2.5` advantage, so the slice weight must be greater
than `2.5` for the same-slice endpoint to win in that example. Production
values depend on the other configured scorers and their runtime scores.

## Fail-Closed Behavior

With `sum` or `max-role`, a revision is ineligible until every required role
has at least one Ready Pod. If no revision survives, request control returns
HTTP 503.

After EPP startup or restart, the in-memory distribution is empty until Pod
notifications deliver the initial snapshot. During that interval, `sum` and
`max-role` return HTTP 503 rather than route without cross-role coverage
information. A follow-up change is required to make receipt of the initial Pod
snapshot a condition of EPP readiness.

A strict header with no matching endpoint also returns HTTP 503. The plugin
never silently substitutes another revision or crosses revisions.

## Metrics

- `llm_d_epp_disaggregatedset_strict_header_no_match_total`: strict header
  selections that matched no endpoint and failed closed.
- `llm_d_epp_disaggregatedset_revision_gating_share`: current weighted share
  from `0` to `1` for each observed revision. Incomplete revisions report `0`.

The share gauge removes a revision's series when that revision disappears from
the observed Pod set.
