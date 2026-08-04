# Disaggregated Rollout Screener

**Type:** `disagg-rollout-screener`  
**Interfaces:** `requestcontrol.Screener`, `requestcontrol.ResponseHeaderProcessor`

Screens located endpoints before data producers, admission plugins, and scheduling profiles run. It keeps incompatible prefill and decode revisions from being paired during a `DisaggregatedSet` rollout.

Most endpoint-selection plugins should implement a scheduling `Filter`. Use this Screener only for constraints that must apply globally to every scheduling profile.

## What It Does

The plugin can apply two mandatory constraints:

1. **Revision coverage:** a revision is eligible only when every configured role has at least one Ready Pod.
2. **Strict header selection:** when a configured request header is present, only endpoints whose label matches that value remain eligible.

If several revisions have complete role coverage, the Screener chooses one revision using the configured rollout weight:

- `sum`: `sum(Ready Pods for each required role)`
- `max-role`: `max(Ready Pods among the required roles)`

The selected endpoint's configured labels are written to response headers. A coordinator or client can forward those headers to a later decode request, which keeps both request legs on the same revision.

## Fail-Closed Behavior

With active revision gating, a revision share of zero is not eligible:

- For a revision present in the observed Pod counts, zero means at least one required role has no Ready Pods.
- A revision absent from the cached distribution, including while Pod notifications are still warming the cache, also resolves to zero.

Both cases are intentionally fail-closed. If screening removes every located endpoint, request control returns HTTP 503. This differs from omitting `revisionGating` or setting its mode to `disabled`, which disables revision gating entirely.

A strict header with no matching endpoint also removes every candidate and returns HTTP 503. The plugin never silently crosses revisions or substitutes a different strict label value.

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
| `name` | string | Stable selector name used by metrics. |
| `headerName` | string | Request and response header carrying the selected label value. |
| `labelKey` | string | Endpoint label compared with the header. |
| `mode` | string | `strict` screens candidates globally; `prefer` is consumed by the separate preference scorer. |

## Configuration

```yaml
apiVersion: llm-d.ai/v1alpha1
kind: EndpointPickerConfig
plugins:
- type: disagg-rollout-screener
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

The Screener is discovered from the top-level `plugins` list and runs once per request. Do not add it to a scheduling profile.

## Topologies

The same configuration supports:

- one EPP running separate prefill and decode scheduling profiles;
- separate prefill and decode EPPs observing the same `DisaggregatedSet` Pod scope.

For two EPPs, the prefill-side coordinator must forward the stamped revision header to the decode-side request.

## Metrics

- `llm_d_epp_disagg_header_stamped_total`
- `llm_d_epp_disagg_screening_outcome_total`
- `llm_d_epp_disagg_gating_dropped_total`

All metrics are labeled with bounded selector, mode, outcome, or revision values as appropriate.
