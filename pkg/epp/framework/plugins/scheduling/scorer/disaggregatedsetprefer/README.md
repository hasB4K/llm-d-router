# DisaggregatedSet Preference Scorer

**Type:** `disaggregatedset-prefer-scorer`
**Interface:** `scheduling.Scorer`

Adds soft affinity for endpoints whose labels match request headers configured as `prefer` selectors on a [`disaggregatedset-rollout-screener`](../../../requestcontrol/screener/disaggregatedsetrollout/README.md). Non-matching endpoints remain eligible.

## What It Does

For each active prefer selector, an endpoint receives:

- `1` when its configured label matches the request header;
- `0` when it does not match.

When several prefer selectors have request headers, their scores are averaged. A missing request header does not participate in the average. If no endpoint matches a header, every endpoint receives zero for that selector and scheduling continues normally.

The scheduling profile multiplies the resulting `[0,1]` score by the configured plugin weight and adds it to the other weighted scorer results.

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `screenerRef` | string | Yes | Name of the `disaggregatedset-rollout-screener` containing the `prefer` header selectors. |

## DisaggregatedSet Slice Affinity

A `DisaggregatedSet` slice groups cooperating role replicas through the `disaggregatedset.x-k8s.io/slice` label. A slice can represent endpoints placed within one NVL72 NVLink domain. Preferring the same slice can avoid a slower cross-domain KV-cache transfer while retaining fallback capacity when that slice is unavailable or overloaded.

The scorer weight should represent **how preferable it is to run within the same NVL72 domain instead of doing a cross-domain transfer**, relative to the weights of the other scorers in that profile. It is not a latency value in milliseconds.

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
- type: disaggregatedset-prefer-scorer
  name: slice-affinity
  parameters:
    screenerRef: rollout-screener
- type: weighted-random-picker
  name: picker

schedulingProfiles:
- name: decode
  plugins:
  - pluginRef: slice-affinity
    weight: 3
  - pluginRef: picker
```

In a two-EPP topology, the coordinator must copy `x-disagg-slice` from the
prefill response into the decode request. The current llm-d coordinator does
not yet do this; a follow-up coordinator PR will add support. The Screener
stamps the prefill response header from the selected prefill endpoint's slice
label.

## Tuning the Weight

Benchmark same-slice and cross-slice KV transfers with representative traffic.
Choose a weight that normally avoids the cross-domain transfer but still lets
other scorers select a healthier endpoint when the same-slice endpoint is
overloaded.

A matching endpoint receives the full slice weight; a non-matching endpoint
receives zero. Consider this illustrative example:

```text
loadWeight = 5
sameSliceLoadScore = 0.4
crossSliceLoadScore = 0.9

sameSliceWeightedLoadScore = loadWeight * sameSliceLoadScore = 2.0
crossSliceWeightedLoadScore = loadWeight * crossSliceLoadScore = 4.5
crossSliceAdvantage = 4.5 - 2.0 = 2.5
```

The slice scorer adds `sliceWeight` only to the same-slice endpoint. In this
example, `sliceWeight` must therefore be greater than `crossSliceAdvantage`
(`2.5`) for the same NVL72 domain to win. This threshold is specific to the
example; production values depend on the other configured scorers and their
runtime scores.

## Operational Notes

- This is a soft preference. It never removes an endpoint.
- With no `x-disagg-slice` header, slice affinity contributes zero everywhere.
- With a stale or unknown slice header, all endpoints receive zero and normal scoring continues.
- Configure the scorer only in profiles where the preference should affect endpoint selection.
