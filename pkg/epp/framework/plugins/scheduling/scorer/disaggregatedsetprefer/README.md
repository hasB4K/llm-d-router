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
      namespace: llm-d
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

The component scheduling the first role must forward `x-disagg-slice` with the later role's request. The Screener stamps this header from the selected endpoint label.

## Benchmarking and Tuning the Weight

Benchmark with the model, KV-cache sizes, concurrency, and network topology used in production:

1. Force prefill and decode onto the same slice and record KV-transfer latency and end-to-end latency, especially p95.
2. Force them onto different slices and repeat the measurement.
3. Repeat while increasing load on the same-slice decode endpoints to find when avoiding the cross-domain transfer is no longer worth the added queue or load.
4. Compare that crossover with the weighted score difference produced by the other configured scorers.

The scheduler adds weighted scores. For a matching slice, this scorer contributes its full weight; for a non-match, it contributes zero. Therefore a same-slice endpoint wins over a cross-slice endpoint when:

```text
sliceWeight > otherWeightedScore(crossSlice) - otherWeightedScore(sameSlice)
```

For example, suppose a load scorer has weight `5` and returns `0.4` for the same-slice endpoint and `0.9` for a less-loaded cross-slice endpoint. The cross-slice advantage from load is:

```text
5 * (0.9 - 0.4) = 2.5
```

A slice weight above `2.5`, such as `3`, prefers the same NVL72 domain. A weight below `2.5` lets the load advantage select the cross-domain endpoint. Use the measured same-domain versus cross-domain latency penalty to decide which side of that crossover should win, then validate the chosen value with representative traffic.

## Operational Notes

- This is a soft preference. It never removes an endpoint.
- With no `x-disagg-slice` header, slice affinity contributes zero everywhere.
- With a stale or unknown slice header, all endpoints receive zero and normal scoring continues.
- Configure the scorer only in profiles where the preference should affect endpoint selection.
