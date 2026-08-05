# Header Label Affinity Scorer

**Type:** `header-label-affinity-scorer`
**Interface:** `scheduling.Scorer`

Adds soft affinity for endpoints whose configured label equals a request
header. A matching endpoint receives a score of `1`; every other endpoint
receives `0` and remains eligible.

Use a separate plugin instance for each header-to-label mapping. This allows
each preference to have its own scheduling-profile weight.

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `headerName` | string | Yes | Request header containing the preferred label value. |
| `labelKey` | string | Yes | Endpoint label compared with the request header. |

## Configuration

```yaml
plugins:
- type: header-label-affinity-scorer
  name: slice-affinity
  parameters:
    headerName: x-disagg-slice
    labelKey: disaggregatedset.x-k8s.io/slice
- type: header-label-affinity-scorer
  name: zone-affinity
  parameters:
    headerName: x-preferred-zone
    labelKey: topology.kubernetes.io/zone
- type: weighted-random-picker
  name: picker

schedulingProfiles:
- name: decode
  plugins:
  - pluginRef: slice-affinity
    weight: 3
  - pluginRef: zone-affinity
    weight: 1
  - pluginRef: picker
```

The scorer does not write response headers. Protocols that return the selected
label to a later request must configure response-header stamping separately.

## Operational Notes

- A missing request header contributes zero to every endpoint.
- An unknown header value contributes zero to every endpoint.
- Missing endpoint labels receive zero.
- The scheduling profile multiplies the score by the plugin weight and adds it
  to the other weighted scorer results.
