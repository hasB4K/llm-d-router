apiVersion: v1
kind: ConfigMap
metadata:
  name: __NAME__
  namespace: __NAMESPACE__
data:
  disaggregation.yaml: |
    apiVersion: llm-d.ai/v1alpha1
    kind: EndpointPickerConfig
    plugins:
    - type: disaggregatedset-rollout-screener
      name: rollout-screener
      parameters:
        scope:
          labelSelector: "disaggregatedset.x-k8s.io/name=revision-rollout"
        headerSelectors:
        - name: revision
          headerName: x-disagg-revision
          labelKey: disaggregatedset.x-k8s.io/revision
          mode: strict
        revisionGating:
          mode: __MODE__
          requireRoles:
            values: [prefill, decode]
    - type: prefill-filter
    - type: decode-filter
    - type: weighted-random-picker
      name: picker
    - type: always-disagg-pd-decider
    - type: disagg-profile-handler
      parameters:
        deciders:
          prefill: always-disagg-pd-decider
    schedulingProfiles:
    - name: prefill
      plugins:
      - pluginRef: prefill-filter
      - pluginRef: picker
    - name: decode
      plugins:
      - pluginRef: decode-filter
      - pluginRef: picker
