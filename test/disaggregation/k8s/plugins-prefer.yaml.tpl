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
        - name: role
          headerName: x-preferred-role
          labelKey: disaggregatedset.x-k8s.io/role
          mode: prefer
        revisionGating:
          mode: disabled
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
