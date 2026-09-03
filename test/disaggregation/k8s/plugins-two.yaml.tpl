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
        revisionGating:
          revisionHeaderName: x-disagg-revision
          revisionLabelKey: disaggregatedset.x-k8s.io/revision
          roleLabelKey: disaggregatedset.x-k8s.io/role
          mode: __MODE__
          requiredRoles: [prefill, decode]
    - type: weighted-random-picker
      name: picker
    schedulingProfiles:
    - name: default
      plugins:
      - pluginRef: picker
