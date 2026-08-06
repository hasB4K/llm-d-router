apiVersion: disaggregatedset.x-k8s.io/v1
kind: DisaggregatedSet
metadata:
  name: revision-rollout
  namespace: __NAMESPACE__
spec:
  roles:
  - name: prefill
    metadata:
      labels:
        app: disagg-rollout-backend
        llm-d.ai/role: prefill
    spec:
      replicas: __PREFILL_REPLICAS__
      rolloutStrategy:
        rollingUpdateConfiguration:
          maxSurge: 1
          maxUnavailable: 0
      leaderWorkerTemplate:
        size: 1
        workerTemplate:
          metadata:
            labels:
              app: disagg-rollout-backend
              llm-d.ai/role: prefill
          spec:
            terminationGracePeriodSeconds: 10
            containers:
            - name: server
              image: __BACKEND_IMAGE__
              imagePullPolicy: Always
              command: ["sh", "-c"]
              args:
              - |
                if [ "$AUTO_READY" = "true" ]; then touch /tmp/ready; fi
                exec python /server.py
              env:
              - name: AUTO_READY
                value: "__AUTO_READY__"
              - name: ROLLOUT_TOKEN
                value: "__ROLLOUT_TOKEN__"
              - name: ROLE
                valueFrom:
                  fieldRef:
                    fieldPath: metadata.labels['disaggregatedset.x-k8s.io/role']
              - name: REVISION
                valueFrom:
                  fieldRef:
                    fieldPath: metadata.labels['disaggregatedset.x-k8s.io/revision']
              ports:
              - name: http
                containerPort: 8080
              readinessProbe:
                exec:
                  command: ["test", "-f", "/tmp/ready"]
                periodSeconds: 2
                timeoutSeconds: 5
              livenessProbe:
                httpGet:
                  path: /health
                  port: 8080
                initialDelaySeconds: 2
                periodSeconds: 2
                timeoutSeconds: 5
  - name: decode
    metadata:
      labels:
        app: disagg-rollout-backend
        llm-d.ai/role: decode
    spec:
      replicas: __DECODE_REPLICAS__
      rolloutStrategy:
        rollingUpdateConfiguration:
          maxSurge: 1
          maxUnavailable: 0
      leaderWorkerTemplate:
        size: 1
        workerTemplate:
          metadata:
            labels:
              app: disagg-rollout-backend
              llm-d.ai/role: decode
          spec:
            terminationGracePeriodSeconds: 10
            containers:
            - name: server
              image: __BACKEND_IMAGE__
              imagePullPolicy: Always
              command: ["sh", "-c"]
              args:
              - |
                if [ "$AUTO_READY" = "true" ]; then touch /tmp/ready; fi
                exec python /server.py
              env:
              - name: AUTO_READY
                value: "__AUTO_READY__"
              - name: ROLLOUT_TOKEN
                value: "__ROLLOUT_TOKEN__"
              - name: ROLE
                valueFrom:
                  fieldRef:
                    fieldPath: metadata.labels['disaggregatedset.x-k8s.io/role']
              - name: REVISION
                valueFrom:
                  fieldRef:
                    fieldPath: metadata.labels['disaggregatedset.x-k8s.io/revision']
              ports:
              - name: http
                containerPort: 8080
              readinessProbe:
                exec:
                  command: ["test", "-f", "/tmp/ready"]
                periodSeconds: 2
                timeoutSeconds: 5
              livenessProbe:
                httpGet:
                  path: /health
                  port: 8080
                initialDelaySeconds: 2
                periodSeconds: 2
                timeoutSeconds: 5
