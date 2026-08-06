apiVersion: v1
kind: ServiceAccount
metadata:
  name: __NAME__
  namespace: __NAMESPACE__
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: __NAME__
  namespace: __NAMESPACE__
rules:
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "watch", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: __NAME__
  namespace: __NAMESPACE__
subjects:
- kind: ServiceAccount
  name: __NAME__
  namespace: __NAMESPACE__
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: __NAME__
---
apiVersion: v1
kind: Service
metadata:
  name: __NAME__
  namespace: __NAMESPACE__
spec:
  selector:
    app: __NAME__
  ports:
  - name: grpc-ext-proc
    port: 9002
    protocol: TCP
  - name: http
    port: 8081
    protocol: TCP
    targetPort: 8081
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: __NAME__
  namespace: __NAMESPACE__
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: __NAME__
  template:
    metadata:
      labels:
        app: __NAME__
    spec:
      serviceAccountName: __NAME__
      terminationGracePeriodSeconds: 130
      containers:
      - name: envoy-sidecar
        image: docker.io/envoyproxy/envoy:distroless-v1.33.2
        imagePullPolicy: IfNotPresent
        command: ["envoy"]
        args:
        - --service-node
        - envoy-sidecar
        - --log-level
        - info
        - --cpuset-threads
        - --drain-strategy
        - immediate
        - --drain-time-s
        - "60"
        - -c
        - /etc/envoy/envoy.yaml
        ports:
        - containerPort: 8081
          name: http
        - containerPort: 19001
          name: ready
        readinessProbe:
          httpGet:
            path: /ready
            port: 19001
          periodSeconds: 5
          timeoutSeconds: 1
        volumeMounts:
        - mountPath: /etc/envoy
          name: envoy-config
          readOnly: true
      - name: epp
        image: __EPP_IMAGE__
        imagePullPolicy: Always
        args:
        - --allow-experimental-plugins
        - --endpoint-selector
        - "__ENDPOINT_SELECTOR__"
        - --endpoint-target-ports
        - "8080"
        - --zap-encoder
        - json
        - --config-file
        - /config/disaggregation.yaml
        - --tracing=false
        - --metrics-endpoint-auth=false
        ports:
        - name: grpc
          containerPort: 9002
        - name: grpc-health
          containerPort: 9003
        - name: metrics
          containerPort: 9090
        livenessProbe:
          grpc:
            port: 9003
            service: inference-extension
          initialDelaySeconds: 5
          periodSeconds: 10
        readinessProbe:
          grpc:
            port: 9003
            service: inference-extension
          periodSeconds: 2
        env:
        - name: NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        volumeMounts:
        - name: plugins-config
          mountPath: /config
      volumes:
      - name: envoy-config
        configMap:
          name: disagg-matrix-envoy
          items:
          - key: envoy.yaml
            path: envoy.yaml
      - name: plugins-config
        configMap:
          name: __NAME__
