/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package disaggregation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	sourcenotifications "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/notifications"
)

const (
	// RouterType is the plugin type for revision gating, strict header
	// filtering, and response-header stamping.
	RouterType = "disaggregation-router"
	// PreferScorerType is the plugin type for soft header-label affinity.
	PreferScorerType = "disaggregation-prefer-scorer"

	podExtractorType = "disaggregation-pod-extractor"
)

var podGVK = schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}

// Controller gates revisions and applies strict selectors before scheduling.
// ResponseHeader stamps the selected endpoint's labels.
type Controller struct {
	typedName        fwkplugin.TypedName
	config           Config
	scope            labels.Selector
	revisionLabelKey string
	roleLabelKey     string
	rand01           func() float64

	mu           sync.RWMutex
	pods         map[types.NamespacedName]podInfo
	distribution revisionDistribution
}

type podInfo struct {
	revision string
	role     string
}

type revisionDistribution struct {
	roleCounts map[string]map[string]int
	shares     map[string]float64
}

var (
	_ fwkplugin.Plugin              = (*Controller)(nil)
	_ fwkrc.Screener                = (*Controller)(nil)
	_ fwkrc.ResponseHeaderProcessor = (*Controller)(nil)
	_ fwkdl.Registrant              = (*Controller)(nil)
	_ fwkdl.NotificationExtractor   = (*podNotificationHandler)(nil)
)

// RouterFactory creates a disaggregation-router from normal plugin parameters.
func RouterFactory(name string, parameters *json.Decoder, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	if name == "" {
		name = RouterType
	}
	config := Config{}
	if parameters == nil {
		return nil, errors.New("disaggregation-router requires parameters")
	}
	if err := parameters.Decode(&config); err != nil {
		return nil, fmt.Errorf("decode disaggregation-router parameters: %w", err)
	}
	if config.Scope.Namespace == "" {
		config.Scope.Namespace = os.Getenv("NAMESPACE")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	scope, err := labels.Parse(config.Scope.LabelSelector)
	if err != nil {
		return nil, fmt.Errorf("parse scope.labelSelector: %w", err)
	}
	registerMetrics()
	return newController(name, config, scope), nil
}

func newController(name string, config Config, scope labels.Selector) *Controller {
	revisionLabelKey := ""
	roleLabelKey := ""
	if config.RevisionGating != nil {
		revisionLabelKey = config.RevisionGating.RevisionLabelKey
		roleLabelKey = config.RevisionGating.RoleLabelKey
	}
	return &Controller{
		typedName:        fwkplugin.TypedName{Type: RouterType, Name: name},
		config:           config,
		scope:            scope,
		revisionLabelKey: revisionLabelKey,
		roleLabelKey:     roleLabelKey,
		rand01:           rand.Float64,
		pods:             make(map[types.NamespacedName]podInfo),
	}
}

func (c *Controller) TypedName() fwkplugin.TypedName { return c.typedName }

// RegisterDependencies requests the framework-owned core/v1 Pod notification
// source. No controller-runtime Manager or Kubernetes client enters the plugin.
func (c *Controller) RegisterDependencies(registrar fwkdl.Registrar) error {
	handler := &podNotificationHandler{controller: c}
	return registrar.Register(fwkdl.PendingRegistration{
		Owner:      c.typedName,
		SourceType: sourcenotifications.NotificationSourceType,
		Extractor:  handler,
		DefaultSource: sourcenotifications.NewK8sNotificationSource(
			sourcenotifications.NotificationSourceType,
			c.typedName.Name+"/pod",
			podGVK,
		),
	})
}

// ResponseHeader stamps configured selector values from the selected endpoint.
func (c *Controller) ResponseHeader(_ context.Context, _ *fwksched.InferenceRequest, response *fwkrc.Response, endpoint *fwkdl.EndpointMetadata) {
	if endpoint == nil || response == nil || response.Headers == nil {
		return
	}
	for _, selector := range c.config.HeaderSelectors {
		if value := endpoint.Labels[selector.LabelKey]; value != "" {
			response.Headers[selector.HeaderName] = value
			recordHeaderStamped(selector.Name)
		}
	}
}

type podNotificationHandler struct {
	controller *Controller
}

func (h *podNotificationHandler) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Type: podExtractorType, Name: h.controller.typedName.Name + "/pod"}
}

func (h *podNotificationHandler) GVK() schema.GroupVersionKind { return podGVK }

func (h *podNotificationHandler) Extract(_ context.Context, event fwkdl.NotificationEvent) error {
	if event.Object == nil {
		return nil
	}
	key := types.NamespacedName{Name: event.Object.GetName(), Namespace: event.Object.GetNamespace()}
	if event.Type == fwkdl.EventDelete {
		h.controller.removePod(key)
		return nil
	}

	pod := &corev1.Pod{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(event.Object.Object, pod); err != nil {
		return fmt.Errorf("convert Pod notification %s: %w", key, err)
	}
	if !h.controller.acceptsPod(pod) {
		h.controller.removePod(key)
		return nil
	}

	h.controller.mu.Lock()
	h.controller.pods[key] = podInfo{
		revision: pod.Labels[h.controller.revisionLabelKey],
		role:     pod.Labels[h.controller.roleLabelKey],
	}
	h.controller.rebuildDistributionLocked()
	h.controller.mu.Unlock()
	return nil
}

func (c *Controller) acceptsPod(pod *corev1.Pod) bool {
	if pod == nil || !c.config.RevisionGating.Active() {
		return false
	}
	if pod.Namespace != c.config.Scope.Namespace {
		return false
	}
	if !c.scope.Matches(labels.Set(pod.Labels)) || !isPodReady(pod) {
		return false
	}
	return pod.Labels[c.revisionLabelKey] != "" && pod.Labels[c.roleLabelKey] != ""
}

func (c *Controller) removePod(key types.NamespacedName) {
	c.mu.Lock()
	delete(c.pods, key)
	c.rebuildDistributionLocked()
	c.mu.Unlock()
}

func (c *Controller) rebuildDistributionLocked() {
	var requiredRoles []string
	var mode GatingMode
	if c.config.RevisionGating.Active() {
		requiredRoles = c.config.RevisionGating.RequireRoles.Values
		mode = c.config.RevisionGating.Mode
	}
	roleCounts := make(map[string]map[string]int)
	for _, pod := range c.pods {
		incrementRoleCount(roleCounts, pod)
	}
	c.distribution = newRevisionDistribution(roleCounts, requiredRoles, mode)
}

func incrementRoleCount(counts map[string]map[string]int, pod podInfo) {
	if counts[pod.revision] == nil {
		counts[pod.revision] = make(map[string]int)
	}
	counts[pod.revision][pod.role]++
}

func newRevisionDistribution(roleCounts map[string]map[string]int, requiredRoles []string, mode GatingMode) revisionDistribution {
	weights := make(map[string]int, len(roleCounts))
	total := 0
	for revision, perRole := range roleCounts {
		weight := revisionWeight(perRole, requiredRoles, mode == GatingModeMaxRole)
		if weight == 0 {
			continue
		}
		weights[revision] = weight
		total += weight
	}

	shares := make(map[string]float64, len(weights))
	if total > 0 {
		for revision, weight := range weights {
			shares[revision] = float64(weight) / float64(total)
		}
	}
	return revisionDistribution{roleCounts: roleCounts, shares: shares}
}

func (c *Controller) distributionSnapshot() revisionDistribution {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.distribution
}

func isPodReady(pod *corev1.Pod) bool {
	if pod == nil || pod.Status.Phase != corev1.PodRunning || pod.DeletionTimestamp != nil {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}
