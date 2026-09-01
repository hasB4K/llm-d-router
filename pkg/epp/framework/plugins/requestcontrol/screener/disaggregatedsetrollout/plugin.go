/*
Copyright 2026 The Kubernetes Authors.

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

package disaggregatedsetrollout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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
	podutil "github.com/llm-d/llm-d-router/pkg/epp/util/pod"
)

const (
	// PluginType is the plugin type for compatibility decision gating, strict
	// header screening, and response-header stamping.
	PluginType       = "disaggregatedset-rollout-screener"
	podExtractorType = "disaggregatedset-rollout-pod-extractor"
)

var podGVK = schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}

// Screener gates complete compatibility decisions before scheduling.
// ResponseHeader stamps the selected endpoint's labels.
type Screener struct {
	typedName                     fwkplugin.TypedName
	config                        Config
	scope                         labels.Selector
	revisionLabelKey              string
	roleLabelKey                  string
	strictDecisionLabelKeys       []string
	compatibilityDecisionStateKey fwkdl.StateKey
	syncer                        fwkdl.CrossReplicaSyncer
	localDecisions                localGetOrSet

	mu           sync.RWMutex
	pods         map[types.NamespacedName]podInfo
	distribution compatibilityDistribution
}

type podInfo struct {
	revision string
	role     string
	labels   map[string]string
}

type compatibilityDistribution struct {
	roleCounts map[string]map[string]int
	shares     map[string]float64
	domains    map[string]decisionDomain
}

// compatibilityDecision is the complete cross-role compatibility boundary
// selected for a request. Revision is always present while Labels contains the
// values of every strict, non-revision selector, keyed by Kubernetes label key.
type compatibilityDecision struct {
	Revision string            `json:"revision"`
	Labels   map[string]string `json:"labels,omitempty"`
}

// decisionDomain is a complete compatibility decision with its Ready Pod
// counts. A domain is eligible only when every required role has a Ready Pod.
type decisionDomain struct {
	key        string
	decision   compatibilityDecision
	roleCounts map[string]int
	weight     int
}

var (
	_ fwkplugin.Plugin                 = (*Screener)(nil)
	_ fwkrc.Screener                   = (*Screener)(nil)
	_ fwkrc.ResponseHeaderProcessor    = (*Screener)(nil)
	_ fwkdl.Registrant                 = (*Screener)(nil)
	_ fwkdl.CrossReplicaSyncerConsumer = (*Screener)(nil)
	_ fwkdl.NotificationExtractor      = (*podNotificationHandler)(nil)
)

// Factory creates a disaggregatedset-rollout-screener from normal plugin parameters.
func Factory(name string, parameters *json.Decoder, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	if name == "" {
		name = PluginType
	}
	config := Config{}
	if parameters == nil {
		return nil, errors.New("disaggregatedset-rollout-screener requires parameters")
	}
	if err := parameters.Decode(&config); err != nil {
		return nil, fmt.Errorf("decode disaggregatedset-rollout-screener parameters: %w", err)
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	scope, err := labels.Parse(config.Scope.LabelSelector)
	if err != nil {
		return nil, fmt.Errorf("parse scope.labelSelector: %w", err)
	}
	registerMetrics()
	return newScreener(name, config, scope), nil
}

func newScreener(name string, config Config, scope labels.Selector) *Screener {
	revisionLabelKey := ""
	roleLabelKey := ""
	if config.RevisionGating != nil {
		revisionLabelKey = config.RevisionGating.RevisionLabelKey
		roleLabelKey = config.RevisionGating.RoleLabelKey
	}
	strictLabelKeys := strictDecisionLabelKeys(config.HeaderSelectors, revisionLabelKey)
	return &Screener{
		typedName:                     fwkplugin.TypedName{Type: PluginType, Name: name},
		config:                        config,
		scope:                         scope,
		revisionLabelKey:              revisionLabelKey,
		roleLabelKey:                  roleLabelKey,
		strictDecisionLabelKeys:       strictLabelKeys,
		compatibilityDecisionStateKey: fwkdl.StateKey("disaggregatedset-rollout:" + name),
		pods:                          make(map[types.NamespacedName]podInfo),
	}
}

func (c *Screener) TypedName() fwkplugin.TypedName { return c.typedName }

// SetCrossReplicaSyncer configures cross-replica compatibility-decision coordination.
func (c *Screener) SetCrossReplicaSyncer(syncer fwkdl.CrossReplicaSyncer) error {
	if syncer == nil {
		return errors.New("cross-replica syncer must not be nil")
	}
	c.syncer = syncer
	return nil
}

// RegisterDependencies requests the framework-owned core/v1 Pod notification
// source. No controller-runtime Manager or Kubernetes client enters the plugin.
func (c *Screener) RegisterDependencies(registrar fwkdl.Registrar) error {
	if !c.config.RevisionGating.Active() {
		return nil
	}
	handler := &podNotificationHandler{screener: c}
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

// ResponseHeader stamps configured selector values after the selected upstream
// endpoint begins responding.
func (c *Screener) ResponseHeader(_ context.Context, _ *fwksched.InferenceRequest, response *fwkrc.Response, endpoint *fwkdl.EndpointMetadata) {
	if endpoint == nil || response == nil || response.Headers == nil {
		return
	}
	for _, selector := range c.config.HeaderSelectors {
		if value := endpoint.Labels[selector.LabelKey]; value != "" {
			response.Headers[selector.HeaderName] = value
		}
	}
}

type podNotificationHandler struct {
	screener *Screener
}

func (h *podNotificationHandler) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Type: podExtractorType, Name: h.screener.typedName.Name + "/pod"}
}

func (h *podNotificationHandler) GVK() schema.GroupVersionKind { return podGVK }

func (h *podNotificationHandler) Extract(_ context.Context, event fwkdl.NotificationEvent) error {
	if event.Object == nil {
		return nil
	}
	key := types.NamespacedName{Name: event.Object.GetName(), Namespace: event.Object.GetNamespace()}
	if event.Type == fwkdl.EventDelete {
		h.screener.removePod(key)
		return nil
	}

	pod := &corev1.Pod{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(event.Object.Object, pod); err != nil {
		return fmt.Errorf("convert Pod notification %s: %w", key, err)
	}
	if !h.screener.acceptsPod(pod) {
		h.screener.removePod(key)
		return nil
	}

	h.screener.mu.Lock()
	h.screener.pods[key] = h.screener.newPodInfo(pod)
	h.screener.rebuildDistributionLocked()
	h.screener.mu.Unlock()
	return nil
}

func (c *Screener) acceptsPod(pod *corev1.Pod) bool {
	if pod == nil || !c.config.RevisionGating.Active() {
		return false
	}
	if !c.scope.Matches(labels.Set(pod.Labels)) || !podutil.IsPodReady(pod) {
		return false
	}
	return pod.Labels[c.revisionLabelKey] != "" && pod.Labels[c.roleLabelKey] != ""
}

func (c *Screener) removePod(key types.NamespacedName) {
	c.mu.Lock()
	delete(c.pods, key)
	c.rebuildDistributionLocked()
	c.mu.Unlock()
}

func (c *Screener) rebuildDistributionLocked() {
	var requiredRoles []string
	var mode GatingMode
	if c.config.RevisionGating.Active() {
		requiredRoles = c.config.RevisionGating.RequireRoles.Values
		mode = c.config.RevisionGating.Mode
	}
	roleCounts := make(map[string]map[string]int)
	domainCounts := make(map[string]decisionDomain)
	for _, pod := range c.pods {
		incrementRoleCount(roleCounts, pod)
		decision, ok := c.decisionForPod(pod)
		if !ok {
			continue
		}
		key := c.decisionKey(decision)
		domain := domainCounts[key]
		if domain.roleCounts == nil {
			domain = decisionDomain{
				key:        key,
				decision:   decision,
				roleCounts: make(map[string]int),
			}
		}
		domain.roleCounts[pod.role]++
		domainCounts[key] = domain
	}
	previous := c.distribution
	c.distribution = newCompatibilityDistribution(roleCounts, domainCounts, requiredRoles, mode)
	recordRevisionGatingShares(c.typedName.Name, mode, previous, c.distribution)
}

func (c *Screener) newPodInfo(pod *corev1.Pod) podInfo {
	var values map[string]string
	if len(c.strictDecisionLabelKeys) > 0 {
		values = make(map[string]string, len(c.strictDecisionLabelKeys))
		for _, labelKey := range c.strictDecisionLabelKeys {
			values[labelKey] = pod.Labels[labelKey]
		}
	}
	return podInfo{
		revision: pod.Labels[c.revisionLabelKey],
		role:     pod.Labels[c.roleLabelKey],
		labels:   values,
	}
}

func strictDecisionLabelKeys(selectors []HeaderSelector, revisionLabelKey string) []string {
	seen := make(map[string]struct{}, len(selectors))
	keys := make([]string, 0, len(selectors))
	for _, selector := range selectors {
		if selector.Mode != ModeStrict || selector.LabelKey == revisionLabelKey {
			continue
		}
		if _, exists := seen[selector.LabelKey]; exists {
			continue
		}
		seen[selector.LabelKey] = struct{}{}
		keys = append(keys, selector.LabelKey)
	}
	sort.Strings(keys)
	return keys
}

func (c *Screener) decisionForPod(pod podInfo) (compatibilityDecision, bool) {
	decision := compatibilityDecision{Revision: pod.revision}
	if decision.Revision == "" {
		return compatibilityDecision{}, false
	}
	if len(c.strictDecisionLabelKeys) == 0 {
		return decision, true
	}

	decision.Labels = make(map[string]string, len(c.strictDecisionLabelKeys))
	for _, labelKey := range c.strictDecisionLabelKeys {
		value := pod.labels[labelKey]
		if value == "" {
			return compatibilityDecision{}, false
		}
		decision.Labels[labelKey] = value
	}
	return decision, true
}

func incrementRoleCount(counts map[string]map[string]int, pod podInfo) {
	if counts[pod.revision] == nil {
		counts[pod.revision] = make(map[string]int)
	}
	counts[pod.revision][pod.role]++
}

func newCompatibilityDistribution(
	roleCounts map[string]map[string]int,
	domainCounts map[string]decisionDomain,
	requiredRoles []string,
	mode GatingMode,
) compatibilityDistribution {
	covered := make(map[string]map[string]int, len(domainCounts))
	domains := make(map[string]decisionDomain, len(domainCounts))
	for key, domain := range domainCounts {
		if _, ok := revisionSumWeight(domain.roleCounts, requiredRoles); !ok {
			continue
		}
		covered[key] = domain.roleCounts
		domains[key] = domain
	}

	if mode == GatingModeMaxRole {
		role := dominantRole(covered, requiredRoles)
		for key, domain := range domains {
			domain.weight = domain.roleCounts[role]
			domains[key] = domain
		}
	} else {
		for key, domain := range domains {
			weight, _ := revisionSumWeight(domain.roleCounts, requiredRoles)
			domain.weight = weight
			domains[key] = domain
		}
	}

	total := 0
	for _, domain := range domains {
		total += domain.weight
	}

	shares := make(map[string]float64, len(roleCounts))
	if total > 0 {
		for key, domain := range domains {
			shares[domain.decision.Revision] += float64(domain.weight) / float64(total)
			domains[key] = domain
		}
	}
	return compatibilityDistribution{roleCounts: roleCounts, shares: shares, domains: domains}
}

func dominantRole(covered map[string]map[string]int, requiredRoles []string) string {
	dominant := ""
	dominantCount := -1
	for _, role := range requiredRoles {
		total := 0
		for _, perRole := range covered {
			total += perRole[role]
		}
		// RequiredRoles order resolves ties deterministically.
		if total > dominantCount {
			dominant = role
			dominantCount = total
		}
	}
	return dominant
}

func (c *Screener) distributionSnapshot() compatibilityDistribution {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.distribution
}
