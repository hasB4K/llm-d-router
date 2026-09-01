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
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/log"

	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

var errInvalidSharedDecision = errors.New("cross-replica syncer returned an invalid compatibility decision")

// Screen applies revision gating and strict selectors before scheduling
// profiles observe the endpoint set.
func (c *Screener) Screen(ctx context.Context, request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) []fwksched.Endpoint {
	if !c.config.RevisionGating.Active() {
		return c.screenStrictSelectors(ctx, request, endpoints)
	}
	if request == nil {
		return nil
	}

	// TODO: Make the initial Pod snapshot a condition of EPP readiness.
	// Until a generic readiness hook exists, an empty snapshot fails closed.
	domains := c.eligibleDomains(request, endpoints, c.distributionSnapshot())
	if len(domains) == 0 {
		return nil
	}

	chosen := pickWeightedDecision(domains, rand.Float64())
	if chosen.Revision == "" {
		return nil
	}
	if decisionID := compatibilityDecisionID(request); decisionID != "" {
		var err error
		chosen, err = c.getOrSetDecision(ctx, decisionID, chosen)
		if err != nil {
			log.FromContext(ctx).Error(err, "failed to coordinate compatibility decision")
			return nil
		}
	}

	// A decision returned by a different EPP replica must satisfy this
	// request's strict headers and be covered by this local Pod snapshot.
	if !c.matchesStrictHeaders(request, chosen) || !containsDecision(domains, c.decisionKey(chosen)) {
		return nil
	}
	return c.applyDecision(endpoints, chosen)
}

// compatibilityDecisionID uses the coordinator-provided revision-decision ID
// as the correlation key for the complete compatibility decision.
func compatibilityDecisionID(request *fwksched.InferenceRequest) string {
	if request == nil {
		return ""
	}
	if decisionID := request.Headers[reqcommon.RevisionDecisionIDHeaderKey]; decisionID != "" {
		return decisionID
	}
	return request.Headers[reqcommon.RequestIDHeaderKey]
}

func (c *Screener) getOrSetDecision(ctx context.Context, id string, candidate compatibilityDecision) (compatibilityDecision, error) {
	encodedCandidate, err := json.Marshal(candidate)
	if err != nil {
		return compatibilityDecision{}, err
	}

	var actual string
	if c.syncer == nil {
		actual, _ = c.localDecisions.GetOrSet(id, string(encodedCandidate))
	} else {
		value, _, getOrSetErr := c.syncer.GetOrSet(ctx, c.compatibilityDecisionStateKey, id, string(encodedCandidate))
		if getOrSetErr != nil {
			return compatibilityDecision{}, getOrSetErr
		}
		var ok bool
		actual, ok = value.(string)
		if !ok {
			return compatibilityDecision{}, errInvalidSharedDecision
		}
	}

	decision := compatibilityDecision{}
	if err := json.Unmarshal([]byte(actual), &decision); err != nil || !c.validDecision(decision) {
		return compatibilityDecision{}, errInvalidSharedDecision
	}
	return decision, nil
}

func (c *Screener) validDecision(decision compatibilityDecision) bool {
	if decision.Revision == "" || len(decision.Labels) != len(c.strictDecisionLabelKeys) {
		return false
	}
	for _, labelKey := range c.strictDecisionLabelKeys {
		if decision.Labels[labelKey] == "" {
			return false
		}
	}
	return true
}

func (c *Screener) eligibleDomains(
	request *fwksched.InferenceRequest,
	endpoints []fwksched.Endpoint,
	distribution compatibilityDistribution,
) []decisionDomain {
	result := make([]decisionDomain, 0, len(distribution.domains))
	for _, domain := range distribution.domains {
		if !c.matchesStrictHeaders(request, domain.decision) || !c.hasEndpointForDecision(endpoints, domain.decision) {
			continue
		}
		result = append(result, domain)
	}
	return result
}

func (c *Screener) matchesStrictHeaders(request *fwksched.InferenceRequest, decision compatibilityDecision) bool {
	if request == nil {
		return true
	}
	for _, selector := range c.config.HeaderSelectors {
		if selector.Mode != ModeStrict {
			continue
		}
		requested := request.Headers[selector.HeaderName]
		if requested == "" {
			continue
		}

		actual := decision.Labels[selector.LabelKey]
		if selector.LabelKey == c.revisionLabelKey {
			actual = decision.Revision
		}
		if actual != requested {
			return false
		}
	}
	return true
}

func (c *Screener) hasEndpointForDecision(endpoints []fwksched.Endpoint, decision compatibilityDecision) bool {
	for _, endpoint := range endpoints {
		if c.endpointMatchesDecision(endpoint, decision) {
			return true
		}
	}
	return false
}

func (c *Screener) applyDecision(endpoints []fwksched.Endpoint, decision compatibilityDecision) []fwksched.Endpoint {
	result := make([]fwksched.Endpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if c.endpointMatchesDecision(endpoint, decision) {
			result = append(result, endpoint)
		}
	}
	return result
}

func (c *Screener) endpointMatchesDecision(endpoint fwksched.Endpoint, decision compatibilityDecision) bool {
	if endpoint == nil || endpoint.GetMetadata() == nil {
		return false
	}
	endpointLabels := endpoint.GetMetadata().Labels
	if endpointLabels[c.revisionLabelKey] != decision.Revision {
		return false
	}
	for _, labelKey := range c.strictDecisionLabelKeys {
		if endpointLabels[labelKey] != decision.Labels[labelKey] {
			return false
		}
	}
	return true
}

func (c *Screener) decisionKey(decision compatibilityDecision) string {
	var builder strings.Builder
	appendDecisionPart(&builder, decision.Revision)
	for _, labelKey := range c.strictDecisionLabelKeys {
		appendDecisionPart(&builder, decision.Labels[labelKey])
	}
	return builder.String()
}

func appendDecisionPart(builder *strings.Builder, value string) {
	builder.WriteString(strconv.Itoa(len(value)))
	builder.WriteByte(':')
	builder.WriteString(value)
}

func containsDecision(domains []decisionDomain, key string) bool {
	for _, domain := range domains {
		if domain.key == key {
			return true
		}
	}
	return false
}

func (c *Screener) screenStrictSelectors(_ context.Context, request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) []fwksched.Endpoint {
	current := endpoints
	if request == nil || len(current) == 0 {
		return current
	}
	for _, selector := range c.config.HeaderSelectors {
		if selector.Mode != ModeStrict {
			continue
		}
		requested := request.Headers[selector.HeaderName]
		if requested == "" {
			continue
		}
		matched := make([]fwksched.Endpoint, 0, len(current))
		for _, endpoint := range current {
			if endpoint == nil || endpoint.GetMetadata() == nil {
				continue
			}
			if endpoint.GetMetadata().Labels[selector.LabelKey] == requested {
				matched = append(matched, endpoint)
			}
		}
		current = matched
		if len(matched) == 0 {
			recordStrictHeaderNoMatch(c.typedName.Name, selector.Name)
		}
		if len(current) == 0 {
			return current
		}
	}
	return current
}

func pickWeightedDecision(domains []decisionDomain, draw float64) compatibilityDecision {
	if len(domains) == 0 {
		return compatibilityDecision{}
	}
	ordered := append([]decisionDomain(nil), domains...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].key < ordered[j].key })

	total := 0
	for _, domain := range ordered {
		total += domain.weight
	}
	if total == 0 {
		return compatibilityDecision{}
	}

	x := draw * float64(total)
	cumulative := 0
	for _, domain := range ordered {
		cumulative += domain.weight
		if x < float64(cumulative) {
			return domain.decision
		}
	}
	return ordered[len(ordered)-1].decision
}

func revisionSumWeight(perRole map[string]int, required []string) (int, bool) {
	weight := 0
	for _, role := range required {
		count := perRole[role]
		if count == 0 {
			return 0, false
		}
		weight += count
	}
	return weight, true
}
