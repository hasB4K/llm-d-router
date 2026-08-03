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
	"sort"

	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

// Screen applies revision gating and strict selectors before scheduling
// profiles observe the endpoint set.
func (c *Controller) Screen(ctx context.Context, request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) []fwksched.Endpoint {
	current := append(make([]fwksched.Endpoint, 0, len(endpoints)), endpoints...)
	if c.config.RevisionGating.Active() {
		if request == nil {
			return nil
		}
		seenRevisions := uniqueRevisions(current, c.revisionLabelKey)
		distribution := c.distributionSnapshot()
		shares := make(map[string]float64, len(seenRevisions))
		allowedRevisions := make(map[string]struct{}, len(seenRevisions))
		for revision := range seenRevisions {
			share := distribution.shares[revision]
			if share == 0 {
				recordGatingDropped(revision)
				continue
			}
			shares[revision] = share
			allowedRevisions[revision] = struct{}{}
		}
		chosenRevision := ""
		if !c.hasStrictHeader(request) {
			chosenRevision = c.pickWeightedRevision(shares)
		}
		current = c.applyRevisionDecision(current, allowedRevisions, chosenRevision)
		if len(current) == 0 {
			return nil
		}
	}
	return c.filterStrictSelectors(ctx, request, current)
}

func (c *Controller) applyRevisionDecision(
	endpoints []fwksched.Endpoint,
	allowedRevisions map[string]struct{},
	chosenRevision string,
) []fwksched.Endpoint {
	result := make([]fwksched.Endpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint == nil || endpoint.GetMetadata() == nil {
			continue
		}
		revision := endpoint.GetMetadata().Labels[c.revisionLabelKey]
		if _, covered := allowedRevisions[revision]; !covered {
			continue
		}
		if chosenRevision != "" && revision != chosenRevision {
			continue
		}
		result = append(result, endpoint)
	}
	return result
}

func (c *Controller) filterStrictSelectors(_ context.Context, request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) []fwksched.Endpoint {
	current := append(make([]fwksched.Endpoint, 0, len(endpoints)), endpoints...)
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
			recordFilterOutcome(selector.Name, selector.Mode, filterOutcomeNoMatchStrict)
		} else {
			recordFilterOutcome(selector.Name, selector.Mode, filterOutcomeMatched)
		}
		if len(current) == 0 {
			return current
		}
	}
	return current
}

func (c *Controller) hasStrictHeader(request *fwksched.InferenceRequest) bool {
	if request == nil {
		return false
	}
	for _, selector := range c.config.HeaderSelectors {
		if selector.Mode == ModeStrict && request.Headers[selector.HeaderName] != "" {
			return true
		}
	}
	return false
}

func uniqueRevisions(endpoints []fwksched.Endpoint, revisionLabelKey string) map[string]struct{} {
	seen := make(map[string]struct{})
	for _, endpoint := range endpoints {
		if endpoint == nil || endpoint.GetMetadata() == nil {
			continue
		}
		if revision := endpoint.GetMetadata().Labels[revisionLabelKey]; revision != "" {
			seen[revision] = struct{}{}
		}
	}
	return seen
}

func revisionWeight(perRole map[string]int, required []string, useMaxRole bool) int {
	weight := 0
	for _, role := range required {
		count := perRole[role]
		if count == 0 {
			return 0
		}
		if useMaxRole {
			weight = max(weight, count)
		} else {
			weight += count
		}
	}
	return weight
}

func (c *Controller) pickWeightedRevision(shares map[string]float64) string {
	revisions := make([]string, 0, len(shares))
	total := 0.0
	for revision, share := range shares {
		revisions = append(revisions, revision)
		total += share
	}
	if total == 0 {
		return ""
	}
	sort.Strings(revisions)
	if len(revisions) == 1 {
		return revisions[0]
	}
	x := c.rand01() * total
	cumulative := 0.0
	for _, revision := range revisions {
		cumulative += shares[revision]
		if x < cumulative {
			return revision
		}
	}
	return revisions[len(revisions)-1]
}

type preferScorerParameters struct {
	RouterRef string `json:"routerRef" pluginRef:""`
}

// PreferScorerConfigParser exposes routerRef to the plugin dependency loader.
func PreferScorerConfigParser(parameters *json.Decoder, _ fwkplugin.Handle) (any, error) {
	config := preferScorerParameters{}
	if parameters == nil {
		return nil, errors.New("disaggregation-prefer-scorer requires parameters")
	}
	if err := parameters.Decode(&config); err != nil {
		return nil, fmt.Errorf("decode disaggregation-prefer-scorer parameters: %w", err)
	}
	if config.RouterRef == "" {
		return nil, errors.New("routerRef is required")
	}
	return config, nil
}

// PreferScorerFactory creates a soft-affinity scorer using the referenced
// router's prefer selectors.
func PreferScorerFactory(name string, parameters *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	parsed, err := PreferScorerConfigParser(parameters, handle)
	if err != nil {
		return nil, err
	}
	config := parsed.(preferScorerParameters)
	plugin := handle.Plugin(config.RouterRef)
	router, ok := plugin.(*Controller)
	if !ok {
		return nil, fmt.Errorf("routerRef %q is not a %s plugin", config.RouterRef, RouterType)
	}
	if !router.config.HasHeaderSelectorsInMode(ModePrefer) {
		return nil, fmt.Errorf("routerRef %q has no prefer-mode header selectors", config.RouterRef)
	}
	if name == "" {
		name = PreferScorerType
	}
	return &preferScorer{
		typedName: fwkplugin.TypedName{Type: PreferScorerType, Name: name},
		router:    router,
	}, nil
}

type preferScorer struct {
	typedName fwkplugin.TypedName
	router    *Controller
}

var _ fwksched.Scorer = (*preferScorer)(nil)

func (s *preferScorer) TypedName() fwkplugin.TypedName { return s.typedName }

func (s *preferScorer) Category() fwksched.ScorerCategory { return fwksched.Affinity }

func (s *preferScorer) Score(_ context.Context, request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) map[fwksched.Endpoint]float64 {
	scores := make(map[fwksched.Endpoint]float64, len(endpoints))
	for _, endpoint := range endpoints {
		scores[endpoint] = 0
	}
	if request == nil {
		return scores
	}

	activeSelectors := 0
	for _, selector := range s.router.config.HeaderSelectors {
		if selector.Mode != ModePrefer {
			continue
		}
		requested := request.Headers[selector.HeaderName]
		if requested == "" {
			continue
		}
		activeSelectors++
		matched := false
		for _, endpoint := range endpoints {
			if endpoint == nil || endpoint.GetMetadata() == nil {
				continue
			}
			if endpoint.GetMetadata().Labels[selector.LabelKey] == requested {
				scores[endpoint]++
				matched = true
			}
		}
		if matched {
			recordFilterOutcome(selector.Name, selector.Mode, filterOutcomeMatched)
		} else {
			recordFilterOutcome(selector.Name, selector.Mode, filterOutcomeNoMatchPreferFallback)
		}
	}
	if activeSelectors > 1 {
		for endpoint := range scores {
			scores[endpoint] /= float64(activeSelectors)
		}
	}
	return scores
}
