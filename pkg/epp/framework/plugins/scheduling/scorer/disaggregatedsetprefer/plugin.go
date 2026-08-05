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

package disaggregatedsetprefer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	disaggregatedsetrollout "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/screener/disaggregatedsetrollout"
)

const PluginType = "disaggregatedset-prefer-scorer"

type parameters struct {
	ScreenerRef string `json:"screenerRef" pluginRef:""`
}

// ConfigParser exposes screenerRef to the plugin dependency loader.
func ConfigParser(rawParameters *json.Decoder, _ fwkplugin.Handle) (any, error) {
	config := parameters{}
	if rawParameters == nil {
		return nil, errors.New("disaggregatedset-prefer-scorer requires parameters")
	}
	if err := rawParameters.Decode(&config); err != nil {
		return nil, fmt.Errorf("decode disaggregatedset-prefer-scorer parameters: %w", err)
	}
	if config.ScreenerRef == "" {
		return nil, errors.New("screenerRef is required")
	}
	return config, nil
}

// Factory creates a soft-affinity scorer from a rollout screener's prefer
// selectors.
func Factory(name string, rawParameters *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	parsed, err := ConfigParser(rawParameters, handle)
	if err != nil {
		return nil, err
	}
	config := parsed.(parameters)
	plugin := handle.Plugin(config.ScreenerRef)
	screener, ok := plugin.(*disaggregatedsetrollout.Screener)
	if !ok {
		return nil, fmt.Errorf("screenerRef %q is not a %s plugin", config.ScreenerRef, disaggregatedsetrollout.PluginType)
	}
	selectors := screener.PreferenceSelectors()
	if len(selectors) == 0 {
		return nil, fmt.Errorf("screenerRef %q has no prefer-mode header selectors", config.ScreenerRef)
	}
	if name == "" {
		name = PluginType
	}
	return &Scorer{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: name},
		selectors: selectors,
	}, nil
}

// Scorer gives endpoints matching configured prefer selectors a soft affinity
// score without removing non-matching endpoints.
type Scorer struct {
	typedName fwkplugin.TypedName
	selectors []disaggregatedsetrollout.HeaderSelector
}

var _ fwksched.Scorer = (*Scorer)(nil)

func (s *Scorer) TypedName() fwkplugin.TypedName { return s.typedName }

func (s *Scorer) Category() fwksched.ScorerCategory { return fwksched.Affinity }

func (s *Scorer) Score(_ context.Context, request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) map[fwksched.Endpoint]float64 {
	scores := make(map[fwksched.Endpoint]float64, len(endpoints))
	for _, endpoint := range endpoints {
		scores[endpoint] = 0
	}
	if request == nil {
		return scores
	}

	activeSelectors := 0
	for _, selector := range s.selectors {
		requested := request.Headers[selector.HeaderName]
		if requested == "" {
			continue
		}
		activeSelectors++
		for _, endpoint := range endpoints {
			if endpoint == nil || endpoint.GetMetadata() == nil {
				continue
			}
			if endpoint.GetMetadata().Labels[selector.LabelKey] == requested {
				scores[endpoint]++
			}
		}
	}
	if activeSelectors > 0 {
		for endpoint := range scores {
			scores[endpoint] /= float64(activeSelectors)
		}
	}
	return scores
}
