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

package requestcontrol

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

type mockPreSchedulingCandidateFilter struct {
	name   string
	filter func([]fwksched.Endpoint) []fwksched.Endpoint
}

func (f *mockPreSchedulingCandidateFilter) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Type: "mock-pre-scheduling-candidate-filter", Name: f.name}
}

func (f *mockPreSchedulingCandidateFilter) FilterCandidates(
	_ context.Context,
	_ *fwksched.InferenceRequest,
	endpoints []fwksched.Endpoint,
) []fwksched.Endpoint {
	return f.filter(endpoints)
}

func TestConfigAddPluginsCollectsPreSchedulingCandidateFilters(t *testing.T) {
	filter := &mockPreSchedulingCandidateFilter{name: "candidate-filter", filter: func(endpoints []fwksched.Endpoint) []fwksched.Endpoint {
		return endpoints
	}}
	config := NewConfig()

	config.AddPlugins(filter)

	require.Len(t, config.preSchedulingCandidateFilters, 1)
	assert.Same(t, filter, config.preSchedulingCandidateFilters[0])
	var _ fwkrc.PreSchedulingCandidateFilter = filter
}

func TestRunPreSchedulingCandidateFiltersIntersectsIndependentSubsets(t *testing.T) {
	endpoints := []fwksched.Endpoint{
		candidateEndpoint("a"),
		candidateEndpoint("b"),
		candidateEndpoint("c"),
	}
	secondInput := []fwksched.Endpoint(nil)
	config := NewConfig().WithPreSchedulingCandidateFilters(
		&mockPreSchedulingCandidateFilter{name: "drop-a", filter: func(endpoints []fwksched.Endpoint) []fwksched.Endpoint {
			return endpoints[1:]
		}},
		&mockPreSchedulingCandidateFilter{name: "keep-c", filter: func(endpoints []fwksched.Endpoint) []fwksched.Endpoint {
			secondInput = append(secondInput, endpoints...)
			return endpoints[2:]
		}},
	)
	director := &Director{requestControlPlugins: *config}

	result := director.runPreSchedulingCandidateFilters(context.Background(), &fwksched.InferenceRequest{}, endpoints)

	require.Len(t, secondInput, 3)
	assert.Equal(t, "a", secondInput[0].GetMetadata().Name)
	require.Len(t, result, 1)
	assert.Equal(t, "c", result[0].GetMetadata().Name)
}

func TestRunPreSchedulingCandidateFiltersAllObserveOriginalSetAfterEmptyResult(t *testing.T) {
	endpoints := []fwksched.Endpoint{candidateEndpoint("a"), candidateEndpoint("b")}
	secondCalled := false
	config := NewConfig().WithPreSchedulingCandidateFilters(
		&mockPreSchedulingCandidateFilter{name: "empty", filter: func([]fwksched.Endpoint) []fwksched.Endpoint {
			return nil
		}},
		&mockPreSchedulingCandidateFilter{name: "observe", filter: func(got []fwksched.Endpoint) []fwksched.Endpoint {
			secondCalled = true
			require.Len(t, got, len(endpoints))
			return got
		}},
	)
	director := &Director{requestControlPlugins: *config}

	result := director.runPreSchedulingCandidateFilters(context.Background(), &fwksched.InferenceRequest{}, endpoints)

	assert.True(t, secondCalled)
	assert.Empty(t, result)
}

func TestRunPreSchedulingCandidateFiltersIsolatesPluginInputs(t *testing.T) {
	endpoints := []fwksched.Endpoint{candidateEndpoint("a"), candidateEndpoint("b")}
	var secondInput []fwksched.Endpoint
	config := NewConfig().WithPreSchedulingCandidateFilters(
		&mockPreSchedulingCandidateFilter{name: "mutate-input", filter: func(got []fwksched.Endpoint) []fwksched.Endpoint {
			got[0] = candidateEndpoint("replacement")
			return got
		}},
		&mockPreSchedulingCandidateFilter{name: "observe", filter: func(got []fwksched.Endpoint) []fwksched.Endpoint {
			secondInput = append(secondInput, got...)
			return got
		}},
	)
	director := &Director{requestControlPlugins: *config}

	director.runPreSchedulingCandidateFilters(context.Background(), &fwksched.InferenceRequest{}, endpoints)

	require.Len(t, secondInput, 2)
	assert.Equal(t, "a", secondInput[0].GetMetadata().Name)
	assert.Equal(t, "a", endpoints[0].GetMetadata().Name)
}

func candidateEndpoint(name string) fwksched.Endpoint {
	return fwksched.NewEndpoint(&fwkdl.EndpointMetadata{
		ID:   types.NamespacedName{Namespace: "default", Name: name},
		Name: name,
	}, nil, nil)
}
