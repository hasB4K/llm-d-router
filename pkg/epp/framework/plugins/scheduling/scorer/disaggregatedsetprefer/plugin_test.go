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
	"testing"

	"k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	disaggregatedsetrollout "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/screener/disaggregatedsetrollout"
	testutils "github.com/llm-d/llm-d-router/test/utils"
)

func TestScorerMatchesWithoutRemovingCandidates(t *testing.T) {
	scorer := &Scorer{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: "prefer"},
		selectors: []disaggregatedsetrollout.HeaderSelector{{
			Name: "revision", HeaderName: "x-disagg-revision", LabelKey: "revision", Mode: disaggregatedsetrollout.ModePrefer,
		}},
	}
	candidates := []fwksched.Endpoint{endpoint("v1", map[string]string{"revision": "v1"}), endpoint("v2", map[string]string{"revision": "v2"})}
	request := &fwksched.InferenceRequest{Headers: map[string]string{"x-disagg-revision": "v2"}}

	scores := scorer.Score(context.Background(), request, candidates)
	if scores[candidates[0]] != 0 || scores[candidates[1]] != 1 {
		t.Fatalf("unexpected prefer scores: %v", scores)
	}

	request.Headers["x-disagg-revision"] = "missing"
	scores = scorer.Score(context.Background(), request, candidates)
	if len(scores) != len(candidates) || scores[candidates[0]] != 0 || scores[candidates[1]] != 0 {
		t.Fatalf("no-match must leave every candidate at zero: %v", scores)
	}
	if scorer.Category() != fwksched.Affinity {
		t.Fatalf("prefer scorer category = %q, want %q", scorer.Category(), fwksched.Affinity)
	}
}

func TestScorerAveragesMultipleSelectors(t *testing.T) {
	scorer := &Scorer{selectors: []disaggregatedsetrollout.HeaderSelector{
		{Name: "revision", HeaderName: "x-disagg-revision", LabelKey: "revision", Mode: disaggregatedsetrollout.ModePrefer},
		{Name: "slice", HeaderName: "x-disagg-slice", LabelKey: "slice", Mode: disaggregatedsetrollout.ModePrefer},
	}}
	candidates := []fwksched.Endpoint{
		endpoint("both", map[string]string{"revision": "v2", "slice": "s2"}),
		endpoint("revision", map[string]string{"revision": "v2", "slice": "s1"}),
		endpoint("neither", map[string]string{"revision": "v1", "slice": "s1"}),
	}
	request := &fwksched.InferenceRequest{Headers: map[string]string{
		"x-disagg-revision": "v2",
		"x-disagg-slice":    "s2",
	}}

	scores := scorer.Score(context.Background(), request, candidates)

	if scores[candidates[0]] != 1 || scores[candidates[1]] != 0.5 || scores[candidates[2]] != 0 {
		t.Fatalf("unexpected multi-selector scores: %v", scores)
	}
}

func TestFactoryLinksScreener(t *testing.T) {
	screenerConfig := json.RawMessage(`{
		"scope":{"labelSelector":"disaggregatedset.x-k8s.io/name=my-set","namespace":"default"},
		"headerSelectors":[{"name":"slice","headerName":"x-disagg-slice","labelKey":"disaggregatedset.x-k8s.io/slice","mode":"prefer"}]
	}`)
	plugin, err := disaggregatedsetrollout.Factory("test-screener", fwkplugin.StrictDecoder(screenerConfig), nil)
	if err != nil {
		t.Fatalf("create screener: %v", err)
	}
	handle := testutils.NewTestHandle(context.Background())
	handle.AddPlugin("test-screener", plugin)
	raw := json.RawMessage(`{"screenerRef":"test-screener"}`)

	created, err := Factory("prefer", fwkplugin.StrictDecoder(raw), handle)
	if err != nil {
		t.Fatalf("Factory: %v", err)
	}
	scorer := created.(*Scorer)
	if len(scorer.selectors) != 1 || scorer.selectors[0].Name != "slice" {
		t.Fatalf("unexpected selectors: %#v", scorer.selectors)
	}
}

func endpoint(name string, labels map[string]string) fwksched.Endpoint {
	return fwksched.NewEndpoint(&fwkdl.EndpointMetadata{
		ID:     types.NamespacedName{Namespace: "default", Name: name},
		Name:   name,
		Labels: labels,
	}, nil, nil)
}
