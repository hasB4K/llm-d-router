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

// Metric tests share process-global Prometheus collectors registered once
// via sync.Once. Do NOT call t.Parallel() in this file because the shared
// registry would race.
package disaggregation

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

// resetMetrics zeros the underlying collectors between tests. Registration is
// process-global via sync.Once so it cannot be re-registered, but Reset() clears
// the observed samples.
func resetMetrics(t *testing.T) {
	t.Helper()
	registerMetrics()
	headerStampedTotal.Reset()
	filterOutcomeTotal.Reset()
	gatingDroppedTotal.Reset()
}

// --- Header stamped --------------------------------------------------------

func TestMetric_HeaderStamped_IncrementsPerSelector(t *testing.T) {
	resetMetrics(t)
	controller := newTestController(validConfig())
	controller.ResponseHeader(context.Background(), nil,
		&fwkrc.Response{Headers: map[string]string{}},
		&fwkdl.EndpointMetadata{Labels: revLabels("v1")},
	)
	if got := testutil.ToFloat64(headerStampedTotal.WithLabelValues("revision")); got != 1 {
		t.Fatalf("want 1, got %v", got)
	}
}

func TestMetric_HeaderStamped_SkipsMissingLabel(t *testing.T) {
	resetMetrics(t)
	controller := newTestController(validConfig())
	controller.ResponseHeader(context.Background(), nil,
		&fwkrc.Response{Headers: map[string]string{}},
		&fwkdl.EndpointMetadata{Labels: map[string]string{}},
	)
	if got := testutil.ToFloat64(headerStampedTotal.WithLabelValues("revision")); got != 0 {
		t.Fatalf("want 0 (no label), got %v", got)
	}
}

// --- Filter outcomes -------------------------------------------------------

func TestMetric_FilterOutcome_Matched(t *testing.T) {
	resetMetrics(t)
	config := validConfig()
	config.RevisionGating = nil
	controller := newTestController(config)
	controller.filterStrictSelectors(context.Background(),
		&fwksched.InferenceRequest{Headers: map[string]string{"x-disagg-revision": "v1"}},
		[]fwksched.Endpoint{endpoint("p1", revLabels("v1"))},
	)
	got := testutil.ToFloat64(filterOutcomeTotal.WithLabelValues("revision", string(ModeStrict), filterOutcomeMatched))
	if got != 1 {
		t.Fatalf("matched: want 1, got %v", got)
	}
}

func TestMetric_FilterOutcome_NoMatchStrict(t *testing.T) {
	resetMetrics(t)
	config := validConfig()
	config.RevisionGating = nil
	controller := newTestController(config)
	controller.filterStrictSelectors(context.Background(),
		&fwksched.InferenceRequest{Headers: map[string]string{"x-disagg-revision": "v99"}},
		[]fwksched.Endpoint{endpoint("p1", revLabels("v1"))},
	)
	got := testutil.ToFloat64(filterOutcomeTotal.WithLabelValues("revision", string(ModeStrict), filterOutcomeNoMatchStrict))
	if got != 1 {
		t.Fatalf("no_match_strict: want 1, got %v", got)
	}
}

func TestMetric_FilterOutcome_NoMatchPreferFallback(t *testing.T) {
	resetMetrics(t)
	config := validConfig()
	config.RevisionGating = nil
	config.HeaderSelectors[0].Mode = ModePrefer
	controller := newTestController(config)
	scorer := &preferScorer{router: controller}
	scorer.Score(context.Background(),
		&fwksched.InferenceRequest{Headers: map[string]string{"x-disagg-revision": "v99"}},
		[]fwksched.Endpoint{endpoint("p1", revLabels("v1"))},
	)
	got := testutil.ToFloat64(filterOutcomeTotal.WithLabelValues("revision", string(ModePrefer), filterOutcomeNoMatchPreferFallback))
	if got != 1 {
		t.Fatalf("prefer_fallback: want 1, got %v", got)
	}
}

// --- RevisionGating dropped -------------------------------------------------------

func TestMetric_GatingDropped_OncePerRevisionPerCall(t *testing.T) {
	resetMetrics(t)
	controller := newTestController(validConfig())
	seedPods(t, controller,
		readyPod("p1", "v1", "prefill"),
		readyPod("p2", "v1", "prefill"),
		readyPod("p3", "v1", "prefill"),
		readyPod("p4", "v2", "prefill"),
		readyPod("d4", "v2", "decode"),
	)
	pods := []fwksched.Endpoint{
		endpoint("p1", revLabels("v1")),
		endpoint("p2", revLabels("v1")),
		endpoint("p3", revLabels("v1")),
		endpoint("p4", revLabels("v2")),
	}

	request := &fwksched.InferenceRequest{Headers: map[string]string{}}
	controller.Screen(context.Background(), request, pods)

	// Three v1 endpoints hit the gate in one call. The counter should read 1,
	// not 3.
	if got := testutil.ToFloat64(gatingDroppedTotal.WithLabelValues("v1")); got != 1 {
		t.Fatalf("v1 dropped once per call: want 1, got %v", got)
	}
	if got := testutil.ToFloat64(gatingDroppedTotal.WithLabelValues("v2")); got != 0 {
		t.Fatalf("v2 satisfied gate: want 0, got %v", got)
	}
}
