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

package disaggrollout

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Metric namespace/subsystem. Landed under the shared llm_d_epp subsystem so
// operators can grep for one namespace when debugging disagg deployments.
const (
	metricsSubsystem = "llm_d_epp"
	metricsPrefix    = "disagg"
)

var (
	headerStampedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: metricsSubsystem,
			Name:      metricsPrefix + "_header_stamped_total",
			Help:      "Response headers stamped by the disaggregation screener, by selector name.",
		},
		[]string{"selector"},
	)

	filterOutcomeTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: metricsSubsystem,
			Name:      metricsPrefix + "_filter_outcome_total",
			Help:      "Per-selector outcomes: matched, no_match_strict, no_match_prefer_fallback.",
		},
		[]string{"selector", "mode", "outcome"},
	)

	gatingDroppedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: metricsSubsystem,
			Name:      metricsPrefix + "_gating_dropped_total",
			Help:      "Requests where the gating filter dropped at least one candidate, by revision.",
		},
		[]string{"revision"},
	)
)

var registerMetricsOnce sync.Once

// registerMetrics attaches disaggregation collectors to the controller-runtime
// metrics registry. It is safe to call multiple times.
func registerMetrics() {
	registerMetricsOnce.Do(func() {
		ctrlmetrics.Registry.MustRegister(
			headerStampedTotal,
			filterOutcomeTotal,
			gatingDroppedTotal,
		)
	})
}

// Filter outcome labels attached to disagg_filter_outcome_total. "absent"
// (no header sent) is deliberately NOT recorded. It is the silent default
// on every request that doesn't opt in, so the counter would balloon with
// near-zero-signal increments.
const (
	// filterOutcomeMatched: header matched at least one candidate. Strict mode
	// narrows the set; prefer mode gives matches a soft affinity score.
	filterOutcomeMatched = "matched"
	// filterOutcomeNoMatchStrict: strict-mode header matched zero candidates;
	// survivor set became empty and the framework will return 503. This is
	// the "no fallback" case operators alert on.
	filterOutcomeNoMatchStrict = "no_match_strict"
	// filterOutcomeNoMatchPreferFallback: prefer-mode header matched zero
	// candidates, so every candidate receives the same zero affinity. Not an
	// error and is expected during rollouts before the client updates its header.
	filterOutcomeNoMatchPreferFallback = "no_match_prefer_fallback"
)

func recordHeaderStamped(selectorName string) {
	headerStampedTotal.WithLabelValues(selectorName).Inc()
}

func recordFilterOutcome(selectorName string, mode SelectorMode, outcome string) {
	filterOutcomeTotal.WithLabelValues(selectorName, string(mode), outcome).Inc()
}

func recordGatingDropped(revision string) {
	gatingDroppedTotal.WithLabelValues(revision).Inc()
}
