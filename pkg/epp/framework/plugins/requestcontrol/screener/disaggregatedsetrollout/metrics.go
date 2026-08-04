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
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	metricsSubsystem = "llm_d_epp"
	metricsPrefix    = "disaggregatedset"
)

var (
	strictHeaderNoMatchTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: metricsSubsystem,
			Name:      metricsPrefix + "_strict_header_no_match_total",
			Help:      "Strict header selections that matched no endpoint and failed closed.",
		},
		[]string{"plugin", "selector"},
	)

	revisionGatingShare = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: metricsSubsystem,
			Name:      metricsPrefix + "_revision_gating_share",
			Help:      "Current weighted revision gating share from 0 to 1; incomplete revisions have share 0.",
		},
		[]string{"plugin", "mode", "revision"},
	)
)

var registerMetricsOnce sync.Once

func registerMetrics() {
	registerMetricsOnce.Do(func() {
		ctrlmetrics.Registry.MustRegister(
			strictHeaderNoMatchTotal,
			revisionGatingShare,
		)
	})
}

func recordStrictHeaderNoMatch(pluginName, selectorName string) {
	strictHeaderNoMatchTotal.WithLabelValues(pluginName, selectorName).Inc()
}

func recordRevisionGatingShares(
	pluginName string,
	mode GatingMode,
	previous revisionDistribution,
	current revisionDistribution,
) {
	for revision := range previous.roleCounts {
		if _, exists := current.roleCounts[revision]; !exists {
			revisionGatingShare.DeleteLabelValues(pluginName, string(mode), revision)
		}
	}
	for revision := range current.roleCounts {
		revisionGatingShare.WithLabelValues(pluginName, string(mode), revision).Set(current.shares[revision])
	}
}
