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

package datalayer

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	extractormocks "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/extractor/mocks"
	sourcenotifications "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/notifications"
)

type initialSyncCaptureExtractor struct {
	*extractormocks.NotificationExtractor
	initialSyncComplete bool
}

func (e *initialSyncCaptureExtractor) InitialSyncComplete() { e.initialSyncComplete = true }

func TestNotificationReconcilerInitialSync(t *testing.T) {
	gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-a", Namespace: "default"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-b", Namespace: "default"}},
	).Build()
	syncExtractor := &initialSyncCaptureExtractor{
		NotificationExtractor: extractormocks.NewNotificationExtractor("initial-sync"),
	}
	regular := extractormocks.NewNotificationExtractor("regular")
	reconciler := &notificationReconciler{
		client:                client,
		src:                   sourcenotifications.NewK8sNotificationSource(sourcenotifications.NotificationSourceType, "pods", gvk),
		extractors:            []fwkdl.NotificationExtractor{syncExtractor, regular},
		initialSyncExtractors: []fwkdl.NotificationSyncExtractor{syncExtractor},
		gvk:                   gvk,
		log:                   logr.Discard(),
	}

	if err := reconciler.runInitialSync(context.Background()); err != nil {
		t.Fatalf("runInitialSync: %v", err)
	}
	if !syncExtractor.initialSyncComplete {
		t.Fatal("extractor was not notified of initial synchronization")
	}
	got := syncExtractor.GetEvents()
	if len(got) != 2 {
		t.Fatalf("got %d initial events, want 2", len(got))
	}
	for _, event := range got {
		if event.Type != fwkdl.EventAddOrUpdate {
			t.Fatalf("initial event type = %v, want add or update", event.Type)
		}
		if event.Object == nil || event.Object.GroupVersionKind() != gvk {
			t.Fatalf("initial event has unexpected object: %#v", event.Object)
		}
	}
	if got := regular.GetEvents(); len(got) != 0 {
		t.Fatalf("regular extractor received initial synchronization events: %#v", got)
	}
}
