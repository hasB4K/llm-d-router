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
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

// BindNotificationSource registers a watcher/reconciler for the source's GVK.
// The framework core owns the cache and reconciliation; the source only receives
// deep-copied events via Notify.
func BindNotificationSource(src fwkdl.NotificationSource, extractors []fwkdl.NotificationExtractor, mgr ctrl.Manager) error {
	gvk := src.GVK()
	log := mgr.GetLogger().WithName("notification-controller").WithValues("gvk", gvk.Kind)

	reconciler := &notificationReconciler{
		client:     mgr.GetClient(),
		src:        src,
		extractors: extractors,
		gvk:        gvk,
		log:        log,
	}
	for _, extractor := range extractors {
		if syncExtractor, ok := extractor.(fwkdl.NotificationSyncExtractor); ok {
			reconciler.initialSyncExtractors = append(reconciler.initialSyncExtractors, syncExtractor)
		}
	}

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)

	// use the source's name to make the controller name unique
	// This allows multiple notification sources for the same GVK
	// (needed in tests, Configure() sources still imposes
	// one source per GVK).
	controllerName := "notify_" + strings.ToLower(gvk.Kind) + "_" + src.TypedName().Name

	builder := ctrl.NewControllerManagedBy(mgr).
		// Naming the controller allows you to see specific metrics/logs for this watch
		Named(controllerName)
	if len(reconciler.initialSyncExtractors) == 0 {
		return builder.For(obj).
			WithEventFilter(predicate.ResourceVersionChangedPredicate{}).
			Complete(reconciler)
	}
	watchSource := source.Kind(
		mgr.GetCache(),
		obj,
		&handler.TypedEnqueueRequestForObject[*unstructured.Unstructured]{},
		predicate.TypedResourceVersionChangedPredicate[*unstructured.Unstructured]{},
	)
	return builder.WatchesRawSource(&notificationInitialSyncSource{
		SyncingSource: watchSource,
		reconciler:    reconciler,
	}).Complete(reconciler)
}

// Reconciler for notifications. This is a generic reconciler that can be used for any GVK.
type notificationReconciler struct {
	client                client.Client
	src                   fwkdl.NotificationSource
	extractors            []fwkdl.NotificationExtractor
	initialSyncExtractors []fwkdl.NotificationSyncExtractor
	gvk                   schema.GroupVersionKind
	log                   logr.Logger
}

type notificationInitialSyncSource struct {
	source.SyncingSource
	reconciler *notificationReconciler
}

// WaitForSync initializes extractors after the watch handler is synced and
// before the controller starts reconciliation workers.
func (s *notificationInitialSyncSource) WaitForSync(ctx context.Context) error {
	if err := s.SyncingSource.WaitForSync(ctx); err != nil {
		return err
	}
	return s.reconciler.runInitialSync(ctx)
}

// Reconciler carries out the actual notification logic.
func (rn *notificationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := rn.log.WithValues("resource", req.NamespacedName, "gvk", rn.gvk.String())

	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(rn.gvk)

	event := &fwkdl.NotificationEvent{
		Type:   fwkdl.EventAddOrUpdate,
		Object: u,
	}

	err := rn.client.Get(ctx, req.NamespacedName, u)
	if err != nil {
		if apierrors.IsNotFound(err) {
			u.SetName(req.Name)
			u.SetNamespace(req.Namespace)
			event.Type = fwkdl.EventDelete
		} else {
			log.Error(err, "failed to fetch resource from cache")
			return ctrl.Result{}, err
		}
	}

	return rn.dispatch(ctx, log, event)
}

func (rn *notificationReconciler) runInitialSync(ctx context.Context) error {
	objects := &unstructured.UnstructuredList{}
	objects.SetGroupVersionKind(rn.gvk.GroupVersion().WithKind(rn.gvk.Kind + "List"))
	if err := rn.client.List(ctx, objects); err != nil {
		return fmt.Errorf("list initial %s snapshot: %w", rn.gvk, err)
	}

	for i := range objects.Items {
		object := objects.Items[i].DeepCopy()
		object.SetGroupVersionKind(rn.gvk)
		processed, err := rn.src.Notify(ctx, fwkdl.NotificationEvent{
			Type:   fwkdl.EventAddOrUpdate,
			Object: object,
		})
		if err != nil {
			return fmt.Errorf("process initial %s item: %w", rn.gvk, err)
		}
		if processed == nil {
			continue
		}
		for _, extractor := range rn.initialSyncExtractors {
			if err := extractor.Extract(ctx, *processed); err != nil {
				return fmt.Errorf("extract initial %s item with %s: %w", rn.gvk, extractor.TypedName(), err)
			}
		}
	}

	for _, extractor := range rn.initialSyncExtractors {
		extractor.InitialSyncComplete()
	}
	return nil
}

func (rn *notificationReconciler) dispatch(ctx context.Context, log logr.Logger, event *fwkdl.NotificationEvent) (ctrl.Result, error) {
	log.V(logging.TRACE).Info("processing notification", "eventType", event.Type)

	processed, err := rn.src.Notify(ctx, *event)
	if err != nil {
		log.Error(err, "notifier failed to process event")
		return ctrl.Result{}, err
	}
	if processed == nil {
		return ctrl.Result{}, nil
	}

	for _, ext := range rn.extractors {
		if err := ext.Extract(ctx, *processed); err != nil {
			log.Error(err, "extractor failed", "extractor", ext.TypedName())
		}
	}

	return ctrl.Result{}, nil
}
