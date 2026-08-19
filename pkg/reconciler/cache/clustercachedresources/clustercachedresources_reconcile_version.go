/*
Copyright 2026 The kcp Authors.

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

package clustercachedresources

import (
	"context"
	"slices"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kcp-dev/logicalcluster/v3"
	cachev1alpha1 "github.com/kcp-dev/sdk/apis/cache/v1alpha1"
	conditionsv1alpha1 "github.com/kcp-dev/sdk/apis/third_party/conditions/apis/conditions/v1alpha1"
	"github.com/kcp-dev/sdk/apis/third_party/conditions/util/conditions"

	"github.com/kcp-dev/kcp/pkg/informer"
)

// versionResolver validates that spec.version is currently served by the source workspace,
// tears down any running replication controller whose GVR no longer matches, and gates the
// rest of the reconciler chain until the desired version is confirmed available.
type versionResolver struct {
	getServedGVKs func(cluster logicalcluster.Name, gr schema.GroupResource) ([]schema.GroupVersionKind, error)

	controllerRegistry                   *controllerRegistry
	localDiscoveringDynamicKcpInformers  *informer.DiscoveringDynamicSharedInformerFactory
	globalDiscoveringDynamicKcpInformers *informer.DiscoveringDynamicSharedInformerFactory
}

func (r *versionResolver) reconcile(ctx context.Context, ccr *cachev1alpha1.ClusterCachedResource) (reconcileStatus, error) {
	// During deletion, skip all version checking and teardown.
	// status.storageVersion is persisted from the last successful reconcile; purge and
	// replication use it directly. The chain must be allowed through to drain the cache
	// and remove the finalizer.
	if !ccr.DeletionTimestamp.IsZero() {
		return reconcileStatusContinue, nil
	}

	cluster := logicalcluster.From(ccr)
	gr := schema.GroupResource{Group: ccr.Spec.Group, Resource: ccr.Spec.Resource}
	desired := ccr.Spec.Version

	gvks, err := r.getServedGVKs(cluster, gr)
	if err != nil {
		return reconcileStatusStopAndRequeue, err
	}

	served := sets.New[string]()
	for _, gvk := range gvks {
		served.Insert(gvk.Version)
	}

	// Tear down the running controller if:
	//   (a) its GVR version differs from spec.version (user changed the pin), or
	//   (b) spec.version is no longer served (version removed from source).
	// Both facts come from the single getServedGVKs call — no extra mapper round-trip.
	controllerName := replicationControllerName(ccr)
	if controller := r.controllerRegistry.get(controllerName); controller != nil {
		activeGVR := controller.CurrentGVR()
		if activeGVR.Version != desired || !served.Has(desired) {
			r.controllerRegistry.unregister(controllerName)
			r.localDiscoveringDynamicKcpInformers.ForgetResource(activeGVR)
			r.globalDiscoveringDynamicKcpInformers.ForgetResource(activeGVR)
		}
	}

	if served.Has(desired) {
		conditions.MarkTrue(ccr, cachev1alpha1.StorageVersionAvailable)
		if ccr.Status.StorageVersion != desired {
			ccr.Status.StorageVersion = desired
			if !slices.Contains(ccr.Status.StoredVersions, desired) {
				ccr.Status.StoredVersions = append(ccr.Status.StoredVersions, desired)
			}
			return reconcileStatusStopAndRequeue, nil
		}
		return reconcileStatusContinue, nil
	}

	if !conditions.IsFalse(ccr, cachev1alpha1.StorageVersionAvailable) {
		conditions.MarkFalse(ccr, cachev1alpha1.StorageVersionAvailable,
			cachev1alpha1.RequestedVersionNotServedReason,
			conditionsv1alpha1.ConditionSeverityError,
			"version %s is not served", desired)
		return reconcileStatusStopAndRequeue, nil
	}

	return reconcileStatusStop, nil
}
