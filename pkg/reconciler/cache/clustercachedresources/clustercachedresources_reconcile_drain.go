/*
Copyright 2025 The kcp Authors.

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

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/klog/v2"

	cachev1alpha1 "github.com/kcp-dev/sdk/apis/cache/v1alpha1"
)

// versionDrainer removes cached objects that were replicated under a previous version of a
// resource after the preferred API version has changed. It mirrors the CRD status.storedVersions
// drain pattern: a version is removed from status.ReplicatedVersions only after its cached
// objects have been fully deleted.
//
// The synthetic CRD in the cache server is built from status.ReplicatedVersions (see crd_lister.go),
// so both old and new versions remain accessible in the cache during the drain window.
type versionDrainer struct {
	listCacheResourcesForVersion   func(ctx context.Context, version string, ccr *cachev1alpha1.ClusterCachedResource) (*unstructured.UnstructuredList, error)
	deleteCacheResourcesForVersion func(ctx context.Context, version string, ccr *cachev1alpha1.ClusterCachedResource) error
}

func (r *versionDrainer) reconcile(ctx context.Context, rctx *reconcileContext, clusterCachedResource *cachev1alpha1.ClusterCachedResource) (reconcileStatus, error) {
	// Only drain during normal operation; deletion is handled by purge.
	if !clusterCachedResource.DeletionTimestamp.IsZero() {
		return reconcileStatusContinue, nil
	}
	if clusterCachedResource.Status.IdentityHash == "" {
		return reconcileStatusContinue, nil
	}

	logger := klog.FromContext(ctx)
	currentVersion := rctx.resolvedGVR.Version

	// Build the new list: keep current version and any stale version that is not yet empty.
	retained := []string{currentVersion}
	for _, version := range clusterCachedResource.Status.ReplicatedVersions {
		if version == currentVersion {
			continue
		}
		resources, err := r.listCacheResourcesForVersion(ctx, version, clusterCachedResource)
		if err != nil {
			// Cannot determine count — retain conservatively.
			logger.Error(err, "failed to list cache resources for stale version, retaining", "version", version)
			retained = append(retained, version)
			continue
		}
		if len(resources.Items) == 0 {
			logger.V(2).Info("stale version fully drained, removing from replicatedVersions", "version", version)
			continue
		}
		// Objects still present — issue a delete and keep the version in the list.
		logger.V(2).Info("draining stale version from cache", "version", version, "count", len(resources.Items))
		if err := r.deleteCacheResourcesForVersion(ctx, version, clusterCachedResource); err != nil {
			logger.Error(err, "failed to delete cache resources for stale version", "version", version)
		}
		retained = append(retained, version)
	}

	clusterCachedResource.Status.ReplicatedVersions = retained
	return reconcileStatusContinue, nil
}
