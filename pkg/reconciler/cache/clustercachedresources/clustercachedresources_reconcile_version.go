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

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/kcp-dev/logicalcluster/v3"
	cachev1alpha1 "github.com/kcp-dev/sdk/apis/cache/v1alpha1"
)

// versionResolver is the first reconciler in the chain. It discovers the preferred API version
// for the group+resource named in the spec and populates reconcileContext.resolvedGVR so that
// all downstream reconcilers can use a complete GVR without re-doing discovery.
type versionResolver struct {
	getPreferredGVR func(cluster logicalcluster.Name, gr schema.GroupResource) (schema.GroupVersionResource, error)
}

func (r *versionResolver) reconcile(ctx context.Context, rctx *reconcileContext, clusterCachedResource *cachev1alpha1.ClusterCachedResource) (reconcileStatus, error) {
	gr := schema.GroupResource{
		Group:    clusterCachedResource.Spec.Group,
		Resource: clusterCachedResource.Spec.Resource,
	}

	gvr, err := r.getPreferredGVR(logicalcluster.From(clusterCachedResource), gr)
	if err != nil {
		// During deletion: if the resource is gone from the API but we still have stored versions,
		// fall back to the first stored version so the purge and drain steps can proceed.
		if !clusterCachedResource.DeletionTimestamp.IsZero() && len(clusterCachedResource.Status.ReplicatedVersions) > 0 {
			rctx.resolvedGVR = schema.GroupVersionResource{
				Group:    gr.Group,
				Version:  clusterCachedResource.Status.ReplicatedVersions[0],
				Resource: gr.Resource,
			}
			return reconcileStatusContinue, nil
		}
		return reconcileStatusStopAndRequeue, err
	}

	rctx.resolvedGVR = gvr
	return reconcileStatusContinue, nil
}
