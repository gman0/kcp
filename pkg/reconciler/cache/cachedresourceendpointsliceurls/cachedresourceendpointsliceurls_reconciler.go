/*
Copyright 2025 The KCP Authors.

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

package cachedresourceendpointsliceurls

import (
	"context"
	"net/url"
	"path"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"

	"github.com/kcp-dev/logicalcluster/v3"

	virtualworkspacesoptions "github.com/kcp-dev/kcp/cmd/virtual-workspaces/options"
	"github.com/kcp-dev/kcp/pkg/logging"
	replicationvw "github.com/kcp-dev/kcp/pkg/virtual/replication"
	apisv1alpha2 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha2"
	cachev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/cache/v1alpha1"
	corev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/core/v1alpha1"
	"github.com/kcp-dev/kcp/sdk/apis/third_party/conditions/util/conditions"
	cachev1alpha1apply "github.com/kcp-dev/kcp/sdk/client/applyconfiguration/cache/v1alpha1"
)

type endpointsReconciler struct {
	getMyShard                       func() (*corev1alpha1.Shard, error)
	getCachedResource                func(path logicalcluster.Path, name string) (*cachev1alpha1.CachedResource, error)
	getAPIExportByCachedResource     func(cr *cachev1alpha1.CachedResource) (*apisv1alpha2.APIExport, error)
	listAPIBindingsByAPIExport       func(apiexport *apisv1alpha2.APIExport) ([]*apisv1alpha2.APIBinding, error)
	patchCachedResourceEndpointSlice func(ctx context.Context, cluster logicalcluster.Path, patch *cachev1alpha1apply.CachedResourceEndpointSliceApplyConfiguration) error
	shardName                        string
}

type result struct {
	url    string
	remove bool
}

func (c *controller) reconcile(ctx context.Context, cachedResourceEndpointSlice *cachev1alpha1.CachedResourceEndpointSlice) (bool, error) {
	r := &endpointsReconciler{
		getMyShard:                       c.getMyShard,
		getCachedResource:                c.getCachedResource,
		getAPIExportByCachedResource:     c.getAPIExportByCachedResource,
		listAPIBindingsByAPIExport:       c.listAPIBindingsByAPIExport,
		patchCachedResourceEndpointSlice: c.patchCachedResourceEndpointSlice,
		shardName:                        c.shardName,
	}

	return r.reconcile(ctx, cachedResourceEndpointSlice)
}

func (r *endpointsReconciler) reconcile(ctx context.Context, slice *cachev1alpha1.CachedResourceEndpointSlice) (bool, error) {
	for _, condition := range slice.Status.Conditions {
		if !conditions.IsTrue(slice, condition.Type) {
			return false, nil
		}
	}

	selector, err := labels.Parse(slice.Status.ShardSelector)
	if err != nil {
		return false, err
	}

	cachedResourcePath := logicalcluster.NewPath(slice.Spec.CachedResource.Path)
	if cachedResourcePath.Empty() {
		cachedResourcePath = logicalcluster.From(slice).Path()
	}

	cr, err := r.getCachedResource(cachedResourcePath, slice.Spec.CachedResource.Name)
	if err != nil {
		return false, err
	}

	apiExport, err := r.getAPIExportByCachedResource(cr)
	if err != nil {
		return false, err
	}

	thisShard, err := r.getMyShard()
	if err != nil {
		return true, err
	}

	rs, err := r.updateEndpoints(ctx, slice, cr, apiExport, thisShard, selector)
	if err != nil {
		return true, err
	}
	if rs == nil {
		// No change, nothing to do.
		return false, nil
	}

	// Patch the object.
	patch := cachev1alpha1apply.CachedResourceEndpointSlice(slice.Name)
	if rs.remove {
		patch.WithStatus(cachev1alpha1apply.CachedResourceEndpointSliceStatus())
	} else {
		patch.WithStatus(cachev1alpha1apply.CachedResourceEndpointSliceStatus().
			WithCachedResourceEndpoints(cachev1alpha1apply.CachedResourceEndpoint().WithURL(rs.url)))
	}
	err = r.patchCachedResourceEndpointSlice(ctx, logicalcluster.From(slice).Path(), patch)
	if err != nil {
		return true, err
	}

	return false, nil
}

func (r *endpointsReconciler) updateEndpoints(ctx context.Context,
	slice *cachev1alpha1.CachedResourceEndpointSlice,
	cr *cachev1alpha1.CachedResource,
	crApiExport *apisv1alpha2.APIExport,
	shard *corev1alpha1.Shard,
	selector labels.Selector,
) (*result, error) {
	logger := klog.FromContext(ctx)
	if shard.Spec.VirtualWorkspaceURL == "" {
		return nil, nil
	}

	bindings, err := r.listAPIBindingsByAPIExport(crApiExport)
	if err != nil {
		return nil, err
	}

	if selector.Matches(labels.Set(shard.Labels)) { // We are in a partition.
		if len(bindings) == 0 { // We have no consumers.
			return &result{
				remove: true,
			}, nil
		}
	} else { // We are not in a partition.
		if len(bindings) == 0 { // We have no consumers, we can remove the endpoint.
			return &result{
				remove: true,
			}, nil
		} else {
			// We are not in a partition, but we have consumers.
			// Do nothing, as we are on the way to be orphaned.
			return nil, nil
		}
	}

	vwURL, err := url.Parse(shard.Spec.VirtualWorkspaceURL)
	if err != nil {
		logger = logging.WithObject(logger, shard)
		logger.Error(
			err, "error parsing shard.spec.virtualWorkspaceURL",
			"VirtualWorkspaceURL", shard.Spec.VirtualWorkspaceURL,
		)
		return nil, nil
	}

	// Formats the Replication VW URL like so:
	//   <Shard URL>/services/replication/<CachedResource cluster>/<CachedResource name>
	vwURL.Path = path.Join(
		vwURL.Path,
		virtualworkspacesoptions.DefaultRootPathPrefix,
		replicationvw.VirtualWorkspaceName,
		logicalcluster.From(cr).String(),
		cr.Name,
	)
	completeVWAddr := vwURL.String()

	for _, u := range slice.Status.CachedResourceEndpoints {
		if u.URL == completeVWAddr {
			// VW URL already in the endpoint slice, nothing to do.
			return nil, nil
		}
	}

	return &result{
		url: completeVWAddr,
	}, nil
}
