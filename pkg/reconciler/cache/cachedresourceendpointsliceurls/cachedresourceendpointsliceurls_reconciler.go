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
	"fmt"
	"net/url"
	"path"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"

	kcpcache "github.com/kcp-dev/apimachinery/v2/pkg/cache"
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

type slicesReconciler struct {
	getMyShard                                  func() (*corev1alpha1.Shard, error)
	getCachedResource                           func(path logicalcluster.Path, name string) (*cachev1alpha1.CachedResource, error)
	listAPIBindingsByAPIExport                  func(apiexport *apisv1alpha2.APIExport) ([]*apisv1alpha2.APIBinding, error)
	listAPIExportsByCachedResourceEndpointSlice func(slice *cachev1alpha1.CachedResourceEndpointSlice) ([]*apisv1alpha2.APIExport, error)
	patchCachedResourceEndpointSlice            func(ctx context.Context, cluster logicalcluster.Path, patch *cachev1alpha1apply.CachedResourceEndpointSliceApplyConfiguration) error
	shardName                                   string

	getAPIExport                   func(path logicalcluster.Path, name string) (*apisv1alpha2.APIExport, error)
	getCachedResourceEndpointSlice func(path logicalcluster.Path, name string) (*cachev1alpha1.CachedResourceEndpointSlice, error)
}

type result struct {
	url    string
	remove bool
}

func (c *controller) reconcile(ctx context.Context, binding *apisv1alpha2.APIBinding) (bool, error) {
	return slicesReconciler{
		getMyShard:                       c.getMyShard,
		listAPIBindingsByAPIExport:       c.listAPIBindingsByAPIExport,
		patchCachedResourceEndpointSlice: c.patchCachedResourceEndpointSlice,
		shardName:                        c.shardName,
	}.reconcile(ctx, binding)
}

func (r slicesReconciler) reconcile(ctx context.Context, binding *apisv1alpha2.APIBinding) (bool, error) {
	exportPath := logicalcluster.NewPath(binding.Spec.Reference.Export.Path)
	if exportPath.Empty() {
		exportPath = logicalcluster.From(binding).Path()
	}
	export, err := r.getAPIExport(exportPath, binding.Spec.Reference.Export.Name)
	if err != nil {
		return true, err
	}

	type sliceRef struct {
		path logicalcluster.Path
		name string
	}
	var slices []sliceRef

	for _, exportedResource := range export.Spec.Resources {
		virtualResource := exportedResource.Storage.Virtual
		if virtualResource == nil {
			continue
		}

		apiVersion, err := schema.ParseGroupVersion(virtualResource.APIVersion)
		if err != nil {
			return true, fmt.Errorf("failed to parse virtual resource apiVersion %q: %v", virtualResource.APIVersion, err)
		}

		if apiVersion.Group != cachev1alpha1.SchemeGroupVersion.Group {
			continue
		}
		if virtualResource.Kind != "CachedResourceEndpointSlice" {
			continue
		}

		slicePath := logicalcluster.NewPath(virtualResource.Path)
		if slicePath.Empty() {
			slicePath = logicalcluster.From(export).Path()
		}
		slices = append(slices, sliceRef{
			path: slicePath,
			name: virtualResource.Name,
		})
	}

	if len(slices) > 0 {
		names := make([]string, len(slices))
		for i := range slices {
			names[i] = fmt.Sprintf("%s|%s", slices[i].path, slices[i].name)
		}
		fmt.Printf("\n\n ### binding %s|%s has %d CachedResourceEndpointSlices: %v #\n", logicalcluster.From(binding), binding.Name, len(names), names)
	} else {
		fmt.Printf("\n\n ### binding %s|%s has no CachedResourceEndpointSlices #\n")
	}

	for _, sliceRef := range slices {
		slice, err := r.getCachedResourceEndpointSlice(sliceRef.path, sliceRef.name)
		if err != nil {
			return true, err
		}
		retry, err := endpointsReconciler{}.reconcile(ctx, slice)
		if err != nil {
			return retry, err
		}
	}

	return false, nil
}

type endpointsReconciler struct {
}

func (r endpointsReconciler) reconcile(ctx context.Context, slice *cachev1alpha1.CachedResourceEndpointSlice) (bool, error) {

}

func (r *endpointsReconciler) updateEndpoints(ctx context.Context,
	slice *cachev1alpha1.CachedResourceEndpointSlice,
	bindings []*apisv1alpha2.APIBinding,
	shard *corev1alpha1.Shard,
	selector labels.Selector,
) (*result, error) {
	logger := klog.FromContext(ctx)
	if shard.Spec.VirtualWorkspaceURL == "" {
		return nil, nil
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
