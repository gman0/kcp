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
	apiexportbuilder "github.com/kcp-dev/kcp/pkg/virtual/apiexport/builder"
	apisv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha1"
	apisv1alpha2 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha2"
	cachev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/cache/v1alpha1"
	corev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/core/v1alpha1"
	"github.com/kcp-dev/kcp/sdk/apis/third_party/conditions/util/conditions"
	cachev1alpha1apply "github.com/kcp-dev/kcp/sdk/client/applyconfiguration/cache/v1alpha1"
)

type endpointsReconciler struct {
	getMyShard                       func() (*corev1alpha1.Shard, error)
	getAPIExport                     func(path logicalcluster.Path, name string) (*apisv1alpha2.APIExport, error)
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
		getAPIExport:                     c.getAPIExport,
		listAPIBindingsByAPIExport:       c.listAPIBindingsByAPIExport,
		shardName:                        c.shardName,
		patchCachedResourceEndpointSlice: nil,
	}

}

func (r *endpointsReconciler) reconcile(ctx context.Context, cachedResourceEndpointSlice *cachev1alpha1.CachedResourceEndpointSlice) (bool, error) {

}

/*// Formats the Replication VW URL like so:
//   /services/replication/<CachedResource cluster>/<CachedResource name>
addr.Path = path.Join(
	addr.Path,
	virtualworkspacesoptions.DefaultRootPathPrefix,
	"replication",
	logicalcluster.From(endpoints).String(),
	endpoints.Spec.CachedResource.Name,
)*/
