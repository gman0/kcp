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

package authorizer

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"

	"github.com/kcp-dev/logicalcluster/v3"

	"github.com/kcp-dev/kcp/pkg/indexers"
	vrcontext "github.com/kcp-dev/kcp/pkg/virtual/framework/virtualresource/context"
	apisv1alpha2 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha2"
	kcpinformers "github.com/kcp-dev/kcp/sdk/client/informers/externalversions"
)

type boundVirtualResourceAuthorizer struct {
	getAPIBindingByIdentityAndGR func(cluster logicalcluster.Name, apiExportIdentity string, gr schema.GroupResource) (*apisv1alpha2.APIBinding, error)

	delegate authorizer.Authorizer
}

// NewBoundVirtualResourceAuthorizer creates an authorizer that checks the resource
// in the request is a bound virtual resource. Wildcard or non-resource requests
// are delgated.
func NewBoundVirtualResourceAuthorizer(
	delegate authorizer.Authorizer,
	localKcpInformers kcpinformers.SharedInformerFactory,
	globalKcpInformers kcpinformers.SharedInformerFactory,
) authorizer.Authorizer {
	return &boundVirtualResourceAuthorizer{
		getAPIBindingByIdentityAndGR: func(cluster logicalcluster.Name, apiExportIdentity string, gr schema.GroupResource) (*apisv1alpha2.APIBinding, error) {
			bindings, err := indexers.ByIndex[*apisv1alpha2.APIBinding](
				localKcpInformers.Apis().V1alpha2().APIBindings().Informer().GetIndexer(),
				indexers.APIBindingByIdentityAndGroupResource,
				indexers.IdentityGroupResourceKeyFunc(apiExportIdentity, gr.Group, gr.Resource),
			)
			if err != nil {
				return nil, err
			}
			if len(bindings) == 0 {
				return nil, nil
			}
			return bindings[0], nil
		},
		delegate: delegate,
	}
}

func (a *boundVirtualResourceAuthorizer) Authorize(ctx context.Context, attr authorizer.Attributes) (authorizer.Decision, string, error) {
	targetCluster, err := genericapirequest.ValidClusterFrom(ctx)
	if err != nil {
		return authorizer.DecisionNoOpinion, "", fmt.Errorf("error getting valid cluster from context: %w", err)
	}

	apiExportIdentity, hasAPIExportIdentity := vrcontext.VirtualResourceAPIExportIdentityFrom(ctx)
	if !hasAPIExportIdentity {
		return authorizer.DecisionDeny, "APIExport identity missing in context", nil
	}

	if targetCluster.Wildcard || attr.GetResource() == "" {
		// If the target is wildcard, or it's a non-resource URL request,
		// we can skip checking the APIBinding in the target cluster.
		return a.delegate.Authorize(ctx, attr)
	}

	binding, err := a.getAPIBindingByIdentityAndGR(targetCluster.Name, apiExportIdentity, schema.GroupResource{
		Group:    attr.GetAPIGroup(),
		Resource: attr.GetResource(),
	})
	if err != nil || binding == nil {
		return authorizer.DecisionDeny, "could not find suitable APIBinding in target logical cluster", nil
	}

	// Check that this is a bound virtual resource.
	for _, boundResource := range binding.Status.BoundResources {
		if boundResource.Group == attr.GetAPIGroup() && boundResource.Resource == attr.GetResource() {
			// Virtual resources have zero storage versions.
			if len(boundResource.StorageVersions) == 0 {
				return a.delegate.Authorize(ctx, attr)
			}
			break // This bound resource is not virtual, fall through to DecisionDeny.
		}
	}

	return authorizer.DecisionDeny, "failed to find suitable reason to allow access in APIBinding", nil
}
