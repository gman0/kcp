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

	kcpkubeclientset "github.com/kcp-dev/client-go/kubernetes"
	"github.com/kcp-dev/logicalcluster/v3"

	"github.com/kcp-dev/kcp/pkg/authorization/delegated"
	"github.com/kcp-dev/kcp/pkg/indexers"
	"github.com/kcp-dev/kcp/pkg/informer"
	dynamiccontext "github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/context"
	vrcontext "github.com/kcp-dev/kcp/pkg/virtual/framework/virtualresource/context"
	"github.com/kcp-dev/kcp/pkg/virtual/replication/apidomainkey"
	apisv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha1"
	apisv1alpha2 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha2"
	cachev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/cache/v1alpha1"
	kcpinformers "github.com/kcp-dev/kcp/sdk/client/informers/externalversions"
)

type apiExportsContentAuthorizer struct {
	delegate authorizer.Authorizer

	newDelegatedAuthorizer func(clusterName logicalcluster.Name) (authorizer.Authorizer, error)

	getAPIExportsByIdentity func(identity string) ([]*apisv1alpha2.APIExport, error)
	getCachedResource       func(cluster logicalcluster.Name, name string) (*cachev1alpha1.CachedResource, error)
}

// NewAPIExportsContentAuthorizer creates an authorizer that checks that the user has suitable permissions
// to the associated APIExport's content subresource -- similar to APIExport VW content authorizer.
// The APIExports are retrieved by their identity, possibly in different workspaces. The user making the request
// must have apiexports/content permissions to all of them in order for the request to be allowed.
func NewAPIExportsContentAuthorizer(
	delegate authorizer.Authorizer,
	kubeClusterClient kcpkubeclientset.ClusterInterface,
	localKcpInformers kcpinformers.SharedInformerFactory,
	globalKcpInformers kcpinformers.SharedInformerFactory,
) authorizer.Authorizer {
	return &apiExportsContentAuthorizer{
		delegate: delegate,
		newDelegatedAuthorizer: func(clusterName logicalcluster.Name) (authorizer.Authorizer, error) {
			return delegated.NewDelegatedAuthorizer(clusterName, kubeClusterClient, delegated.Options{})
		},
		getCachedResource: informer.NewScopedGetterWithFallback(localKcpInformers.Cache().V1alpha1().CachedResources().Lister(), globalKcpInformers.Cache().V1alpha1().CachedResources().Lister()),
		getAPIExportsByIdentity: func(identity string) ([]*apisv1alpha2.APIExport, error) {
			return indexers.ByIndex[*apisv1alpha2.APIExport](globalKcpInformers.Apis().V1alpha2().APIExports().Informer().GetIndexer(), indexers.APIExportByIdentity, identity)
		},
	}
}

func (a *apiExportsContentAuthorizer) Authorize(ctx context.Context, attr authorizer.Attributes) (authorizer.Decision, string, error) {
	parsedKey, err := apidomainkey.Parse(dynamiccontext.APIDomainKeyFrom(ctx))
	if err != nil {
		return authorizer.DecisionNoOpinion, "",
			fmt.Errorf("invalid API domain key")
	}

	cachedResource, err := a.getCachedResource(parsedKey.CachedResourceCluster, parsedKey.CachedResourceName)
	if err != nil {
		return authorizer.DecisionNoOpinion, "failed to retrieve CachedResource", err
	}

	apiExportIdentity, hasAPIExportIdentity := vrcontext.VirtualResourceAPIExportIdentityFrom(ctx)
	if !hasAPIExportIdentity {
		return authorizer.DecisionNoOpinion, "APIExport identity missing in context", nil
	}

	candidateExports, err := a.getAPIExportsByIdentity(apiExportIdentity)
	if err != nil {
		return authorizer.DecisionDeny, "failed to list APIExports by identity", err
	}

	SARAttributes := authorizer.AttributesRecord{
		APIGroup:        apisv1alpha1.SchemeGroupVersion.Group,
		APIVersion:      apisv1alpha1.SchemeGroupVersion.Version,
		User:            attr.GetUser(),
		Verb:            attr.GetVerb(),
		Resource:        "apiexports",
		ResourceRequest: true,
		Subresource:     "content",
	}

	wrappedGVR := schema.GroupVersionResource(cachedResource.Spec.GroupVersionResource)
	for _, export := range candidateExports {
		for _, res := range export.Spec.Resources {
			if res.Storage.Virtual != nil &&
				res.Storage.Virtual.IdentityHash == cachedResource.Status.IdentityHash &&
				res.Group == wrappedGVR.Group &&
				res.Name == wrappedGVR.Resource {
				authz, err := a.newDelegatedAuthorizer(logicalcluster.From(export))
				if err != nil {
					return authorizer.DecisionNoOpinion, "",
						fmt.Errorf("error creating delegated authorizer for API export %q, workspace %q: %w", export.Name, logicalcluster.From(export), err)
				}
				SARAttributes.Name = export.Name
				dec, reason, err := authz.Authorize(ctx, SARAttributes)
				fmt.Printf("### apiExportsContentAuthorizer dec=%v reason=%q err=%v\n", dec, reason, err)
				if err != nil {
					return authorizer.DecisionNoOpinion, "",
						fmt.Errorf("error authorizing RBAC in API export %q, workspace %q: %w", export.Name, logicalcluster.From(export), err)
				}
				if dec != authorizer.DecisionAllow {
					return authorizer.DecisionDeny, reason, nil
				}
			}
		}
	}

	return a.delegate.Authorize(ctx, attr)
}
