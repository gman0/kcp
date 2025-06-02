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

package builder

import (
	"context"
	"encoding/json"
	"fmt"
	// "net/http"
	// "text/template/parse"

	// "net/http/httputil"
	// goerrors "errors"
	// "net/url"
	// "path"
	"strings"

	// authenticationv1 "k8s.io/api/authentication/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	// "k8s.io/apiserver/pkg/authentication/serviceaccount"
	"github.com/kcp-dev/kcp/pkg/authorization"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/rest"

	// "k8s.io/client-go/tools/cache"
	// "k8s.io/kubernetes/pkg/registry/rbac/validation"
	//"k8s.io/client-go/transport"
	// "k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	kcpdynamic "github.com/kcp-dev/client-go/dynamic"
	kcpkubernetesclientset "github.com/kcp-dev/client-go/kubernetes"
	"github.com/kcp-dev/logicalcluster/v3"

	rootphase0 "github.com/kcp-dev/kcp/config/root-phase0"
	// "github.com/kcp-dev/kcp/pkg/authorization/bootstrap"
	// "github.com/kcp-dev/kcp/pkg/authorization/delegated"
	authdelegated "github.com/kcp-dev/kcp/pkg/authorization/delegated"
	"github.com/kcp-dev/kcp/pkg/virtual/framework"
	virtualworkspacesdynamic "github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic"
	"github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/apidefinition"
	"github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/apiserver"
	dynamiccontext "github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/context"
	"github.com/kcp-dev/kcp/pkg/virtual/framework/forwardingregistry"

	//"github.com/kcp-dev/kcp/pkg/virtual/framework/handler"
	"github.com/kcp-dev/kcp/pkg/virtual/framework/rootapiserver"
	"github.com/kcp-dev/kcp/pkg/virtual/replication"
	apisv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha1"
	cachev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/cache/v1alpha1"
	corev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/core/v1alpha1"
	kcpinformers "github.com/kcp-dev/kcp/sdk/client/informers/externalversions"
	// cachev1alpha1informers "github.com/kcp-dev/kcp/sdk/client/informers/externalversions/cache/v1alpha1"
)

func BuildVirtualWorkspace(
	cfg *rest.Config,
	rootPathPrefix string,
	dynamicClusterClient kcpdynamic.ClusterInterface,
	kubeClusterClient kcpkubernetesclientset.ClusterInterface,
	wildcardKcpInformers kcpinformers.SharedInformerFactory,
) ([]rootapiserver.NamedVirtualWorkspace, error) {
	if !strings.HasSuffix(rootPathPrefix, "/") {
		rootPathPrefix += "/"
	}

	cachedResourceSch := apisv1alpha1.APIResourceSchema{}
	if err := rootphase0.Unmarshal("apiresourceschema-cachedresources.cache.kcp.io.yaml", &cachedResourceSch); err != nil {
		return nil, fmt.Errorf("failed to unmarshal logicalclusters resource: %w", err)
	}
	bs, err := json.Marshal(&apiextensionsv1.JSONSchemaProps{
		Type:                   "object",
		XPreserveUnknownFields: ptr.To(true),
	})
	if err != nil {
		return nil, err
	}
	for i := range cachedResourceSch.Spec.Versions {
		v := &cachedResourceSch.Spec.Versions[i]
		v.Schema.Raw = bs // wipe schemas. We don't want validation here.
	}

	scopedCachedResourceContent := &virtualworkspacesdynamic.DynamicVirtualWorkspace{
		RootPathResolver: framework.RootPathResolverFunc(func(urlPath string, requestContext context.Context) (accepted bool, prefixToStrip string, completedContext context.Context) {
			cluster, apiDomain, prefixToStrip, ok := digestUrl(urlPath, rootPathPrefix)
			fmt.Printf("\n\n\nXXXX digestUrl(%q, %q) -> cluster=%q,apiDomain=%q,prefixToStrip=%s,ok=%v\n\n\n", urlPath, rootPathPrefix, cluster.Name.String(), apiDomain, prefixToStrip, ok)
			if !ok {
				return false, "", requestContext
			}

			/*if !cluster.Wildcard {
				// this virtual workspace requires that a wildcard be provided
				return false, "", requestContext
			}*/

			completedContext = genericapirequest.WithCluster(requestContext, cluster)
			completedContext = dynamiccontext.WithAPIDomainKey(completedContext, apiDomain)
			return true, prefixToStrip, completedContext
		}),
		Authorizer: newAuth(kubeClusterClient),
		ReadyChecker: framework.ReadyFunc(func() error {
			return nil
		}),
		BootstrapAPISetManagement: func(mainConfig genericapiserver.CompletedConfig) (apidefinition.APIDefinitionSetGetter, error) {
			return &singleResourceAPIDefinitionSetProvider{
				config:               mainConfig,
				dynamicClusterClient: dynamicClusterClient,
				exposeSubresources:   false,
				resource:             &cachedResourceSch,
				storageProvider: func(ctx context.Context, dynamicClusterClientFunc forwardingregistry.DynamicClusterClientFunc) (apiserver.RestProviderFunc, error) {
					return forwardingregistry.ProvideReadOnlyRestStorage(ctx, dynamicClusterClientFunc, nil, nil)
				},
			}, nil
		},
	}

	return []rootapiserver.NamedVirtualWorkspace{
		{Name: replication.VirtualWorkspaceName, VirtualWorkspace: scopedCachedResourceContent},
	}, nil
}

type myAuth struct {
	kubeClusterClient kcpkubernetesclientset.ClusterInterface
}

func newAuth(kubeClusterClient kcpkubernetesclientset.ClusterInterface) authorizer.Authorizer {
	return authorization.NewDecorator("virtual.cachedresource.cache.authorization.kcp.io", &myAuth{
		kubeClusterClient: kubeClusterClient,
	}).AddAuditLogging().AddAnonymization()
}

func (a *myAuth) Authorize(ctx context.Context, attr authorizer.Attributes) (authorized authorizer.Decision, reason string, err error) {
	apiDomainKey := dynamiccontext.APIDomainKeyFrom(ctx)
	clusterName, cachedResource, err := splitDomainKey(apiDomainKey)
	if err != nil {
		return authorizer.DecisionNoOpinion, "", fmt.Errorf("invalid API domain key: %v", err)
	}

	return authorizer.DecisionAllow, fmt.Sprintf("CachedResource: %q, workspace: %q RBAC decision: %v",
		cachedResource, clusterName, reason), nil

	SARAttributes := authorizer.AttributesRecord{
		APIGroup:   apisv1alpha1.SchemeGroupVersion.Group,
		APIVersion: apisv1alpha1.SchemeGroupVersion.Version,
		User:       attr.GetUser(),
		Verb:       attr.GetVerb(),
		// Name:            cachedResourceName,
		Resource:        "cachedresources",
		ResourceRequest: false,
		//Subresource:     "content",
	}

	authz, err := authdelegated.NewDelegatedAuthorizer(clusterName, a.kubeClusterClient, authdelegated.Options{})
	dec, reason, err := authz.Authorize(ctx, SARAttributes)
	if err != nil {
		return authorizer.DecisionNoOpinion, "",
			fmt.Errorf("error authorizing RBAC in CachedResource %q, workspace %q: %w", cachedResource, clusterName, err)
	}

	return dec, fmt.Sprintf("CachedResource: %q, workspace: %q RBAC decision: %v",
		cachedResource, clusterName, reason), nil
}

func digestUrl(urlPath, rootPathPrefix string) (
	cluster genericapirequest.Cluster,
	key dynamiccontext.APIDomainKey,
	logicalPath string,
	accepted bool,
) {
	if !strings.HasPrefix(urlPath, rootPathPrefix) {
		return genericapirequest.Cluster{}, "", "", false
	}

	// Incoming requests to this virtual workspace will look like:
	//  /services/apiexport/root:org:ws/<apiexport-name>/clusters/*/api/v1/configmaps
	//                     └────────────────────────┐
	// Where the withoutRootPathPrefix starts here: ┘
	withoutRootPathPrefix := strings.TrimPrefix(urlPath, rootPathPrefix)

	parts := strings.SplitN(withoutRootPathPrefix, "/", 3)
	if len(parts) < 3 {
		return genericapirequest.Cluster{}, "", "", false
	}

	cachedResourceClusterName, cachedResourceName := logicalcluster.Name(parts[0]), parts[1]
	if cachedResourceClusterName == "" {
		return genericapirequest.Cluster{}, "", "", false
	}
	if cachedResourceName == "" {
		return genericapirequest.Cluster{}, "", "", false
	}

	realPath := "/"
	if len(parts) > 2 {
		realPath += parts[2]
	}

	//  /services/apiexport/root:org:ws/<apiexport-name>/clusters/*/api/v1/configmaps
	//                     ┌────────────────────────────┘
	// We are now here: ───┘
	// Now, we parse out the logical cluster.
	if !strings.HasPrefix(realPath, "/clusters/") {
		return genericapirequest.Cluster{}, "", "", false
	}

	withoutClustersPrefix := strings.TrimPrefix(realPath, "/clusters/")
	parts = strings.SplitN(withoutClustersPrefix, "/", 2)
	path := logicalcluster.NewPath(parts[0])
	realPath = "/"
	if len(parts) > 1 {
		realPath += parts[1]
	}

	cluster = genericapirequest.Cluster{}
	if path == logicalcluster.Wildcard {
		cluster.Wildcard = true
	} else {
		var ok bool
		cluster.Name, ok = path.Name()
		if !ok {
			return genericapirequest.Cluster{}, "", "", false
		}
	}

	key = buildDomainKey(cachedResourceClusterName, cachedResourceName)
	return cluster, dynamiccontext.APIDomainKey(key), strings.TrimSuffix(urlPath, realPath), true
}

func buildDomainKey(clusterName logicalcluster.Name, cachedResource string) dynamiccontext.APIDomainKey {
	return dynamiccontext.APIDomainKey(fmt.Sprintf("%s/%s", clusterName, cachedResource))
}

func splitDomainKey(key dynamiccontext.APIDomainKey) (cachedResourceCluster logicalcluster.Name, cachedResourceName string, err error) {
	parts := strings.Split(string(key), "/")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid APIDomainKey %q for replication VW", string(key))
	}

	cachedResourceCluster, cachedResourceName = logicalcluster.Name(parts[0]), parts[1]
	return
}

type singleResourceAPIDefinitionSetProvider struct {
	config               genericapiserver.CompletedConfig
	dynamicClusterClient kcpdynamic.ClusterInterface
	resource             *apisv1alpha1.APIResourceSchema
	exposeSubresources   bool
	storageProvider      func(ctx context.Context, dynamicClusterClientFunc forwardingregistry.DynamicClusterClientFunc) (apiserver.RestProviderFunc, error)
}

func (a *singleResourceAPIDefinitionSetProvider) GetAPIDefinitionSet(ctx context.Context, key dynamiccontext.APIDomainKey) (apis apidefinition.APIDefinitionSet, apisExist bool, err error) {
	clientFactory := func(ctx context.Context) (kcpdynamic.ClusterInterface, error) {
		return a.dynamicClusterClient, nil
	}

	restProvider, err := a.storageProvider(ctx, clientFactory)
	if err != nil {
		return nil, false, err
	}

	apiDefinition, err := apiserver.CreateServingInfoFor(
		a.config,
		a.resource,
		corev1alpha1.SchemeGroupVersion.Version,
		restProvider,
	)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create serving info: %w", err)
	}

	apis = apidefinition.APIDefinitionSet{
		schema.GroupVersionResource{
			Group:    cachev1alpha1.SchemeGroupVersion.Group,
			Version:  cachev1alpha1.SchemeGroupVersion.Version,
			Resource: "objectresources",
		}: apiDefinition,
	}

	return apis, len(apis) > 0, nil
}

var _ apidefinition.APIDefinitionSetGetter = &singleResourceAPIDefinitionSetProvider{}
