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
	"text/template/parse"
	// "encoding/json"
	"fmt"
	"net/http"

	// "net/http/httputil"
	goerrors "errors"
	"net/url"
	"path"
	"strings"

	// authenticationv1 "k8s.io/api/authentication/v1"
	// apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	// "k8s.io/apiserver/pkg/authentication/serviceaccount"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/rest"

	// "k8s.io/client-go/tools/cache"
	// "k8s.io/kubernetes/pkg/registry/rbac/validation"
	// "k8s.io/client-go/transport"
	"k8s.io/klog/v2"
	// "k8s.io/utils/ptr"

	kcpdynamic "github.com/kcp-dev/client-go/dynamic"
	kcpkubernetesclientset "github.com/kcp-dev/client-go/kubernetes"
	"github.com/kcp-dev/logicalcluster/v3"

	// rootphase0 "github.com/kcp-dev/kcp/config/root-phase0"
	// "github.com/kcp-dev/kcp/pkg/authorization/bootstrap"
	"github.com/kcp-dev/kcp/pkg/authorization/delegated"
	"github.com/kcp-dev/kcp/pkg/reconciler/topology/partitionset"
	"github.com/kcp-dev/kcp/pkg/server/requestinfo"
	"github.com/kcp-dev/kcp/pkg/virtual/framework"
	virtualworkspacesdynamic "github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic"
	"github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/apidefinition"
	"github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/apiserver"
	dynamiccontext "github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/context"

	// "github.com/kcp-dev/kcp/pkg/virtual/framework/handler"
	"github.com/kcp-dev/kcp/pkg/virtual/framework/rootapiserver"
	"github.com/kcp-dev/kcp/pkg/virtual/replication"
	apisv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha1"
	corev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/core/v1alpha1"
	"github.com/kcp-dev/kcp/sdk/apis/tenancy/initialization"
	tenancyv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/tenancy/v1alpha1"
	kcpinformers "github.com/kcp-dev/kcp/sdk/client/informers/externalversions"
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

	readyCh := make(chan struct{})

	boundOrClaimedWorkspaceContent := &virtualworkspacesdynamic.DynamicVirtualWorkspace{
		RootPathResolver: framework.RootPathResolverFunc(func(urlPath string, ctx context.Context) (accepted bool, prefixToStrip string, completedContext context.Context) {
			cluster, apiDomain, prefixToStrip, ok := digestUrl(urlPath, rootPathPrefix)
			if !ok {
				return false, "", ctx
			}

			completedContext = genericapirequest.WithCluster(ctx, cluster)
			completedContext = dynamiccontext.WithAPIDomainKey(completedContext, apiDomain)
			return true, prefixToStrip, completedContext
		}),

		ReadyChecker: framework.ReadyFunc(func() error {
			select {
			case <-readyCh:
				return nil
			default:
				return goerrors.New("apiexport virtual workspace controllers are not started")
			}
		}),

		BootstrapAPISetManagement: func(mainConfig genericapiserver.CompletedConfig) (apidefinition.APIDefinitionSetGetter, error) {
			defer close(readyCh)

			return &singleResourceAPIDefinitionSetProvider{
				config:               mainConfig,
				dynamicClusterClient: dynamicClusterClient,
				exposeSubresources:   false,
				resource:             &apisv1alpha1.APIResourceSchema{},
				storageProvider:      delegatingLogicalClusterReadOnlyRestStorage,
			}, nil
		},
		Authorizer: &authAny{},
	}

	return []rootapiserver.NamedVirtualWorkspace{
		{Name: replication.VirtualWorkspaceName, VirtualWorkspace: boundOrClaimedWorkspaceContent},
	}, nil
}

var resolver = requestinfo.NewFactory()

type authAny struct{}

func (_ *authAny) Authorize(ctx context.Context, a authorizer.Attributes) (authorized authorizer.Decision, reason string, err error) {
	fmt.Printf("\n\n\nXXX replication VW: authAny XXX\n\n\n")
	return authorizer.DecisionAllow, "HEHE", nil
}

/*func newAuthorizer(kubeClusterClient, deepSARClient kcpkubernetesclientset.ClusterInterface, cachedKcpInformers, kcpInformers kcpinformers.SharedInformerFactory) authorizer.Authorizer {
	maximalPermissionAuth := virtualapiexportauth.NewMaximalPermissionAuthorizer(deepSARClient, cachedKcpInformers.Apis().V1alpha2().APIExports())
	maximalPermissionAuth = authorization.NewDecorator("virtual.apiexport.maxpermissionpolicy.authorization.kcp.io", maximalPermissionAuth).AddAuditLogging().AddAnonymization().AddReasonAnnotation()

	apiExportsContentAuth := virtualapiexportauth.NewAPIExportsContentAuthorizer(maximalPermissionAuth, kubeClusterClient)
	apiExportsContentAuth = authorization.NewDecorator("virtual.apiexport.content.authorization.kcp.io", apiExportsContentAuth).AddAuditLogging().AddAnonymization()

	boundApiAuth := virtualapiexportauth.NewBoundAPIAuthorizer(apiExportsContentAuth, kcpInformers.Apis().V1alpha2().APIBindings(), cachedKcpInformers.Apis().V1alpha2().APIExports(), kubeClusterClient)
	boundApiAuth = authorization.NewDecorator("virtual.apiexport.boundapi.authorization.kcp.io", boundApiAuth).AddAuditLogging().AddAnonymization()

	return boundApiAuth
}*/

func isLogicalClusterRequest(path string) bool {
	info, err := resolver.NewRequestInfo(&http.Request{URL: &url.URL{Path: path}})
	if err != nil {
		return false
	}
	return info.IsResourceRequest && info.APIGroup == corev1alpha1.SchemeGroupVersion.Group && info.Resource == "logicalclusters"
}

func digestUrl(urlPath, rootPathPrefix string) (
	cluster genericapirequest.Cluster,
	key dynamiccontext.APIDomainKey,
	logicalPath string,
	accepted bool,
) {
	fmt.Printf("\n\n\nXXX replication VW: url=%s XXX\n\n\n", urlPath)

	if !strings.HasPrefix(urlPath, rootPathPrefix) {
		return genericapirequest.Cluster{}, dynamiccontext.APIDomainKey(""), "", false
	}
	withoutRootPathPrefix := strings.TrimPrefix(urlPath, rootPathPrefix)

	// Incoming requests to this virtual workspace will look like:
	//  /services/replication/<Cluster path or wildcard>:<APIExport>/<CachedResource>
	//                        ^
	//                        |
	//                        +----------------------+
	//                                               |
	// Where the withoutRootPathPrefix starts here:  +
	// Now, we parse out the logical cluster.
	parts := strings.SplitN(withoutRootPathPrefix, "/", 2)
	if len(parts) != 2 {
		return genericapirequest.Cluster{}, dynamiccontext.APIDomainKey(""), "", false
	}

	logicalclusterPath, apiExportName := logicalcluster.NewPath(parts[0]).Split()
	if logicalclusterPath.Empty() || apiExportName == "" {
		return genericapirequest.Cluster{}, dynamiccontext.APIDomainKey(""), "", false
	}

	realPath := "/"
	if parts[1] != "" {
		realPath += parts[1]
	}

	withoutClusterAndExportPrefix := parts[1]
	if strings.Contains(withoutClusterAndExportPrefix, "/") {
		// Unexpected tail on the path.
		return genericapirequest.Cluster{}, dynamiccontext.APIDomainKey(""), "", false
	}

	cachedResourceName := withoutClusterAndExportPrefix
	cluster = genericapirequest.Cluster{}
	if logicalclusterPath == logicalcluster.Wildcard {
		cluster.Wildcard = true
	} else {
		var ok bool
		cluster.Name, ok = logicalclusterPath.Name()
		if !ok {
			return genericapirequest.Cluster{}, "", "", false
		}
	}

	key = buildDomainKey(logicalclusterPath, apiExportName, cachedResourceName)
	return cluster, key, strings.TrimSuffix(urlPath, realPath), true
}

// URLFor returns the absolute path for the specified initializer.
func URLFor(initializerName corev1alpha1.LogicalClusterInitializer) string {
	// TODO(ncdc): make /services hard-coded everywhere instead of configurable.
	return path.Join("/services", replication.VirtualWorkspaceName, string(initializerName))
}

type singleResourceAPIDefinitionSetProvider struct {
	config               genericapiserver.CompletedConfig
	dynamicClusterClient kcpdynamic.ClusterInterface
	resource             *apisv1alpha1.APIResourceSchema
	exposeSubresources   bool
	storageProvider      func(ctx context.Context, clusterClient kcpdynamic.ClusterInterface, initializer corev1alpha1.LogicalClusterInitializer) (apiserver.RestProviderFunc, error)
}

func buildDomainKey(clusterPath logicalcluster.Path, apiExportName, cachedResourceName string) dynamiccontext.APIDomainKey {
	return dynamiccontext.APIDomainKey(fmt.Sprintf("%s:%s/%s", clusterPath.String(), apiExportName, cachedResourceName))
}

func splitDomainKey(key dynamiccontext.APIDomainKey) (clusterPath logicalcluster.Path, apiExportName, cachedResourceName string, err error) {
	parts := strings.Split(string(key), "/")
	if len(parts) == 2 {
		return logicalcluster.None, "", "", fmt.Errorf("%q is invalid APIDomainKey for replication VW", string(key))
	}

	clusterPath, apiExportName = logicalcluster.NewPath(parts[0]).Split()
	if clusterPath.Empty() || apiExportName == "" {
		return logicalcluster.None, "", "", fmt.Errorf("invalid APIExport reference %q in APIDomainKey %q for replication VW", parts[0], string(key))
	}

	if parts[1] == "" {
		return logicalcluster.None, "", "", fmt.Errorf("empty CachedResource name in APIDomainKey %q for replication VW", string(key))
	}

	cachedResourceName = parts[1]

	return
}

func (a *singleResourceAPIDefinitionSetProvider) GetAPIDefinitionSet(ctx context.Context, key dynamiccontext.APIDomainKey) (apis apidefinition.APIDefinitionSet, apisExist bool, err error) {
	clusterPath, apiExportName, cachedResourceName, err := splitDomainKey(key)
	if err != nil {
		return nil, false, err
	}

	restProvider, err := a.storageProvider(ctx, a.dynamicClusterClient, corev1alpha1.LogicalClusterInitializer(key))
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
			Group:    corev1alpha1.SchemeGroupVersion.Group,
			Version:  corev1alpha1.SchemeGroupVersion.Version,
			Resource: "logicalclusters",
		}: apiDefinition,
	}

	return apis, len(apis) > 0, nil
}

var _ apidefinition.APIDefinitionSetGetter = &singleResourceAPIDefinitionSetProvider{}

func authorizerWithCache(ctx context.Context, cache delegated.Cache, attr authorizer.Attributes) (authorizer.Decision, string, error) {
	clusterName, name, err := initialization.TypeFrom(corev1alpha1.LogicalClusterInitializer(dynamiccontext.APIDomainKeyFrom(ctx)))
	if err != nil {
		klog.FromContext(ctx).V(2).Info(err.Error())
		return authorizer.DecisionNoOpinion, "unable to determine initializer", fmt.Errorf("access not permitted")
	}

	authz, err := cache.Get(clusterName)
	if err != nil {
		return authorizer.DecisionNoOpinion, "error", err
	}

	SARAttributes := authorizer.AttributesRecord{
		APIGroup:        tenancyv1alpha1.SchemeGroupVersion.Group,
		APIVersion:      tenancyv1alpha1.SchemeGroupVersion.Version,
		User:            attr.GetUser(),
		Verb:            "initialize",
		Name:            name,
		Resource:        "workspacetypes",
		ResourceRequest: true,
	}

	return authz.Authorize(ctx, SARAttributes)
}
