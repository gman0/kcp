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
	// "encoding/json"

	// "encoding/json"
	"errors"
	"fmt"
	// "net/http"
	// "text/template/parse"

	// "net/http/httputil"
	// "net/url"
	// "path"
	"strings"

	// authenticationv1 "k8s.io/api/authentication/v1"
	// apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	// "k8s.io/apimachinery/pkg/api/meta"
	// "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	// "k8s.io/apiserver/pkg/authentication/serviceaccount"
	"github.com/kcp-dev/kcp/pkg/authorization"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	// "k8s.io/client-go/tools/cache"
	// "k8s.io/kubernetes/pkg/registry/rbac/validation"
	//"k8s.io/client-go/transport"
	"k8s.io/klog/v2"

	kcpdynamic "github.com/kcp-dev/client-go/dynamic"
	kcpkubernetesclientset "github.com/kcp-dev/client-go/kubernetes"
	"github.com/kcp-dev/logicalcluster/v3"

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
	kcpclientset "github.com/kcp-dev/kcp/sdk/client/clientset/versioned/cluster"
	// XXX
	"time"

	"github.com/kcp-dev/kcp/pkg/reconciler/apis/apibinding"
	"github.com/kcp-dev/kcp/pkg/virtual/apiexport/schemas/builtin"
	// kubecorev1 "k8s.io/api/core/v1"
)

// cacheclient "github.com/kcp-dev/kcp/pkg/cache/client"
// "github.com/kcp-dev/kcp/pkg/cache/client/shard"

func dummyCacheKcpSharedInformerFactory(kcpCacheClusterClient kcpclientset.ClusterInterface) context.CancelFunc {
	const resyncPeriod = 10 * time.Hour

	ctx, cancel := context.WithCancel(context.Background())

	cacheKcpSharedInformerFactory := kcpinformers.NewSharedInformerFactoryWithOptions(
		kcpCacheClusterClient,
		resyncPeriod,
	)

	go cacheKcpSharedInformerFactory.Cache().V1alpha1().CachedObjects().Informer().Run(ctx.Done())
	cacheKcpSharedInformerFactory.Start(ctx.Done())
	synced := cacheKcpSharedInformerFactory.WaitForCacheSync(ctx.Done())

	syncedStrMap := make(map[string]bool)
	for k, v := range synced {
		syncedStrMap[k.String()] = v
	}
	fmt.Printf("\n\n\n>>><<< syncedStrMap=%#v <<<\n\n\n", syncedStrMap)

	listCacheObjs := func(cluster logicalcluster.Name) []string {
		cacheObjs, err := cacheKcpSharedInformerFactory.Cache().V1alpha1().CachedObjects().Lister().List(labels.Everything())
		if err != nil {
			return []string{fmt.Sprintf("!!! err=%v !!!", err)}
		}
		names := make([]string, len(cacheObjs))
		for i := range cacheObjs {
			names[i] = cacheObjs[i].Name
		}
		return names
	}

	cacheKcpSharedInformerFactory.Cache().V1alpha1().CachedObjects().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			fmt.Printf("\n\n\n>>><<< ADDED %s ; %v <<<\n\n\n", obj.(runtime.Object), listCacheObjs(logicalcluster.From(obj.(logicalcluster.Object))))
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			fmt.Printf("\n\n\n>>><<< UPDATED %s ; %v <<<\n\n\n", newObj.(runtime.Object), listCacheObjs(logicalcluster.From(newObj.(logicalcluster.Object))))
		},
		DeleteFunc: func(obj interface{}) {
			fmt.Printf("\n\n\n>>><<< DELETED %s ; %v <<<\n\n\n", obj.(runtime.Object), listCacheObjs(logicalcluster.From(obj.(logicalcluster.Object))))
		},
	})

	return cancel
}

func BuildVirtualWorkspace(
	cfg *rest.Config,
	rootPathPrefix string,
	kcpClusterClient kcpclientset.ClusterInterface,
	dynamicClusterClient kcpdynamic.ClusterInterface,
	kubeClusterClient kcpkubernetesclientset.ClusterInterface,
	wildcardKcpInformers kcpinformers.SharedInformerFactory,
	kcpCacheClusterClient kcpclientset.ClusterInterface, // <-- ...
	cacheKcpInformers kcpinformers.SharedInformerFactory,
) ([]rootapiserver.NamedVirtualWorkspace, error) {
	if !strings.HasSuffix(rootPathPrefix, "/") {
		rootPathPrefix += "/"
	}

	fmt.Printf("\n\n\n=== 1 cachedObj has synced: %v ===\n\n\n", cacheKcpInformers.Cache().V1alpha1().CachedObjects().Informer().HasSynced())

	readyCh := make(chan struct{})

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
			select {
			case <-readyCh:
				return nil
			default:
				return errors.New("replication virtual workspace controllers are not started")
			}
		}),
		BootstrapAPISetManagement: func(mainConfig genericapiserver.CompletedConfig) (apidefinition.APIDefinitionSetGetter, error) {
			if err := mainConfig.AddPostStartHook(replication.VirtualWorkspaceName, func(hookContext genericapiserver.PostStartHookContext) error {
				defer close(readyCh)

				for name, informer := range map[string]cache.SharedIndexInformer{
					"cachedresources": cacheKcpInformers.Cache().V1alpha1().CachedObjects().Informer(),
				} {
					if !cache.WaitForNamedCacheSync(name, hookContext.Done(), informer.HasSynced) {
						klog.Background().Error(nil, "informer not synced")
						return nil
					}
				}

				return nil
			}); err != nil {
				return nil, err
			}

			return &singleResourceAPIDefinitionSetProvider{
				KcpCacheClusterClient: kcpCacheClusterClient,
				wildcardKcpInformers:  wildcardKcpInformers,
				kcpClusterClient:      kcpClusterClient,

				config:               mainConfig,
				dynamicClusterClient: dynamicClusterClient,
				exposeSubresources:   false,
				storageProvider: func(ctx context.Context, dynamicClusterClientFunc forwardingregistry.DynamicClusterClientFunc, cachedResource *cachev1alpha1.CachedResource, sch *apisv1alpha1.APIResourceSchema) (apiserver.RestProviderFunc, error) {
					return forwardingregistry.ProvideReadOnlyRestStorage(
						ctx,
						dynamicClusterClientFunc,
						withUnwrapping(cachedResource, sch, kcpCacheClusterClient),
						nil,
					)
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
	storageProvider      func(ctx context.Context, dynamicClusterClientFunc forwardingregistry.DynamicClusterClientFunc, cachedResource *cachev1alpha1.CachedResource, sch *apisv1alpha1.APIResourceSchema) (apiserver.RestProviderFunc, error)

	KcpCacheClusterClient kcpclientset.ClusterInterface // <-- ...
	wildcardKcpInformers  kcpinformers.SharedInformerFactory
	kcpClusterClient      kcpclientset.ClusterInterface
}

func getResourceBindingsAnnJSON(lc *corev1alpha1.LogicalCluster) string {
	const jsonEmptyObj = "{}"

	if lc == nil {
		return jsonEmptyObj
	}

	ann := lc.Annotations[apibinding.ResourceBindingsAnnotationKey]
	if ann == "" {
		ann = jsonEmptyObj
	}

	return ann
}

func (a *singleResourceAPIDefinitionSetProvider) getAPIResourceSchema(
	ctx context.Context,
	clusterName logicalcluster.Name,
	gvr schema.GroupVersionResource,
) (*apisv1alpha1.APIResourceSchema, error) {
	if gvr.Group == "" {
		// Assume built-in types.
		return builtin.GetBuiltInAPISchema(apisv1alpha1.GroupResource{Group: "", Resource: gvr.Resource})
	}

	lc, err := a.kcpClusterClient.CoreV1alpha1().LogicalClusters().Cluster(clusterName.Path()).Get(ctx, "cluster", metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	resBindingsAnnStr := getResourceBindingsAnnJSON(lc)
	resBindingsAnn, err := apibinding.UnmarshalResourceBindingsAnnotation(resBindingsAnnStr)

	bindingName := ""
	for gr, v := range resBindingsAnn {
		if v.CRD {
			continue
		}
		if gr == gvr.GroupResource().String() {
			bindingName = v.Name
		}
	}

	if bindingName == "" {
		return nil, fmt.Errorf("no binding for %s found in %s", gvr.GroupResource().String(), clusterName)
	}

	apiBinding, err := a.kcpClusterClient.ApisV1alpha2().APIBindings().Cluster(clusterName.Path()).Get(ctx, bindingName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get APIBinding %s in %s", bindingName, clusterName)
	}

	apiExport, err := a.kcpClusterClient.ApisV1alpha2().APIExports().Cluster(logicalcluster.NewPath(apiBinding.Spec.Reference.Export.Path)).
		Get(ctx, apiBinding.Spec.Reference.Export.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get APIExport %s|%s referenced by APIBinding %s|%s",
			apiBinding.Spec.Reference.Export.Path, apiBinding.Spec.Reference.Export.Name,
			bindingName, clusterName,
		)
	}

	schName := ""
	for _, exportResource := range apiExport.Spec.Resources {
		if exportResource.Group == gvr.Group && exportResource.Name == gvr.Resource {
			schName = exportResource.Schema
		}
	}

	return a.kcpClusterClient.ApisV1alpha1().APIResourceSchemas().Cluster(logicalcluster.NewPath(apiBinding.Spec.Reference.Export.Path)).
		Get(ctx, schName, metav1.GetOptions{})
}

func (a *singleResourceAPIDefinitionSetProvider) GetAPIDefinitionSet(ctx context.Context, key dynamiccontext.APIDomainKey) (apis apidefinition.APIDefinitionSet, apisExist bool, err error) {
	clusterName, cachedResourceName, err := splitDomainKey(key)
	if err != nil {
		return nil, false, err
	}

	cachedResource, err := a.kcpClusterClient.CacheV1alpha1().CachedResources().Cluster(clusterName.Path()).
		Get(ctx, cachedResourceName, metav1.GetOptions{})
	if err != nil {
		return nil, false, err
	}

	sch, err := a.getAPIResourceSchema(
		ctx, clusterName, schema.GroupVersionResource(cachedResource.Spec.GroupVersionResource),
	)
	if err != nil {
		return nil, false, fmt.Errorf("XXX failed to get APIResourceSchema for CachedResource %s: %v", cachedResourceName, err)
	}

	cachedobjs, err := a.KcpCacheClusterClient.CacheV1alpha1().Cluster(clusterName.Path()).CachedObjects().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, false, fmt.Errorf("failed to list CachedObjs: %v", err)
	}
	names := make([]string, len(cachedobjs.Items))
	for i := range cachedobjs.Items {
		names[i] = cachedobjs.Items[i].Name
	}
	fmt.Printf("\n\n\n ^^^ CachedObjs:%v ^^^\n\n\n", names)

	clientFactory := func(ctx context.Context) (kcpdynamic.ClusterInterface, error) {
		return a.dynamicClusterClient, nil
	}

	restProvider, err := a.storageProvider(ctx, clientFactory, cachedResource, sch)
	if err != nil {
		return nil, false, err
	}

	apiDefinition, err := apiserver.CreateServingInfoFor(
		a.config,
		sch,
		cachedResource.Spec.Version,
		restProvider,
	)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create serving info: %w", err)
	}

	apis = apidefinition.APIDefinitionSet{
		schema.GroupVersionResource{
			Group:    cachedResource.Spec.Group,
			Version:  cachedResource.Spec.Version,
			Resource: cachedResource.Spec.Resource,
		}: apiDefinition,
	}

	return apis, len(apis) > 0, nil
}

var _ apidefinition.APIDefinitionSetGetter = &singleResourceAPIDefinitionSetProvider{}
