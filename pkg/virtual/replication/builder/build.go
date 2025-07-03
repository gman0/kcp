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
	// "k8s.io/apimachinery/pkg/labels"
	// "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	// "k8s.io/utils/ptr"

	// "k8s.io/apiserver/pkg/authentication/serviceaccount"
	"github.com/kcp-dev/kcp/pkg/authorization"
	// apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	//
	//cacheclient "github.com/kcp-dev/kcp/pkg/cache/client"
	//"github.com/kcp-dev/kcp/pkg/cache/client/shard"
	//

	"github.com/kcp-dev/kcp/pkg/virtual/framework"
	virtualworkspacesdynamic "github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic"
	"github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/apidefinition"
	"github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/apiserver"
	dynamiccontext "github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/context"
	"github.com/kcp-dev/kcp/pkg/virtual/framework/forwardingregistry"
	"github.com/kcp-dev/kcp/pkg/virtual/replication/apidomainkey"

	//"github.com/kcp-dev/kcp/pkg/virtual/framework/handler"
	"github.com/kcp-dev/kcp/pkg/indexers"
	cachedresourcesreplication "github.com/kcp-dev/kcp/pkg/reconciler/cache/cachedresources/replication"
	"github.com/kcp-dev/kcp/pkg/virtual/apiexport/schemas/builtin"
	"github.com/kcp-dev/kcp/pkg/virtual/framework/rootapiserver"
	"github.com/kcp-dev/kcp/pkg/virtual/replication"
	replicationauthorizer "github.com/kcp-dev/kcp/pkg/virtual/replication/authorizer"
	apisv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha1"
	apisv1alpha2 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha2"

	corev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/core/v1alpha1"
	kcpinformers "github.com/kcp-dev/kcp/sdk/client/informers/externalversions"

	// cachev1alpha1informers "github.com/kcp-dev/kcp/sdk/client/informers/externalversions/cache/v1alpha1"
	kcpclientset "github.com/kcp-dev/kcp/sdk/client/clientset/versioned/cluster"
	// XXX
	// "time"
	"github.com/kcp-dev/kcp/pkg/reconciler/apis/apibinding"
	// "github.com/kcp-dev/kcp/pkg/virtual/apiexport/schemas/builtin"
	// kubecorev1 "k8s.io/api/core/v1"
)

// cacheclient "github.com/kcp-dev/kcp/pkg/cache/client"

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

	// builtinContent := &virtualworkspacesdynamic.DynamicVirtualWorkspace{}

	apiExportContent := &virtualworkspacesdynamic.DynamicVirtualWorkspace{
		RootPathResolver: framework.RootPathResolverFunc(func(urlPath string, requestContext context.Context) (accepted bool, prefixToStrip string, completedContext context.Context) {
			cachedResourceCluster, apiDomain, prefixToStrip, ok := digestURL(urlPath, rootPathPrefix)
			fmt.Printf("\n\n\nXXXX digestUrl(%q, %q) -> apiDomain=%q,prefixToStrip=%s,ok=%v\n\n\n", urlPath, rootPathPrefix, apiDomain, prefixToStrip, ok)
			if !ok {
				return false, "", requestContext
			}

			/*if !cluster.Wildcard {
				// this virtual workspace requires that a wildcard be provided
				return false, "", requestContext
			}*/

			// completedContext = genericapirequest.WithShard(completedContext, "root")
			completedContext = genericapirequest.WithCluster(requestContext, genericapirequest.Cluster{Name: cachedResourceCluster})
			// completedContext = cacheclient.WithShardInContext(completedContext, shard.Name("*"))
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

				indexers.AddIfNotPresentOrDie(
					cacheKcpInformers.Cache().V1alpha1().CachedObjects().Informer().GetIndexer(),
					cache.Indexers{
						cachedresourcesreplication.ByGVRAndLogicalClusterAndNamespace: cachedresourcesreplication.IndexByGVRAndLogicalClusterAndNamespace,
					},
				)
				indexers.AddIfNotPresentOrDie(
					cacheKcpInformers.Apis().V1alpha2().APIExports().Informer().GetIndexer(),
					cache.Indexers{
						indexers.ByLogicalClusterPathAndName: indexers.IndexByLogicalClusterPathAndName,
					},
				)
				indexers.AddIfNotPresentOrDie(
					wildcardKcpInformers.Apis().V1alpha2().APIExports().Informer().GetIndexer(),
					cache.Indexers{
						indexers.ByLogicalClusterPathAndName: indexers.IndexByLogicalClusterPathAndName,
					},
				)

				for name, informer := range map[string]cache.SharedIndexInformer{
					"cachedresources":    cacheKcpInformers.Cache().V1alpha1().CachedObjects().Informer(),
					"apiexports":         cacheKcpInformers.Apis().V1alpha2().APIExports().Informer(),
					"apiresourceschemas": cacheKcpInformers.Apis().V1alpha1().APIResourceSchemas().Informer(),
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

				getAPIExportByPath: func(path logicalcluster.Path, name string) (*apisv1alpha2.APIExport, error) {
					return indexers.ByPathAndNameWithFallback[*apisv1alpha2.APIExport](
						apisv1alpha1.Resource("apiexports"),
						wildcardKcpInformers.Apis().V1alpha2().APIExports().Informer().GetIndexer(),
						cacheKcpInformers.Apis().V1alpha2().APIExports().Informer().GetIndexer(),
						path,
						name,
					)
				},

				getAPIResourceSchemaByName: func(cluster logicalcluster.Name, name string) (*apisv1alpha1.APIResourceSchema, error) {
					return cacheKcpInformers.Apis().V1alpha1().APIResourceSchemas().Cluster(cluster).Lister().Get(name)
				},

				config:               mainConfig,
				dynamicClusterClient: dynamicClusterClient,
				exposeSubresources:   false,
				storageProvider: func(ctx context.Context, dynamicClusterClientFunc forwardingregistry.DynamicClusterClientFunc, sch *apisv1alpha1.APIResourceSchema, version string) (apiserver.RestProviderFunc, error) {
					return forwardingregistry.ProvideReadOnlyRestStorage(
						ctx,
						dynamicClusterClientFunc,
						withUnwrapping(sch, version, cacheKcpInformers),
						nil,
					)
				},
			}, nil
		},
	}

	return []rootapiserver.NamedVirtualWorkspace{
		{Name: replication.VirtualWorkspaceName, VirtualWorkspace: apiExportContent},
	}, nil
}

func digestURL(urlPath, rootPathPrefix string) (
	cluster logicalcluster.Name,
	key dynamiccontext.APIDomainKey,
	logicalPath string,
	accepted bool,
) {
	if !strings.HasPrefix(urlPath, rootPathPrefix) {
		return logicalcluster.Name(""), "", "", false
	}

	// Incoming requests to this virtual workspace will look like:
	//  /services/apiexport/root:org:ws/<apiexport-name>/clusters/*/api/v1/configmaps
	//                     └────────────────────────┐
	// Where the withoutRootPathPrefix starts here: ┘
	withoutRootPathPrefix := strings.TrimPrefix(urlPath, rootPathPrefix)

	parts := strings.SplitN(withoutRootPathPrefix, "/", 3)
	if len(parts) < 3 {
		return logicalcluster.Name(""), "", "", false
	}

	cachedResourceClusterName, cachedResourceName := parts[0], parts[1]
	if cachedResourceClusterName == "" {
		return logicalcluster.Name(""), "", "", false
	}
	if cachedResourceName == "" {
		return logicalcluster.Name(""), "", "", false
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
		return logicalcluster.Name(""), "", "", false
	}

	withoutClustersPrefix := strings.TrimPrefix(realPath, "/clusters/")
	parts = strings.SplitN(withoutClustersPrefix, "/", 2)
	if parts[0] != cachedResourceClusterName {
		//return logicalcluster.Name(""), "", "", false
	}
	realPath = "/"
	if len(parts) > 1 {
		realPath += parts[1]
	}

	key = apidomainkey.New(logicalcluster.Name(cachedResourceClusterName), cachedResourceName)
	return logicalcluster.Name(cachedResourceClusterName), dynamiccontext.APIDomainKey(key), strings.TrimSuffix(urlPath, realPath), true
}

func newAuth(deepSARClient kcpkubernetesclientset.ClusterInterface) authorizer.Authorizer {
	wrappedResourceAuthorizer := replicationauthorizer.NewWrappedResourceAuthorizer(deepSARClient)
	wrappedResourceAuthorizer = authorization.NewDecorator("virtual.replication.wrappedresource.authorization.kcp.io", wrappedResourceAuthorizer).AddAuditLogging().AddAnonymization().AddReasonAnnotation()

	return wrappedResourceAuthorizer
}

type singleResourceAPIDefinitionSetProvider struct {
	config               genericapiserver.CompletedConfig
	dynamicClusterClient kcpdynamic.ClusterInterface
	resource             *apisv1alpha1.APIResourceSchema
	exposeSubresources   bool
	storageProvider      func(ctx context.Context, dynamicClusterClientFunc forwardingregistry.DynamicClusterClientFunc, sch *apisv1alpha1.APIResourceSchema, version string) (apiserver.RestProviderFunc, error)

	KcpCacheClusterClient kcpclientset.ClusterInterface // <-- ...
	wildcardKcpInformers  kcpinformers.SharedInformerFactory
	kcpClusterClient      kcpclientset.ClusterInterface
	globalClusterClient   kcpclientset.ClusterInterface

	getAPIExportByPath         func(path logicalcluster.Path, name string) (*apisv1alpha2.APIExport, error)
	getAPIResourceSchemaByName func(cluster logicalcluster.Name, name string) (*apisv1alpha1.APIResourceSchema, error)
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

	apiExport, err := a.getAPIExportByPath(logicalcluster.NewPath(apiBinding.Spec.Reference.Export.Path), apiBinding.Spec.Reference.Export.Name)
	if err != nil {
		allApiExports, listErr := a.kcpClusterClient.Cluster(logicalcluster.NewPath(apiBinding.Spec.Reference.Export.Path)).ApisV1alpha2().APIExports().List(ctx, metav1.ListOptions{})
		fmt.Printf("\n\n\nALL EXPORTS: exports=%#v err=%v <>\n\n", allApiExports, err)
		return nil, fmt.Errorf("failed to get APIExport %s|%s referenced by APIBinding %s|%s: err=%v, listErr=%v",
			apiBinding.Spec.Reference.Export.Path, apiBinding.Spec.Reference.Export.Name,
			clusterName, bindingName, err, listErr,
		)
	}
	apiExportClusterName := logicalcluster.From(apiExport)

	schName := ""
	for _, exportResource := range apiExport.Spec.Resources {
		if exportResource.Group == gvr.Group && exportResource.Name == gvr.Resource {
			schName = exportResource.Schema
		}
	}

	return a.getAPIResourceSchemaByName(apiExportClusterName, schName)
}

/*func getCachedResourcesForSchema(
	ctx context.Context,
	sch *apisv1alpha1.APIResourceSchema,
	wildcardKcpInformers kcpinformers.SharedInformerFactory,
) ([]*cachev1alpha1.CachedResource, error) {
	var cachedResources []*cachev1alpha1.CachedResource

	var version string
	for i := range sch.Spec.Versions {
		if sch.Spec.Versions[i].Served && sch.Spec.Versions[i].Storage {
			version = sch.Spec.Versions[i].Name
		}
	}
	if version == "" {
		return nil, fmt.Errorf("XXX could not find appropriate schema version")
	}

	objs, err := wildcardKcpInformers.Cache().V1alpha1().CachedResources().Informer().GetIndexer().ByIndex(
		cachedresourcesreplication.ByGVRAndShard,
		cachedresourcesreplication.GVRAndShard(
			schema.GroupVersionResource{},
			"root",
		),
	)
	if err != nil {
		return nil, fmt.Errorf("XXX could not list CachedResources by index: %v", err)
	}

	for i := range objs {
		cachedResources = append(cachedResources, objs[i].(*cachev1alpha1.CachedResource))
	}

	return cachedResources, nil
}*/

func (a *singleResourceAPIDefinitionSetProvider) GetAPIDefinitionSet(ctx context.Context, key dynamiccontext.APIDomainKey) (apis apidefinition.APIDefinitionSet, apisExist bool, err error) {
	parsedKey, err := apidomainkey.Parse(key)
	if err != nil {
		return nil, false, err
	}

	clientFactory := func(ctx context.Context) (kcpdynamic.ClusterInterface, error) {
		return a.dynamicClusterClient, nil
	}

	cachedResource, err := a.kcpClusterClient.CacheV1alpha1().CachedResources().Cluster(parsedKey.CachedResourceCluster.Path()).
		Get(ctx, parsedKey.CachedResourceName, metav1.GetOptions{})
	if err != nil {
		return nil, false, err
	}

	wrappedGVR := schema.GroupVersionResource(cachedResource.Spec.GroupVersionResource)
	wrappedSch, err := a.getAPIResourceSchema(ctx, parsedKey.CachedResourceCluster, wrappedGVR)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get schema for wrapped object in CachedResource %s|%s: %v", parsedKey.CachedResourceCluster, parsedKey.CachedResourceName, err)
	}

	restProvider, err := a.storageProvider(ctx, clientFactory, wrappedSch, wrappedGVR.Version)
	if err != nil {
		return nil, false, err
	}

	apiDefinition, err := apiserver.CreateServingInfoFor(
		a.config,
		wrappedSch,
		wrappedGVR.Version,
		restProvider,
	)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create serving info: %w", err)
	}

	return apidefinition.APIDefinitionSet{
		wrappedGVR: apiDefinition,
	}, true, nil
}

var _ apidefinition.APIDefinitionSetGetter = &singleResourceAPIDefinitionSetProvider{}

/*cachedobjs, err := a.KcpCacheClusterClient.CacheV1alpha1().Cluster(parsedKey.APIExportCluster.Path()).CachedObjects().List(ctx, metav1.ListOptions{})
if err != nil {
	return nil, false, fmt.Errorf("failed to list CachedObjs: %v", err)
}
names := make([]string, len(cachedobjs.Items))
for i := range cachedobjs.Items {
	names[i] = cachedobjs.Items[i].Name
}
fmt.Printf("\n\n\n ^^^ CachedObjs:%v ^^^\n\n\n", names)*/

/*func dummyCacheKcpSharedInformerFactory(kcpCacheClusterClient kcpclientset.ClusterInterface) context.CancelFunc {
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
}*/
