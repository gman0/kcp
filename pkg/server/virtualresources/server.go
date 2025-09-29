package virtualresources

import (
	// "encoding/json"
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	autoscaling "k8s.io/api/autoscaling/v1"
	apiextensionshelpers "k8s.io/apiextensions-apiserver/pkg/apihelpers"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/endpoints/discovery"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	genericapiserver "k8s.io/apiserver/pkg/server"
	// discoveryclient "k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"

	"github.com/kcp-dev/logicalcluster/v3"

	"github.com/kcp-dev/kcp/pkg/endpointslice"
	"github.com/kcp-dev/kcp/pkg/indexers"
	kcpfilters "github.com/kcp-dev/kcp/pkg/server/filters"
	apisv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha1"
	apisv1alpha2 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha2"
	corev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/core/v1alpha1"
	apisv1alpha2informers "github.com/kcp-dev/kcp/sdk/client/informers/externalversions/apis/v1alpha2"
)

var (
	scheme = runtime.NewScheme()
	codecs = serializer.NewCodecFactory(scheme)

	// if you modify this, make sure you update the crEncoder
	unversionedVersion = schema.GroupVersion{Group: "", Version: "v1"}
	unversionedTypes   = []runtime.Object{
		&metav1.Status{},
		&metav1.WatchEvent{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	}
)

func init() {
	// we need to add the options to empty v1
	metav1.AddToGroupVersion(scheme, schema.GroupVersion{Group: "", Version: "v1"})

	scheme.AddUnversionedTypes(unversionedVersion, unversionedTypes...)
}

type apiDef struct {
	apiGroups    map[string]metav1.APIGroup
	apiResources map[schema.GroupVersion]map[string]metav1.APIResource

	endpoints           map[schema.GroupResource]string
	apiExportIdentities map[schema.GroupResource]string
}

type Server struct {
	GenericAPIServer *genericapiserver.GenericAPIServer
	Extra            *ExtraConfig
	delegate         genericapiserver.DelegationTarget
	vwTlsConfig      *tls.Config

	getThisShard                 func() (*corev1alpha1.Shard, error)
	getCRD                       func(cluster logicalcluster.Name, name string) (*apiextensionsv1.CustomResourceDefinition, error)
	getUnstructuredEndpointSlice func(ctx context.Context, cluster logicalcluster.Name, gvr schema.GroupVersionResource, name string) (*unstructured.Unstructured, error)
}

func NewServer(c CompletedConfig, delegationTarget genericapiserver.DelegationTarget) (*Server, error) {
	s := &Server{
		Extra:    c.Extra,
		delegate: delegationTarget,

		getUnstructuredEndpointSlice: func(ctx context.Context, cluster logicalcluster.Name, gvr schema.GroupVersionResource, name string) (*unstructured.Unstructured, error) {
			list, err := c.Extra.DynamicClusterClient.Cluster(cluster.Path()).Resource(gvr).List(ctx, metav1.ListOptions{})
			if err != nil {
				return nil, err
			}

			if len(list.Items) == 0 {
				return nil, apierrors.NewNotFound(gvr.GroupResource(), name)
			}

			var slice *unstructured.Unstructured
			for _, item := range list.Items {
				if item.GetName() == name {
					if slice != nil {
						return nil, apierrors.NewInternalError(fmt.Errorf("multiple objects found"))
					}
					slice = &item
				}
			}

			return slice, nil
		},
		getCRD: func(clusterName logicalcluster.Name, name string) (*apiextensionsv1.CustomResourceDefinition, error) {
			return c.Extra.CRDLister.Lister().Cluster(clusterName).Get(name)
		},
	}

	tlsConfig, err := rest.TLSConfigFor(c.Extra.VWClientConfig)
	if err != nil {
		return nil, err
	}
	s.vwTlsConfig = tlsConfig

	s.GenericAPIServer, err = c.Generic.New("virtual-resources-root-apiserver", delegationTarget)
	if err != nil {
		return nil, err
	}

	// We perform only APIResource discovery. Group discovery is delegated to apiextensions-server.
	s.GenericAPIServer.DiscoveryGroupManager = nil
	s.GenericAPIServer.Handler.NonGoRestfulMux.HandlePrefix("/apis/", s.newApisHandler())

	return s, nil
}

/*dc, err := discoveryclient.NewDiscoveryClientForConfig(&config)
if err != nil {
	return fmt.Errorf("failed to create discovery client for gr=%q, endpoint=%q: %v", gr, config.Host, err)
}

// Get API groups from the VW.

apiGroupList, err := dc.ServerGroups()
if err != nil {
	return fmt.Errorf("discovery client failed to list api groups for endpoint=%q: %v", config.Host, err)
}

// Find the group we want to bind.

var apiGroup *metav1.APIGroup
for _, group := range apiGroupList.Groups {
	if group.Name == gr.Group {
		apiGroup = group.DeepCopy()
		break
	}
}
if apiGroup == nil {
	return fmt.Errorf("group %s not found in %s discovery", gr.Group, vrEndpointURL)
}

// Get all versions in the found group that are serving the bound resource.

var apiResources []metav1.APIResource
for _, version := range apiGroup.Versions {
	apiResourceList, err := dc.ServerResourcesForGroupVersion(version.GroupVersion)
	if err != nil {
		return fmt.Errorf("discovery client failed to list resources for group/version %s in %s: %v", version.GroupVersion, vrEndpointURL, err)
	}

	for _, res := range apiResourceList.APIResources {
		if res.Name == gr.Resource {
			res = *res.DeepCopy()
			if res.Group == "" {
				res.Group = apiGroup.Name
			}
			if res.Version == "" {
				res.Version = version.Version
			}
			apiResources = append(apiResources, res)
			break
		}
	}
}*/

func splitPath(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return []string{}
	}
	return strings.Split(path, "/")
}

func (s *Server) newApisHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pathParts := splitPath(r.URL.Path)
		switch len(pathParts) {
		case 3:
			s.handleAPIResourceList(w, r)
			return
		default:
			s.handleResource(w, r)
			return
		}
	}
}

func apiResourcesForGroupVersion(requestedGroup, requestedVersion string, crds []*apiextensionsv1.CustomResourceDefinition) []metav1.APIResource {
	apiResourcesForDiscovery := []metav1.APIResource{}

	for _, crd := range crds {
		if requestedGroup != crd.Spec.Group {
			continue
		}

		if !apiextensionshelpers.IsCRDConditionTrue(crd, apiextensionsv1.Established) {
			continue
		}

		var (
			storageVersionHash string
			subresources       *apiextensionsv1.CustomResourceSubresources
			foundVersion       = false
		)

		for _, v := range crd.Spec.Versions {
			if !v.Served {
				continue
			}

			// HACK: support the case when we add core resources through CRDs (KCP scenario)
			groupVersion := crd.Spec.Group + "/" + v.Name
			if crd.Spec.Group == "" {
				groupVersion = v.Name
			}

			gv := metav1.GroupVersion{Group: groupVersion, Version: v.Name}

			if v.Name == requestedVersion {
				foundVersion = true
				subresources = v.Subresources
			}
			if v.Storage {
				storageVersionHash = discovery.StorageVersionHash(logicalcluster.From(crd), gv.Group, gv.Version, crd.Spec.Names.Kind)
			}
		}

		if !foundVersion {
			// This CRD doesn't have the requested version
			continue
		}

		verbs := metav1.Verbs([]string{"delete", "deletecollection", "get", "list", "patch", "create", "update", "watch"})
		// if we're terminating we don't allow some verbs
		if apiextensionshelpers.IsCRDConditionTrue(crd, apiextensionsv1.Terminating) {
			verbs = metav1.Verbs([]string{"delete", "deletecollection", "get", "list", "watch"})
		}

		if crd.Annotations[apisv1alpha1.AnnotationSchemaVirtualStorageIdentityKey] != "" {
			verbs = metav1.Verbs([]string{"get", "list", "watch"})
		}

		apiResourcesForDiscovery = append(apiResourcesForDiscovery, metav1.APIResource{
			Name:               crd.Status.AcceptedNames.Plural,
			SingularName:       crd.Status.AcceptedNames.Singular,
			Namespaced:         crd.Spec.Scope == apiextensionsv1.NamespaceScoped,
			Kind:               crd.Status.AcceptedNames.Kind,
			Verbs:              verbs,
			ShortNames:         crd.Status.AcceptedNames.ShortNames,
			Categories:         crd.Status.AcceptedNames.Categories,
			StorageVersionHash: storageVersionHash,
		})

		if subresources != nil && subresources.Status != nil {
			statusVerbs := metav1.Verbs([]string{"get", "patch", "update"})
			if crd.Annotations[apisv1alpha1.AnnotationSchemaVirtualStorageIdentityKey] != "" {
				statusVerbs = metav1.Verbs([]string{"get"})
			}

			apiResourcesForDiscovery = append(apiResourcesForDiscovery, metav1.APIResource{
				Name:       crd.Status.AcceptedNames.Plural + "/status",
				Namespaced: crd.Spec.Scope == apiextensionsv1.NamespaceScoped,
				Kind:       crd.Status.AcceptedNames.Kind,
				Verbs:      statusVerbs,
			})
		}

		if subresources != nil && subresources.Scale != nil {
			scaleVerbs := metav1.Verbs([]string{"get", "patch", "update"})
			if crd.Annotations[apisv1alpha1.AnnotationSchemaVirtualStorageIdentityKey] != "" {
				scaleVerbs = metav1.Verbs([]string{"get"})
			}

			apiResourcesForDiscovery = append(apiResourcesForDiscovery, metav1.APIResource{
				Group:      autoscaling.GroupName,
				Version:    "v1",
				Kind:       "Scale",
				Name:       crd.Status.AcceptedNames.Plural + "/scale",
				Namespaced: crd.Spec.Scope == apiextensionsv1.NamespaceScoped,
				Verbs:      scaleVerbs,
			})
		}
	}

	return apiResourcesForDiscovery
}

func (s *Server) handleAPIResourceList(w http.ResponseWriter, r *http.Request) {
	pathParts := splitPath(r.URL.Path)
	// only match /apis/<group>/<version>
	if len(pathParts) != 3 || pathParts[0] != "apis" {
		s.delegate.UnprotectedHandler().ServeHTTP(w, r)
		return
	}

	clusterName, wildcard, err := genericapirequest.ClusterNameOrWildcardFrom(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if wildcard {
		// this is the only case where wildcard works for a list because this is our special CRD lister that handles it.
		clusterName = "*"
	}

	requestedGroup := pathParts[1]
	requestedVersion := pathParts[2]

	crds, err := s.Extra.APIBindingAwareCRDLister.Cluster(clusterName).List(r.Context(), labels.Everything())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	apiResources := apiResourcesForGroupVersion(requestedGroup, requestedVersion, crds) // TODO: VR verbs should be moved out
	// StorageAPIBindingAwareCRDLister?

	resourceListerFunc := discovery.APIResourceListerFunc(func() []metav1.APIResource {
		return apiResources
	})

	discovery.NewAPIVersionHandler(codecs, schema.GroupVersion{Group: requestedGroup, Version: requestedVersion}, resourceListerFunc).ServeHTTP(w, r)
}

func (s *Server) handleResource(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	requestInfo, ok := apirequest.RequestInfoFrom(ctx)
	if !ok {
		responsewriters.ErrorNegotiated(
			apierrors.NewInternalError(fmt.Errorf("no RequestInfo found in the context")),
			codecs, schema.GroupVersion{}, w, r,
		)
		return
	}
	if !requestInfo.IsResourceRequest {
		pathParts := splitPath(requestInfo.Path)
		// Only match /apis/<group>/<version>.
		// Registered under /apis.
		if len(pathParts) == 3 {
			s.handleAPIResourceList(w, r)
			return
		}

		// Group discovery is left to the delegate (apiextensions apiserver).
		s.delegate.UnprotectedHandler().ServeHTTP(w, r)
		return
	}

	// Check there is a bound CRD, and is healthy
	// * ns in req mismatch
	// * list watch possible across namespaces (should be handled by VW?)
	// * crd has NamesAccepted and Established
	// * schema negotiation not needed for now
	// * no special handling for terminating state because we don't handle Create for now
	// * wildcard partial metadata?
	// Get a binding
	// Get its export
	// Get VR URL
	// Serve from it

	cluster := apirequest.ClusterFrom(r.Context())
	if cluster == nil {
		http.Error(w, fmt.Sprintf("cluster not in request"), http.StatusInternalServerError)
		return
	}

	gr := schema.GroupResource{
		Group:    requestInfo.APIGroup,
		Resource: requestInfo.Resource,
	}
	if gr.Group == "" {
		gr.Group = "core"
	}

	// partialMetadataRequest := kcpfilters.IsPartialMetadataRequest(ctx)
	identity := kcpfilters.IdentityFromContext(ctx)

	apiBinding, err := s.getAPIBindingForRequest(*cluster, gr, identity)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if apiBinding == nil {
		// Not a virtual resource: the resource is not provided by an APIBinding.
		s.delegate.UnprotectedHandler().ServeHTTP(w, r)
		return
	}

	// The associated CRD must be healthy.

	var crdName string
	for _, boundResource := range apiBinding.Status.BoundResources {
		if boundResource.Group == gr.Group && boundResource.Resource == gr.Resource {
			crdName = boundResource.Schema.UID
			break
		}
	}
	if crdName == "" {
		// This should not happen, the indexers returned a binding for this specific GR.
		responsewriters.ErrorNegotiated(
			apierrors.NewInternalError(fmt.Errorf("resource not available")),
			codecs, schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}, w, r,
		)
		return
	}
	// We do what the apiextensions apiserver does: delegate on not found or !NamesAccepted or !Established, otherwise we fail.
	crd, err := s.getCRD(logicalcluster.Name("system:system-crds"), crdName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			s.delegate.UnprotectedHandler().ServeHTTP(w, r)
			return
		}
		utilruntime.HandleError(err)
		responsewriters.ErrorNegotiated(
			apierrors.NewInternalError(fmt.Errorf("error resolving resource: %v", err)),
			codecs, schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}, w, r,
		)
		return
	}
	if !apiextensionshelpers.IsCRDConditionTrue(crd, apiextensionsv1.NamesAccepted) &&
		!apiextensionshelpers.IsCRDConditionTrue(crd, apiextensionsv1.Established) {
		s.delegate.UnprotectedHandler().ServeHTTP(w, r)
		return
	}

	// Get the origin APIExport, and check that the resource has virtual storage. Otherwise delegate the request.

	apiExportPath := logicalcluster.NewPath(apiBinding.Spec.Reference.Export.Path)
	if apiExportPath.Empty() {
		apiExportPath = logicalcluster.NewPath(logicalcluster.From(apiBinding).String())
	}
	apiExport, err := getAPIExportByPath(
		apiExportPath,
		apiBinding.Spec.Reference.Export.Name,
		s.Extra.LocalAPIExportInformer,
		s.Extra.GlobalAPIExportInformer,
	)

	var virtualStorageDef *apisv1alpha2.ResourceSchemaStorageVirtual
	for _, resource := range apiExport.Spec.Resources {
		if resource.Storage.Virtual != nil &&
			resource.Group == gr.Group &&
			resource.Name == gr.Resource {
			virtualStorageDef = resource.Storage.Virtual
		}
	}
	if virtualStorageDef == nil {
		// Not a virtual resource: the binding's export doesn't define such resource with virtual storage.
		s.delegate.UnprotectedHandler().ServeHTTP(w, r)
		return
	}

	// We have a virtual resource. Get the endpoint URL, create a proxy handler and serve from that endpoint.

	vrEndpointURL, err := s.getVirtualResourceURL(ctx, logicalcluster.From(apiExport), virtualStorageDef)
	if err != nil {
		utilruntime.HandleError(err)
		responsewriters.ErrorNegotiated(
			apierrors.NewInternalError(fmt.Errorf("error resolving resource: %v", err)),
			codecs, schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}, w, r,
		)
		return
	}

	vrHandler, err := newProxy(cluster, vrEndpointURL, apiExport.Status.IdentityHash, s.vwTlsConfig)
	if err != nil {
		utilruntime.HandleError(err)
		responsewriters.ErrorNegotiated(
			apierrors.NewInternalError(fmt.Errorf("error serving resource: %v", err)),
			codecs, schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}, w, r,
		)
		return
	}

	vrHandler.ServeHTTP(w, r)
	return
}

func (s *Server) getVirtualResourceURL(ctx context.Context, apiExportCluster logicalcluster.Name, virtual *apisv1alpha2.ResourceSchemaStorageVirtual) (string, error) {
	slice, err := s.getUnstructuredEndpointSlice(ctx, apiExportCluster, schema.GroupVersionResource{
		Group:    virtual.Group,
		Version:  virtual.Version,
		Resource: virtual.Resource,
	}, virtual.Name)
	if err != nil {
		return "", err
	}

	urls, err := endpointslice.ListURLsFromUnstructured(*slice)
	if err != nil {
		return "", err
	}

	return endpointslice.FindOneURL(s.Extra.ShartVirtualWorkspaceURLGetter(), urls)
}

func (s *Server) getAPIBindingForRequest(
	cluster genericapirequest.Cluster,
	gr schema.GroupResource,
	identity string,
) (*apisv1alpha2.APIBinding, error) {
	var (
		apiBindings []*apisv1alpha2.APIBinding
		err         error
	)
	if cluster.Wildcard {
		apiBindings, err = indexers.ByIndex[*apisv1alpha2.APIBinding](
			s.Extra.APIBindingInformer.Informer().GetIndexer(),
			indexers.APIBindingByIdentityAndGroupResource,
			indexers.IdentityGroupResourceKeyFunc(identity, gr.Group, gr.Resource),
		)
	} else {
		apiBindings, err = indexers.ByIndex[*apisv1alpha2.APIBinding](
			s.Extra.APIBindingInformer.Informer().GetIndexer(),
			indexers.APIBindingByBoundResources,
			indexers.APIBindingBoundResourceValue(cluster.Name, gr.Group, gr.Resource),
		)
	}
	if err != nil {
		return nil, err
	}

	if len(apiBindings) > 0 {
		return apiBindings[0], nil
	}

	// This GR does not seem to be provided by an APIBinding.
	return nil, nil
}

func getAPIExportByPath(clusterPath logicalcluster.Path, name string, local, global apisv1alpha2informers.APIExportClusterInformer) (*apisv1alpha2.APIExport, error) {
	return indexers.ByPathAndNameWithFallback[*apisv1alpha2.APIExport](
		apisv1alpha2.Resource("apiexports"),
		local.Informer().GetIndexer(),
		global.Informer().GetIndexer(),
		clusterPath,
		name,
	)
}

func newProxy(cluster *genericapirequest.Cluster, vwURL, apiExportIdentity string, vwTLSConfig *tls.Config) (http.Handler, error) {
	scopedURL, err := url.Parse(urlWithCluster(vwURL, apiExportIdentity, cluster))
	if err != nil {
		return nil, err
	}

	handler := httputil.NewSingleHostReverseProxy(scopedURL)
	handler.Transport = &http.Transport{
		TLSClientConfig: vwTLSConfig,
	}

	return handler, nil
}

func urlWithCluster(vwURL, apiExportIdentity string, cluster *genericapirequest.Cluster) string {
	clusterName := cluster.Name
	if cluster.Wildcard {
		clusterName = "*"
	}
	return fmt.Sprintf("%s:%s/clusters/%s", vwURL, apiExportIdentity, clusterName)
}
