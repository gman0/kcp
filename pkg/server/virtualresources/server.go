package virtualresources

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	apiextensionshelpers "k8s.io/apiextensions-apiserver/pkg/apihelpers"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	genericapiserver "k8s.io/apiserver/pkg/server"

	"k8s.io/client-go/rest"

	"github.com/kcp-dev/logicalcluster/v3"

	"github.com/kcp-dev/kcp/pkg/endpointslice"
	"github.com/kcp-dev/kcp/pkg/indexers"
	kcpfilters "github.com/kcp-dev/kcp/pkg/server/filters"
	apisv1alpha2 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha2"
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

type Server struct {
	GenericAPIServer *genericapiserver.GenericAPIServer
	Extra            *ExtraConfig
	delegate         genericapiserver.DelegationTarget
	vwTlsConfig      *tls.Config

	getCRD                       func(cluster logicalcluster.Name, name string) (*apiextensionsv1.CustomResourceDefinition, error)
	getUnstructuredEndpointSlice func(ctx context.Context, cluster logicalcluster.Name, gvr schema.GroupVersionResource, name string) (*unstructured.Unstructured, error)
	getAPIExportByPath           func(clusterPath logicalcluster.Path, name string) (*apisv1alpha2.APIExport, error)
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
		getAPIExportByPath: func(clusterPath logicalcluster.Path, name string) (*apisv1alpha2.APIExport, error) {
			return indexers.ByPathAndNameWithFallback[*apisv1alpha2.APIExport](
				apisv1alpha2.Resource("apiexports"),
				c.Extra.LocalAPIExportInformer.Informer().GetIndexer(),
				c.Extra.GlobalAPIExportInformer.Informer().GetIndexer(),
				clusterPath,
				name,
			)
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

func splitPath(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return []string{}
	}
	return strings.Split(path, "/")
}

func (s *Server) newApisHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if len(splitPath(r.URL.Path)) > 3 {
			s.handleResource(w, r)
			return
		}

		s.delegate.UnprotectedHandler().ServeHTTP(w, r)
	}
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
		// Discovery requests should have been caught earlier.
		// Maybe the delegate knows what to do.
		s.delegate.UnprotectedHandler().ServeHTTP(w, r)
		return
	}

	clusterNameOrWildcard, wildcard, err := genericapirequest.ClusterNameOrWildcardFrom(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if wildcard {
		clusterNameOrWildcard = "*"
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

	apiBinding, err := s.getAPIBindingForRequest(clusterNameOrWildcard.String(), gr, identity)
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
	crd, err := s.getCRD(logicalcluster.Name("system:bound-crds"), crdName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// XXX: This should return 404 -- maybe the CRD is created by the time apiextensions delegate finishes and takes over and this would race.
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
		// Same as above -- this should be 404.
		s.delegate.UnprotectedHandler().ServeHTTP(w, r)
		return
	}

	// Get the origin APIExport, and check that the resource has virtual storage. Otherwise delegate the request.

	apiExportPath := logicalcluster.NewPath(apiBinding.Spec.Reference.Export.Path)
	if apiExportPath.Empty() {
		apiExportPath = logicalcluster.NewPath(logicalcluster.From(apiBinding).String())
	}
	apiExport, err := s.getAPIExportByPath(apiExportPath, apiBinding.Spec.Reference.Export.Name)
	if err != nil {
		utilruntime.HandleError(err)
		responsewriters.ErrorNegotiated(
			apierrors.NewInternalError(fmt.Errorf("error resolving resource: %v", err)),
			codecs, schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}, w, r,
		)
		return
	}

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

	vrHandler, err := newProxy(clusterNameOrWildcard.String(), vrEndpointURL, apiExport.Status.IdentityHash, s.vwTlsConfig)
	if err != nil {
		utilruntime.HandleError(err)
		responsewriters.ErrorNegotiated(
			apierrors.NewInternalError(fmt.Errorf("error serving resource: %v", err)),
			codecs, schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}, w, r,
		)
		return
	}

	vrHandler.ServeHTTP(w, r)
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

	return endpointslice.FindOneURL(s.Extra.ShardVirtualWorkspaceURLGetter(), urls)
}

func (s *Server) getAPIBindingForRequest(
	clusterNameOrWildcard string,
	gr schema.GroupResource,
	identity string,
) (*apisv1alpha2.APIBinding, error) {
	var (
		apiBindings []*apisv1alpha2.APIBinding
		err         error
	)
	if clusterNameOrWildcard == "*" {
		apiBindings, err = indexers.ByIndex[*apisv1alpha2.APIBinding](
			s.Extra.APIBindingInformer.Informer().GetIndexer(),
			indexers.APIBindingByIdentityAndGroupResource,
			indexers.IdentityGroupResourceKeyFunc(identity, gr.Group, gr.Resource),
		)
	} else {
		apiBindings, err = indexers.ByIndex[*apisv1alpha2.APIBinding](
			s.Extra.APIBindingInformer.Informer().GetIndexer(),
			indexers.APIBindingByBoundResources,
			indexers.APIBindingBoundResourceValue(logicalcluster.Name(clusterNameOrWildcard), gr.Group, gr.Resource),
		)
	}
	if err != nil {
		return nil, err
	}

	if len(apiBindings) > 0 {
		// Matching cluster/identity and bound GR should mean we have the correct APIBinding.
		// This is similar to what we're doing in apiBindingAwareCRDLister when selecting
		// a binding by identity wildcard.
		return apiBindings[0], nil
	}

	// This GR does not seem to be provided by an APIBinding.
	return nil, nil
}

func newProxy(clusterNameOrWildcard string, vwURL, apiExportIdentity string, vwTLSConfig *tls.Config) (http.Handler, error) {
	scopedURL, err := url.Parse(virtualResourceURLWithCluster(vwURL, apiExportIdentity, clusterNameOrWildcard))
	if err != nil {
		return nil, err
	}

	handler := httputil.NewSingleHostReverseProxy(scopedURL)
	handler.Transport = &http.Transport{
		TLSClientConfig: vwTLSConfig,
	}

	return handler, nil
}

func virtualResourceURLWithCluster(vwURL, apiExportIdentity string, clusterNameOrWildcard string) string {
	// Formats the URL like so:
	//     <Virtual resource VW endpoint>:<APIExport identity>/clusters/<Target cluster>
	return fmt.Sprintf("%s:%s/clusters/%s", vwURL, apiExportIdentity, clusterNameOrWildcard)
}
