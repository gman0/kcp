package virtualresources

import (
	// "encoding/json"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"

	// apierrors "k8s.io/apimachinery/pkg/api/errors"
	// metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	// "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/warning"

	"github.com/kcp-dev/logicalcluster/v3"
	discoveryclient "k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

var (
	errorScheme = runtime.NewScheme()
	errorCodecs = serializer.NewCodecFactory(errorScheme)
)

func init() {
	errorScheme.AddUnversionedTypes(metav1.Unversioned,
		&metav1.Status{},
	)
}

type Server struct {
	GenericAPIServer *genericapiserver.GenericAPIServer
	Extra            *ExtraConfig
	delegate         genericapiserver.DelegationTarget
	vwTlsConfig      *tls.Config

	groupManagers *clusterAwareGroupManager
	handlers      *proxyToVirtualWorkspace

	lock          sync.RWMutex
	groupInfos    map[logicalcluster.Name]map[string]metav1.APIGroup
	resourceInfos map[logicalcluster.Name]map[schema.GroupVersion]metav1.APIResource
	grEndpointMap map[logicalcluster.Name]map[schema.GroupResource]string
}

type resourceInfo struct {
	group            string
	resource         string
	versions         []metav1.GroupVersionForDiscovery
	preferredVersion metav1.GroupVersionForDiscovery
}

func NewServer(c CompletedConfig, delegationTarget genericapiserver.DelegationTarget) (*Server, error) {
	handlers, err := newProxyToVirtualWorkspace(c.Extra.VWClientConfig)
	if err != nil {
		return nil, err
	}

	s := &Server{
		Extra:         c.Extra,
		delegate:      delegationTarget,
		groupManagers: newClusterAwareGroupManager(c.Generic.DiscoveryAddresses, c.Generic.Serializer),
		handlers:      handlers,
		groupInfos:    make(map[logicalcluster.Name]map[string]metav1.APIGroup),
		resourceInfos: make(map[logicalcluster.Name]map[schema.GroupVersion]metav1.APIResource),
		grEndpointMap: make(map[logicalcluster.Name]map[schema.GroupResource]string),
	}

	tlsConfig, err := rest.TLSConfigFor(c.Extra.VWClientConfig)
	if err != nil {
		return nil, err
	}
	s.vwTlsConfig = tlsConfig

	// c.Generic.BuildHandlerChainFunc = s.buildHandlerChain(c, delegationTarget)
	// c.Generic.ReadyzChecks = append(c.Generic.ReadyzChecks, asHealthChecks(c.Extra.VirtualWorkspaces)...)
	// apiBindings lister synced ^

	s.GenericAPIServer, err = c.Generic.New("virtual-resources-root-apiserver", delegationTarget)
	if err != nil {
		return nil, err
	}
	s.GenericAPIServer.DiscoveryGroupManager = s.groupManagers

	apisHandler := s.newApisHandler()

	s.GenericAPIServer.Handler.NonGoRestfulMux.Handle("/apis", apisHandler)
	s.GenericAPIServer.Handler.NonGoRestfulMux.HandlePrefix("/apis/", apisHandler)

	return s, nil
}

func (s *Server) addHandlerFor(cluster logicalcluster.Name, gr schema.GroupResource, vwEndpointURL string) error {
	config := *s.Extra.VWClientConfig
	config.Host = vwEndpointURL + fmt.Sprintf("/clusters/%s", cluster.String())

	dc, err := discoveryclient.NewDiscoveryClientForConfig(&config)
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
		return fmt.Errorf("group %s not found in %s discovery", gr.Group, vwEndpointURL)
	}

	// Get all versions in the found group that are serving the bound resource.

	var apiResources []metav1.APIResource
	for _, version := range apiGroup.Versions {
		apiResourceList, err := dc.ServerResourcesForGroupVersion(version.GroupVersion)
		if err != nil {
			return fmt.Errorf("discovery client failed to list resources for group/version %s in %s: %v", version.GroupVersion, vwEndpointURL, err)
		}

		for _, res := range apiResourceList.APIResources {
			if res.Name == gr.Resource {
				apiResources = append(apiResources, *res.DeepCopy())
				break
			}
		}
	}

	if apiResources == nil {
		return fmt.Errorf("resource %s/%s not found in %s", gr.Group, gr.Resource, vwEndpointURL)
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	// Store the group we've found.

	s.groupManagers.AddGroupForCluster(cluster, gr.Group)

	if _, ok := s.groupInfos[cluster]; !ok {
		s.groupInfos[cluster] = make(map[string]metav1.APIGroup)
	}
	s.groupInfos[cluster][gr.Group] = *apiGroup

	// Store resource's gv.

	if _, ok := s.resourceInfos[cluster]; !ok {
		s.resourceInfos[cluster] = make(map[schema.GroupVersion]metav1.APIResource)
	}
	scopedResourceInfos := s.resourceInfos[cluster]
	for _, res := range apiResources {
		scopedResourceInfos[schema.GroupVersion{
			Group:   res.Group,
			Version: res.Version,
		}] = res
	}

	// Store the vw url.

	s.grEndpointMap[cluster][gr] = vwEndpointURL

	return nil
}

func (s *Server) removeHandlerFor(cluster logicalcluster.Name, gr schema.GroupResource, vwEndpointURL string) error {
	return nil
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
		pathParts := splitPath(r.URL.Path)
		fmt.Printf("\nAAAA path=%v\n", pathParts)
		switch len(pathParts) {
		case 1:
			s.handleAPIGroupList(w, r)
			return
		case 3:
			s.handleAPIResourceList(w, r)
			return
		default:
			s.handleResource(w, r)
			return
		}
	}
}

func (s *Server) handleAPIGroupList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	cluster := genericapirequest.ClusterFrom(ctx)
	if cluster == nil {
		warning.AddWarning(ctx, "", "cluster missing in context")
		s.delegate.UnprotectedHandler().ServeHTTP(w, r)
		return
	}

	s.lock.RLock()
	defer s.lock.RUnlock()

	knownGroups := s.groupInfos[cluster.Name]
	groupList := &metav1.APIGroupList{}
	groupList.Groups = make([]metav1.APIGroup, 0, len(knownGroups))

	for _, res := range knownGroups {
		groupList.Groups = append(groupList.Groups, res)
	}

	responsewriters.WriteObjectNegotiated(s.GenericAPIServer.Serializer, negotiation.DefaultEndpointRestrictions, schema.GroupVersion{}, w, r, http.StatusOK, groupList, false)
}

func (s *Server) handleAPIResourceList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	cluster := genericapirequest.ClusterFrom(ctx)
	if cluster == nil {
		warning.AddWarning(ctx, "", "cluster missing in context")
		s.delegate.UnprotectedHandler().ServeHTTP(w, r)
		return
	}

	s.lock.RLock()
	defer s.lock.RUnlock()

	knownVersionedResources := s.resourceInfos[cluster.Name]
	apiResourceList := &metav1.APIResourceList{}
	apiResourceList.APIResources = make([]metav1.APIResource, 0, len(knownVersionedResources))

	for _, res := range knownVersionedResources {
		apiResourceList.APIResources = append(apiResourceList.APIResources, res)
	}

	responsewriters.WriteObjectNegotiated(s.GenericAPIServer.Serializer, negotiation.DefaultEndpointRestrictions, schema.GroupVersion{}, w, r, http.StatusOK, apiResourceList, false)
}

func (s *Server) handleResource(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	cluster := genericapirequest.ClusterFrom(ctx)
	if cluster == nil {
		warning.AddWarning(ctx, "", "cluster missing in context")
		s.delegate.UnprotectedHandler().ServeHTTP(w, r)
		return
	}

	reqInfo, hasReqInfo := genericapirequest.RequestInfoFrom(ctx)
	if !hasReqInfo {
		warning.AddWarning(ctx, "", "request info missing in context")
		s.delegate.UnprotectedHandler().ServeHTTP(w, r)
		return
	}

	s.lock.RLock()
	defer s.lock.RUnlock()

	var vwUrl string
	if endpoints := s.grEndpointMap[cluster.Name]; endpoints != nil {
		vwUrl = endpoints[schema.GroupResource{
			Group:    reqInfo.APIGroup,
			Resource: reqInfo.Resource,
		}]
	}
	if vwUrl == "" {
		s.delegate.UnprotectedHandler().ServeHTTP(w, r)
		return
	}

	scopedURL, err := url.Parse(fmt.Sprintf("%s/clusters/%s", vwUrl, cluster.Name))
	if err != nil {
		s.delegate.UnprotectedHandler().ServeHTTP(w, r)
		return
	}

	handler := httputil.NewSingleHostReverseProxy(scopedURL)
	handler.Transport = &http.Transport{
		TLSClientConfig: s.vwTlsConfig,
	}

	handler.ServeHTTP(w, r)
}
