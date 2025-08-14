package virtualresources

import (
	"context"
	"fmt"

	"github.com/kcp-dev/logicalcluster/v3"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/discovery"
	clientopenapi "k8s.io/client-go/openapi"
	clientopenapi3 "k8s.io/client-go/openapi3"
	"k8s.io/client-go/rest"
	"k8s.io/kube-openapi/pkg/cached"
	"k8s.io/kube-openapi/pkg/spec3"
)

func (s *Server) OpenAPIv3SpecGetter() func(ctx context.Context) (map[string]cached.Value[*spec3.OpenAPI], error) {
	return func(ctx context.Context) (map[string]cached.Value[*spec3.OpenAPI], error) {
		cluster, err := genericapirequest.ClusterNameFrom(ctx)
		if err != nil {
			return nil, err
		}

		endpointsForGroupResource := s.getEndpointsForCluster(cluster)

		fmt.Printf("<<OpenAPIv3SpecGetter>> 1 grEndpoints=%#v\n", endpointsForGroupResource)

		type trackedResources struct {
			resourcesForGV       map[schema.GroupVersion]sets.Set[string]
			resourcesForEndpoint map[string]map[schema.GroupResource]map[string]struct{}
		}

		tracked := func() trackedResources {
			s.lock.RLock()
			defer s.lock.RUnlock()

			resourcesForGroupVersion := s.resourcesForGroupVersion[cluster]
			if len(resourcesForGroupVersion) == 0 {
				return trackedResources{}
			}

			return trackedResources{}
		}()

		specs := make(map[string]cached.Value[*spec3.OpenAPI])

		for vwURL, resourcesForGroupVersions := range tracked.resourcesForEndpoint {
			vwOpenAPIv3Client, err := newOpenAPIv3Client(s.Extra.VWClientConfig, vwURL, cluster)
			if err != nil {
				return nil, fmt.Errorf("failed to create discovery client for virtual workspace %s: %v", vwURL, err)
			}

			for groupVersion, resources := range resourcesForGroupVersions {

			}
		}

		endpoints := make(map[string]struct{})
		groupPaths := make(map[string]struct{})
		for gr, endpoint := range endpointsForGroupResource {
			endpoints[endpoint] = struct{}{}
			groupPaths[fmt.Sprintf("apis/%s", gr.Group)] = struct{}{}
		}

		fmt.Printf("<<OpenAPIv3SpecGetter>> 2 endpoints=%#v\n", endpoints)

		for vwURL := range endpoints {

			clientopenapi3.NewRoot(vwOpenAPIv3Client)

			openApiv3Root := clientopenapi3.NewRoot(vwOpenAPIv3Client)
			gv := schema.GroupVersion{Group: "wildwest.dev", Version: "v1alpha1"}
			spec, err := openApiv3Root.GVSpec(gv)

			if err != nil {
				return nil, fmt.Errorf("failed to get %s OpenAPIv3 spec for virtual workspace %s: %v", gv, vwURL, err)
			}
			fmt.Printf("\n\n\n<<OpenAPIv3SpecGetter>> szpek %#v <>\n\n", spec)
		}

		return nil, nil
	}
}

func newOpenAPIv3Client(config *rest.Config, vwURL string, cluster logicalcluster.Name) (clientopenapi.Client, error) {
	vwConfig := *config
	vwConfig.Host = urlWithCluster(vwURL, cluster)

	vwDiscoveryClient, err := discovery.NewDiscoveryClientForConfig(&vwConfig)
	if err != nil {
		return nil, err
	}

	return vwDiscoveryClient.OpenAPIV3(), nil
}
