package virtualresources

import (
	"fmt"
	"net/http"

	// apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	// "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	// "k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/warning"
	"k8s.io/client-go/discovery"
	componentbaseversion "k8s.io/component-base/version"

	"github.com/kcp-dev/logicalcluster/v3"

	virtualcontext "github.com/kcp-dev/kcp/pkg/virtual/framework/context"
	wildwestv1alpha1 "github.com/kcp-dev/kcp/test/e2e/fixtures/wildwest/apis/wildwest/v1alpha1"
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
}

type dummyStorage struct{}

func (_ *dummyStorage) New() runtime.Object {
	fmt.Printf("\n<XXX> dummyStorage.New()\n")
	sch := runtime.NewScheme()
	wildwestv1alpha1.AddToScheme(sch)

	obj, err := sch.New(wildwestv1alpha1.SchemeGroupVersion.WithKind("Cowboy"))
	if err != nil {
		fmt.Printf("\n<XXX> dummyStorage.New() failed to create obj err=%v\n", err)
		return nil
	}

	return obj
}

func (_ *dummyStorage) Destroy() {
	fmt.Printf("\n<XXX> dummyStorage.Destroy()\n")
}

func NewServer(c CompletedConfig, delegationTarget genericapiserver.DelegationTarget) (*Server, error) {
	c.Generic.BuildHandlerChainFunc = buildHandlerChain(c, delegationTarget)
	// c.Generic.ReadyzChecks = append(c.Generic.ReadyzChecks, asHealthChecks(c.Extra.VirtualWorkspaces)...)
	// apiBindings lister synced ^

	genericServer, err := c.Generic.New("virtual-resources-root-apiserver", delegationTarget)
	if err != nil {
		return nil, err
	}

	// testing

	sch := runtime.NewScheme()
	wildwestv1alpha1.AddToScheme(sch)
	codecs := serializer.NewCodecFactory(sch)
	cowboy := genericapiserver.NewDefaultAPIGroupInfo(
		"wildwest.dev",
		sch,
		metav1.ParameterCodec,
		codecs,
	)
	cowboy.VersionedResourcesStorageMap["v1alpha1"] = map[string]rest.Storage{
		"cowboys": &dummyStorage{},
	}
	fmt.Printf("\n<XXX> cowboys APIGroupInfo=%#v <>\n", cowboy)
	genericServer.InstallAPIGroup(&cowboy)
	groups, _ := genericServer.DiscoveryGroupManager.Groups(nil, &http.Request{})
	fmt.Printf("\n<XXX> cowboys genericServer.DiscoveryGroupManager.Groups=%#v <>\n", groups)

	return &Server{
		GenericAPIServer: genericServer,
		Extra:            c.Extra,
	}, nil
}

func (s *Server) addHandlerFor(cluster logicalcluster.Name, gr schema.GroupResource, vwEndpointURL string) error {
	config := *s.Extra.VWClientConfig
	config.Host = vwEndpointURL + fmt.Sprintf("/clusters/%s", cluster.String())

	dc, err := discovery.NewDiscoveryClientForConfig(&config)
	if err != nil {
		return fmt.Errorf("failed to create discovery client for gr=%q, endpoint=%q: %v", gr, config.Host, err)
	}

	apiGroupList, err := dc.ServerGroups()
	if err != nil {
		return fmt.Errorf("discovery client failed to list api groups for endpoint=%q: %v", config.Host, err)
	}

	fmt.Printf("\n<><> DISCOVERED APIS %#v <>\n", apiGroupList)

	return nil
}

func buildHandlerChain(c CompletedConfig, delegateAPIServer genericapiserver.DelegationTarget) func(http.Handler, *genericapiserver.Config) http.Handler {
	return func(apiHandler http.Handler, genericConfig *genericapiserver.Config) http.Handler {
		delegateAfterDefaultHandlerChain := genericapiserver.DefaultBuildHandlerChain(
			http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if _, virtualWorkspaceNameExists := virtualcontext.VirtualWorkspaceNameFrom(req.Context()); virtualWorkspaceNameExists {
					delegatedHandler := delegateAPIServer.UnprotectedHandler()
					if delegatedHandler != nil {
						delegatedHandler.ServeHTTP(w, req)
					}
					return
				}
				apiHandler.ServeHTTP(w, req)
			}), c.Generic.Config)

		fmt.Printf("\n<XXX> virtualresources buildHandlerChain entrypoint <> \n")

		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			requestContext := req.Context()

			fmt.Printf("\n<XXX> virtualresources buildHandlerChain cluster=%#v <> \n", genericapirequest.ClusterFrom(requestContext))

			// detect old kubectl plugins and inject warning headers
			if req.UserAgent() == "Go-http-client/2.0" {
				// TODO(sttts): in the future compare the plugin version to the server version and warn outside of skew compatibility guarantees.
				warning.AddWarning(requestContext, "",
					fmt.Sprintf("You are using an old kubectl-kcp plugin. Please update to a version matching the kcp server version %q.", componentbaseversion.Get().GitVersion))
			}

			/*for _, vw := range c.Extra.VirtualWorkspaces {
				if accepted, prefixToStrip, completedContext := vw.ResolveRootPath(req.URL.Path, requestContext); accepted {
					req.URL.Path = strings.TrimPrefix(req.URL.Path, prefixToStrip)
					newURL, err := url.Parse(req.URL.String())
					if err != nil {
						responsewriters.ErrorNegotiated(
							apierrors.NewInternalError(fmt.Errorf("unable to resolve %s, err %w", req.URL.Path, err)),
							errorCodecs, schema.GroupVersion{},
							w, req)
						return
					}
					req.URL = newURL
					req = req.WithContext(virtualcontext.WithVirtualWorkspaceName(completedContext, vw.Name))
					break
				}
			}*/
			delegateAfterDefaultHandlerChain.ServeHTTP(w, req)
		})
	}
}
