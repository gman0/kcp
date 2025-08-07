package virtualresources

import (
	"fmt"
	"net/http"
	"strings"

	"net/http/httputil"
	"net/url"

	// apierrors "k8s.io/apimachinery/pkg/api/errors"
	// metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	// "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	// "k8s.io/apimachinery/pkg/runtime"

	// "k8s.io/apimachinery/pkg/runtime/schema"
	// "k8s.io/apimachinery/pkg/runtime/schema"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	// reststorage "k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/client-go/rest"

	// "k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/warning"
	// "k8s.io/client-go/discovery"
	componentbaseversion "k8s.io/component-base/version"

	// "github.com/kcp-dev/logicalcluster/v3"
	// "k8s.io/apimachinery/pkg/runtime/serializer"

	virtualcontext "github.com/kcp-dev/kcp/pkg/virtual/framework/context"
	// "k8s.io/kube-aggregator/pkg/controllers/openapi/aggregator"
	// "k8s.io/kube-openapi/pkg/validation/spec"
	// apisv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha1"
	// apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	// apiextapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
)

func splitPath(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return []string{}
	}
	return strings.Split(path, "/")
}

func (s *Server) buildHandlerChain(c CompletedConfig, delegateAPIServer genericapiserver.DelegationTarget) func(http.Handler, *genericapiserver.Config) http.Handler {
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

			pathParts := splitPath(req.URL.Path)
			if len(pathParts) != 3 || pathParts[0] != "apis" {
				delegateAfterDefaultHandlerChain.ServeHTTP(w, req)
				fmt.Printf("<<VIRTUALRESOURCESAPISERVER>> pathParts=%v not fit, delegating", pathParts)
				return
			}

			cluster := genericapirequest.ClusterFrom(requestContext)

			if req.URL.Path == "/openapi/v2" {
				fmt.Printf("<<VIRTUALRESOURCESAPISERVER>> /openapi/v2")
				func() {
					s.lock.Lock()
					defer s.lock.Unlock()
				}()
			}

			fmt.Printf("\n<XXX> virtualresources buildHandlerChain cluster=%#v <> \n", cluster)

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
					req.URL = newURLreq)
					req = req.WithContext(virtualcontext.WithVirtualWorkspaceName(completedContext, vw.Name))
					break
				}
			}*/
			delegateAfterDefaultHandlerChain.ServeHTTP(w, req)
		})
	}
}

type vwProxy struct {
	*httputil.ReverseProxy
}

func newVWProxy(vwURL string, cfg *rest.Config) (*vwProxy, error) {
	tlsConfig, err := rest.TLSConfigFor(cfg)
	if err != nil {
		return nil, err
	}

	vwAddr, err := url.Parse(vwURL)
	if err != nil {
		return nil, err
	}
	proxy := httputil.NewSingleHostReverseProxy(vwAddr)
	proxy.Transport = &http.Transport{
		TLSClientConfig: tlsConfig,
	}

	return &vwProxy{
		ReverseProxy: proxy,
	}, nil
}

func (r *vwProxy) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	fmt.Printf("\n\n<<VWPROXY>> path=%s start\n", req.URL.Path)
	r.ReverseProxy.ServeHTTP(w, req)
	fmt.Printf("\n\n<<VWPROXY>> path=%s finish\n", req.URL.Path)
}

type openapiHandler struct {
	s *Server
}

func (r *openapiHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	cluster := genericapirequest.ClusterFrom(req.Context())
	fmt.Printf("\n\n<<OPENAPIV2HANDLER>> START with cluster in context %v\n\n", cluster)

	r.s.lock.Lock()
	defer r.s.lock.Unlock()

	for gr, proxy := range r.s.vwHandlers[cluster.Name] {
		fmt.Printf("\n\n<<OPENAPIV2HANDLER>> gr=%v \n\n", gr)
		proxy.ServeHTTP(w, req)
		break
	}

	fmt.Printf("\n\n<<OPENAPIV2HANDLER>> FINISH with cluster in context %v\n\n", cluster)
}

type versionDiscoveryHandler struct{}

func (r *versionDiscoveryHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {

}

type groupDiscoveryHandler struct{}

func (r *groupDiscoveryHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {

}

type delegateOnly struct {
	delegate http.Handler
}

func (r *delegateOnly) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	fmt.Printf("\n\n<<DELEGATEONLY>> path=%s\n\n", req.URL.Path)
	r.delegate.ServeHTTP(w, req)
}
