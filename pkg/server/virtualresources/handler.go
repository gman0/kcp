package virtualresources

import (
	"crypto/tls"
	"fmt"
	"net/http"

	"sync"

	"net/http/httputil"
	"net/url"

	// apierrors "k8s.io/apimachinery/pkg/api/errors"
	// metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	// "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	// "k8s.io/apimachinery/pkg/runtime"

	// "k8s.io/apimachinery/pkg/runtime/schema"
	// "k8s.io/apimachinery/pkg/runtime/schema"

	// reststorage "k8s.io/apiserver/pkg/registry/rest"
	"github.com/kcp-dev/logicalcluster/v3"
	"k8s.io/client-go/rest"
)

type proxyToVirtualWorkspace struct {
	tlsConfig *tls.Config

	lock     sync.Mutex
	handlers map[logicalcluster.Name]*httputil.ReverseProxy
}

func newProxyToVirtualWorkspace(config *rest.Config) (*proxyToVirtualWorkspace, error) {
	tlsConfig, err := rest.TLSConfigFor(config)
	if err != nil {
		return nil, err
	}

	return &proxyToVirtualWorkspace{
		tlsConfig: tlsConfig,
		handlers:  make(map[logicalcluster.Name]*httputil.ReverseProxy),
	}, nil
}

func (p *proxyToVirtualWorkspace) proxyFor(cluster logicalcluster.Name, vwBaseAddress string) (*httputil.ReverseProxy, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if handler := p.handlers[cluster]; handler != nil {
		return handler, nil
	}

	scopedURL, err := url.Parse(fmt.Sprintf("%s/clusters/%s", vwBaseAddress, cluster))
	if err != nil {
		return nil, err
	}

	handler := httputil.NewSingleHostReverseProxy(scopedURL)
	handler.Transport = &http.Transport{
		TLSClientConfig: p.tlsConfig,
	}

	p.handlers[cluster] = handler
	return handler, nil
}

func (p *proxyToVirtualWorkspace) removeProxyFor(cluster logicalcluster.Name) {
	p.lock.Lock()
	defer p.lock.Unlock()
	delete(p.handlers, cluster)
}
