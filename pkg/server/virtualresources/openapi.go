package virtualresources

import (
	"context"
	"fmt"
	"net/http"

	"k8s.io/apimachinery/pkg/runtime/schema"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	openapiv3aggregator "k8s.io/kube-aggregator/pkg/controllers/openapiv3/aggregator"
	"k8s.io/kube-openapi/pkg/spec3"
)

func (s *Server) OpenAPIv3SpecGetter() func(ctx context.Context, gvs []schema.GroupVersion) ([]*spec3.OpenAPI, error) {
	return func(ctx context.Context, gvs []schema.GroupVersion) ([]*spec3.OpenAPI, error) {
		cluster := genericapirequest.ClusterFrom(ctx)

		withCluster := func(handler http.Handler) http.HandlerFunc {
			return func(res http.ResponseWriter, req *http.Request) {
				req = req.Clone(genericapirequest.WithCluster(ctx, *cluster))
				handler.ServeHTTP(res, req)
			}
		}

		specDownloader := openapiv3aggregator.NewDownloader()
		specDiscovery, n, err := specDownloader.OpenAPIV3Root(withCluster(proxy))
		if err != nil {
			return nil, fmt.Errorf("failed to download openapiv3 spec: %v", err)
		}

	}
}
