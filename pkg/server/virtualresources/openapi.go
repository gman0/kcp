package virtualresources

import (
	"context"
	"fmt"
	"net/http"

	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	openapiv3aggregator "k8s.io/kube-aggregator/pkg/controllers/openapiv3/aggregator"
	"k8s.io/kube-openapi/pkg/spec3"
)

func (s *Server) OpenAPIv3SpecGetter() func(ctx context.Context) ([]*spec3.OpenAPI, error) {
	return func(ctx context.Context) ([]*spec3.OpenAPI, error) {
		cluster := genericapirequest.ClusterFrom(ctx)

		withCluster := func(handler http.Handler) http.HandlerFunc {
			return func(res http.ResponseWriter, req *http.Request) {
				req = req.Clone(genericapirequest.WithCluster(ctx, *cluster))
				handler.ServeHTTP(res, req)
			}
		}

		specDownloader := openapiv3aggregator.NewDownloader()

		specDiscovery, retCode, err := specDownloader.OpenAPIV3Root(withCluster(nil /* proxy */))
		if err != nil {
			return nil, fmt.Errorf("failed to download openapiv3 spec: %v", err)
		}

		fmt.Printf("<<OpenAPIv3SpecGetter>> retCode=%d discovery=%#v\n", retCode, specDiscovery)

		return nil, nil
	}
}
