package virtualresources

import (
	apiextensionsapiserverkcp "k8s.io/apiextensions-apiserver/pkg/kcp"
	"k8s.io/apimachinery/pkg/runtime"
	apiopenapi "k8s.io/apiserver/pkg/endpoints/openapi"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/rest"
	openapicommon "k8s.io/kube-openapi/pkg/common"

	kcpapiextensionsv1informers "github.com/kcp-dev/client-go/apiextensions/informers/apiextensions/v1"
	kcpdynamic "github.com/kcp-dev/client-go/dynamic"

	apisv1alpha2informers "github.com/kcp-dev/kcp/sdk/client/informers/externalversions/apis/v1alpha2"
	corev1alpha1informers "github.com/kcp-dev/kcp/sdk/client/informers/externalversions/core/v1alpha1"
	apisv1alpha2listers "github.com/kcp-dev/kcp/sdk/client/listers/apis/v1alpha2"
)

type Config struct {
	Generic *genericapiserver.Config
	Extra   ExtraConfig
}

type ExtraConfig struct {
	VWClientConfig       *rest.Config
	DynamicClusterClient kcpdynamic.ClusterInterface

	ShartVirtualWorkspaceURLGetter func() string

	CRDLister                  kcpapiextensionsv1informers.CustomResourceDefinitionClusterInformer
	APIBindingAwareCRDLister   apiextensionsapiserverkcp.ClusterAwareCRDClusterLister
	APIBindingInformer         apisv1alpha2informers.APIBindingClusterInformer
	APIBindingLister           apisv1alpha2listers.APIBindingClusterLister
	LocalAPIExportInformer     apisv1alpha2informers.APIExportClusterInformer
	GlobalAPIExportInformer    apisv1alpha2informers.APIExportClusterInformer
	GlobalShardClusterInformer corev1alpha1informers.ShardClusterInformer
}

type completedConfig struct {
	Generic genericapiserver.CompletedConfig
	Extra   *ExtraConfig
}

type CompletedConfig struct {
	// Embed a private pointer that cannot be instantiated outside of this package.
	*completedConfig
}

// Complete fills in any fields not set that are required to have valid data. It's mutating the receiver.
func (c *Config) Complete() CompletedConfig {
	if c == nil {
		return CompletedConfig{}
	}

	cfg := completedConfig{
		c.Generic.Complete(nil),
		&c.Extra,
	}

	cfg.Generic.OpenAPIV3Config = genericapiserver.DefaultOpenAPIV3Config(
		func(rc openapicommon.ReferenceCallback) map[string]openapicommon.OpenAPIDefinition {
			return map[string]openapicommon.OpenAPIDefinition{}
		},
		apiopenapi.NewDefinitionNamer(runtime.NewScheme()),
	)

	return CompletedConfig{&cfg}
}

func (c *completedConfig) WithOpenAPIAggregationController(delegatedAPIServer *genericapiserver.GenericAPIServer) error {
	return nil
}

func NewConfig(cfg *genericapiserver.Config, vwClientConfig *rest.Config,
	dynamicClusterClient kcpdynamic.ClusterInterface,
	shartVirtualWorkspaceURLGetter func() string,
	crdLister kcpapiextensionsv1informers.CustomResourceDefinitionClusterInformer,
	apiBindingAwareCRDLister apiextensionsapiserverkcp.ClusterAwareCRDClusterLister,
	apiBindingInformer apisv1alpha2informers.APIBindingClusterInformer,
	localAPIExportInformer apisv1alpha2informers.APIExportClusterInformer,
	globalAPIExportInformer apisv1alpha2informers.APIExportClusterInformer,
	globalShardClusterInformer corev1alpha1informers.ShardClusterInformer,
) (*Config, error) {
	rest.AddUserAgent(vwClientConfig, "kcp-virtual-resources-apiserver")
	cfg.SkipOpenAPIInstallation = true

	ret := &Config{
		Generic: cfg,
		Extra: ExtraConfig{
			VWClientConfig:       vwClientConfig,
			DynamicClusterClient: dynamicClusterClient,

			ShartVirtualWorkspaceURLGetter: shartVirtualWorkspaceURLGetter,

			CRDLister:                  crdLister,
			APIBindingAwareCRDLister:   apiBindingAwareCRDLister,
			APIBindingInformer:         apiBindingInformer,
			LocalAPIExportInformer:     localAPIExportInformer,
			GlobalAPIExportInformer:    globalAPIExportInformer,
			GlobalShardClusterInformer: globalShardClusterInformer,
		},
	}

	return ret, nil
}
