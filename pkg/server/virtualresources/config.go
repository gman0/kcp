package virtualresources

import (
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/rest"
	utilversion "k8s.io/component-base/version"
)

type Config struct {
	Generic *genericapiserver.RecommendedConfig
	Extra   ExtraConfig
}

type ExtraConfig struct {
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
		c.Generic.Complete(),
		&c.Extra,
	}

	return CompletedConfig{&cfg}
}

func (c *completedConfig) WithOpenAPIAggregationController(delegatedAPIServer *genericapiserver.GenericAPIServer) error {
	return nil
}

func NewConfig(recommendedConfig *genericapiserver.RecommendedConfig) (*Config, error) {
	// Loopback is not wired for now, since virtual workspaces are expected to delegate to
	// some APIServer.
	// The RootAPIServer is just a proxy to the various virtual workspaces.
	// We might consider a giving a special meaning to a global loopback config, in the future
	// but that's not the case for now.
	recommendedConfig.Config.LoopbackClientConfig = &rest.Config{
		Host: "loopback-config-not-wired-for-now",
	}
	recommendedConfig.EffectiveVersion = utilversion.DefaultKubeEffectiveVersion()

	ret := &Config{
		Generic: recommendedConfig,
		Extra:   ExtraConfig{},
	}

	return ret, nil
}
