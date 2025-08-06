package virtualresources

import (
	"encoding/json"
	"fmt"

	"sync"

	// apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	// "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/schema"

	// "k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	genericapiserver "k8s.io/apiserver/pkg/server"

	"github.com/kcp-dev/logicalcluster/v3"
	"k8s.io/apimachinery/pkg/runtime/serializer"

	"k8s.io/client-go/discovery"

	apisv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
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

	lock       sync.RWMutex
	vwHandlers map[logicalcluster.Name]map[schema.GroupResource]*vwProxy
}

func NewServer(c CompletedConfig, delegationTarget genericapiserver.DelegationTarget) (*Server, error) {
	s := &Server{
		Extra:      c.Extra,
		vwHandlers: make(map[logicalcluster.Name]map[schema.GroupResource]*vwProxy),
	}

	// c.Generic.BuildHandlerChainFunc = s.buildHandlerChain(c, delegationTarget)
	// c.Generic.ReadyzChecks = append(c.Generic.ReadyzChecks, asHealthChecks(c.Extra.VirtualWorkspaces)...)
	// apiBindings lister synced ^

	var err error
	s.GenericAPIServer, err = c.Generic.New("virtual-resources-root-apiserver", delegationTarget)
	if err != nil {
		return nil, err
	}

	// s.GenericAPIServer.Handler.NonGoRestfulMux.HandlePrefix("/", &delegateOnly{delegate: delegationTarget.UnprotectedHandler()})
	s.GenericAPIServer.Handler.NonGoRestfulMux.Handle("/openapi/v2", &openapiv2Handler{s})

	return s, nil
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

	s.lock.Lock()
	defer s.lock.Unlock()

	proxy, err := newVWProxy(vwEndpointURL, s.Extra.VWClientConfig)
	if err != nil {
		return fmt.Errorf("failed to create vw proxy: %v", err)
	}

	if vwHandlers := s.vwHandlers[cluster]; vwHandlers != nil {
		vwHandlers[gr] = proxy
	} else {
		s.vwHandlers[cluster] = map[schema.GroupResource]*vwProxy{
			gr: proxy,
		}
	}

	for _, apiGroup := range apiGroupList.Groups {
		s.GenericAPIServer.DiscoveryGroupManager.AddGroup(apiGroup)
	}

	return nil
}

/*func (s *Server) addHandlerFor(cluster logicalcluster.Name, gr schema.GroupResource, vwEndpointURL string) error {
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

	var discoveredVersions []string
	var preferredVersion metav1.GroupVersionForDiscovery
	for _, group := range apiGroupList.Groups {
		if group.Name == gr.Group {
			for _, version := range group.Versions {
				discoveredVersions = append(discoveredVersions, version.Version)
			}
			preferredVersion = group.PreferredVersion
			break
		}
	}
	if len(discoveredVersions) == 0 {
		return fmt.Errorf("group %s not found in %s discovery", gr.Group, vwEndpointURL)
	}

	var servedInVersions []string
	for _, version := range discoveredVersions {
		groupVersion := fmt.Sprintf("%s/%s", gr.Group, version)
		apiResourceList, err := dc.ServerResourcesForGroupVersion(groupVersion)
		if err != nil {
			return fmt.Errorf("discovery client failed to list resources for group/version %s in %s: %v", groupVersion, vwEndpointURL, err)
		}

		for _, res := range apiResourceList.APIResources {
			if res.Name == gr.Resource {
				fmt.Printf("\n<><> DISCOVERED RESOURCE %#v <>\n", res)
				servedInVersions = append(servedInVersions, version)
				break
			}
		}
	}
	if len(servedInVersions) == 0 {
		return fmt.Errorf("resource %s/%s not found in %s", gr.Group, gr.Resource, vwEndpointURL)
	}

	sch := runtime.NewScheme()
	codecs := serializer.NewCodecFactory(sch)

	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(gr.Group, sch, metav1.ParameterCodec, codecs)
	apiGroupInfo.PrioritizedVersions = []schema.GroupVersion{{Group: gr.Group, Version: preferredVersion.Version}}
	for _, servedVersion := range servedInVersions {
		apiGroupInfo.VersionedResourcesStorageMap[servedVersion] = map[string]reststorage.Storage{
			gr.Resource: NewDummyStorage(gr.WithVersion(servedVersion)),
		}
	}

	return s.GenericAPIServer.InstallAPIGroup(&apiGroupInfo)
}*/

func boundCRDName(schema *apisv1alpha1.APIResourceSchema) string {
	return string(schema.UID)
}

func generateCRD(schema *apisv1alpha1.APIResourceSchema) (*apiextensionsv1.CustomResourceDefinition, error) {
	fmt.Printf("\n\n<> generating CRD for G=%s,K=%s,R=%s\n\n", schema.Spec.Group, schema.Spec.Names.Kind, schema.Spec.Names.Plural)
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: boundCRDName(schema),
			Annotations: map[string]string{
				apisv1alpha1.AnnotationBoundCRDKey:      "",
				apisv1alpha1.AnnotationSchemaClusterKey: logicalcluster.From(schema).String(),
				apisv1alpha1.AnnotationSchemaNameKey:    schema.Name,
			},
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: schema.Spec.Group,
			Names: schema.Spec.Names,
			Scope: schema.Spec.Scope,
		},
	}

	// Propagate the protected API approval annotation, `api-approved.kubernetes.io`, if any.
	// API groups that match `*.k8s.io` or `*.kubernetes.io` are owned by the Kubernetes community,
	// and protected by API review. The API server rejects the creation of a CRD whose group is
	// protected, unless the approval annotation is present.
	// See https://github.com/kubernetes/enhancements/pull/1111 for more details.
	if value, found := schema.Annotations[apiextensionsv1.KubeAPIApprovedAnnotation]; found {
		crd.Annotations[apiextensionsv1.KubeAPIApprovedAnnotation] = value
	}

	switch schema.Spec.NameValidation {
	case "PathSegmentName":
		crd.Annotations[apiextapiserver.KcpValidateNameAnnotationKey] = "path-segment"
	}

	for _, version := range schema.Spec.Versions {
		crdVersion := apiextensionsv1.CustomResourceDefinitionVersion{
			Name:                     version.Name,
			Served:                   version.Served,
			Storage:                  version.Storage,
			Deprecated:               version.Deprecated,
			DeprecationWarning:       version.DeprecationWarning,
			Subresources:             &version.Subresources,
			AdditionalPrinterColumns: version.AdditionalPrinterColumns,
		}

		var validation apiextensionsv1.CustomResourceValidation
		if err := json.Unmarshal(version.Schema.Raw, &validation.OpenAPIV3Schema); err != nil {
			return nil, err
		}
		crdVersion.Schema = &validation

		crd.Spec.Versions = append(crd.Spec.Versions, crdVersion)
	}

	if len(schema.Spec.Versions) > 1 && schema.Spec.Conversion == nil {
		return nil, fmt.Errorf("multiple versions specified but no conversion strategy")
	}

	if len(schema.Spec.Versions) > 1 {
		conversion := &apiextensionsv1.CustomResourceConversion{
			Strategy: apiextensionsv1.ConversionStrategyType(schema.Spec.Conversion.Strategy),
		}

		if schema.Spec.Conversion.Strategy == "Webhook" {
			conversion.Webhook = &apiextensionsv1.WebhookConversion{
				ConversionReviewVersions: schema.Spec.Conversion.Webhook.ConversionReviewVersions,
				ClientConfig: &apiextensionsv1.WebhookClientConfig{
					URL:      &(schema.Spec.Conversion.Webhook.ClientConfig.URL),
					CABundle: schema.Spec.Conversion.Webhook.ClientConfig.CABundle,
				},
			}
		}

		crd.Spec.Conversion = conversion
	}

	return crd, nil
}
