package virtualresources

import (
	"sync"

	// apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	// apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	// discoveryclient "k8s.io/client-go/discovery"

	// kcpapiextensionsv1listers "github.com/kcp-dev/client-go/apiextensions/listers/apiextensions/v1"
	"github.com/kcp-dev/logicalcluster/v3"

	apisv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha1"
)

type virtualResourceInfo struct {
	virtualWorkspaceEndpoint string
	boundCRD                 string
	versionedVerbs           map[string]map[string][]string // Version -> Resource or Subresource -> Verbs
}

type resourceDiscovery struct {
	versionedResourceLists map[string]map[string]metav1.APIResourceList // Group -> Version -> APIResourceList
	resourceVersions       map[string]map[string]sets.Set[string]       // Group -> Resource -> Versions
}

func newResourceDiscovery() *resourceDiscovery {
	return &resourceDiscovery{
		versionedResourceLists: make(map[string]map[string]metav1.APIResourceList),
		resourceVersions:       make(map[string]map[string]sets.Set[string]),
	}
}

type resourceDiscoverySet struct {
	lock               sync.RWMutex
	discoveryByCluster map[logicalcluster.Name]*resourceDiscovery

	// wildcardEndpoints  map[string]map[schema.GroupResource]string // APIExport identity -> GR -> VR endpoint URL
}

/*func (s *resourceDiscoverySet) upsertAPI(cluster logicalcluster.Name, boundCRD *apiextensionsv1.CustomResourceDefinition, apiExportidentity, vrEndpointUrl string, vrDiscoveryClient discoveryclient.DiscoveryInterface) error {
	versionedVerbs := make(map[string]map[string][]string) // Version -> Resource or Subresource -> Verbs
	for _, version := range boundCRD.Spec.Versions {
		if !version.Served {
			continue
		}

		discoveredAPIResources, err := vrDiscoveryClient.ServerResourcesForGroupVersion(schema.GroupVersion{
			Group:   boundCRD.Spec.Group,
			Version: version.Name,
		}.String())
		if err != nil {
			return err
		}

		resourceVerbs := make(map[string][]string)
		for _, apiResource := range discoveredAPIResources.APIResources {
			resourceVerbs[apiResource.Name] = apiResource.Verbs
		}
		versionedVerbs[version.Name] = resourceVerbs
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	d, ok := s.discoveryByCluster[cluster]
	if !ok {
		d = newResourceDiscovery()
		s.discoveryByCluster[cluster] = d
	}

	resources, ok := d.groupResources[boundCRD.Spec.Group]
	if !ok {
		resources = make(map[string]*virtualResourceInfo)
		d.groupResources[boundCRD.Spec.Group] = resources
	}

	resources[boundCRD.Status.AcceptedNames.Plural] = &virtualResourceInfo{
		virtualWorkspaceEndpoint: vrEndpointUrl,
		// apiExportIdentity:        apiExportidentity,
		boundCRD:       boundCRD.Name,
		versionedVerbs: versionedVerbs,
	}

	return nil
}*/

type apiResourceListGetter func(gv schema.GroupVersion) (metav1.APIResourceList, error)

func (s *resourceDiscoverySet) upsertAPI(cluster logicalcluster.Name, sch *apisv1alpha1.APIResourceSchema, discoverResources apiResourceListGetter) error {
	// ^^ sch needs to be crd: binding may have been updated because of something else, and vr export too but it's not ready yet

	discoveredVersionedApiResourceList := make(map[string]metav1.APIResourceList)
	discoveredVersions := sets.New[string]()

	for _, version := range sch.Spec.Versions {
		if !version.Served {
			continue
		}

		discoveredVersions.Insert(version.Name)

		resourcesForVersion, err := discoverResources(schema.GroupVersion{
			Group:   sch.Spec.Group,
			Version: version.Name,
		})
		if err != nil {
			return err
		}
		namedResourcesForVersion := namedAPIResources(resourcesForVersion)

		resourceList := metav1.APIResourceList{
			GroupVersion: metav1.GroupVersion{
				Group:   sch.Spec.Group,
				Version: version.Name,
			}.String(),
		}
		if res, ok := namedResourcesForVersion[sch.Spec.Names.Plural]; ok {
			resourceList.APIResources = append(resourceList.APIResources, res)
		}
		discoveredVersionedApiResourceList[version.Name] = resourceList
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	d := s.discoveryByCluster[cluster]
	if d == nil {
		d = newResourceDiscovery()
		s.discoveryByCluster[cluster] = d
	}

	resourceVersionsForGroup := d.resourceVersions[sch.Spec.Group]
	if resourceVersionsForGroup == nil {
		resourceVersionsForGroup = make(map[string]sets.Set[string])
		d.resourceVersions[sch.Spec.Group] = resourceVersionsForGroup
	}
	knownResourceVersions := resourceVersionsForGroup[sch.Spec.Names.Plural]
	if knownResourceVersions == nil {
		knownResourceVersions = sets.New[string]()
		resourceVersionsForGroup[sch.Spec.Names.Plural] = knownResourceVersions
	}

	// Clean up resources for versions that are not served anymore.

	/*for versionForDeletion := range knownResourceVersions.Difference(discoveredVersions) {
		// don't know resource names....
	}*/

	// Store resource->versions mapping.

	resourceVersionsForGroup[sch.Spec.Names.Plural] = discoveredVersions

	// Store discovered APIResourceList.

	knownVersionedResources := d.versionedResourceLists[sch.Spec.Group]
	if knownVersionedResources == nil {
		knownVersionedResources = make(map[string]metav1.APIResourceList)
		d.versionedResourceLists[sch.Spec.Group] = knownVersionedResources
	}

	for version, resourceList := range discoveredVersionedApiResourceList {
		knownVersionedResources[version] = aggregateAPIResourceLists(knownVersionedResources[version], resourceList)
	}

	return nil
}

func namedAPIResources(list metav1.APIResourceList) map[string]metav1.APIResource {
	m := make(map[string]metav1.APIResource)
	for _, resource := range list.APIResources {
		m[resource.Name] = resource
	}
	return m
}

func apiResourceListForResource(resource string, list metav1.APIResourceList) metav1.APIResourceList {
	forResource := metav1.APIResourceList{
		GroupVersion: list.GroupVersion,
	}

	for _, res := range list.APIResources {
		if res.Name == resource {
			forResource.APIResources = append(forResource.APIResources, res)
		}
	}

	return forResource
}

func aggregateAPIResourceLists(a, b metav1.APIResourceList) metav1.APIResourceList {
	seen := sets.New[string]()
	merged := []metav1.APIResource{}

	for _, r := range a.APIResources {
		merged = append(merged, r)
		seen.Insert(r.Name)
	}

	for _, r := range b.APIResources {
		if !seen.Has(r.Name) {
			merged = append(merged, r)
			seen.Insert(r.Name)
		}
	}

	return metav1.APIResourceList{
		GroupVersion: a.GroupVersion,
		APIResources: merged,
	}
}

func removeFromAPIResourceList(resource string, apiResourceList *metav1.APIResourceList) {

}

func (s *resourceDiscoverySet) removeGroupResource(cluster logicalcluster.Name, gr schema.GroupResource) {
	s.lock.Lock()
	defer s.lock.Unlock()

	d := s.discoveryByCluster[cluster]
	if d == nil {
		return
	}

	resourceVersions := d.resourceVersions[gr.Group]
	if resourceVersions == nil {
		return
	}

	versionsForResource := resourceVersions[gr.Resource]
	if versionsForResource == nil {
		return
	}

	versionedResourceLists := d.versionedResourceLists[gr.Group]
	if versionedResourceLists == nil {
		return
	}

	/*for version := range versionsForResource {
		//versionedResourceLists[version]
	}*/

	return
}

func (s *resourceDiscoverySet) apiResourceList(cluster genericapirequest.Cluster, gv schema.GroupVersion) (metav1.APIResourceList, bool, error) {
	s.lock.RLock()
	s.lock.RUnlock()

	d := s.discoveryByCluster[cluster.Name]
	if d == nil {
		return metav1.APIResourceList{}, false, nil
	}

	/*d.groupResources[gv.Group]*/

	return metav1.APIResourceList{}, false, nil
}

func (s *resourceDiscoverySet) virtualResourceURL() (string, error) {
	return "", nil
}
