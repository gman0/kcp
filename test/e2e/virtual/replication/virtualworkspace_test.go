/*
Copyright 2025 The KCP Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package replication

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	// "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	// "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	// "k8s.io/apimachinery/pkg/watch"
	// "k8s.io/client-go/discovery/cached/memory"
	kubernetesclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	// "k8s.io/client-go/restmapper"
	// "k8s.io/kube-openapi/pkg/util/sets"
	"k8s.io/utils/ptr"

	kcpapiextensionsclientset "github.com/kcp-dev/client-go/apiextensions/client"
	// kcpdiscovery "github.com/kcp-dev/client-go/discovery"
	kcpdynamic "github.com/kcp-dev/client-go/dynamic"
	kcpkubernetesclientset "github.com/kcp-dev/client-go/kubernetes"
	"github.com/kcp-dev/logicalcluster/v3"

	// "github.com/kcp-dev/kcp/config/helpers"
	apisv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha1"
	apisv1alpha2 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha2"
	cachev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/cache/v1alpha1"
	"github.com/kcp-dev/kcp/sdk/apis/core"
	"github.com/kcp-dev/kcp/sdk/apis/third_party/conditions/util/conditions"
	kcpclientset "github.com/kcp-dev/kcp/sdk/client/clientset/versioned/cluster"
	kcptesting "github.com/kcp-dev/kcp/sdk/testing"
	kcptestinghelpers "github.com/kcp-dev/kcp/sdk/testing/helpers"
	"github.com/kcp-dev/kcp/test/e2e/fixtures/wildwest"
	wildwestv1alpha1 "github.com/kcp-dev/kcp/test/e2e/fixtures/wildwest/apis/wildwest/v1alpha1"
	wildwestclientset "github.com/kcp-dev/kcp/test/e2e/fixtures/wildwest/client/clientset/versioned/cluster"
	"github.com/kcp-dev/kcp/test/e2e/framework"
)

func TestCachedResourceVirtualWorkspace(t *testing.T) {
	t.Parallel()
	framework.Suite(t, "control-plane")

	server := kcptesting.SharedKcpServer(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cfg := server.BaseConfig(t)

	kcpClusterClient, err := kcpclientset.NewForConfig(cfg)
	require.NoError(t, err, "failed to construct kcp cluster client for server")

	kcpDynClusterClient, err := kcpdynamic.NewForConfig(cfg)
	require.NoError(t, err, "failed to construct kcp dynamic cluster client for server")

	kubeClusterClient, err := kcpkubernetesclientset.NewForConfig(cfg)
	require.NoError(t, err, "failed to construct kube cluster client for server")

	kcpApiExtensionClusterClient, err := kcpapiextensionsclientset.NewForConfig(cfg)
	require.NoError(t, err)

	kcpCRDClusterClient := kcpApiExtensionClusterClient.ApiextensionsV1().CustomResourceDefinitions()

	orgPath, _ := kcptesting.NewWorkspaceFixture(t, server, core.RootCluster.Path(), kcptesting.WithType(core.RootCluster.Path(), "organization"))
	serviceProviderPath, serviceProviderWS := kcptesting.NewWorkspaceFixture(t, server, orgPath)
	consumerPath, consumerWorkspace := kcptesting.NewWorkspaceFixture(t, server, orgPath)
	consumerClusterName := logicalcluster.Name(consumerWorkspace.Spec.Cluster)

	framework.AdmitWorkspaceAccess(ctx, t, kubeClusterClient, serviceProviderPath, []string{"user-1"}, nil, false)
	framework.AdmitWorkspaceAccess(ctx, t, kubeClusterClient, consumerPath, []string{"user-1"}, nil, false)

	//
	// Prepare the provider cluster.
	//

	// Prepare wildwest.dev resource "sheriffs":
	// * Create CRD and generate its associated APIResourceSchema to be used with an APIExport later.
	// * Create a CachedResource for that resource and wait until it's ready.

	gvr := wildwestv1alpha1.SchemeGroupVersion.WithResource("sheriffs")

	crd := wildwest.CRD(t, metav1.GroupResource(gvr.GroupResource()))
	sch, err := apisv1alpha1.CRDToAPIResourceSchema(crd, "today")
	require.NoError(t, err)
	t.Logf("Creating Sheriff CRD in %q", serviceProviderPath)
	wildwest.Create(t, serviceProviderPath, kcpCRDClusterClient, metav1.GroupResource(gvr.GroupResource()))
	t.Logf("Creating a Sheriff in %q", serviceProviderPath)
	sheriffOne, err := createSheriff(ctx, kcpDynClusterClient, logicalcluster.Name(serviceProviderWS.Spec.Cluster), &wildwestv1alpha1.Sheriff{
		ObjectMeta: metav1.ObjectMeta{
			Name: "sheriff-1",
		},
	})
	require.NoError(t, err)

	t.Logf("Creating %s APIResourceSchema in %q", sch.Name, serviceProviderPath)
	_, err = kcpClusterClient.Cluster(serviceProviderPath).ApisV1alpha1().APIResourceSchemas().Create(ctx, sch, metav1.CreateOptions{})
	require.NoError(t, err)

	// Create a CachedResource for that CR.
	t.Logf("Creating CachedResource for %s in %q", gvr, serviceProviderPath)
	cachedResource, err := kcpClusterClient.Cluster(serviceProviderPath).CacheV1alpha1().CachedResources().Create(ctx, &cachev1alpha1.CachedResource{
		ObjectMeta: metav1.ObjectMeta{
			Name: gvr.GroupResource().String(),
		},
		Spec: cachev1alpha1.CachedResourceSpec{
			GroupVersionResource: cachev1alpha1.GroupVersionResource(gvr),
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	cachedResourceName := cachedResource.Name
	kcptestinghelpers.EventuallyCondition(t, func() (conditions.Getter, error) {
		cachedResource, err = kcpClusterClient.Cluster(serviceProviderPath).CacheV1alpha1().CachedResources().Get(ctx, cachedResourceName, metav1.GetOptions{})
		return cachedResource, err
	}, kcptestinghelpers.Is(cachev1alpha1.ReplicationStarted), fmt.Sprintf("CachedResource %s should become ready", cachedResourceName))

	// Create an APIExport for the created CachedResource.
	t.Logf("Creating APIExport for Sheriff CachedResource in %q", serviceProviderPath)
	apiExport, err := kcpClusterClient.Cluster(serviceProviderPath).ApisV1alpha2().APIExports().Create(ctx, &apisv1alpha2.APIExport{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cached-wildwest-provider",
		},
		Spec: apisv1alpha2.APIExportSpec{
			Resources: []apisv1alpha2.ResourceSchema{
				{
					Group:  "wildwest.dev",
					Name:   "sheriffs",
					Schema: "today.sheriffs.wildwest.dev",
					Storage: apisv1alpha2.ResourceSchemaStorage{
						Virtual: &apisv1alpha2.ResourceSchemaStorageVirtual{
							Reference: corev1.TypedLocalObjectReference{
								APIGroup: ptr.To(cachev1alpha1.SchemeGroupVersion.Group),
								Kind:     "CachedResourceEndpointSlice",
								Name:     "sheriffs.wildwest.dev",
							},
							IdentityHash: cachedResource.Status.IdentityHash,
						},
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	// Wait for export's identity to be available. We'll need it later.
	apiExportName := apiExport.Name
	kcptestinghelpers.EventuallyCondition(t, func() (conditions.Getter, error) {
		apiExport, err = kcpClusterClient.Cluster(serviceProviderPath).ApisV1alpha2().APIExports().Get(ctx, apiExportName, metav1.GetOptions{})
		return apiExport, err
	}, kcptestinghelpers.Is(apisv1alpha2.APIExportIdentityValid), fmt.Sprintf("APIExport %s should have its identity ready", apiExportName))

	//
	// Prepare the consumer cluster.
	//

	// Bind that APIExport in consumer workspaces.
	t.Logf("Binding %s|%s in %s", serviceProviderPath, apiExport.Name, consumerPath)
	kcptestinghelpers.Eventually(t, func() (bool, string) {
		_, err = kcpClusterClient.Cluster(consumerPath).ApisV1alpha2().APIBindings().Create(ctx, &apisv1alpha2.APIBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: "cached-wildwest",
			},
			Spec: apisv1alpha2.APIBindingSpec{
				Reference: apisv1alpha2.BindingReference{
					Export: &apisv1alpha2.ExportBindingReference{
						Path: serviceProviderPath.String(),
						Name: apiExport.Name,
					},
				},
			},
		}, metav1.CreateOptions{})
		if err != nil {
			return false, fmt.Sprintf("failed to create APIBinding: %v", err)
		}
		return true, ""
	}, wait.ForeverTestTimeout, time.Second*1, "waiting to create apibinding")
	// Wait until the APIs are available.
	t.Logf("Waiting for %s API to appear in %q", gvr, consumerPath)
	kcptestinghelpers.Eventually(t, func() (bool, string) {
		groupList, err := kcpClusterClient.Cluster(consumerPath).Discovery().ServerGroups()
		if err != nil {
			return false, fmt.Sprintf("failed to retrieve APIResourceList from discovery: %v", err)
		}
		return slices.ContainsFunc(groupList.Groups, func(e metav1.APIGroup) bool {
			return e.Name == wildwestv1alpha1.SchemeGroupVersion.Group
		}), fmt.Sprintf("wildwest.dev group not found in %q", consumerPath)
	}, wait.ForeverTestTimeout, time.Second*1, "waiting for wildwest.dev group in %q", consumerPath)
	kcptestinghelpers.Eventually(t, func() (bool, string) {
		resourceList, err := kcpClusterClient.Cluster(consumerPath).Discovery().ServerResourcesForGroupVersion("wildwest.dev/v1alpha1")
		if err != nil {
			return false, fmt.Sprintf("failed to retrieve APIResourceList from discovery: %v", err)
		}
		return slices.ContainsFunc(resourceList.APIResources, func(e metav1.APIResource) bool {
			return e.Name == gvr.Resource
		}), fmt.Sprintf("%s API not found in %q", gvr, consumerPath)
	}, wait.ForeverTestTimeout, time.Second*1, "waiting for wildwest.dev group in %q", consumerPath)

	//
	// Verify.
	//

	// At this point we should have a CachedResourceEndpointSlice ready with one consumer.
	cres, err := kcpClusterClient.Cluster(serviceProviderPath).CacheV1alpha1().CachedResourceEndpointSlices().Get(ctx, cachedResourceName, metav1.GetOptions{})
	require.NoError(t, err)
	cachedResourceVWCfg := rest.CopyConfig(cfg)
	var found bool
	cachedResourceVWCfg.Host, found, err = framework.VirtualWorkspaceURL(ctx, kcpClusterClient, consumerWorkspace,
		framework.ReplicationVirtualWorkspaceURLs(cres))
	require.NoError(t, err)
	require.True(t, found, "expected to have found a suitable VW url for in %v endpoint slice", cres.Status.CachedResourceEndpoints)
	cachedResourceVWCfg.Host += ":" + apiExport.Status.IdentityHash // Required to by virtual resources / Replication VW.

	// Construct the VW client config.
	user1CachedResourceVWCfg := framework.StaticTokenUserConfig("user-1", cachedResourceVWCfg)
	user1CachedResourceDynClient, err := kcpdynamic.NewForConfig(user1CachedResourceVWCfg)
	require.NoError(t, err)

	t.Logf("=== get ===")
	{
		t.Logf("Verify that user-1 cannot GET")
		_, err = getSheriff(ctx, user1CachedResourceDynClient, consumerClusterName, sheriffOne.Name)
		require.True(t, apierrors.IsForbidden(err))

		t.Logf("Give user-1 GET access to the virtual workspace and apiexport content")
		/*admit(t, kubeClusterClient.Cluster(consumerPath), "user-1-vw-get", "user-1", "User",
		[]string{"get"}, wildwestv1alpha1.SchemeGroupVersion.Group, "cowboys", "")*/
		admit(t, kubeClusterClient.Cluster(serviceProviderPath), "user-1-apiexport-content-get", "user-1", "User",
			[]string{"get"}, apisv1alpha2.SchemeGroupVersion.Group, "apiexports/content", "")

		t.Logf("Verify that user-1 can now get sheriff")
		kcptestinghelpers.Eventually(t, func() (bool, string) {
			var err error
			_, err = getSheriff(ctx, user1CachedResourceDynClient, consumerClusterName, sheriffOne.Name)
			if apierrors.IsForbidden(err) {
				return false, fmt.Sprintf("waiting until rbac cache is primed: %v", err)
			}
			if apierrors.IsNotFound(err) {
				return false, fmt.Sprintf("waiting until the sheriff is in cache: %v", err)
			}
			require.NoError(t, err)
			return true, ""
		}, wait.ForeverTestTimeout, time.Millisecond*100, "expected user-1 to list cowboys")
	}

	sheriffLabels := map[string]string{"hello": "world"}
	t.Logf("Creating another Sheriff in %q", serviceProviderPath)
	sheriffTwo, err := createSheriff(ctx, kcpDynClusterClient, logicalcluster.Name(serviceProviderWS.Spec.Cluster), &wildwestv1alpha1.Sheriff{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "sheriff-2",
			Labels: sheriffLabels,
		},
	})
	require.NoError(t, err)

	t.Logf("=== list ===")
	{
		t.Logf("Verify that user-1 cannot LIST")
		_, err = listSheriffs(ctx, user1CachedResourceDynClient, consumerClusterName)
		require.True(t, apierrors.IsForbidden(err))

		t.Logf("Give user-1 LIST access to the virtual workspace")
		/*admit(t, kubeClusterClient.Cluster(consumerPath), "user-1-vw-list", "user-1", "User",
		[]string{"list"}, wildwestv1alpha1.SchemeGroupVersion.Group, "cowboys", "")*/
		admit(t, kubeClusterClient.Cluster(serviceProviderPath), "user-1-apiexport-content-list", "user-1", "User",
			[]string{"list"}, apisv1alpha2.SchemeGroupVersion.Group, "apiexports/content", "")

		t.Logf("Verify that user-1 can now LIST sheriffs")
		var sherrifList *wildwestv1alpha1.SheriffList
		kcptestinghelpers.Eventually(t, func() (bool, string) {
			var err error
			sherrifList, err = listSheriffs(ctx, user1CachedResourceDynClient, consumerClusterName)
			if apierrors.IsForbidden(err) {
				return false, fmt.Sprintf("waiting until rbac cache is primed: %v", err)
			}
			if len(sherrifList.Items) < 2 {
				return false, fmt.Sprintf("waiting until there are two items in list, have %d", len(sherrifList.Items))
			}
			require.NoError(t, err)
			return true, ""
		}, wait.ForeverTestTimeout, time.Millisecond*100, "expected user-1 to list sheriffs")
		require.Len(t, sherrifList.Items, 2, "expected to find exactly two sheriffs")

		t.Logf("Verify that both of the created sheriffs are listed")
		sheriffNames := sets.NewString()
		for i := range sherrifList.Items {
			sheriffNames.Insert(sherrifList.Items[i].Name)
		}
		require.True(t, sheriffNames.HasAll(sheriffOne.Name, sheriffTwo.Name),
			"expected the list to contain both sheriffs, %s and %s, have %s", sheriffOne.Name, sheriffTwo.Name, sheriffNames.List())
	}

	/*t.Logf("=== watch ===")
	{
		t.Logf("Verify that user-1 cannot WATCH")
		_, err = wwUser1CachedResourceVWConfig.Cluster(consumerClusterName.Path()).WildwestV1alpha1().Cowboys("default").
			Watch(ctx, metav1.ListOptions{})
		require.True(t, apierrors.IsForbidden(err))

		t.Logf("Give user-1 WATCH access to the virtual workspace")
		admit(t, kubeClusterClient.Cluster(consumerPath), "user-1-vw-watch", "user-1", "User",
			[]string{"watch"}, wildwestv1alpha1.SchemeGroupVersion.Group, "cowboys", "")

		t.Logf("Verify that user-1 can now WATCH cowboys")
		var cowboyWatch watch.Interface
		kcptestinghelpers.Eventually(t, func() (bool, string) {
			var err error
			cowboyWatch, err = wwUser1CachedResourceVWConfig.Cluster(consumerClusterName.Path()).WildwestV1alpha1().Cowboys("default").Watch(ctx, metav1.ListOptions{
				LabelSelector: labels.SelectorFromSet(labels.Set(cowboyLabels)).String(),
			})
			if apierrors.IsForbidden(err) {
				return false, fmt.Sprintf("waiting until rbac cache is primed: %v", err)
			}
			require.NoError(t, err)
			return true, ""
		}, wait.ForeverTestTimeout, time.Millisecond*100, "expected user-1 to list cowboys")

		cowboyWatchCh := cowboyWatch.ResultChan()
		waitForEvent := func() (watch.Event, bool) {
			var event watch.Event
			var more bool
			kcptestinghelpers.Eventually(t, func() (bool, string) {
				event, more = <-cowboyWatchCh
				return true, ""
			}, time.Second*2, time.Millisecond*100, "expected to get a watch event")
			return event, more
		}
		checkEvent := func(actualEvent watch.Event,
			expectedEventType watch.EventType,
			expectedNext, actualNext bool,
			checkCowboy func(cowboy *wildwestv1alpha1.Cowboy),
		) {
			require.Equal(t, expectedNext, actualNext, "unexpected channel state")
			if !expectedNext {
				// We don't expect any more events, nothing to check anymore.
				return
			}

			require.Equal(t, expectedEventType, actualEvent.Type, "unexpected event type")

			if checkCowboy != nil {
				cowboy := actualEvent.Object.(*wildwestv1alpha1.Cowboy)
				checkCowboy(cowboy)
			}
		}

		t.Logf("Set labels on first cowboy to %v", cowboyLabels)
		setLabelsOnCowboyInConsumer(ctx, t, consumerPath, wildwestClusterClient, cowboyName1, cowboyLabels)
		t.Logf("Verify that the second watched event is the first cowboy with updated labels %v", cowboyLabels)
		e, next := waitForEvent()
		checkEvent(e, watch.Modified, true, next, func(cowboy *wildwestv1alpha1.Cowboy) {
			require.Equal(t, cowboyName1, cowboy.Name, "expected to receive the first cowboy")
			require.Equal(t, cowboyLabels, cowboy.GetLabels(), "expected the cowboy to have labels defined")
		})

		t.Logf("Verify that stopping the watch works")
		cowboyWatch.Stop()
		e, next = waitForEvent()
		checkEvent(e, watch.Error, false, next, nil)
	}*/
}

func createCowboyInConsumer(ctx context.Context, t *testing.T, consumer1Workspace logicalcluster.Path, wildwestClusterClient wildwestclientset.ClusterInterface, labels map[string]string) string {
	t.Helper()

	cowboyClusterClient := wildwestClusterClient.Cluster(consumer1Workspace).WildwestV1alpha1().Cowboys("default")
	t.Logf("Create a cowboy CR in consumer workspace %q", consumer1Workspace)
	cowboy, err := cowboyClusterClient.Create(ctx, newCowboy("default", labels), metav1.CreateOptions{})
	require.NoError(t, err, "error creating cowboy in consumer workspace %q", consumer1Workspace)

	return cowboy.Name
}

func setLabelsOnCowboyInConsumer(ctx context.Context, t *testing.T, consumer1Workspace logicalcluster.Path, wildwestClusterClient wildwestclientset.ClusterInterface, name string, labels map[string]string) {
	t.Helper()

	cowboyClusterClient := wildwestClusterClient.Cluster(consumer1Workspace).WildwestV1alpha1().Cowboys("default")
	t.Logf("Update a cowboy CR in consumer workspace %q with labels %v", consumer1Workspace, labels)
	cowboy, err := cowboyClusterClient.Get(ctx, name, metav1.GetOptions{})
	require.NoError(t, err, "error getting cowboy %s in default namespace in consumer workspace %q", name, consumer1Workspace)

	cowboy.ObjectMeta.SetLabels(labels)
	_, err = cowboyClusterClient.Update(ctx, cowboy, metav1.UpdateOptions{})
	require.NoError(t, err, "error updating cowboy %s in default namespace in consumer workspace %q", name, consumer1Workspace)
}

func createCachedResourceAndCachedResourceEndpointSliceInConsumer(ctx context.Context, t *testing.T, consumer1Workspace logicalcluster.Path, kcpClusterClient kcpclientset.ClusterInterface, cowboyName string) {
	t.Helper()

	cachedResourcesClient := kcpClusterClient.Cluster(consumer1Workspace).CacheV1alpha1().CachedResources()
	cachedResourceEndpointSlicesClient := kcpClusterClient.Cluster(consumer1Workspace).CacheV1alpha1().CachedResourceEndpointSlices()

	t.Logf("Make sure there are no CachedResourceEndpointSlices in consumer workspace %q at the beginning of the test", consumer1Workspace)
	var slices *cachev1alpha1.CachedResourceEndpointSliceList
	require.Eventually(t, func() bool {
		var err error
		slices, err = cachedResourceEndpointSlicesClient.List(ctx, metav1.ListOptions{})
		return err == nil
	}, wait.ForeverTestTimeout, 100*time.Millisecond, "expected to be able to list")
	require.Zero(t, len(slices.Items), "expected 0 CachedResourceEndpointSlices inside consumer workspace %q", consumer1Workspace)

	t.Logf("Make sure there are no CachedResources in consumer workspace %q at the beginning of the test", consumer1Workspace)
	var cachedResources *cachev1alpha1.CachedResourceList
	require.Eventually(t, func() bool {
		var err error
		cachedResources, err = kcpClusterClient.Cluster(consumer1Workspace).CacheV1alpha1().CachedResources().List(ctx, metav1.ListOptions{})
		return err == nil
	}, wait.ForeverTestTimeout, 100*time.Millisecond, "expected to be able to list")
	require.Zero(t, len(cachedResources.Items), "expected 0 CachedResourceEndpointSlices inside consumer workspace %q", consumer1Workspace)

	t.Logf("Create and wait for a CachedResource to cache Cowboys in consumer workspace %q", consumer1Workspace)
	cowboysCachedResource := newCachedResourceForCowboy("cowboys")
	_, err := cachedResourcesClient.Create(ctx, cowboysCachedResource, metav1.CreateOptions{})
	require.NoError(t, err, "error creating CachedResource in consumer workspace %q", consumer1Workspace)
	require.Eventually(t, func() bool {
		var err error
		cowboysCachedResource, err = cachedResourcesClient.Get(ctx, "cowboys", metav1.GetOptions{})
		return err == nil && cowboysCachedResource.Status.ResourceCounts != nil && cowboysCachedResource.Status.ResourceCounts.Local > 0 && cowboysCachedResource.Status.ResourceCounts.Cache > 0
	}, wait.ForeverTestTimeout, 100*time.Millisecond,
		"expected to have CachedResource with non-zero resource count in consumer workspace %q", consumer1Workspace)
	require.EqualValues(t, 1, cowboysCachedResource.Status.ResourceCounts.Local,
		"expected to have exactly one Cowboy object in consumer workspace %q to be collected by CachedResource", consumer1Workspace)
	require.EqualValues(t, 1, cowboysCachedResource.Status.ResourceCounts.Cache,
		"expected to have exactly one CachedObject created for cowboy CachedResource in consumer workspace %q", consumer1Workspace)

	t.Logf("Make sure exactly one CachedResourceEndpointSlice was automatically created as a result of creating a CachedResource in consumer workspace %q", consumer1Workspace)
	require.Eventually(t, func() bool {
		var err error
		slices, err = cachedResourceEndpointSlicesClient.List(ctx, metav1.ListOptions{})
		return err == nil && len(slices.Items) > 0
	}, wait.ForeverTestTimeout, 100*time.Millisecond, "expected to have a non-empty list of CachedResourceEndpointSlices")
	require.EqualValues(t, 1, len(slices.Items), "expected exactly one CachedResourceEndpointSlices inside consumer workspace %q", consumer1Workspace)
}

func newCachedResourceForCowboy(name string) *cachev1alpha1.CachedResource {
	return &cachev1alpha1.CachedResource{
		TypeMeta: metav1.TypeMeta{
			APIVersion: cachev1alpha1.SchemeGroupVersion.String(),
			Kind:       "CachedResource",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: cachev1alpha1.CachedResourceSpec{
			GroupVersionResource: cachev1alpha1.GroupVersionResource{
				Group:    wildwestv1alpha1.SchemeGroupVersion.Group,
				Version:  wildwestv1alpha1.SchemeGroupVersion.Version,
				Resource: "cowboys",
			},
		},
	}
}

func newCowboy(namespace string, labels map[string]string) *wildwestv1alpha1.Cowboy {
	return &wildwestv1alpha1.Cowboy{
		TypeMeta: metav1.TypeMeta{
			APIVersion: wildwestv1alpha1.SchemeGroupVersion.String(),
			Kind:       "Cowboy",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    namespace,
			GenerateName: "cowboy-",
			Labels:       labels,
		},
		Spec: wildwestv1alpha1.CowboySpec{
			Intent: "whoa!",
		},
	}
}

func createClusterRoleAndBindings(name, subjectName, subjectKind string, verbs []string, resources ...string) (*rbacv1.ClusterRole, *rbacv1.ClusterRoleBinding) {
	var total = len(resources) / 3

	rules := make([]rbacv1.PolicyRule, total)

	for i := range total {
		group := resources[i*3]
		resource := resources[i*3+1]
		resourceName := resources[i*3+2]

		r := rbacv1.PolicyRule{
			Verbs:     verbs,
			APIGroups: []string{group},
			Resources: []string{resource},
		}

		if resourceName != "" {
			r.ResourceNames = []string{resourceName}
		}

		rules[i] = r
	}

	return &rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
			Rules: rules,
		}, &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
			Subjects: []rbacv1.Subject{
				{
					Kind: subjectKind,
					Name: subjectName,
				},
			},
			RoleRef: rbacv1.RoleRef{
				APIGroup: rbacv1.SchemeGroupVersion.Group,
				Kind:     "ClusterRole",
				Name:     name,
			},
		}
}

func admit(t *testing.T, kubeClusterClient kubernetesclientset.Interface, ruleName, subjectName, subjectKind string, verbs []string, resources ...string) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	t.Cleanup(cancelFunc)

	cr, crb := createClusterRoleAndBindings(ruleName, subjectName, subjectKind, verbs, resources...)
	_, err := kubeClusterClient.RbacV1().ClusterRoles().Create(ctx, cr, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = kubeClusterClient.RbacV1().ClusterRoleBindings().Create(ctx, crb, metav1.CreateOptions{})
	require.NoError(t, err)
}

func createSheriff(ctx context.Context, c kcpdynamic.ClusterInterface, cluster logicalcluster.Name, sheriff *wildwestv1alpha1.Sheriff) (*wildwestv1alpha1.Sheriff, error) {
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(sheriff)
	if err != nil {
		return nil, err
	}
	u := &unstructured.Unstructured{
		Object: m,
	}
	u.SetAPIVersion(wildwestv1alpha1.SchemeGroupVersion.String())
	u.SetKind("Sheriff")

	u, err = c.Cluster(cluster.Path()).Resource(
		wildwestv1alpha1.SchemeGroupVersion.WithResource("sheriffs"),
	).Create(ctx, u, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}

	createdSheriff := &wildwestv1alpha1.Sheriff{}
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, createdSheriff)
	return createdSheriff, err
}

func listSheriffs(ctx context.Context, c kcpdynamic.ClusterInterface, cluster logicalcluster.Name) (*wildwestv1alpha1.SheriffList, error) {
	uList, err := c.Cluster(cluster.Path()).Resource(
		wildwestv1alpha1.SchemeGroupVersion.WithResource("sheriffs"),
	).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var sheriffs wildwestv1alpha1.SheriffList
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(uList.UnstructuredContent(), &sheriffs)
	if err != nil {
		return nil, err
	}

	return &sheriffs, nil
}

func getSheriff(ctx context.Context, c kcpdynamic.ClusterInterface, cluster logicalcluster.Name, name string) (*wildwestv1alpha1.Sheriff, error) {
	u, err := c.Cluster(cluster.Path()).Resource(
		wildwestv1alpha1.SchemeGroupVersion.WithResource("sheriffs"),
	).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	var sheriff wildwestv1alpha1.Sheriff
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(u.UnstructuredContent(), &sheriff)
	if err != nil {
		return nil, err
	}

	return &sheriff, nil
}
