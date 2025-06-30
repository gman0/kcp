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

package builder

import (
	"testing"

	// "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// "k8s.io/apiserver/pkg/authorization/authorizer"
	//"k8s.io/client-go/rest"

	// kcpdynamic "github.com/kcp-dev/client-go/dynamic"
	// kcpkubernetesclientset "github.com/kcp-dev/client-go/kubernetes"

	"github.com/kcp-dev/logicalcluster/v3"

	// "github.com/kcp-dev/kcp/pkg/virtual/framework"
	"github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/context"
	// kcpinformers "github.com/kcp-dev/kcp/sdk/client/informers/externalversions"
)

func TestDigestUrl(t *testing.T) {
	rootPathPrefix := "/services/replication/"
	testCases := []struct {
		urlPath             string
		expectedAccept      bool
		expectedCluster     logicalcluster.Name
		expectedKey         context.APIDomainKey
		expectedLogicalPath string
	}{
		{
			urlPath:             "/services/replication/my-cluster/my-cachedresource/clusters/my-cluster/apis",
			expectedAccept:      true,
			expectedKey:         "my-cluster/my-cachedresource",
			expectedCluster:     logicalcluster.Name("my-cluster"),
			expectedLogicalPath: "/services/replication/my-cluster/my-cachedresource/clusters/my-cluster",
		},
		{
			urlPath:             "/services/replication/my-cluster/my-cachedresource/clusters/my-cluster",
			expectedAccept:      true,
			expectedKey:         "my-cluster/my-cachedresource",
			expectedCluster:     logicalcluster.Name("my-cluster"),
			expectedLogicalPath: "/services/replication/my-cluster/my-cachedresource/clusters/my-cluster",
		},
		{
			urlPath:             "/services/replication/my-cluster/my-cachedresource/clusters/other-cluster",
			expectedAccept:      false,
			expectedKey:         "",
			expectedCluster:     logicalcluster.Name(""),
			expectedLogicalPath: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.urlPath, func(t *testing.T) {
			clusterName, key, logicalPath, accepted := digestURL(tc.urlPath, rootPathPrefix)
			require.Equal(t, tc.expectedAccept, accepted, "Accepted should match expected value")
			require.Equal(t, tc.expectedKey, key, "Key should match expected value")
			require.Equal(t, tc.expectedCluster, clusterName, "cluster name should match expected value")
			require.Equal(t, tc.expectedLogicalPath, logicalPath, "LogicalPath should match expected value")
		})
	}
}

//func TestIsLogicalClusterRequest(t *testing.T) {
//	testCases := []struct {
//		path     string
//		expected bool
//	}{
//		{"/apis/core.kcp.io/v1alpha1/logicalclusters", true},
//		{"/apis/othergroup/v1alpha1/logicalclusters", false},
//		{"/apis/core.kcp.io/v1alpha1/otherresource", false},
//	}
//
//	for _, tc := range testCases {
//		t.Run(tc.path, func(t *testing.T) {
//			result := isLogicalClusterRequest(tc.path)
//			require.Equal(t, tc.expected, result, "Result should match expected value")
//		})
//	}
//}
//
//func TestBuildVirtualWorkspace(t *testing.T) {
//	rootPathPrefix := "/services/initializingworkspaces/"
//	cfg := &rest.Config{
//		Host: "https://example.com",
//	}
//	kubeClusterClient, err := kcpkubernetesclientset.NewForConfig(cfg)
//	require.NoError(t, err, "ClusterClientSet should not return an error")
//	dynamicClusterClient, err := kcpdynamic.NewForConfig(cfg)
//	require.NoError(t, err, "ClusterClientSet should not return an error")
//	wildcardKcpInformers := kcpinformers.NewSharedInformerFactory(nil, 0)
//
//	virtualWorkspaces, err := BuildVirtualWorkspace(cfg, rootPathPrefix, dynamicClusterClient, kubeClusterClient, wildcardKcpInformers)
//	require.NoError(t, err, "BuildVirtualWorkspace should not return an error")
//
//	assert.Len(t, virtualWorkspaces, 3, "There should be three virtual workspaces")
//	expectedNames := map[string]struct{}{
//		/*wildcardLogicalClustersName: {},
//		logicalClustersName:         {},
//		workspaceContentName:        {},*/
//	}
//
//	for _, vw := range virtualWorkspaces {
//		t.Run(vw.Name, func(t *testing.T) {
//			assert.NotNil(t, vw.VirtualWorkspace, "VirtualWorkspace should not be nil")
//			assert.Implements(t, (*framework.RootPathResolver)(nil), vw.VirtualWorkspace, "VirtualWorkspace should implement RootPathResolver")
//			assert.Implements(t, (*authorizer.Authorizer)(nil), vw.VirtualWorkspace, "VirtualWorkspace should implement Authorizer")
//			assert.Implements(t, (*framework.ReadyChecker)(nil), vw.VirtualWorkspace, "VirtualWorkspace should implement ReadyChecker")
//			assert.NotNil(t, vw.VirtualWorkspace.Register, "Register should not be nil")
//
//			_, exists := expectedNames[vw.Name]
//			assert.True(t, exists, "VirtualWorkspace name should be one of the expected names")
//		})
//	}
//}
