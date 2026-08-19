/*
Copyright 2025 The kcp Authors.

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

package clustercachedresources

import (
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/kcp-dev/logicalcluster/v3"
	cachev1alpha1 "github.com/kcp-dev/sdk/apis/cache/v1alpha1"
)

const (
	// ByGRAndLogicalCluster is the name for the index that indexes by an object's group+resource and logical cluster.
	ByGRAndLogicalCluster = "kcp-byGRAndLogicalCluster"

	// ByIdentityAndGroupResource is the name for the index that indexes by an object's identity hash and group/resource.
	ByIdentityAndGroupResource = "kcp-byIdentityAndGroupResource"

	ByGroupResource = "kcp-byGroupResource"
)

// IndexByGRAndLogicalCluster is an index function that indexes by an object's group+resource and logical cluster.
func IndexByGRAndLogicalCluster(obj interface{}) ([]string, error) {
	clusterCachedResource := obj.(*cachev1alpha1.ClusterCachedResource)

	if clusterCachedResource.Status.IdentityHash == "" {
		return []string{}, nil
	}
	if clusterCachedResource.Annotations == nil ||
		clusterCachedResource.Annotations[AnnotationResourceKind] == "" ||
		clusterCachedResource.Annotations[AnnotationResourceScope] == "" {
		return []string{}, nil
	}

	return []string{
		GRAndLogicalClusterKey(
			schema.GroupResource(clusterCachedResource.Spec.GroupResource),
			logicalcluster.From(clusterCachedResource),
		),
	}, nil
}

// IndexByIdentityAndGroupResource is an index function that indexes by an object's identity hash and group/resource.
// Objects with an empty identity hash are not indexed.
func IndexByIdentityAndGroupResource(obj interface{}) ([]string, error) {
	clusterCachedResource := obj.(*cachev1alpha1.ClusterCachedResource)

	if clusterCachedResource.Status.IdentityHash == "" {
		return []string{}, nil
	}
	if clusterCachedResource.Annotations == nil ||
		clusterCachedResource.Annotations[AnnotationResourceKind] == "" ||
		clusterCachedResource.Annotations[AnnotationResourceScope] == "" {
		return []string{}, nil
	}

	gr := schema.GroupResource(clusterCachedResource.Spec.GroupResource)
	return []string{IdentityAndGroupResourceKey(clusterCachedResource.Status.IdentityHash, gr)}, nil
}

// IdentityAndGroupResourceKey creates an index key from an identity hash and group/resource.
func IdentityAndGroupResourceKey(identity string, gr schema.GroupResource) string {
	return identity + "|" + gr.String()
}

// IndexByGroupResource is an index function that indexes by an object's group/resource.
func IndexByGroupResource(obj interface{}) ([]string, error) {
	clusterCachedResource := obj.(*cachev1alpha1.ClusterCachedResource)
	if clusterCachedResource.Status.IdentityHash == "" {
		return []string{}, nil
	}
	if clusterCachedResource.Annotations == nil ||
		clusterCachedResource.Annotations[AnnotationResourceKind] == "" ||
		clusterCachedResource.Annotations[AnnotationResourceScope] == "" {
		return []string{}, nil
	}
	gr := schema.GroupResource(clusterCachedResource.Spec.GroupResource)
	return []string{GroupResourceKey(gr)}, nil
}

// GroupResourceKey creates an index key from a group/resource.
func GroupResourceKey(gr schema.GroupResource) string {
	return gr.String()
}

// GRAndLogicalClusterKey creates an index key from a group+resource and logical cluster.
// Key will be in the form of resource.group|cluster.
func GRAndLogicalClusterKey(gr schema.GroupResource, cluster logicalcluster.Name) string {
	var key string
	if gr.Group == "" {
		gr.Group = "core"
	}
	key += gr.Resource + "." + gr.Group
	if !cluster.Empty() {
		key += "|" + cluster.String()
	}
	return key
}
