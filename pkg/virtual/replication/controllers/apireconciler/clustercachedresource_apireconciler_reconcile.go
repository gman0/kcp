/*
Copyright 2026 The kcp Authors.

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

package apireconciler

import (
	"context"
	"fmt"
	"maps"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	k8sversion "k8s.io/apimachinery/pkg/version"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	"github.com/kcp-dev/logicalcluster/v3"
	apisv1alpha2 "github.com/kcp-dev/sdk/apis/apis/v1alpha2"
	cachev1alpha1 "github.com/kcp-dev/sdk/apis/cache/v1alpha1"
	"github.com/kcp-dev/sdk/apis/third_party/conditions/util/conditions"
	"github.com/kcp-dev/virtual-workspace-framework/pkg/dynamic/apidefinition"
	dynamiccontext "github.com/kcp-dev/virtual-workspace-framework/pkg/dynamic/context"
)

func findResourceSchemaByClusterCachedResourceEndpointSlice(
	export *apisv1alpha2.APIExport,
	endpointSlice *cachev1alpha1.ClusterCachedResourceEndpointSlice,
) *apisv1alpha2.ResourceSchema {
	resIdx := slices.IndexFunc(export.Spec.Resources, func(res apisv1alpha2.ResourceSchema) bool {
		return res.Storage.Virtual != nil &&
			ptr.Deref(res.Storage.Virtual.Reference.APIGroup, "") == cachev1alpha1.SchemeGroupVersion.Group &&
			res.Storage.Virtual.Reference.Kind == "ClusterCachedResourceEndpointSlice" &&
			res.Storage.Virtual.Reference.Name == endpointSlice.Name
	})
	if resIdx >= 0 {
		return &export.Spec.Resources[resIdx]
	}
	return nil
}

func (c *APIReconciler) reconcile(ctx context.Context, endpointSlice *cachev1alpha1.ClusterCachedResourceEndpointSlice, apiDomainKey dynamiccontext.APIDomainKey) error {
	logger := klog.FromContext(ctx)

	if endpointSlice == nil {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		delete(c.apiSets, apiDomainKey)
		return nil
	}

	if !conditions.IsTrue(endpointSlice, cachev1alpha1.ClusterCachedResourceValid) ||
		!conditions.IsTrue(endpointSlice, cachev1alpha1.APIExportValid) {
		logger.V(2).Info("ClusterCachedResourceEndpointSlice not ready")
		return nil
	}

	// Extract the ClusterCachedResource and APIExport referenced by the endpoint slice.

	clusterCachedResourcePath := logicalcluster.NewPath(endpointSlice.Spec.ClusterCachedResource.Path)
	if clusterCachedResourcePath.Empty() {
		clusterCachedResourcePath = logicalcluster.From(endpointSlice).Path()
	}
	clusterCachedResource, err := c.getClusterCachedResourceByPath(clusterCachedResourcePath, endpointSlice.Spec.ClusterCachedResource.Name)
	if err != nil {
		logger.Error(err, "failed to get ClusterCachedResource for ClusterCachedResourceEndpointSlice")
		return err
	}

	exportPath := logicalcluster.NewPath(endpointSlice.Spec.APIExport.Path)
	if exportPath.Empty() {
		exportPath = logicalcluster.From(endpointSlice).Path()
	}
	export, err := c.getAPIExportByPath(exportPath, endpointSlice.Spec.APIExport.Name)
	if err != nil {
		logger.Error(err, "failed to get APIExport for ClusterCachedResourceEndpointSlice")
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	// Next, we should be able to find this slice referenced in the export's resources.

	res := findResourceSchemaByClusterCachedResourceEndpointSlice(export, endpointSlice)
	if res == nil {
		logger.Error(nil, "APIExport doesn't export this ClusterCachedResourceEndpointSlice")
		return nil
	}

	// Get the schema, and check that this actually belongs to the GVR of this ClusterCachedResource.

	sch, err := c.getAPIResourceSchema(logicalcluster.From(export), res.Schema)
	if err != nil {
		logger.Error(err, "failed to get APIResourceSchema for the APIExport")
		return err
	}

	gr := schema.GroupResource(clusterCachedResource.Spec.GroupResource)

	c.mutex.RLock()
	oldApiSet := c.apiSets[apiDomainKey]
	c.mutex.RUnlock()
	// These are the actual API definitions that will be applied.
	newApiSet := make(apidefinition.APIDefinitionSet)

	// These are just for logging so that we can report what's actually changing.
	apisToKeep := sets.New[schema.GroupVersionResource]()
	apisToAdd := sets.New[schema.GroupVersionResource]()
	apisToRemove := sets.New[schema.GroupVersionResource]()

	versionsServedBySchema := sets.New[string]()
	for _, version := range sch.Spec.Versions {
		if version.Served {
			versionsServedBySchema.Insert(version.Name)
		}
	}
	versionsServedByCCR := sets.New[string]()
	for _, version := range clusterCachedResource.Status.StoredVersions {
		versionsServedByCCR.Insert(version)
	}
	versionsToServe := versionsServedBySchema.Intersection(versionsServedByCCR)

	// Resolve GVRs for the API set to be served.
	for version := range versionsToServe {
		gvr := gr.WithVersion(version)

		/*if apiDef, gvrAlreadyServing := oldApiSet[gvr]; gvrAlreadyServing {
			apisToKeep.Insert(gvr)
			newApiSet[gvr] = apiDef
			continue
		}*/

		logger.Info("creating API definition", "gvr", gvr)
		apiDefinition, err := c.createAPIDefinition(sch, version, clusterCachedResource, export)
		if err != nil {
			// TODO(ncdc): would be nice to expose some sort of user-visible error
			logger.Error(err, "error creating api definition", "gvr", gvr)
			return err
		}
		apisToAdd.Insert(gvr)
		newApiSet[gvr] = apiResourceSchemaApiDefinition{
			APIDefinition: apiDefinition,
			UID:           sch.UID,
			IdentityHash:  clusterCachedResource.Status.IdentityHash,
		}
	}
	// Just note down anything else we haven't noticed.
	// Since they are not part of the schema, they must be removed.
	for gvr := range oldApiSet {
		if !apisToKeep.Has(gvr) && !apisToAdd.Has(gvr) {
			apisToRemove.Insert(gvr)
		}
	}

	sortedGVRs := func(gvrs sets.Set[schema.GroupVersionResource]) []string {
		sortedByVersions := slices.SortedFunc(maps.Keys(gvrs), func(a, b schema.GroupVersionResource) int {
			// GR is guaranteed to be the same for all, so we compare only version.
			return k8sversion.CompareKubeAwareVersionStrings(a.Version, b.Version)
		})
		list := make([]string, 0, len(sortedByVersions))
		for _, gvr := range sortedByVersions {
			list = append(list, fmt.Sprintf("%s.%s.%s", gvr.Resource, gvr.Version, gvr.Group))
		}
		return list
	}
	if len(apisToAdd) > 0 || len(apisToRemove) > 0 {
		logger.V(2).Info("updating APIs",
			"added", sortedGVRs(apisToAdd),
			"preserved", sortedGVRs(apisToKeep),
			"removed", sortedGVRs(apisToRemove),
		)
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.apiSets[apiDomainKey] = newApiSet
	return nil
}

type apiResourceSchemaApiDefinition struct {
	apidefinition.APIDefinition

	UID          types.UID
	IdentityHash string
}
