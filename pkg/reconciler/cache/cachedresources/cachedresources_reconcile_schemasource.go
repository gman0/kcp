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

package cachedresources

import (
	"context"
	"fmt"

	apiextensionshelpers "k8s.io/apiextensions-apiserver/pkg/apihelpers"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"

	"github.com/kcp-dev/logicalcluster/v3"

	"github.com/kcp-dev/kcp/pkg/reconciler/apis/apibinding"
	apisv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha1"
	apisv1alpha2 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha2"
	cachev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/cache/v1alpha1"
	corev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/core/v1alpha1"
	conditionsv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/third_party/conditions/apis/conditions/v1alpha1"
	"github.com/kcp-dev/kcp/sdk/apis/third_party/conditions/util/conditions"
)

type schemaSource struct {
	getLogicalCluster    func(cluster logicalcluster.Name) (*corev1alpha1.LogicalCluster, error)
	getAPIBinding        func(cluster logicalcluster.Name, name string) (*apisv1alpha2.APIBinding, error)
	getAPIExport         func(path logicalcluster.Path, name string) (*apisv1alpha2.APIExport, error)
	getAPIResourceSchema func(cluster logicalcluster.Name, name string) (*apisv1alpha1.APIResourceSchema, error)
	listCRDsByGR         func(cluster logicalcluster.Name, gr schema.GroupResource) ([]*apiextensionsv1.CustomResourceDefinition, error)
}

func (r *schemaSource) reconcile(ctx context.Context, cachedResource *cachev1alpha1.CachedResource) (reconcileStatus, error) {
	logger := klog.FromContext(ctx)

	if !cachedResource.DeletionTimestamp.IsZero() {
		return reconcileStatusContinue, nil
	}
	if conditions.IsTrue(cachedResource, cachev1alpha1.CachedResourceSchemaSourceValid) {
		return reconcileStatusContinue, nil
	}

	gvr := schema.GroupVersionResource{
		Group:    cachedResource.Spec.Group,
		Version:  cachedResource.Spec.Version,
		Resource: cachedResource.Spec.Resource,
	}
	cluster := logicalcluster.From(cachedResource)

	// Find out where the schema comes from.

	// Start with inspecting the LogicalCluster to find out if we have an associated
	// APIBinding for this GR, which could mean the schema comes from an APIResourceSchema.

	lc, err := r.getLogicalCluster(cluster)
	if err != nil {
		return reconcileStatusStopAndRequeue, err
	}

	boundResources, err := apibinding.GetResourceBindings(lc)
	if err != nil {
		return reconcileStatusStopAndRequeue, err
	}

	if bindingLock, found := boundResources[gvr.GroupResource().String()]; found {
		if bindingLock.Name != "" {
			// This resource's schema originates from an APIResourceSchema
			// because we have an associated APIBinding.

			conditions.MarkTrue(cachedResource, cachev1alpha1.CachedResourceSchemaSourceValid)
			cachedResource.Status.ResourceSchemaSource = &cachev1alpha1.CachedResourceSchemaSource{
				APIResourceSchema: &cachev1alpha1.APIResourceSchemaSource{},
			}

			return reconcileStatusStopAndRequeue, nil
		} else if bindingLock.CRD {
			// The resource is backed by a CRD. Fall through to find that CRD.
		} else {
			// This should never happen! Neither APIBinding or CRD are present in the binding lock.
			// We can drop this item from the queue (reconcileStatusStop). We'll try again once the LogicalCluster is updated.

			logger.Error(nil, "failed to process bindings annotation on LogicalCluster",
				"LogicalCluster", fmt.Sprintf("%s|%s", cluster, corev1alpha1.LogicalClusterName),
				"annotationKey", apibinding.ResourceBindingsAnnotationKey,
				"annotation", lc.Annotations[apibinding.ResourceBindingsAnnotationKey])

			return reconcileStatusStop, nil
		}
	}

	// It's probably a CRD.

	setNotReadyCond := func() {
		conditions.MarkFalse(
			cachedResource,
			cachev1alpha1.CachedResourceSchemaSourceValid,
			cachev1alpha1.SchemaNotReadyReason,
			conditionsv1alpha1.ConditionSeverityError,
			"API not ready",
		)
	}

	crds, err := r.listCRDsByGR(cluster, gvr.GroupResource())
	if err != nil {
		return reconcileStatusStopAndRequeue, err
	}

	if len(crds) != 1 {
		// Zero or >1 is bad news.
		setNotReadyCond()
		return reconcileStatusStop, nil
	}

	crd := crds[0]

	if apiextensionshelpers.IsCRDConditionFalse(crd, apiextensionsv1.Established) {
		setNotReadyCond()
		return reconcileStatusStop, nil
	}

	var hasVersion bool
	for _, version := range crd.Status.StoredVersions {
		if version == gvr.Version {
			hasVersion = true
			break
		}
	}
	if !hasVersion {
		setNotReadyCond()
		return reconcileStatusStop, nil
	}

	// It's definitely a CRD!

	cachedResource.Status.ResourceSchemaSource = &cachev1alpha1.CachedResourceSchemaSource{
		CRD: &cachev1alpha1.CRDSchemaSource{
			Name: crd.Name,
		},
	}
	conditions.MarkTrue(cachedResource, cachev1alpha1.CachedResourceSchemaSourceValid)

	return reconcileStatusStopAndRequeue, nil
}
