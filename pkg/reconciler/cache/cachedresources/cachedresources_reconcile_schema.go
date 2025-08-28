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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
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

type resourceSchema struct {
	getLogicalCluster func(cluster logicalcluster.Name) (*corev1alpha1.LogicalCluster, error)
	getAPIBinding     func(cluster logicalcluster.Name, name string) (*apisv1alpha2.APIBinding, error)
	getAPIExport      func(path logicalcluster.Path, name string) (*apisv1alpha2.APIExport, error)

	getAPIResourceSchema func(cluster logicalcluster.Name, name string) (*apisv1alpha1.APIResourceSchema, error)
	getCRD               func(ctx context.Context, cluster logicalcluster.Name, name string) (*apiextensionsv1.CustomResourceDefinition, error)

	createCachedAPIResourceSchema func(ctx context.Context, cluster logicalcluster.Name, sch *apisv1alpha1.APIResourceSchema) error
	updateCreateAPIResourceSchema func(ctx context.Context, cluster logicalcluster.Name, sch *apisv1alpha1.APIResourceSchema) error
}

func CachedAPIResourceSchemaName(cachedResourceUID types.UID) string {
	return fmt.Sprintf("%s.cachedresources.kcp.io", cachedResourceUID)
}

func (r *resourceSchema) reconcile(ctx context.Context, cachedResource *cachev1alpha1.CachedResource) (reconcileStatus, error) {
	if !cachedResource.DeletionTimestamp.IsZero() {
		return reconcileStatusContinue, nil
	}
	if cachedResource.Status.Phase != cachev1alpha1.CachedResourcePhaseInitializing {
		return reconcileStatusContinue, nil
	}
	if cachedResource.Status.ResourceSchemaSource == nil {
		return reconcileStatusStopAndRequeue, nil
	}
	logger := klog.FromContext(ctx)

	gvr := schema.GroupVersionResource{
		Group:    cachedResource.Spec.Group,
		Version:  cachedResource.Spec.Version,
		Resource: cachedResource.Spec.Resource,
	}
	cluster := logicalcluster.From(cachedResource)

	_, err := r.getAPIResourceSchema(cluster, CachedAPIResourceSchemaName(cachedResource.UID))
	cachedSchemaNotFound := apierrors.IsNotFound(err)
	if err != nil && !cachedSchemaNotFound {
		logger.Error(err, "failed to get cached APIResourceSchema")
		return reconcileStatusStopAndRequeue, err
	}

	if cachedResource.Status.ResourceSchemaSource.APIResourceSchema != nil {
		if cachedSchemaNotFound {
			// We need to create it.

			sourceSchema, err := r.getSourceAPIResourceSchema(cluster, gvr.GroupResource())
			if err != nil {
				return reconcileStatusStopAndRequeue, err
			}
			if sourceSchema == nil || !validateAPIResourceSchema(sourceSchema, gvr) {
				// The schema failed validation. We'll mark the schema as not ready
				// and kick it out of the queue.
				conditions.MarkFalse(
					cachedResource,
					cachev1alpha1.CachedResourceSchemaSourceValid,
					cachev1alpha1.SchemaNotReadyReason,
					conditionsv1alpha1.ConditionSeverityError,
					"API not ready",
				)
				return reconcileStatusStop, nil
			}

			if err = r.createCachedAPIResourceSchema(ctx, cluster, sourceSchema); err != nil {
				return reconcileStatusContinue, nil
			}
			return reconcileStatusStopAndRequeue, err
		}

		// The cached APIResoureSchema already exists.
		// No need to check for updates because it is immutable.
		return reconcileStatusContinue, nil
	}

	if cachedResource.Status.ResourceSchemaSource.CRD != nil {
		crd, err := r.getCRD(ctx, cluster, cachedResource.Status.ResourceSchemaSource.CRD.Name)
		if err != nil {
			logger.Error(err, "failed to get CRD")
			if apierrors.IsNotFound(err) {
				// Nothing we can do. We'll retry once we have the CRD available.
				return reconcileStatusStop, nil
			}
			return reconcileStatusStopAndRequeue, err
		}

		if !validateCRD(crd, gvr) {
			// The schema failed validation. We'll mark the schema as not ready
			// and kick it out of the queue.
			conditions.MarkFalse(
				cachedResource,
				cachev1alpha1.CachedResourceSchemaSourceValid,
				cachev1alpha1.SchemaNotReadyReason,
				conditionsv1alpha1.ConditionSeverityError,
				"API not ready",
			)
			return reconcileStatusStop, nil
		}

		if cachedSchemaNotFound || crd.ObjectMeta.ResourceVersion != cachedResource.Status.ResourceSchemaSource.CRD.ResourceVersion {
			// Either the cached schema doesn't exist, or it needs updating.

			sourceSchema, err := apisv1alpha1.CRDToAPIResourceSchema(crd, "prefix")
			if err != nil {
				return reconcileStatusStopAndRequeue, err
			}
			sourceSchema.Name = CachedAPIResourceSchemaName(cachedResource.UID)

			if cachedSchemaNotFound {
				if err = r.createCachedAPIResourceSchema(ctx, cluster, sourceSchema); err != nil {
					return reconcileStatusStopAndRequeue, err
				}
			} else {
				if err = r.updateCreateAPIResourceSchema(ctx, cluster, sourceSchema); err != nil {
					return reconcileStatusStopAndRequeue, err
				}
			}

			cachedResource.Status.ResourceSchemaSource.CRD.ResourceVersion = crd.ObjectMeta.ResourceVersion
			return reconcileStatusStopAndRequeue, nil
		}

		return reconcileStatusContinue, nil
	}

	// This should never happen!

	conditions.MarkFalse(
		cachedResource,
		cachev1alpha1.CachedResourceSchemaSourceValid,
		cachev1alpha1.SchemaInvalidReason,
		conditionsv1alpha1.ConditionSeverityError,
		"resourceSchemaSource is invalid",
	)
	cachedResource.Status.ResourceSchemaSource = nil

	return reconcileStatusStopAndRequeue, nil
}

func (r *resourceSchema) getSourceAPIResourceSchema(cluster logicalcluster.Name, gr schema.GroupResource) (*apisv1alpha1.APIResourceSchema, error) {
	lc, err := r.getLogicalCluster(cluster)
	if err != nil {
		return nil, err
	}

	boundResources, err := apibinding.GetResourceBindings(lc)
	if err != nil {
		return nil, err
	}

	lock, resourceFound := boundResources[gr.String()]
	if !resourceFound {
		return nil, nil
	}

	if lock.Name == "" {
		return nil, nil
	}

	apiBinding, err := r.getAPIBinding(cluster, lock.Name)
	if err != nil {
		return nil, err
	}

	apiExport, err := r.getAPIExport(logicalcluster.NewPath(apiBinding.Spec.Reference.Export.Path), apiBinding.Spec.Reference.Export.Name)
	if err != nil {
		return nil, err
	}

	var schemaName string
	for _, res := range apiExport.Spec.Resources {
		if res.Group != gr.Group || res.Name != gr.Resource {
			continue
		}
		if res.Storage.CRD == nil {
			continue
		}
		schemaName = res.Schema
	}
	if schemaName == "" {
		return nil, nil
	}

	return r.getAPIResourceSchema(logicalcluster.From(apiExport), schemaName)
}

func validateAPIResourceSchema(sch *apisv1alpha1.APIResourceSchema, gvr schema.GroupVersionResource) bool {
	if sch.Spec.Group != gvr.Group || sch.Spec.Names.Plural != gvr.Resource {
		return false
	}
	for i := range sch.Spec.Versions {
		if sch.Spec.Versions[i].Name == gvr.Version {
			return true
		}
	}
	return false
}

func validateCRD(crd *apiextensionsv1.CustomResourceDefinition, gvr schema.GroupVersionResource) bool {
	if crd.Spec.Group != gvr.Group || crd.Spec.Names.Plural != gvr.Resource {
		return false
	}
	if apiextensionshelpers.IsCRDConditionFalse(crd, apiextensionsv1.Established) {
		return false
	}
	for _, storedVersion := range crd.Status.StoredVersions {
		if storedVersion == gvr.Version {
			return true
		}
	}
	return false
}
