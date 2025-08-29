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

	apisv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha1"
	cachev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/cache/v1alpha1"
	conditionsv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/third_party/conditions/apis/conditions/v1alpha1"
	"github.com/kcp-dev/kcp/sdk/apis/third_party/conditions/util/conditions"
)

type resourceSchema struct {
	getAPIResourceSchema func(cluster logicalcluster.Name, name string) (*apisv1alpha1.APIResourceSchema, error)
	getCRD               func(ctx context.Context, cluster logicalcluster.Name, name string) (*apiextensionsv1.CustomResourceDefinition, error)

	createCachedAPIResourceSchema func(ctx context.Context, cluster logicalcluster.Name, sch *apisv1alpha1.APIResourceSchema) error
	updateCreateAPIResourceSchema func(ctx context.Context, cluster logicalcluster.Name, sch *apisv1alpha1.APIResourceSchema) error
}

func CachedAPIResourceSchemaName(cachedResourceUID types.UID) string {
	return fmt.Sprintf("%s.cachedresources.cache.kcp.io", cachedResourceUID)
}

func (r *resourceSchema) reconcile(ctx context.Context, cachedResource *cachev1alpha1.CachedResource) (reconcileStatus, error) {
	if !cachedResource.DeletionTimestamp.IsZero() {
		return reconcileStatusContinue, nil
	}
	if conditions.IsFalse(cachedResource, cachev1alpha1.CachedResourceSchemaSourceValid) {
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

			sourceSchema, err := r.getAPIResourceSchema(logicalcluster.Name(cachedResource.Status.ResourceSchemaSource.APIResourceSchema.ClusterName), cachedResource.Status.ResourceSchemaSource.APIResourceSchema.Name)
			if err != nil {
				logger.Error(err, "failed to get source APIResourceSchema")
				conditions.MarkFalse(
					cachedResource,
					cachev1alpha1.CachedResourceSourceSchemaReplicated,
					cachev1alpha1.SourceSchemaReplicatedFailedReason,
					conditionsv1alpha1.ConditionSeverityError,
					"Failed to get source APIResourceSchema: %v",
					err,
				)
				return reconcileStatusStopAndRequeue, err
			}

			if !validateAPIResourceSchemaForGVR(sourceSchema, gvr) {
				conditions.MarkFalse(
					cachedResource,
					cachev1alpha1.CachedResourceSchemaSourceValid,
					cachev1alpha1.SchemaSourceInvalidReason,
					conditionsv1alpha1.ConditionSeverityError,
					"Schema is not valid. Please contact the APIExport owner to resolve.",
				)
				return reconcileStatusStop, nil
			}

			sch := sourceSchema.DeepCopy()
			sch.Name = CachedAPIResourceSchemaName(cachedResource.UID)
			sch.Annotations = nil

			if err = r.createCachedAPIResourceSchema(ctx, logicalcluster.From(cachedResource), sch); err != nil {
				logger.Error(err, "failed to create the cached APIResourceSchema")
				conditions.MarkFalse(
					cachedResource,
					cachev1alpha1.CachedResourceSourceSchemaReplicated,
					cachev1alpha1.SourceSchemaReplicatedFailedReason,
					conditionsv1alpha1.ConditionSeverityError,
					"Failed to store schema: %v",
					err,
				)
				return reconcileStatusStopAndRequeue, err
			}

			conditions.MarkTrue(cachedResource, cachev1alpha1.CachedResourceSourceSchemaReplicated)
			return reconcileStatusStopAndRequeue, nil
		}

		// The cached APIResoureSchema already exists.
		// No need to check for updates because it is immutable.
		return reconcileStatusContinue, nil
	}

	if cachedResource.Status.ResourceSchemaSource.CRD != nil {
		crd, err := r.getCRD(ctx, cluster, cachedResource.Status.ResourceSchemaSource.CRD.Name)
		if err != nil {
			logger.Error(err, "failed to get source CRD")
			conditions.MarkFalse(
				cachedResource,
				cachev1alpha1.CachedResourceSourceSchemaReplicated,
				cachev1alpha1.SourceSchemaReplicatedFailedReason,
				conditionsv1alpha1.ConditionSeverityError,
				"Failed to get source CRD: %v",
				err,
			)
			return reconcileStatusStopAndRequeue, err
		}

		if apiextensionshelpers.IsCRDConditionFalse(crd, apiextensionsv1.Established) {
			conditions.MarkFalse(
				cachedResource,
				cachev1alpha1.CachedResourceSchemaSourceValid,
				cachev1alpha1.SchemaSourceNotReadyReason,
				conditionsv1alpha1.ConditionSeverityError,
				"API not ready.",
			)
			return reconcileStatusStop, nil
		}

		if !validateCRDForGVR(crd, gvr) {
			conditions.MarkFalse(
				cachedResource,
				cachev1alpha1.CachedResourceSchemaSourceValid,
				cachev1alpha1.SchemaSourceInvalidReason,
				conditionsv1alpha1.ConditionSeverityError,
				"CRD %s does not define the requested resource version.",
				crd.Name,
			)
			return reconcileStatusStop, nil
		}

		if cachedSchemaNotFound || crd.ObjectMeta.ResourceVersion != cachedResource.Status.ResourceSchemaSource.CRD.ResourceVersion {
			// Either the cached schema doesn't exist, or it needs updating.

			sourceSchema, err := apisv1alpha1.CRDToAPIResourceSchema(crd, "prefix")
			if err != nil {
				logger.Error(err, "failed to convert CRD to APIResourceSchema")
				conditions.MarkFalse(
					cachedResource,
					cachev1alpha1.CachedResourceSourceSchemaReplicated,
					cachev1alpha1.SourceSchemaReplicatedFailedReason,
					conditionsv1alpha1.ConditionSeverityError,
					"Internal error while processing source CRD.",
				)
				return reconcileStatusStopAndRequeue, err
			}
			sourceSchema.Name = CachedAPIResourceSchemaName(cachedResource.UID)

			if cachedSchemaNotFound {
				if err = r.createCachedAPIResourceSchema(ctx, cluster, sourceSchema); err != nil {
					logger.Error(err, "failed to create the cached APIResourceSchema")
					conditions.MarkFalse(
						cachedResource,
						cachev1alpha1.CachedResourceSourceSchemaReplicated,
						cachev1alpha1.SourceSchemaReplicatedFailedReason,
						conditionsv1alpha1.ConditionSeverityError,
						"Failed to store schema: %v",
						err,
					)
					return reconcileStatusStopAndRequeue, err
				}
			} else {
				if err = r.updateCreateAPIResourceSchema(ctx, cluster, sourceSchema); err != nil {
					logger.Error(err, "failed to update the cached APIResourceSchema")
					conditions.MarkFalse(
						cachedResource,
						cachev1alpha1.CachedResourceSourceSchemaReplicated,
						cachev1alpha1.SourceSchemaReplicatedFailedReason,
						conditionsv1alpha1.ConditionSeverityError,
						"Failed to update schema: %v",
						err,
					)
					return reconcileStatusStopAndRequeue, err
				}
			}

			conditions.MarkTrue(cachedResource, cachev1alpha1.CachedResourceSourceSchemaReplicated)
			cachedResource.Status.ResourceSchemaSource.CRD.ResourceVersion = crd.ObjectMeta.ResourceVersion
			return reconcileStatusStopAndRequeue, nil
		}

		return reconcileStatusContinue, nil
	}

	// This should never happen!

	conditions.MarkFalse(
		cachedResource,
		cachev1alpha1.CachedResourceSchemaSourceValid,
		cachev1alpha1.SchemaSourceNotReadyReason,
		conditionsv1alpha1.ConditionSeverityError,
		"API not ready.",
	)
	cachedResource.Status.ResourceSchemaSource = nil

	return reconcileStatusStopAndRequeue, nil
}

func validateAPIResourceSchemaForGVR(sch *apisv1alpha1.APIResourceSchema, gvr schema.GroupVersionResource) bool {
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

func validateCRDForGVR(crd *apiextensionsv1.CustomResourceDefinition, gvr schema.GroupVersionResource) bool {
	if crd.Spec.Group != gvr.Group || crd.Spec.Names.Plural != gvr.Resource {
		return false
	}
	for _, version := range crd.Status.StoredVersions {
		if version == gvr.Version {
			return true
		}
	}
	return false
}
