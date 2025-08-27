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
	"reflect"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

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
	getLogicalCluster    func(cluster logicalcluster.Name) (*corev1alpha1.LogicalCluster, error)
	getAPIBinding        func(cluster logicalcluster.Name, name string) (*apisv1alpha2.APIBinding, error)
	getAPIExport         func(path logicalcluster.Path, name string) (*apisv1alpha2.APIExport, error)
	getAPIResourceSchema func(cluster logicalcluster.Name, name string) (*apisv1alpha1.APIResourceSchema, error)
	listCRDsByGR         func(cluster logicalcluster.Name, gr schema.GroupResource) ([]*apiextensionsv1.CustomResourceDefinition, error)
}

func (r *resourceSchema) reconcile(ctx context.Context, cachedResource *cachev1alpha1.CachedResource) (reconcileStatus, error) {
	if !cachedResource.DeletionTimestamp.IsZero() {
		return reconcileStatusContinue, nil
	}
	if cachedResource.Status.Phase != cachev1alpha1.CachedResourcePhaseInitializing {
		return reconcileStatusContinue, nil
	}

	gvr := schema.GroupVersionResource{
		Group:    cachedResource.Spec.Group,
		Version:  cachedResource.Spec.Version,
		Resource: cachedResource.Spec.Resource,
	}
	cluster := logicalcluster.From(cachedResource)

	res, err := r.tryAPIResourceSchema(gvr.GroupResource(), cluster)
	if res.selected {
		if err != nil {
			conditions.Set(cachedResource, res.cond)
			return res.status, err
		}

		if reflect.DeepEqual(res.resSch, cachedResource.Status.Schema) {
			return reconcileStatusContinue, nil
		}

		cachedResource.Status.Schema = res.resSch
		conditions.MarkTrue(cachedResource, cachev1alpha1.CachedSchemaValid)

		return reconcileStatusStopAndRequeue, nil
	}

	if err != nil && !apierrors.IsNotFound(err) {
		return reconcileStatusStopAndRequeue, err
	}

	res, err = r.tryCRD(gvr.GroupResource(), cluster)
	if res.selected {
		if err != nil {
			conditions.Set(cachedResource, res.cond)
			return res.status, err
		}

		if reflect.DeepEqual(res.resSch, cachedResource.Status.Schema) {
			return reconcileStatusContinue, nil
		}

		cachedResource.Status.Schema = res.resSch
		conditions.MarkTrue(cachedResource, cachev1alpha1.CachedSchemaValid)

		return reconcileStatusStopAndRequeue, nil
	}

	if err != nil && !apierrors.IsNotFound(err) {
		return reconcileStatusStopAndRequeue, err
	}

	// If we're here, no schema provider was chosen. We'll wait for to be triggered again.

	conditions.MarkFalse(
		cachedResource,
		cachev1alpha1.CachedSchemaValid,
		cachev1alpha1.SchemaNotReadyReason,
		conditionsv1alpha1.ConditionSeverityError,
		"API %s not ready",
		gvr.String(),
	)

	return reconcileStatusStop, nil
}

type detectedSchema struct {
	selected bool
	status   reconcileStatus
	cond     *conditionsv1alpha1.Condition
	resSch   *cachev1alpha1.CachedResourceSchema
}

func (r *resourceSchema) tryAPIResourceSchema(gr schema.GroupResource, cluster logicalcluster.Name) (detectedSchema, error) {
	lc, err := r.getLogicalCluster(cluster)
	if err != nil {
		return detectedSchema{
			cond: conditions.FalseCondition(
				cachev1alpha1.CachedSchemaValid,
				cachev1alpha1.SchemaNotReadyReason,
				conditionsv1alpha1.ConditionSeverityError,
				"Error getting LogicalCluster %s|%s: %v",
				cluster,
				"cluster",
				err,
			),
			status: reconcileStatusStopAndRequeue,
		}, err
	}

	boundResources, err := apibinding.GetResourceBindings(lc)
	if err != nil {
		return detectedSchema{
			cond: conditions.FalseCondition(
				cachev1alpha1.CachedSchemaValid,
				cachev1alpha1.SchemaNotReadyReason,
				conditionsv1alpha1.ConditionSeverityError,
				"Error reading bound resources on LogicalCluster %s|%s: %v",
				cluster,
				"cluster",
				err,
			),
			status: reconcileStatusStopAndRequeue,
		}, err
	}

	lock, resourceFound := boundResources[gr.String()]
	if !resourceFound {
		return detectedSchema{
			selected: true,
			cond: conditions.FalseCondition(
				cachev1alpha1.CachedSchemaValid,
				cachev1alpha1.SchemaNotReadyReason,
				conditionsv1alpha1.ConditionSeverityError,
				"API %s not ready",
				gr.String(),
			),
			status: reconcileStatusStop,
		}, nil
	}

	if lock.Name == "" {
		return detectedSchema{
			cond: conditions.FalseCondition(
				cachev1alpha1.CachedSchemaValid,
				cachev1alpha1.SchemaInvalidReason,
				conditionsv1alpha1.ConditionSeverityError,
				"Resource %s is not backed by an APIResourceSchema. Please contact the APIExport owner to resolve.",
				gr.String(),
			),
			status: reconcileStatusStop,
		}, nil
	}

	apiBinding, err := r.getAPIBinding(cluster, lock.Name)
	if err != nil {
		return detectedSchema{
			cond: conditions.FalseCondition(
				cachev1alpha1.CachedSchemaValid,
				cachev1alpha1.SchemaNotReadyReason,
				conditionsv1alpha1.ConditionSeverityError,
				"Error getting APIBinding %s|%s: %v",
				cluster,
				lock.Name,
				err,
			),
			status: reconcileStatusStopAndRequeue,
		}, err
	}

	apiExport, err := r.getAPIExport(logicalcluster.NewPath(apiBinding.Spec.Reference.Export.Path), apiBinding.Spec.Reference.Export.Name)
	if err != nil {
		return detectedSchema{
			cond: conditions.FalseCondition(
				cachev1alpha1.CachedSchemaValid,
				cachev1alpha1.SchemaNotReadyReason,
				conditionsv1alpha1.ConditionSeverityError,
				"Error getting APIExport %s|%s for APIBinding %s|%s: %v",
				apiBinding.Spec.Reference.Export.Path,
				apiBinding.Spec.Reference.Export.Name,
				cluster,
				lock.Name,
				err,
			),
			status: reconcileStatusStopAndRequeue,
		}, err
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
		return detectedSchema{
			cond: conditions.FalseCondition(
				cachev1alpha1.CachedSchemaValid,
				cachev1alpha1.SchemaInvalidReason,
				conditionsv1alpha1.ConditionSeverityError,
				"No valid %s resource in APIExport %s|%s. Please contact the APIExport owner to resolve.",
				gr.String(),
				apiBinding.Spec.Reference.Export.Path,
				apiBinding.Spec.Reference.Export.Name,
			),
			status: reconcileStatusStop,
		}, nil
	}

	_, err = r.getAPIResourceSchema(logicalcluster.From(apiExport), schemaName)
	if err != nil {
		return detectedSchema{
			cond: conditions.FalseCondition(
				cachev1alpha1.CachedSchemaValid,
				cachev1alpha1.SchemaNotReadyReason,
				conditionsv1alpha1.ConditionSeverityError,
				"Error getting APIResourceSchema %s|%s: %v",
				apiBinding.Spec.Reference.Export.Path,
				schemaName,
				err,
			),
			status: reconcileStatusStop,
		}, err
	}

	return detectedSchema{
		selected: true,
		resSch: &cachev1alpha1.CachedResourceSchema{
			APIResourceSchema: &cachev1alpha1.APIResourceSchemaReference{
				Name:    schemaName,
				Cluster: cluster.String(),
			},
		},
		status: reconcileStatusStopAndRequeue,
	}, nil
}

func (r *resourceSchema) tryCRD(gr schema.GroupResource, cluster logicalcluster.Name) (detectedSchema, error) {
	crds, err := r.listCRDsByGR(cluster, gr)
	if err != nil {
		return detectedSchema{
			cond: conditions.FalseCondition(
				cachev1alpha1.CachedSchemaValid,
				cachev1alpha1.SchemaNotReadyReason,
				conditionsv1alpha1.ConditionSeverityError,
				"Error listing CRDs in %s: %v",
				cluster,
				err,
			),
			status: reconcileStatusStopAndRequeue,
		}, err
	}

	if len(crds) == 0 {
		return detectedSchema{}, nil
	}
	if len(crds) > 1 {
		return detectedSchema{}, nil
	}

	crd := crds[0]

	return detectedSchema{
		selected: true,
		resSch: &cachev1alpha1.CachedResourceSchema{
			CRD: &cachev1alpha1.CRDReference{
				Cluster: cluster.String(),
				Name:    crd.Name,
			},
		},
	}, nil
}
