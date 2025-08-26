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
	"reflect"

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
}

func (r *resourceSchema) reconcile(ctx context.Context, cachedResource *cachev1alpha1.CachedResource) (reconcileStatus, error) {
	if !cachedResource.DeletionTimestamp.IsZero() {
		return reconcileStatusContinue, nil
	}
	fmt.Printf("### resourceSchema Phase=%s\n", cachedResource.Status.Phase)
	if cachedResource.Status.Phase != cachev1alpha1.CachedResourcePhaseInitializing {
		return reconcileStatusContinue, nil
	}

	gr := schema.GroupResource{
		Group:    cachedResource.Spec.Group,
		Resource: cachedResource.Spec.Resource,
	}
	clusterName := logicalcluster.From(cachedResource)

	lc, err := r.getLogicalCluster(clusterName)
	if err != nil {
		conditions.MarkFalse(
			cachedResource,
			cachev1alpha1.CachedResourceValid,
			cachev1alpha1.APIResourceSchemaInvalidReason,
			conditionsv1alpha1.ConditionSeverityError,
			"Error getting LogicalCluster %s|%s: %v",
			clusterName,
			"cluster",
			err,
		)
		return reconcileStatusStopAndRequeue, err
	}

	boundResources, err := apibinding.GetResourceBindings(lc)
	if err != nil {
		conditions.MarkFalse(
			cachedResource,
			cachev1alpha1.CachedResourceValid,
			cachev1alpha1.APIResourceSchemaInvalidReason,
			conditionsv1alpha1.ConditionSeverityError,
			"Error reading bound resources on LogicalCluster %s|%s: %v",
			clusterName,
			"cluster",
			err,
		)
		return reconcileStatusStop, err
	}

	lock, resourceFound := boundResources[gr.String()]
	if !resourceFound {
		conditions.MarkFalse(
			cachedResource,
			cachev1alpha1.CachedResourceValid,
			cachev1alpha1.APIResourceSchemaInvalidReason,
			conditionsv1alpha1.ConditionSeverityError,
			"Resource lock for %s not ready",
			gr.String(),
		)
		return reconcileStatusStop, nil
	}

	if lock.Name == "" {
		conditions.MarkFalse(
			cachedResource,
			cachev1alpha1.CachedResourceValid,
			cachev1alpha1.APIResourceSchemaInvalidReason,
			conditionsv1alpha1.ConditionSeverityError,
			"Resource %s is not backed by an APIResourceSchema",
			gr.String(),
		)
		return reconcileStatusStop, nil
	}

	apiBinding, err := r.getAPIBinding(clusterName, lock.Name)
	if err != nil {
		conditions.MarkFalse(
			cachedResource,
			cachev1alpha1.CachedResourceValid,
			cachev1alpha1.APIResourceSchemaInvalidReason,
			conditionsv1alpha1.ConditionSeverityError,
			"Error getting APIBinding %s|%s: %v",
			clusterName,
			lock.Name,
			err,
		)
		return reconcileStatusStopAndRequeue, err
	}

	apiExport, err := r.getAPIExport(logicalcluster.NewPath(apiBinding.Spec.Reference.Export.Path), apiBinding.Spec.Reference.Export.Name)
	if err != nil {
		conditions.MarkFalse(
			cachedResource,
			cachev1alpha1.CachedResourceValid,
			cachev1alpha1.APIResourceSchemaInvalidReason,
			conditionsv1alpha1.ConditionSeverityError,
			"Error getting APIExport %s|%s for APIBinding %s|%s: %v",
			apiBinding.Spec.Reference.Export.Path,
			apiBinding.Spec.Reference.Export.Name,
			clusterName,
			lock.Name,
			err,
		)
		return reconcileStatusStopAndRequeue, err
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
		conditions.MarkFalse(
			cachedResource,
			cachev1alpha1.CachedResourceValid,
			cachev1alpha1.APIResourceSchemaInvalidReason,
			conditionsv1alpha1.ConditionSeverityError,
			"No APIResourceSchema for %s resource in APIExport %s|%s: %v",
			apiBinding.Spec.Reference.Export.Path,
			apiBinding.Spec.Reference.Export.Name,
			clusterName,
		)
		return reconcileStatusStop, err
	}

	_, err = r.getAPIResourceSchema(logicalcluster.From(apiExport), schemaName)
	if err != nil {
		conditions.MarkFalse(
			cachedResource,
			cachev1alpha1.CachedResourceValid,
			cachev1alpha1.APIResourceSchemaInvalidReason,
			conditionsv1alpha1.ConditionSeverityError,
			"Error getting APIResourceSchema %s|%s: %v",
			apiBinding.Spec.Reference.Export.Path,
			schemaName,
			err,
		)
		return reconcileStatusStopAndRequeue, err
	}

	newSchema := &cachev1alpha1.CachedAPIResourceSchema{
		Name:    schemaName,
		Cluster: logicalcluster.From(apiExport).String(),
	}

	if reflect.DeepEqual(newSchema, cachedResource.Status.Schema) {
		return reconcileStatusContinue, nil
	}

	cachedResource.Status.Schema = newSchema

	return reconcileStatusStopAndRequeue, nil
}
