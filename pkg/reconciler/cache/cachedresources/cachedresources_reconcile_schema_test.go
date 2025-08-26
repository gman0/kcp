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
	"testing"

	"github.com/stretchr/testify/require"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kcp-dev/logicalcluster/v3"

	"github.com/kcp-dev/kcp/pkg/reconciler/apis/apibinding"
	apisv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha1"
	apisv1alpha2 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha2"
	cachev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/cache/v1alpha1"
	corev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/core/v1alpha1"
	conditionsv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/third_party/conditions/apis/conditions/v1alpha1"
	"github.com/kcp-dev/kcp/sdk/apis/third_party/conditions/util/conditions"
)

func TestReconcileSchema(t *testing.T) {
	tests := map[string]struct {
		CachedResource     *cachev1alpha1.CachedResource
		reconciler         *resourceSchema
		expectedErr        error
		expectedStatus     reconcileStatus
		expectedConditions conditionsv1alpha1.Conditions
		expectedPhase      cachev1alpha1.CachedResourcePhaseType
		expectedSchema     *cachev1alpha1.CachedAPIResourceSchema
	}{
		"has deletion timestamp": {
			CachedResource: &cachev1alpha1.CachedResource{
				ObjectMeta: metav1.ObjectMeta{
					DeletionTimestamp: &metav1.Time{},
				},
			},
			reconciler:     &resourceSchema{},
			expectedStatus: reconcileStatusContinue,
		},
		"has Ready phase": {
			CachedResource: &cachev1alpha1.CachedResource{
				Status: cachev1alpha1.CachedResourceStatus{
					Phase: cachev1alpha1.CachedResourcePhaseReady,
				},
			},
			reconciler:     &resourceSchema{},
			expectedStatus: reconcileStatusContinue,
			expectedPhase:  cachev1alpha1.CachedResourcePhaseReady,
		},
		"LogicalCluster not found": {
			CachedResource: &cachev1alpha1.CachedResource{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{logicalcluster.AnnotationKey: "cluster"},
				},
				Status: cachev1alpha1.CachedResourceStatus{
					Phase: cachev1alpha1.CachedResourcePhaseInitializing,
				},
			},
			reconciler: &resourceSchema{
				getLogicalCluster: func(cluster logicalcluster.Name) (*corev1alpha1.LogicalCluster, error) {
					return nil, apierrors.NewNotFound(corev1alpha1.Resource("logicalclusters"), "cluster")
				},
			},
			expectedStatus: reconcileStatusStopAndRequeue,
			expectedPhase:  cachev1alpha1.CachedResourcePhaseInitializing,
			expectedErr:    apierrors.NewNotFound(corev1alpha1.Resource("logicalclusters"), "cluster"),
			expectedConditions: conditionsv1alpha1.Conditions{
				*conditions.FalseCondition(
					cachev1alpha1.CachedResourceValid,
					cachev1alpha1.APIResourceSchemaInvalidReason,
					conditionsv1alpha1.ConditionSeverityError,
					"Error getting LogicalCluster cluster|cluster: %v",
					apierrors.NewNotFound(corev1alpha1.Resource("logicalclusters"), "cluster"),
				),
			},
		},
		"LogicalCluster bad internal.apis.kcp.io/resource-bindings annotation": {
			CachedResource: &cachev1alpha1.CachedResource{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{logicalcluster.AnnotationKey: "cluster"},
				},
				Status: cachev1alpha1.CachedResourceStatus{
					Phase: cachev1alpha1.CachedResourcePhaseInitializing,
				},
			},
			reconciler: &resourceSchema{
				getLogicalCluster: func(cluster logicalcluster.Name) (*corev1alpha1.LogicalCluster, error) {
					return &corev1alpha1.LogicalCluster{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{apibinding.ResourceBindingsAnnotationKey: "xxx"},
						},
					}, nil
				},
			},
			expectedStatus: reconcileStatusStop,
			expectedPhase:  cachev1alpha1.CachedResourcePhaseInitializing,
			expectedErr:    fmt.Errorf("failed to unmarshal ResourceBindings annotation: invalid character 'x' looking for beginning of value"),
			expectedConditions: conditionsv1alpha1.Conditions{
				*conditions.FalseCondition(
					cachev1alpha1.CachedResourceValid,
					cachev1alpha1.APIResourceSchemaInvalidReason,
					conditionsv1alpha1.ConditionSeverityError,
					"Error reading bound resources on LogicalCluster cluster|cluster: failed to unmarshal ResourceBindings annotation: invalid character 'x' looking for beginning of value",
				),
			},
		},
		"LogicalCluster missing internal.apis.kcp.io/resource-bindings annotation": {
			CachedResource: &cachev1alpha1.CachedResource{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{logicalcluster.AnnotationKey: "cluster"},
				},
				Status: cachev1alpha1.CachedResourceStatus{
					Phase: cachev1alpha1.CachedResourcePhaseInitializing,
				},
				Spec: cachev1alpha1.CachedResourceSpec{
					GroupVersionResource: cachev1alpha1.GroupVersionResource{
						Group:    "wildwest.dev",
						Resource: "cowboys",
						Version:  "v1alpha1",
					},
				},
			},
			reconciler: &resourceSchema{
				getLogicalCluster: func(cluster logicalcluster.Name) (*corev1alpha1.LogicalCluster, error) {
					return &corev1alpha1.LogicalCluster{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{apibinding.ResourceBindingsAnnotationKey: `{"some.other.api": {"n": "binding-name"}}`},
						},
					}, nil
				},
			},
			expectedStatus: reconcileStatusStop,
			expectedPhase:  cachev1alpha1.CachedResourcePhaseInitializing,
			expectedErr:    nil,
			expectedConditions: conditionsv1alpha1.Conditions{
				*conditions.FalseCondition(
					cachev1alpha1.CachedResourceValid,
					cachev1alpha1.APIResourceSchemaInvalidReason,
					conditionsv1alpha1.ConditionSeverityError,
					"API cowboys.wildwest.dev not ready",
				),
			},
		},
		"CRD-based resource not accepted": {
			CachedResource: &cachev1alpha1.CachedResource{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{logicalcluster.AnnotationKey: "cluster"},
				},
				Status: cachev1alpha1.CachedResourceStatus{
					Phase: cachev1alpha1.CachedResourcePhaseInitializing,
				},
				Spec: cachev1alpha1.CachedResourceSpec{
					GroupVersionResource: cachev1alpha1.GroupVersionResource{
						Group:    "wildwest.dev",
						Resource: "cowboys",
						Version:  "v1alpha1",
					},
				},
			},
			reconciler: &resourceSchema{
				getLogicalCluster: func(cluster logicalcluster.Name) (*corev1alpha1.LogicalCluster, error) {
					return &corev1alpha1.LogicalCluster{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{apibinding.ResourceBindingsAnnotationKey: `{"cowboys.wildwest.dev": {"c": true}}`},
						},
					}, nil
				},
			},
			expectedStatus: reconcileStatusStop,
			expectedPhase:  cachev1alpha1.CachedResourcePhaseInitializing,
			expectedErr:    nil,
			expectedConditions: conditionsv1alpha1.Conditions{
				*conditions.FalseCondition(
					cachev1alpha1.CachedResourceValid,
					cachev1alpha1.APIResourceSchemaInvalidReason,
					conditionsv1alpha1.ConditionSeverityError,
					"Resource cowboys.wildwest.dev is not backed by an APIResourceSchema. Please contact the APIExport owner to resolve.",
				),
			},
		},
		"missing APIBinding": {
			CachedResource: &cachev1alpha1.CachedResource{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{logicalcluster.AnnotationKey: "cluster"},
				},
				Status: cachev1alpha1.CachedResourceStatus{
					Phase: cachev1alpha1.CachedResourcePhaseInitializing,
				},
				Spec: cachev1alpha1.CachedResourceSpec{
					GroupVersionResource: cachev1alpha1.GroupVersionResource{
						Group:    "wildwest.dev",
						Resource: "cowboys",
						Version:  "v1alpha1",
					},
				},
			},
			reconciler: &resourceSchema{
				getLogicalCluster: func(cluster logicalcluster.Name) (*corev1alpha1.LogicalCluster, error) {
					return &corev1alpha1.LogicalCluster{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{apibinding.ResourceBindingsAnnotationKey: `{"cowboys.wildwest.dev": {"n": "cowboys-apibinding"}}`},
						},
					}, nil
				},
				getAPIBinding: func(cluster logicalcluster.Name, name string) (*apisv1alpha2.APIBinding, error) {
					return nil, apierrors.NewNotFound(apisv1alpha2.Resource("apibindings"), name)
				},
			},
			expectedStatus: reconcileStatusStopAndRequeue,
			expectedPhase:  cachev1alpha1.CachedResourcePhaseInitializing,
			expectedErr:    apierrors.NewNotFound(apisv1alpha2.Resource("apibindings"), "cowboys-apibinding"),
			expectedConditions: conditionsv1alpha1.Conditions{
				*conditions.FalseCondition(
					cachev1alpha1.CachedResourceValid,
					cachev1alpha1.APIResourceSchemaInvalidReason,
					conditionsv1alpha1.ConditionSeverityError,
					`Error getting APIBinding cluster|cowboys-apibinding: apibindings.apis.kcp.io "cowboys-apibinding" not found`,
				),
			},
		},
		"missing APIExport": {
			CachedResource: &cachev1alpha1.CachedResource{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{logicalcluster.AnnotationKey: "cluster"},
				},
				Status: cachev1alpha1.CachedResourceStatus{
					Phase: cachev1alpha1.CachedResourcePhaseInitializing,
				},
				Spec: cachev1alpha1.CachedResourceSpec{
					GroupVersionResource: cachev1alpha1.GroupVersionResource{
						Group:    "wildwest.dev",
						Resource: "cowboys",
						Version:  "v1alpha1",
					},
				},
			},
			reconciler: &resourceSchema{
				getLogicalCluster: func(cluster logicalcluster.Name) (*corev1alpha1.LogicalCluster, error) {
					return &corev1alpha1.LogicalCluster{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{apibinding.ResourceBindingsAnnotationKey: `{"cowboys.wildwest.dev": {"n": "cowboys-apibinding"}}`},
						},
					}, nil
				},
				getAPIBinding: func(cluster logicalcluster.Name, name string) (*apisv1alpha2.APIBinding, error) {
					return &apisv1alpha2.APIBinding{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{logicalcluster.AnnotationKey: "cluster"},
						},
						Spec: apisv1alpha2.APIBindingSpec{
							Reference: apisv1alpha2.BindingReference{
								Export: &apisv1alpha2.ExportBindingReference{
									Path: "provider",
									Name: "cowboys-apiexport",
								},
							},
						},
					}, nil
				},
				getAPIExport: func(path logicalcluster.Path, name string) (*apisv1alpha2.APIExport, error) {
					return nil, apierrors.NewNotFound(apisv1alpha2.Resource("apiexports"), name)
				},
			},
			expectedStatus: reconcileStatusStopAndRequeue,
			expectedPhase:  cachev1alpha1.CachedResourcePhaseInitializing,
			expectedErr:    apierrors.NewNotFound(apisv1alpha2.Resource("apiexports"), "cowboys-apiexport"),
			expectedConditions: conditionsv1alpha1.Conditions{
				*conditions.FalseCondition(
					cachev1alpha1.CachedResourceValid,
					cachev1alpha1.APIResourceSchemaInvalidReason,
					conditionsv1alpha1.ConditionSeverityError,
					`Error getting APIExport provider|cowboys-apiexport for APIBinding cluster|cowboys-apibinding: apiexports.apis.kcp.io "cowboys-apiexport" not found`,
				),
			},
		},
		"missing cowboys resource in APIExport": {
			CachedResource: &cachev1alpha1.CachedResource{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{logicalcluster.AnnotationKey: "cluster"},
				},
				Status: cachev1alpha1.CachedResourceStatus{
					Phase: cachev1alpha1.CachedResourcePhaseInitializing,
				},
				Spec: cachev1alpha1.CachedResourceSpec{
					GroupVersionResource: cachev1alpha1.GroupVersionResource{
						Group:    "wildwest.dev",
						Resource: "cowboys",
						Version:  "v1alpha1",
					},
				},
			},
			reconciler: &resourceSchema{
				getLogicalCluster: func(cluster logicalcluster.Name) (*corev1alpha1.LogicalCluster, error) {
					return &corev1alpha1.LogicalCluster{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{apibinding.ResourceBindingsAnnotationKey: `{"cowboys.wildwest.dev": {"n": "cowboys-apibinding"}}`},
						},
					}, nil
				},
				getAPIBinding: func(cluster logicalcluster.Name, name string) (*apisv1alpha2.APIBinding, error) {
					return &apisv1alpha2.APIBinding{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{logicalcluster.AnnotationKey: "cluster"},
						},
						Spec: apisv1alpha2.APIBindingSpec{
							Reference: apisv1alpha2.BindingReference{
								Export: &apisv1alpha2.ExportBindingReference{
									Path: "provider",
									Name: "cowboys-apiexport",
								},
							},
						},
					}, nil
				},
				getAPIExport: func(path logicalcluster.Path, name string) (*apisv1alpha2.APIExport, error) {
					return &apisv1alpha2.APIExport{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{logicalcluster.AnnotationKey: "provider"},
						},
						Spec: apisv1alpha2.APIExportSpec{
							Resources: []apisv1alpha2.ResourceSchema{
								{
									Group: "wildwest.dev",
									Name:  "cowgirls",
								},
							},
						},
					}, nil
				},
			},
			expectedStatus: reconcileStatusStop,
			expectedPhase:  cachev1alpha1.CachedResourcePhaseInitializing,
			expectedErr:    nil,
			expectedConditions: conditionsv1alpha1.Conditions{
				*conditions.FalseCondition(
					cachev1alpha1.CachedResourceValid,
					cachev1alpha1.APIResourceSchemaInvalidReason,
					conditionsv1alpha1.ConditionSeverityError,
					`No valid cowboys.wildwest.dev resource in APIExport provider|cowboys-apiexport. Please contact the APIExport owner to resolve.`,
				),
			},
		},
		"cowboys resource in APIExport with invalid storage": {
			CachedResource: &cachev1alpha1.CachedResource{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{logicalcluster.AnnotationKey: "cluster"},
				},
				Status: cachev1alpha1.CachedResourceStatus{
					Phase: cachev1alpha1.CachedResourcePhaseInitializing,
				},
				Spec: cachev1alpha1.CachedResourceSpec{
					GroupVersionResource: cachev1alpha1.GroupVersionResource{
						Group:    "wildwest.dev",
						Resource: "cowboys",
						Version:  "v1alpha1",
					},
				},
			},
			reconciler: &resourceSchema{
				getLogicalCluster: func(cluster logicalcluster.Name) (*corev1alpha1.LogicalCluster, error) {
					return &corev1alpha1.LogicalCluster{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{apibinding.ResourceBindingsAnnotationKey: `{"cowboys.wildwest.dev": {"n": "cowboys-apibinding"}}`},
						},
					}, nil
				},
				getAPIBinding: func(cluster logicalcluster.Name, name string) (*apisv1alpha2.APIBinding, error) {
					return &apisv1alpha2.APIBinding{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{logicalcluster.AnnotationKey: "cluster"},
						},
						Spec: apisv1alpha2.APIBindingSpec{
							Reference: apisv1alpha2.BindingReference{
								Export: &apisv1alpha2.ExportBindingReference{
									Path: "provider",
									Name: "cowboys-apiexport",
								},
							},
						},
					}, nil
				},
				getAPIExport: func(path logicalcluster.Path, name string) (*apisv1alpha2.APIExport, error) {
					return &apisv1alpha2.APIExport{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{logicalcluster.AnnotationKey: "provider"},
						},
						Spec: apisv1alpha2.APIExportSpec{
							Resources: []apisv1alpha2.ResourceSchema{
								{
									Group:   "wildwest.dev",
									Name:    "cowboys",
									Storage: apisv1alpha2.ResourceSchemaStorage{},
								},
							},
						},
					}, nil
				},
			},
			expectedStatus: reconcileStatusStop,
			expectedPhase:  cachev1alpha1.CachedResourcePhaseInitializing,
			expectedErr:    nil,
			expectedConditions: conditionsv1alpha1.Conditions{
				*conditions.FalseCondition(
					cachev1alpha1.CachedResourceValid,
					cachev1alpha1.APIResourceSchemaInvalidReason,
					conditionsv1alpha1.ConditionSeverityError,
					`No valid cowboys.wildwest.dev resource in APIExport provider|cowboys-apiexport. Please contact the APIExport owner to resolve.`,
				),
			},
		},
		"APIResourceSchema not found": {
			CachedResource: &cachev1alpha1.CachedResource{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{logicalcluster.AnnotationKey: "cluster"},
				},
				Status: cachev1alpha1.CachedResourceStatus{
					Phase: cachev1alpha1.CachedResourcePhaseInitializing,
				},
				Spec: cachev1alpha1.CachedResourceSpec{
					GroupVersionResource: cachev1alpha1.GroupVersionResource{
						Group:    "wildwest.dev",
						Resource: "cowboys",
						Version:  "v1alpha1",
					},
				},
			},
			reconciler: &resourceSchema{
				getLogicalCluster: func(cluster logicalcluster.Name) (*corev1alpha1.LogicalCluster, error) {
					return &corev1alpha1.LogicalCluster{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{apibinding.ResourceBindingsAnnotationKey: `{"cowboys.wildwest.dev": {"n": "cowboys-apibinding"}}`},
						},
					}, nil
				},
				getAPIBinding: func(cluster logicalcluster.Name, name string) (*apisv1alpha2.APIBinding, error) {
					return &apisv1alpha2.APIBinding{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{logicalcluster.AnnotationKey: "cluster"},
						},
						Spec: apisv1alpha2.APIBindingSpec{
							Reference: apisv1alpha2.BindingReference{
								Export: &apisv1alpha2.ExportBindingReference{
									Path: "provider",
									Name: "cowboys-apiexport",
								},
							},
						},
					}, nil
				},
				getAPIExport: func(path logicalcluster.Path, name string) (*apisv1alpha2.APIExport, error) {
					return &apisv1alpha2.APIExport{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{logicalcluster.AnnotationKey: "provider"},
						},
						Spec: apisv1alpha2.APIExportSpec{
							Resources: []apisv1alpha2.ResourceSchema{
								{
									Group:  "wildwest.dev",
									Name:   "cowboys",
									Schema: "today.cowboys.wildwest.dev",
									Storage: apisv1alpha2.ResourceSchemaStorage{
										CRD: &apisv1alpha2.ResourceSchemaStorageCRD{},
									},
								},
							},
						},
					}, nil
				},
				getAPIResourceSchema: func(cluster logicalcluster.Name, name string) (*apisv1alpha1.APIResourceSchema, error) {
					return nil, apierrors.NewNotFound(apisv1alpha1.Resource("apiresourceschemas"), name)
				},
			},
			expectedStatus: reconcileStatusStopAndRequeue,
			expectedPhase:  cachev1alpha1.CachedResourcePhaseInitializing,
			expectedErr:    apierrors.NewNotFound(apisv1alpha1.Resource("apiresourceschemas"), "today.cowboys.wildwest.dev"),
			expectedConditions: conditionsv1alpha1.Conditions{
				*conditions.FalseCondition(
					cachev1alpha1.CachedResourceValid,
					cachev1alpha1.APIResourceSchemaInvalidReason,
					conditionsv1alpha1.ConditionSeverityError,
					`Error getting APIResourceSchema provider|today.cowboys.wildwest.dev: apiresourceschemas.apis.kcp.io "today.cowboys.wildwest.dev" not found`,
				),
			},
		},
		"CachedAPIResourceSchema is set": {
			CachedResource: &cachev1alpha1.CachedResource{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{logicalcluster.AnnotationKey: "cluster"},
				},
				Status: cachev1alpha1.CachedResourceStatus{
					Phase: cachev1alpha1.CachedResourcePhaseInitializing,
				},
				Spec: cachev1alpha1.CachedResourceSpec{
					GroupVersionResource: cachev1alpha1.GroupVersionResource{
						Group:    "wildwest.dev",
						Resource: "cowboys",
						Version:  "v1alpha1",
					},
				},
			},
			reconciler: &resourceSchema{
				getLogicalCluster: func(cluster logicalcluster.Name) (*corev1alpha1.LogicalCluster, error) {
					return &corev1alpha1.LogicalCluster{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{apibinding.ResourceBindingsAnnotationKey: `{"cowboys.wildwest.dev": {"n": "cowboys-apibinding"}}`},
						},
					}, nil
				},
				getAPIBinding: func(cluster logicalcluster.Name, name string) (*apisv1alpha2.APIBinding, error) {
					return &apisv1alpha2.APIBinding{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{logicalcluster.AnnotationKey: "cluster"},
						},
						Spec: apisv1alpha2.APIBindingSpec{
							Reference: apisv1alpha2.BindingReference{
								Export: &apisv1alpha2.ExportBindingReference{
									Path: "provider",
									Name: "cowboys-apiexport",
								},
							},
						},
					}, nil
				},
				getAPIExport: func(path logicalcluster.Path, name string) (*apisv1alpha2.APIExport, error) {
					return &apisv1alpha2.APIExport{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{logicalcluster.AnnotationKey: "provider"},
						},
						Spec: apisv1alpha2.APIExportSpec{
							Resources: []apisv1alpha2.ResourceSchema{
								{
									Group:  "wildwest.dev",
									Name:   "cowboys",
									Schema: "today.cowboys.wildwest.dev",
									Storage: apisv1alpha2.ResourceSchemaStorage{
										CRD: &apisv1alpha2.ResourceSchemaStorageCRD{},
									},
								},
							},
						},
					}, nil
				},
				getAPIResourceSchema: func(cluster logicalcluster.Name, name string) (*apisv1alpha1.APIResourceSchema, error) {
					return &apisv1alpha1.APIResourceSchema{}, nil
				},
			},
			expectedStatus:     reconcileStatusStopAndRequeue,
			expectedPhase:      cachev1alpha1.CachedResourcePhaseInitializing,
			expectedErr:        nil,
			expectedConditions: nil,
			expectedSchema: &cachev1alpha1.CachedAPIResourceSchema{
				Name:    "today.cowboys.wildwest.dev",
				Cluster: "provider",
			},
		},
		"CachedAPIResourceSchema is updated": {
			CachedResource: &cachev1alpha1.CachedResource{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{logicalcluster.AnnotationKey: "cluster"},
				},
				Status: cachev1alpha1.CachedResourceStatus{
					Phase: cachev1alpha1.CachedResourcePhaseInitializing,
					Schema: &cachev1alpha1.CachedAPIResourceSchema{
						Name:    "xxx",
						Cluster: "yyy",
					},
				},
				Spec: cachev1alpha1.CachedResourceSpec{
					GroupVersionResource: cachev1alpha1.GroupVersionResource{
						Group:    "wildwest.dev",
						Resource: "cowboys",
						Version:  "v1alpha1",
					},
				},
			},
			reconciler: &resourceSchema{
				getLogicalCluster: func(cluster logicalcluster.Name) (*corev1alpha1.LogicalCluster, error) {
					return &corev1alpha1.LogicalCluster{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{apibinding.ResourceBindingsAnnotationKey: `{"cowboys.wildwest.dev": {"n": "cowboys-apibinding"}}`},
						},
					}, nil
				},
				getAPIBinding: func(cluster logicalcluster.Name, name string) (*apisv1alpha2.APIBinding, error) {
					return &apisv1alpha2.APIBinding{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{logicalcluster.AnnotationKey: "cluster"},
						},
						Spec: apisv1alpha2.APIBindingSpec{
							Reference: apisv1alpha2.BindingReference{
								Export: &apisv1alpha2.ExportBindingReference{
									Path: "provider",
									Name: "cowboys-apiexport",
								},
							},
						},
					}, nil
				},
				getAPIExport: func(path logicalcluster.Path, name string) (*apisv1alpha2.APIExport, error) {
					return &apisv1alpha2.APIExport{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{logicalcluster.AnnotationKey: "provider"},
						},
						Spec: apisv1alpha2.APIExportSpec{
							Resources: []apisv1alpha2.ResourceSchema{
								{
									Group:  "wildwest.dev",
									Name:   "cowboys",
									Schema: "today.cowboys.wildwest.dev",
									Storage: apisv1alpha2.ResourceSchemaStorage{
										CRD: &apisv1alpha2.ResourceSchemaStorageCRD{},
									},
								},
							},
						},
					}, nil
				},
				getAPIResourceSchema: func(cluster logicalcluster.Name, name string) (*apisv1alpha1.APIResourceSchema, error) {
					return &apisv1alpha1.APIResourceSchema{}, nil
				},
			},
			expectedStatus:     reconcileStatusStopAndRequeue,
			expectedPhase:      cachev1alpha1.CachedResourcePhaseInitializing,
			expectedErr:        nil,
			expectedConditions: nil,
			expectedSchema: &cachev1alpha1.CachedAPIResourceSchema{
				Name:    "today.cowboys.wildwest.dev",
				Cluster: "provider",
			},
		},
		"CachedAPIResourceSchema already set": {
			CachedResource: &cachev1alpha1.CachedResource{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{logicalcluster.AnnotationKey: "cluster"},
				},
				Status: cachev1alpha1.CachedResourceStatus{
					Phase: cachev1alpha1.CachedResourcePhaseInitializing,
					Schema: &cachev1alpha1.CachedAPIResourceSchema{
						Name:    "today.cowboys.wildwest.dev",
						Cluster: "provider",
					},
				},
				Spec: cachev1alpha1.CachedResourceSpec{
					GroupVersionResource: cachev1alpha1.GroupVersionResource{
						Group:    "wildwest.dev",
						Resource: "cowboys",
						Version:  "v1alpha1",
					},
				},
			},
			reconciler: &resourceSchema{
				getLogicalCluster: func(cluster logicalcluster.Name) (*corev1alpha1.LogicalCluster, error) {
					return &corev1alpha1.LogicalCluster{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{apibinding.ResourceBindingsAnnotationKey: `{"cowboys.wildwest.dev": {"n": "cowboys-apibinding"}}`},
						},
					}, nil
				},
				getAPIBinding: func(cluster logicalcluster.Name, name string) (*apisv1alpha2.APIBinding, error) {
					return &apisv1alpha2.APIBinding{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{logicalcluster.AnnotationKey: "cluster"},
						},
						Spec: apisv1alpha2.APIBindingSpec{
							Reference: apisv1alpha2.BindingReference{
								Export: &apisv1alpha2.ExportBindingReference{
									Path: "provider",
									Name: "cowboys-apiexport",
								},
							},
						},
					}, nil
				},
				getAPIExport: func(path logicalcluster.Path, name string) (*apisv1alpha2.APIExport, error) {
					return &apisv1alpha2.APIExport{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{logicalcluster.AnnotationKey: "provider"},
						},
						Spec: apisv1alpha2.APIExportSpec{
							Resources: []apisv1alpha2.ResourceSchema{
								{
									Group:  "wildwest.dev",
									Name:   "cowboys",
									Schema: "today.cowboys.wildwest.dev",
									Storage: apisv1alpha2.ResourceSchemaStorage{
										CRD: &apisv1alpha2.ResourceSchemaStorageCRD{},
									},
								},
							},
						},
					}, nil
				},
				getAPIResourceSchema: func(cluster logicalcluster.Name, name string) (*apisv1alpha1.APIResourceSchema, error) {
					return &apisv1alpha1.APIResourceSchema{}, nil
				},
			},
			expectedStatus:     reconcileStatusContinue,
			expectedPhase:      cachev1alpha1.CachedResourcePhaseInitializing,
			expectedErr:        nil,
			expectedConditions: nil,
			expectedSchema: &cachev1alpha1.CachedAPIResourceSchema{
				Name:    "today.cowboys.wildwest.dev",
				Cluster: "provider",
			},
		},
	}

	for testName, tt := range tests {
		t.Run(testName, func(t *testing.T) {
			status, err := tt.reconciler.reconcile(context.Background(), tt.CachedResource)
			for i := range tt.expectedConditions {
				tt.expectedConditions[i].LastTransitionTime = metav1.Time{}
			}
			for i := range tt.CachedResource.Status.Conditions {
				tt.CachedResource.Status.Conditions[i].LastTransitionTime = metav1.Time{}
			}
			if tt.expectedErr != nil {
				require.Error(t, err)
				require.Equal(t, tt.expectedErr.Error(), err.Error())
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.expectedStatus, status)
			require.Equal(t, tt.expectedPhase, tt.CachedResource.Status.Phase)
			require.Equal(t, tt.expectedConditions, tt.CachedResource.Status.Conditions)
			require.Equal(t, tt.expectedSchema, tt.CachedResource.Status.Schema)
		})
	}
}
