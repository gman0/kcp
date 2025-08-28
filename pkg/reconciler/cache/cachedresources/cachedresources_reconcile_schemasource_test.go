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
	// "fmt"
	"testing"

	"github.com/stretchr/testify/require"

	// apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/kcp-dev/logicalcluster/v3"

	"github.com/kcp-dev/kcp/pkg/reconciler/apis/apibinding"
	// apisv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha1"

	// apisv1alpha2 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha2"
	cachev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/cache/v1alpha1"
	corev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/core/v1alpha1"
	conditionsv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/third_party/conditions/apis/conditions/v1alpha1"
	"github.com/kcp-dev/kcp/sdk/apis/third_party/conditions/util/conditions"
)

func TestReconcileSchema(t *testing.T) {
	tests := map[string]struct {
		CachedResource     *cachev1alpha1.CachedResource
		reconciler         *schemaSource
		expectedErr        error
		expectedStatus     reconcileStatus
		expectedConditions conditionsv1alpha1.Conditions
		expectedSchemaSrc  *cachev1alpha1.CachedResourceSchemaSource
	}{
		"has deletion timestamp and should skip": {
			CachedResource: &cachev1alpha1.CachedResource{
				ObjectMeta: metav1.ObjectMeta{
					DeletionTimestamp: ptr.To(metav1.Now()),
				},
			},
			reconciler:     &schemaSource{},
			expectedStatus: reconcileStatusContinue,
		},
		"has CachedResourceSchemaSourceValid=true condition and should skip": {
			CachedResource: &cachev1alpha1.CachedResource{
				Status: cachev1alpha1.CachedResourceStatus{
					Conditions: conditionsv1alpha1.Conditions{
						*conditions.TrueCondition(
							cachev1alpha1.CachedResourceSchemaSourceValid,
						),
					},
				},
			},
			reconciler:     &schemaSource{},
			expectedStatus: reconcileStatusContinue,
			expectedConditions: conditionsv1alpha1.Conditions{
				*conditions.TrueCondition(
					cachev1alpha1.CachedResourceSchemaSourceValid,
				),
			},
		},
		"resource originating from apibinding": {
			CachedResource: &cachev1alpha1.CachedResource{
				Spec: cachev1alpha1.CachedResourceSpec{
					GroupVersionResource: cachev1alpha1.GroupVersionResource{
						Group:    "wildwest.dev",
						Version:  "v1alpha1",
						Resource: "cowboys",
					},
				},
			},
			reconciler: &schemaSource{
				getLogicalCluster: func(cluster logicalcluster.Name) (*corev1alpha1.LogicalCluster, error) {
					return &corev1alpha1.LogicalCluster{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								apibinding.ResourceBindingsAnnotationKey: `{"cowboys.wildwest.dev": {"n": "cowboys-binding"}}`,
							},
						},
					}, nil
				},
			},
			expectedStatus: reconcileStatusStopAndRequeue,
			expectedConditions: conditionsv1alpha1.Conditions{
				*conditions.TrueCondition(
					cachev1alpha1.CachedResourceSchemaSourceValid,
				),
			},
			expectedSchemaSrc: &cachev1alpha1.CachedResourceSchemaSource{
				APIResourceSchema: &cachev1alpha1.APIResourceSchemaSource{},
			},
		},
	}

	for testName, tt := range tests {
		t.Run(testName, func(t *testing.T) {
			status, err := tt.reconciler.reconcile(context.Background(), tt.CachedResource)

			resetLastTransitionTime(tt.expectedConditions)
			resetLastTransitionTime(tt.CachedResource.Status.Conditions)

			if tt.expectedErr != nil {
				require.Error(t, err)
				require.Equal(t, tt.expectedErr.Error(), err.Error())
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.expectedStatus, status, "reconcile status mismatch")
			require.Equal(t, tt.expectedConditions, tt.CachedResource.Status.Conditions, "conditions mismatch")
			require.Equal(t, tt.expectedSchemaSrc, tt.CachedResource.Status.ResourceSchemaSource, "ResourceSchemaSource mismatch")
		})
	}
}

func resetLastTransitionTime(conditions conditionsv1alpha1.Conditions) {
	// We don't care about LastTransitionTime.
	for i := range conditions {
		conditions[i].LastTransitionTime = metav1.Time{}
	}
}
