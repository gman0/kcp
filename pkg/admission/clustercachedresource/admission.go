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

package clustercachedresource

import (
	"context"
	"fmt"
	"io"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/admission"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/tools/cache"

	"github.com/kcp-dev/logicalcluster/v3"
	cachev1alpha1 "github.com/kcp-dev/sdk/apis/cache/v1alpha1"
	kcpinformers "github.com/kcp-dev/sdk/client/informers/externalversions"

	"github.com/kcp-dev/kcp/pkg/indexers"
	clustercachedresourcesreconciler "github.com/kcp-dev/kcp/pkg/reconciler/cache/clustercachedresources"
	"github.com/kcp-dev/kcp/pkg/reconciler/dynamicrestmapper"
)

// PluginName is the name used to identify this admission webhook.
const PluginName = "apis.kcp.io/ClusterCachedResource"

// Ensure that the required admission interfaces are implemented.
var (
	_ = admission.ValidationInterface(&ClusterCachedResourceAdmission{})
	_ = admission.InitializationValidator(&ClusterCachedResourceAdmission{})
)

// Register registers the reserved name admission webhook.
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName,
		func(_ io.Reader) (admission.Interface, error) {
			p := &ClusterCachedResourceAdmission{
				Handler: admission.NewHandler(admission.Create),
			}
			p.listClusterCachedResourcesByGR = func(cluster logicalcluster.Name, gr schema.GroupResource) ([]*cachev1alpha1.ClusterCachedResource, error) {
				return indexers.ByIndex[*cachev1alpha1.ClusterCachedResource](
					p.localClusterCachedResourcesIndexer,
					clustercachedresourcesreconciler.ByGRAndLogicalCluster,
					clustercachedresourcesreconciler.GRAndLogicalClusterKey(gr, cluster),
				)
			}

			return p, nil
		})
}

// ClusterCachedResourceAdmission is an admission plugin for checking APIExport validity.
type ClusterCachedResourceAdmission struct {
	*admission.Handler

	listClusterCachedResourcesByGR     func(cluster logicalcluster.Name, gr schema.GroupResource) ([]*cachev1alpha1.ClusterCachedResource, error)
	localClusterCachedResourcesIndexer cache.Indexer

	dynamicRESTMapper *dynamicrestmapper.DynamicRESTMapper
}

func (adm *ClusterCachedResourceAdmission) SetKcpInformers(local, global kcpinformers.SharedInformerFactory) {
	adm.SetReadyFunc(func() bool {
		return local.Cache().V1alpha1().ClusterCachedResources().Informer().HasSynced()
	})
	adm.localClusterCachedResourcesIndexer = local.Cache().V1alpha1().ClusterCachedResources().Informer().GetIndexer()

	indexers.AddIfNotPresentOrDie(local.Cache().V1alpha1().ClusterCachedResources().Informer().GetIndexer(), cache.Indexers{
		clustercachedresourcesreconciler.ByGRAndLogicalCluster: clustercachedresourcesreconciler.IndexByGRAndLogicalCluster,
	})
}

func (adm *ClusterCachedResourceAdmission) SetDynamicRESTMapper(dynamicRESTMapper *dynamicrestmapper.DynamicRESTMapper) {
	adm.dynamicRESTMapper = dynamicRESTMapper
}

// ValidateInitialization ensures the required injected fields are set.
func (adm *ClusterCachedResourceAdmission) ValidateInitialization() error {
	if adm.localClusterCachedResourcesIndexer == nil {
		return fmt.Errorf(PluginName + " plugin needs a ClusterCachedResource indexer")
	}
	return nil
}

// Validate ensures that the APIExport is valid.
func (adm *ClusterCachedResourceAdmission) Validate(ctx context.Context, a admission.Attributes, _ admission.ObjectInterfaces) error {
	if a.GetResource().GroupResource() != cachev1alpha1.Resource("clustercachedresources") || a.GetKind().GroupKind() != cachev1alpha1.Kind("ClusterCachedResource") {
		return nil
	}
	if a.GetOperation() != admission.Create {
		return nil
	}

	u, ok := a.GetObject().(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("unexpected type %T", a.GetObject())
	}

	clusterCachedResource := &cachev1alpha1.ClusterCachedResource{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, clusterCachedResource); err != nil {
		return fmt.Errorf("failed to convert unstructured to ClusterCachedResource: %w", err)
	}

	return adm.validateV1alpha1(ctx, a, clusterCachedResource)
}

func (adm *ClusterCachedResourceAdmission) validateV1alpha1(ctx context.Context, a admission.Attributes, clusterCachedResource *cachev1alpha1.ClusterCachedResource) error {
	clusterName, err := genericapirequest.ClusterNameFrom(ctx)
	if err != nil {
		return apierrors.NewInternalError(err)
	}

	gr := schema.GroupResource{
		Group:    clusterCachedResource.Spec.Group,
		Resource: clusterCachedResource.Spec.Resource,
	}

	// Advisory scope check — use a partial GVR (no version) so the REST mapper resolves
	// the preferred version. Errors are ignored: the controller will set a condition if needed.
	partialGVR := schema.GroupVersionResource{Group: gr.Group, Resource: gr.Resource}
	scopedDynRESTMapper := adm.dynamicRESTMapper.ForCluster(clusterName)
	kind, err := scopedDynRESTMapper.KindFor(partialGVR)
	if err == nil {
		mapping, err := scopedDynRESTMapper.RESTMapping(kind.GroupKind(), kind.Version)
		if err == nil {
			if mapping.Scope != meta.RESTScopeRoot {
				return admission.NewForbidden(a,
					field.Invalid(
						field.NewPath("spec"),
						gr.String(),
						"Resource referenced in ClusterCachedResource must be cluster-scoped",
					),
				)
			}
		}
	}

	existing, err := adm.listClusterCachedResourcesByGR(clusterName, gr)
	if err != nil {
		return err
	}

	// Make sure there is at most one ClusterCachedResource per group+resource. An entry that
	// matches the incoming object's name is not a real conflict — it is a
	// re-apply of the same object, which the storage layer will surface
	// naturally as AlreadyExists. Rejecting here would mask that error
	// behind a misleading Forbidden and break idempotent client flows.
	for _, e := range existing {
		if e.Name == clusterCachedResource.Name {
			continue
		}
		return admission.NewForbidden(a,
			field.Invalid(
				field.NewPath("spec"),
				gr.String(),
				fmt.Sprintf("ClusterCachedResource for this group+resource already exists in the %q workspace", clusterName)),
		)
	}

	return nil
}
