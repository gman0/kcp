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

package builder

import (
	"context"
	"net/http"

	"fmt"

	"github.com/kcp-dev/kcp/pkg/virtual/replication/apidomainkey"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/storage"
	storageerrors "k8s.io/apiserver/pkg/storage/errors"

	// "github.com/kcp-dev/logicalcluster/v3"

	// cacheclient "github.com/kcp-dev/kcp/pkg/cache/client"
	// "github.com/kcp-dev/kcp/pkg/cache/client/shard"
	"github.com/kcp-dev/kcp/pkg/reconciler/cache/cachedresources/replication"
	dynamiccontext "github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/context"
	"github.com/kcp-dev/kcp/pkg/virtual/framework/forwardingregistry"
	apisv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha1"
	cachev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/cache/v1alpha1"
	kcpclientset "github.com/kcp-dev/kcp/sdk/client/clientset/versioned/cluster"
)

func unwrapCachedObject(obj *cachev1alpha1.CachedObject) (*unstructured.Unstructured, error) {
	inner := &unstructured.Unstructured{}
	if err := inner.UnmarshalJSON(obj.Spec.Raw.Raw); err != nil {
		return nil, fmt.Errorf("failed to decode inner object: %w", err)
	}
	return inner, nil
}

func withUnwrapping(sch *apisv1alpha1.APIResourceSchema, version string, kcpCacheClusterClient kcpclientset.ClusterInterface) forwardingregistry.StorageWrapper {
	wrappedGVR := schema.GroupVersionResource{
		Group:    sch.Spec.Group,
		Version:  version,
		Resource: sch.Spec.Names.Plural,
	}

	namespaced := sch.Spec.Scope == apiextensionsv1.NamespaceScoped
	buildCachedObjName := func(gvr schema.GroupVersionResource, ns, resName string) string {
		if gvr.Group == "" {
			gvr.Group = "core"
		}
		cachedObjName := fmt.Sprintf("%s.%s.%s.%s", gvr.Version, gvr.Resource, gvr.Group, resName)
		if namespaced {
			cachedObjName += "." + ns
		}

		return cachedObjName
	}

	return forwardingregistry.StorageWrapperFunc(func(resource schema.GroupResource, storage *forwardingregistry.StoreFuncs) {
		storage.GetterFunc = func(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
			parsedKey, err := apidomainkey.Parse(dynamiccontext.APIDomainKeyFrom(ctx))
			if err != nil {
				return nil, fmt.Errorf("invalid API domain key: %v", err)
			}

			cachedObjName := buildCachedObjName(schema.GroupVersionResource(wrappedGVR), genericapirequest.NamespaceValue(ctx), name)
			cachedObj, err := kcpCacheClusterClient.CacheV1alpha1().CachedObjects().Cluster(parsedKey.APIExportCluster.Path()).
				Get(ctx, cachedObjName, *options)
			if err != nil {
				return nil, fmt.Errorf("failed to get CachedObject %s for resource %s %s: %v", cachedObjName, wrappedGVR, name, err)
			}
			// TODO: add selectors
			return unwrapCachedObject(cachedObj)
		}
		storage.WatcherFunc = func(ctx context.Context, options *metainternalversion.ListOptions) (watch.Interface, error) {
			parsedKey, err := apidomainkey.Parse(dynamiccontext.APIDomainKeyFrom(ctx))
			if err != nil {
				return nil, fmt.Errorf("invalid API domain key: %v", err)
			}

			innerGVR := schema.GroupVersionResource(wrappedGVR)
			if innerGVR.Group == "" {
				innerGVR.Group = "core"
			}

			if err := checkCrossNamespaceAndWildcard(ctx, innerGVR, namespaced); err != nil {
				return nil, err
			}

			var listOpts metav1.ListOptions
			if err := metainternalversion.Convert_internalversion_ListOptions_To_v1_ListOptions(options, &listOpts, nil); err != nil {
				return nil, err
			}

			labelMap := map[string]string{
				replication.LabelKeyObjectGroup:    innerGVR.Group,
				replication.LabelKeyObjectVersion:  innerGVR.Version,
				replication.LabelKeyObjectResource: innerGVR.Resource,
			}
			if namespaced {
				if requestNamespace, hasNamespace := genericapirequest.NamespaceFrom(ctx); hasNamespace {
					labelMap[replication.LabelKeyObjectOriginalNamespace] = requestNamespace
				}
			}

			listOpts.SetGroupVersionKind(cachev1alpha1.SchemeGroupVersion.WithKind("CachedResource"))
			listOpts.LabelSelector = labels.FormatLabels(labelMap)
			listOpts.FieldSelector = ""

			watchCtx, cancelFn := context.WithCancel(ctx)
			go func() {
				select {
				case <-ctx.Done():
					cancelFn()
				case <-watchCtx.Done():
					return
				}
			}()

			cachedObjWatch, err := kcpCacheClusterClient.Cluster(parsedKey.APIExportCluster.Path()).CacheV1alpha1().CachedObjects().
				Watch(watchCtx, listOpts)
			if err != nil {
				return nil, err
			}

			return newUnwrappingWatch(cachedObjWatch, innerGVR.GroupResource(), options, namespaced), nil
		}
		storage.ListerFunc = func(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
			parsedKey, err := apidomainkey.Parse(dynamiccontext.APIDomainKeyFrom(ctx))
			if err != nil {
				return nil, fmt.Errorf("invalid API domain key: %v", err)
			}

			innerGVR := schema.GroupVersionResource(wrappedGVR)
			if innerGVR.Group == "" {
				innerGVR.Group = "core"
			}

			if err := checkCrossNamespaceAndWildcard(ctx, innerGVR, namespaced); err != nil {
				return nil, err
			}

			var listOpts metav1.ListOptions
			listOpts.TypeMeta = metav1.TypeMeta{}
			if err := metainternalversion.Convert_internalversion_ListOptions_To_v1_ListOptions(options, &listOpts, nil); err != nil {
				return nil, err
			}

			labelMap := map[string]string{
				replication.LabelKeyObjectGroup:    innerGVR.Group,
				replication.LabelKeyObjectVersion:  innerGVR.Version,
				replication.LabelKeyObjectResource: innerGVR.Resource,
			}
			// TODO(gman0): uncomment and finish this once replication for CachedResources fully supports namespaces.
			// if namespaced {
			// 	// Namespace must already be present in the context, otherwise
			// 	// checkCrossNamespaceAndWildcard would have failed earlier.
			// 	requestNamespace, _ := genericapirequest.NamespaceFrom(ctxWithShardAndCluster)
			// 	labelMap[replication.LabelKeyObjectOriginalNamespace] = requestNamespace
			// }

			listOpts.SetGroupVersionKind(cachev1alpha1.SchemeGroupVersion.WithKind("CachedResource"))
			listOpts.LabelSelector = labels.FormatLabels(labelMap)
			listOpts.FieldSelector = ""

			cachedObjs, err := kcpCacheClusterClient.CacheV1alpha1().CachedObjects().Cluster(parsedKey.APIExportCluster.Path()).
				List(ctx, listOpts)
			if err != nil {
				return nil, err
			}

			innerListGVK := schema.GroupVersionKind{
				Group:   wrappedGVR.Group,
				Version: wrappedGVR.Version,
				Kind:    sch.Spec.Names.ListKind,
			}
			if innerListGVK.Kind == "" {
				innerListGVK.Kind = sch.Spec.Names.Kind + "List"
			}

			return newUnwrappingList(innerListGVK, innerGVR.GroupResource(), cachedObjs, options, namespaced)
		}
	})
}

// apiErrorBadRequest returns a apierrors.StatusError with a BadRequest reason.
func apiErrorBadRequest(err error) *apierrors.StatusError {
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    http.StatusBadRequest,
		Message: err.Error(),
	}}
}

func checkCrossNamespaceAndWildcard(ctx context.Context, gvr schema.GroupVersionResource, namespaced bool) error {
	cluster, err := genericapirequest.ValidClusterFrom(ctx)
	if err != nil {
		return apiErrorBadRequest(err)
	}
	namespace, namespaceSet := genericapirequest.NamespaceFrom(ctx)

	if cluster.Wildcard {
		if namespaced && namespaceSet && namespace != metav1.NamespaceAll {
			return apiErrorBadRequest(fmt.Errorf("cross-cluster LIST and WATCH are required to be cross-namespace, not scoped to namespace %s", namespace))
		}
		return nil
	}

	if namespaced {
		if !namespaceSet {
			return apiErrorBadRequest(fmt.Errorf("there should be a Namespace context in a request for a namespaced resource: %s", gvr.String()))
		}
		return nil
	}

	return nil
}

type unwrappingWatch struct {
	resultChan chan watch.Event
	stop       func()
}

func newUnwrappingWatch(cachedObjWatch watch.Interface, innerObjGR schema.GroupResource, innerListOpts *metainternalversion.ListOptions, namespaced bool) *unwrappingWatch {
	w := &unwrappingWatch{
		resultChan: make(chan watch.Event),
	}

	label := labels.Everything()
	if innerListOpts != nil && innerListOpts.LabelSelector != nil {
		label = innerListOpts.LabelSelector
	}
	field := fields.Everything()
	if innerListOpts != nil && innerListOpts.FieldSelector != nil {
		field = innerListOpts.FieldSelector
	}
	attrFunc := storage.DefaultClusterScopedAttr
	if namespaced {
		attrFunc = storage.DefaultNamespaceScopedAttr
	}

	go func() {
		defer close(w.resultChan)
		defer w.Stop()

		for event := range cachedObjWatch.ResultChan() {
			cachedObj, ok := event.Object.(*cachev1alpha1.CachedObject)
			if !ok {
				w.resultChan <- watch.Event{
					Type:   watch.Error,
					Object: &apierrors.NewInternalError(fmt.Errorf("unexpected watch object: %T", event.Object)).ErrStatus,
				}
				continue
			}

			innerObj := &unstructured.Unstructured{}
			if err := innerObj.UnmarshalJSON(cachedObj.Spec.Raw.Raw); err != nil {
				w.resultChan <- watch.Event{
					Type:   watch.Error,
					Object: &apierrors.NewInternalError(fmt.Errorf("failed to decode inner object: %w", err)).ErrStatus,
				}
				continue
			}

			innerObj.SetResourceVersion(cachedObj.GetResourceVersion())

			innerLabels, innerFields, err := attrFunc(innerObj)
			if err != nil {
				w.resultChan <- watch.Event{
					Type:   watch.Error,
					Object: &apierrors.NewInternalError(fmt.Errorf("failed to get inner object attributes: %w", err)).ErrStatus,
				}
				continue
			}
			if !label.Matches(innerLabels) {
				continue
			}
			if !field.Matches(innerFields) {
				continue
			}

			w.resultChan <- watch.Event{
				Type:   event.Type,
				Object: innerObj,
			}
		}
	}()

	w.stop = cachedObjWatch.Stop
	return w
}

func (w *unwrappingWatch) Stop() {
	w.stop()
}

func (w *unwrappingWatch) ResultChan() <-chan watch.Event {
	return w.resultChan
}

func newUnwrappingList(innerListGVK schema.GroupVersionKind, innerObjGR schema.GroupResource, cachedObjList *cachev1alpha1.CachedObjectList, innerListOpts *metainternalversion.ListOptions, namespaced bool) (*unstructured.UnstructuredList, error) {
	innerList := &unstructured.UnstructuredList{}
	innerList.SetGroupVersionKind(innerListGVK)

	label := labels.Everything()
	if innerListOpts != nil && innerListOpts.LabelSelector != nil {
		label = innerListOpts.LabelSelector
	}
	field := fields.Everything()
	if innerListOpts != nil && innerListOpts.FieldSelector != nil {
		field = innerListOpts.FieldSelector
	}
	attrFunc := storage.DefaultClusterScopedAttr
	if namespaced {
		attrFunc = storage.DefaultNamespaceScopedAttr
	}

	for i := range cachedObjList.Items {
		item := &cachedObjList.Items[i]
		innerObj, err := unwrapCachedObject(item)
		if err != nil {
			return nil, fmt.Errorf("failed to unwrap item: %w", err)
		}

		innerLabels, innerFields, err := attrFunc(innerObj)
		if err != nil {
			return nil, storageerrors.InterpretListError(err, innerObjGR)
		}
		if !label.Matches(innerLabels) {
			continue
		}
		if !field.Matches(innerFields) {
			continue
		}

		innerObj.SetResourceVersion(item.GetResourceVersion())
		innerList.Items = append(innerList.Items, *innerObj)
	}

	innerList.SetResourceVersion(cachedObjList.GetResourceVersion())

	return innerList, nil
}
