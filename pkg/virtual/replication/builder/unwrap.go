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

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"

	"github.com/kcp-dev/logicalcluster/v3"

	"github.com/kcp-dev/kcp/pkg/virtual/framework/forwardingregistry"

	cacheclient "github.com/kcp-dev/kcp/pkg/cache/client"
	"github.com/kcp-dev/kcp/pkg/cache/client/shard"
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

func withUnwrapping(parentCtx context.Context, cachedResource *cachev1alpha1.CachedResource, sch *apisv1alpha1.APIResourceSchema, kcpCacheClusterClient kcpclientset.ClusterInterface) forwardingregistry.StorageWrapper {
	namespaceScoped := sch.Spec.Scope == apiextensionsv1.NamespaceScoped
	buildCachedObjName := func(gvr schema.GroupVersionResource, resName string) string {
		if gvr.Group == "" {
			gvr.Group = "core"
		}
		return fmt.Sprintf("%s.%s.%s.%s", gvr.Version, gvr.Resource, gvr.Group, resName)
	}
	return forwardingregistry.StorageWrapperFunc(func(resource schema.GroupResource, storage *forwardingregistry.StoreFuncs) {
		storage.GetterFunc = func(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
			ctxWithCluster := context.WithValue(cacheclient.WithShardInContext(ctx, shard.New("root")), logicalcluster.AnnotationKey, logicalcluster.From(cachedResource))
			cachedObj, err := kcpCacheClusterClient.Cluster(logicalcluster.From(cachedResource).Path()).CacheV1alpha1().CachedObjects().
				Get(ctxWithCluster, buildCachedObjName(schema.GroupVersionResource(cachedResource.Spec.GroupVersionResource), name), metav1.GetOptions{})
			if err != nil {
				return nil, err
			}

			return unwrapCachedObject(cachedObj)
		}
		storage.WatcherFunc = func(ctx context.Context, options *metainternalversion.ListOptions) (watch.Interface, error) {
			if err := checkCrossNamespaceAndWildcard(ctx, schema.GroupVersionResource(cachedResource.Spec.GroupVersionResource), namespaceScoped); err != nil {
				return nil, err
			}
			// TODO: Watch only resources with correct GVR labels
			var v1ListOptions metav1.ListOptions
			if err := metainternalversion.Convert_internalversion_ListOptions_To_v1_ListOptions(options, &v1ListOptions, nil); err != nil {
				return nil, err
			}

			watchCtx, cancelFn := context.WithCancel(ctx)
			go func() {
				select {
				case <-parentCtx.Done():
					cancelFn()
				case <-watchCtx.Done():
					return
				}
			}()

			cachedObjWatch, err := kcpCacheClusterClient.Cluster(logicalcluster.From(cachedResource).Path()).CacheV1alpha1().CachedObjects().
				Watch(watchCtx, v1ListOptions)
			if err != nil {
				return nil, err
			}

			return newUnwrappingWatch(cachedObjWatch), nil
		}
		storage.ListerFunc = func(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
			if err := checkCrossNamespaceAndWildcard(ctx, schema.GroupVersionResource(cachedResource.Spec.GroupVersionResource), namespaceScoped); err != nil {
				return nil, err
			}
			// TODO: Watch only resources with correct GVR labels
			var v1ListOptions metav1.ListOptions
			if err := metainternalversion.Convert_internalversion_ListOptions_To_v1_ListOptions(options, &v1ListOptions, nil); err != nil {
				return nil, err
			}

			cachedObjList, err := kcpCacheClusterClient.Cluster(logicalcluster.From(cachedResource).Path()).CacheV1alpha1().CachedObjects().
				List(ctx, v1ListOptions)
			if err != nil {
				return nil, err
			}

			innerListGVK := schema.GroupVersionKind{
				Group:   cachedResource.Spec.Group,
				Version: cachedResource.Spec.Version,
				Kind:    sch.Spec.Names.ListKind,
			}
			if innerListGVK.Kind == "" {
				innerListGVK.Kind = sch.Spec.Names.Kind + "List"
			}

			return newUnwrappingList(innerListGVK, cachedObjList)
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

func checkCrossNamespaceAndWildcard(ctx context.Context, gvr schema.GroupVersionResource, namespaceScoped bool) error {
	cluster, err := genericapirequest.ValidClusterFrom(ctx)
	if err != nil {
		return apiErrorBadRequest(err)
	}
	namespace, namespaceSet := genericapirequest.NamespaceFrom(ctx)

	if cluster.Wildcard {
		if namespaceScoped && namespaceSet && namespace != metav1.NamespaceAll {
			return apiErrorBadRequest(fmt.Errorf("cross-cluster LIST and WATCH are required to be cross-namespace, not scoped to namespace %s", namespace))
		}
		return nil
	}

	if namespaceScoped {
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

func newUnwrappingWatch(cachedObjWatch watch.Interface) *unwrappingWatch {
	w := &unwrappingWatch{
		resultChan: make(chan watch.Event),
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

			unwrappedObj := &unstructured.Unstructured{}
			if err := unwrappedObj.UnmarshalJSON(cachedObj.Spec.Raw.Raw); err != nil {
				w.resultChan <- watch.Event{
					Type:   watch.Error,
					Object: &apierrors.NewInternalError(fmt.Errorf("failed to decode inner object: %w", err)).ErrStatus,
				}
				continue
			}

			w.resultChan <- watch.Event{
				Type:   event.Type,
				Object: unwrappedObj,
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

func newUnwrappingList(innerListGVK schema.GroupVersionKind, cachedObjList *cachev1alpha1.CachedObjectList) (*unstructured.UnstructuredList, error) {
	result := &unstructured.UnstructuredList{}
	result.SetGroupVersionKind(innerListGVK)

	for i := range cachedObjList.Items {
		obj, err := unwrapCachedObject(&cachedObjList.Items[i])
		if err != nil {
			return nil, fmt.Errorf("failed to unwrap item: %w", err)
		}
		result.Items = append(result.Items, *obj)
	}

	return result, nil
}
