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

package cachedresourceendpointsliceurls

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	kcpcache "github.com/kcp-dev/apimachinery/v2/pkg/cache"
	"github.com/kcp-dev/logicalcluster/v3"

	"github.com/kcp-dev/kcp/pkg/indexers"
	"github.com/kcp-dev/kcp/pkg/logging"
	"github.com/kcp-dev/kcp/pkg/reconciler/apis/apibinding"
	"github.com/kcp-dev/kcp/pkg/reconciler/events"
	apisv1alpha2 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha2"
	cachev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/cache/v1alpha1"
	"github.com/kcp-dev/kcp/sdk/apis/core"
	corev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/core/v1alpha1"
	cachev1alpha1apply "github.com/kcp-dev/kcp/sdk/client/applyconfiguration/cache/v1alpha1"
	kcpclientset "github.com/kcp-dev/kcp/sdk/client/clientset/versioned/cluster"
	apisv1alpha2informers "github.com/kcp-dev/kcp/sdk/client/informers/externalversions/apis/v1alpha2"
	cachev1alpha1informers "github.com/kcp-dev/kcp/sdk/client/informers/externalversions/cache/v1alpha1"
	corev1alpha1informers "github.com/kcp-dev/kcp/sdk/client/informers/externalversions/core/v1alpha1"
)

const (
	ControllerName = "kcp-cachedresourceendpointslice-urls"
)

func NewController(
	shardName string,
	apiBindingInformer apisv1alpha2informers.APIBindingClusterInformer,
	cachedResourceEndpointSliceClusterInformer cachev1alpha1informers.CachedResourceEndpointSliceClusterInformer,
	globalCachedResourceEndpointSliceClusterInformer cachev1alpha1informers.CachedResourceEndpointSliceClusterInformer,
	globalShardClusterInformer corev1alpha1informers.ShardClusterInformer,
	globalAPIExportClusterInformer apisv1alpha2informers.APIExportClusterInformer,
	globalCachedResourcelusterInformer cachev1alpha1informers.CachedResourceClusterInformer,
	localLogicalClusterInformer corev1alpha1informers.LogicalClusterClusterInformer,
	clusterClient kcpclientset.ClusterInterface,
) (*controller, error) {
	c := &controller{
		shardName: shardName,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: ControllerName,
			},
		),
		getMyShard: func() (*corev1alpha1.Shard, error) {
			return globalShardClusterInformer.Cluster(core.RootCluster).Lister().Get(shardName)
		},
		getCachedResource: func(path logicalcluster.Path, name string) (*cachev1alpha1.CachedResource, error) {
			return indexers.ByPathAndName[*cachev1alpha1.CachedResource](cachev1alpha1.Resource("cachedresources"), globalCachedResourcelusterInformer.Informer().GetIndexer(), path, name)
		},
		getCachedResourceEndpointSlice: func(path logicalcluster.Path, name string) (*cachev1alpha1.CachedResourceEndpointSlice, error) {
			obj, err := indexers.ByPathAndNameWithFallback[*cachev1alpha1.CachedResourceEndpointSlice](cachev1alpha1.Resource("cachedresourceendpointslices"), cachedResourceEndpointSliceClusterInformer.Informer().GetIndexer(), globalCachedResourceEndpointSliceClusterInformer.Informer().GetIndexer(), path, name)
			if err != nil {
				return nil, err
			}
			return obj, err
		},
		getAPIExportByCachedResource: func(cr *cachev1alpha1.CachedResource) (*apisv1alpha2.APIExport, error) {
			lc, err := localLogicalClusterInformer.Cluster(logicalcluster.From(cr)).Lister().Get("cluster")
			if err != nil {
				return nil, err
			}
			resBindings, err := apibinding.GetResourceBindings(lc)
			if err != nil {
				return nil, err
			}

			gr := schema.GroupResource{
				Group:    cr.Spec.Group,
				Resource: cr.Spec.Resource,
			}
			bindingLock, hasResource := resBindings[gr.String()]
			if !hasResource {
				return nil, fmt.Errorf("no binding lock for %s found in %s", gr, logicalcluster.From(cr))
			}
			if bindingLock.Name == "" {
				return nil, nil
			}

			apiBinding, err := apiBindingInformer.Cluster(logicalcluster.From(cr)).Lister().Get(bindingLock.Name)
			if err != nil {
				return nil, err
			}

			apiExportPath := logicalcluster.NewPath(apiBinding.Spec.Reference.Export.Path)
			if apiExportPath.Empty() {
				apiExportPath = logicalcluster.From(apiBinding).Path()
			}
			apiExport, err := indexers.ByPathAndName[*apisv1alpha2.APIExport](apisv1alpha2.Resource("apiexports"), globalAPIExportClusterInformer.Informer().GetIndexer(), apiExportPath, apiBinding.Spec.Reference.Export.Name)
			if err != nil {
				return nil, err
			}

			return apiExport, nil
		},
		listAPIBindingsByAPIExport: func(export *apisv1alpha2.APIExport) ([]*apisv1alpha2.APIBinding, error) {
			// binding keys by full path
			keys := sets.New[string]()
			if path := logicalcluster.NewPath(export.Annotations[core.LogicalClusterPathAnnotationKey]); !path.Empty() {
				pathKeys, err := apiBindingInformer.Informer().GetIndexer().IndexKeys(indexers.APIBindingsByAPIExport, path.Join(export.Name).String())
				if err != nil {
					return nil, err
				}
				keys.Insert(pathKeys...)
			}

			clusterKeys, err := apiBindingInformer.Informer().GetIndexer().IndexKeys(indexers.APIBindingsByAPIExport, logicalcluster.From(export).Path().Join(export.Name).String())
			if err != nil {
				return nil, err
			}
			keys.Insert(clusterKeys...)

			bindings := make([]*apisv1alpha2.APIBinding, 0, keys.Len())
			for _, key := range sets.List[string](keys) {
				binding, exists, err := apiBindingInformer.Informer().GetIndexer().GetByKey(key)
				if err != nil {
					utilruntime.HandleError(err)
					continue
				} else if !exists {
					utilruntime.HandleError(fmt.Errorf("APIBinding %q does not exist", key))
					continue
				}
				bindings = append(bindings, binding.(*apisv1alpha2.APIBinding))
			}
			return bindings, nil
		},
		patchCachedResourceEndpointSlice: func(ctx context.Context, cluster logicalcluster.Path, patch *cachev1alpha1apply.CachedResourceEndpointSliceApplyConfiguration) error {
			_, err := clusterClient.CacheV1alpha1().CachedResourceEndpointSlices().Cluster(cluster).ApplyStatus(ctx, patch, metav1.ApplyOptions{
				FieldManager: shardName,
			})
			return err
		},
	}

	logger := logging.WithReconciler(klog.Background(), ControllerName)

	_, _ = cachedResourceEndpointSliceClusterInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueueCachedResourceEndpointSlice(objOrTombstone[*cachev1alpha1.CachedResourceEndpointSlice](obj), logger, "")
		},
		UpdateFunc: func(_, newObj interface{}) {
			c.enqueueCachedResourceEndpointSlice(objOrTombstone[*cachev1alpha1.CachedResourceEndpointSlice](newObj), logger, "")
		},
		DeleteFunc: func(obj interface{}) {
			c.enqueueCachedResourceEndpointSlice(objOrTombstone[*cachev1alpha1.CachedResourceEndpointSlice](obj), logger, "")
		},
	})

	_, _ = globalCachedResourceEndpointSliceClusterInformer.Informer().AddEventHandler(events.WithoutSyncs(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueueCachedResourceEndpointSlice(objOrTombstone[*cachev1alpha1.CachedResourceEndpointSlice](obj), logger, " from cache")
		},
		UpdateFunc: func(_, newObj interface{}) {
			c.enqueueCachedResourceEndpointSlice(objOrTombstone[*cachev1alpha1.CachedResourceEndpointSlice](newObj), logger, " from cache")
		},
		DeleteFunc: func(obj interface{}) {
			c.enqueueCachedResourceEndpointSlice(objOrTombstone[*cachev1alpha1.CachedResourceEndpointSlice](obj), logger, " from cache")
		},
	}))

	return c, nil
}

func (c *controller) enqueueCachedResourceEndpointSlice(obj *cachev1alpha1.CachedResourceEndpointSlice, logger logr.Logger, logSuffix string) {
	key, err := kcpcache.DeletionHandlingMetaClusterNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}

	logger.V(4).Info(fmt.Sprintf("queueing CachedResourceEndpointSlice%s", logSuffix))
	c.queue.Add(key)
}

type controller struct {
	queue     workqueue.TypedRateLimitingInterface[string]
	shardName string

	getMyShard                       func() (*corev1alpha1.Shard, error)
	getCachedResource                func(path logicalcluster.Path, name string) (*cachev1alpha1.CachedResource, error)
	getCachedResourceEndpointSlice   func(path logicalcluster.Path, name string) (*cachev1alpha1.CachedResourceEndpointSlice, error)
	getAPIExportByCachedResource     func(cr *cachev1alpha1.CachedResource) (*apisv1alpha2.APIExport, error)
	listAPIBindingsByAPIExport       func(apiexport *apisv1alpha2.APIExport) ([]*apisv1alpha2.APIBinding, error)
	patchCachedResourceEndpointSlice func(ctx context.Context, cluster logicalcluster.Path, patch *cachev1alpha1apply.CachedResourceEndpointSliceApplyConfiguration) error
}

// Start starts the controller, which stops when ctx.Done() is closed.
func (c *controller) Start(ctx context.Context, numThreads int) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	logger := logging.WithReconciler(klog.FromContext(ctx), ControllerName)
	ctx = klog.NewContext(ctx, logger)
	logger.Info("Starting controller")
	defer logger.Info("Shutting down controller")

	for range numThreads {
		go wait.UntilWithContext(ctx, c.startWorker, time.Second)
	}

	<-ctx.Done()
}

func (c *controller) startWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *controller) processNextWorkItem(ctx context.Context) bool {
	// Wait until there is a new item in the working queue
	k, quit := c.queue.Get()
	if quit {
		return false
	}
	key := k

	logger := logging.WithQueueKey(klog.FromContext(ctx), key)
	ctx = klog.NewContext(ctx, logger)
	logger.V(4).Info("processing key")

	// No matter what, tell the queue we're done with this key, to unblock
	// other workers.
	defer c.queue.Done(key)

	if requeue, err := c.process(ctx, key); err != nil {
		utilruntime.HandleError(fmt.Errorf("%q controller failed to sync %q, err: %w", ControllerName, key, err))
		c.queue.AddRateLimited(key)
		return true
	} else if requeue {
		// only requeue if we didn't error, but we still want to requeue
		c.queue.Add(key)
		return true
	}
	c.queue.Forget(key)
	return true
}

func (c *controller) process(ctx context.Context, key string) (bool, error) {
	clusterName, _, name, err := kcpcache.SplitMetaClusterNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(err)
		return false, nil
	}
	obj, err := c.getCachedResourceEndpointSlice(clusterName.Path(), name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil // object deleted before we handled it
		}
		return false, err
	}

	obj = obj.DeepCopy()

	logger := logging.WithObject(klog.FromContext(ctx), obj)
	ctx = klog.NewContext(ctx, logger)

	var errs []error
	requeue, err := c.reconcile(ctx, obj)
	if err != nil {
		errs = append(errs, err)
	}

	return requeue, utilerrors.NewAggregate(errs)
}

func InstallIndexers(
	localCachedResourceClusterInformer, globalCachedResourceClusterInformer cachev1alpha1informers.CachedResourceClusterInformer,
	localCachedResourceEndpointSliceClusterInformer, globalCachedResourceEndpointSliceClusterInformer cachev1alpha1informers.CachedResourceEndpointSliceClusterInformer,
) {
	indexers.AddIfNotPresentOrDie(localCachedResourceClusterInformer.Informer().GetIndexer(), cache.Indexers{
		indexers.ByLogicalClusterPathAndName: indexers.IndexByLogicalClusterPathAndName,
	})
	indexers.AddIfNotPresentOrDie(globalCachedResourceClusterInformer.Informer().GetIndexer(), cache.Indexers{
		indexers.ByLogicalClusterPathAndName: indexers.IndexByLogicalClusterPathAndName,
	})
	indexers.AddIfNotPresentOrDie(localCachedResourceEndpointSliceClusterInformer.Informer().GetIndexer(), cache.Indexers{
		indexers.ByLogicalClusterPathAndName: indexers.IndexByLogicalClusterPathAndName,
	})
	indexers.AddIfNotPresentOrDie(globalCachedResourceEndpointSliceClusterInformer.Informer().GetIndexer(), cache.Indexers{
		indexers.ByLogicalClusterPathAndName: indexers.IndexByLogicalClusterPathAndName,
	})
}

func objOrTombstone[T runtime.Object](obj any) T {
	if t, ok := obj.(T); ok {
		return t
	}
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		if t, ok := tombstone.Obj.(T); ok {
			return t
		}

		panic(fmt.Errorf("tombstone %T is not a %T", tombstone, new(T)))
	}

	panic(fmt.Errorf("%T is not a %T", obj, new(T)))
}
