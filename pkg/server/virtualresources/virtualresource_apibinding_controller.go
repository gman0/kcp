package virtualresources

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	kcpcache "github.com/kcp-dev/apimachinery/v2/pkg/cache"
	"github.com/kcp-dev/logicalcluster/v3"

	"github.com/kcp-dev/kcp/pkg/logging"
	apisv1alpha2 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha2"
	apisv1alpha2informers "github.com/kcp-dev/kcp/sdk/client/informers/externalversions/apis/v1alpha2"
)

const (
	ControllerName = "kcp-virtualresource-apibinding"
)

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

func NewController(
	apiBindingInformer apisv1alpha2informers.APIBindingClusterInformer,
	vrServer *Server,
) (*Controller, error) {
	c := &Controller{
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: ControllerName,
			},
		),
		server: vrServer,
		getAPIBinding: func(cluster logicalcluster.Name, name string) (*apisv1alpha2.APIBinding, error) {
			return apiBindingInformer.Cluster(cluster).Lister().Get(name)
		},
	}

	logger := logging.WithReconciler(klog.Background(), ControllerName)

	_, _ = apiBindingInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.enqueueAPIBinding(objOrTombstone[*apisv1alpha2.APIBinding](obj), logger) },
		UpdateFunc: func(_, obj interface{}) { c.enqueueAPIBinding(objOrTombstone[*apisv1alpha2.APIBinding](obj), logger) },
		DeleteFunc: func(obj interface{}) { c.enqueueAPIBinding(objOrTombstone[*apisv1alpha2.APIBinding](obj), logger) },
	})

	return c, nil
}

type Controller struct {
	queue workqueue.TypedRateLimitingInterface[string]

	server *Server

	getAPIBinding func(cluster logicalcluster.Name, name string) (*apisv1alpha2.APIBinding, error)
}

func (c *Controller) enqueueAPIBinding(apiBinding *apisv1alpha2.APIBinding, logger logr.Logger) {
	key, err := kcpcache.DeletionHandlingMetaClusterNamespaceKeyFunc(apiBinding)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}

	logging.WithQueueKey(logger, key).V(4).Info(fmt.Sprintf("queueing APIBinding"))
	c.queue.Add(key)
}

// Start starts the controller, which stops when ctx.Done() is closed.
func (c *Controller) Start(ctx context.Context, numThreads int) {
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

func (c *Controller) startWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *Controller) processNextWorkItem(ctx context.Context) bool {
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

func (c *Controller) process(ctx context.Context, key string) (bool, error) {
	logger := klog.FromContext(ctx)
	fmt.Println(" >> 0")
	clusterName, _, name, err := kcpcache.SplitMetaClusterNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(err)
		fmt.Println(" >> 2")
		return false, nil
	}

	binding, err := c.getAPIBinding(clusterName, name)
	var deleted bool
	if err != nil {
		if !apierrors.IsNotFound(err) {
			fmt.Println(" >> 3")
			return true, err
		}
		fmt.Println(" >> 4")

		deleted = true
	}

	if deleted {
		fmt.Println(" >> 5")

		return false, nil
	}

	logger = logging.WithObject(logger, binding)
	ctx = klog.NewContext(ctx, logger)

	for _, boundResource := range binding.Status.BoundResources {
		fmt.Println(" >> 6")

		if boundResource.VirtualResourceURL == "" {
			fmt.Println(" >> 7")

			continue
		}

		gr := schema.GroupResource{
			Group:    boundResource.Group,
			Resource: boundResource.Resource,
		}

		logger.Info("ADDING HANDLER", "gr", gr, "url", boundResource.VirtualResourceURL)

		if err := c.server.addHandlerFor(clusterName, gr, boundResource.VirtualResourceURL); err != nil {
			return true, err
		}
	}
	fmt.Println(" >> 8")

	return false, nil
}
