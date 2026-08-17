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

package clustercachedresources

import (
	"context"
	"fmt"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	kcpapiextensionsclientset "github.com/kcp-dev/client-go/apiextensions/client"
	kcpdynamic "github.com/kcp-dev/client-go/dynamic"
	"github.com/kcp-dev/logicalcluster/v3"
	cachev1alpha1 "github.com/kcp-dev/sdk/apis/cache/v1alpha1"
	"github.com/kcp-dev/sdk/apis/third_party/conditions/util/conditions"
	kcpclientset "github.com/kcp-dev/sdk/client/clientset/versioned/cluster"

	"github.com/kcp-dev/kcp/pkg/informer"
	replicationcontroller "github.com/kcp-dev/kcp/pkg/reconciler/cache/clustercachedresources/replication"
	"github.com/kcp-dev/kcp/pkg/reconciler/dynamicrestmapper"
)

// replication starts the replication machinery for a published resource.
// Or deletes the replication controller if the published resource is being deleted.
type replication struct {
	shardName                            string
	localDynamicClusterClient            kcpdynamic.ClusterInterface
	globalDynamicClusterClient           kcpdynamic.ClusterInterface
	kcpCacheClient                       kcpclientset.ClusterInterface
	dynRESTMapper                        *dynamicrestmapper.DynamicRESTMapper
	localDiscoveringDynamicKcpInformers  *informer.DiscoveringDynamicSharedInformerFactory
	globalDiscoveringDynamicKcpInformers *informer.DiscoveringDynamicSharedInformerFactory
	cacheApiExtensionsClusterClient      kcpapiextensionsclientset.ClusterInterface
	requeueSelf                          func(obj interface{})
	controllerRegistry                   *controllerRegistry
}

func (r *replication) reconcile(ctx context.Context, clusterCachedResource *cachev1alpha1.ClusterCachedResource) (reconcileStatus, error) {
	logger := klog.FromContext(ctx)
	logger.Info("reconciling cached resource", "ClusterCachedResource", clusterCachedResource.Name)

	gvr := schema.GroupVersionResource{
		Group:    clusterCachedResource.Spec.Group,
		Version:  clusterCachedResource.Status.StorageVersion,
		Resource: clusterCachedResource.Spec.Resource,
	}
	cluster := logicalcluster.From(clusterCachedResource)

	// Controller is keyed by (cluster, group, resource) — version is not part of the identity.
	// On version changes the controller is torn down and recreated with the new GVR.
	controllerName := fmt.Sprintf("%s.%s.%s", cluster, gvr.Group, gvr.Resource)

	var resourceLabelSelector labels.Selector
	if clusterCachedResource.Spec.LabelSelector != nil {
		resourceLabelSelector = labels.SelectorFromSet(clusterCachedResource.Spec.LabelSelector.MatchLabels)
	}

	controller := r.controllerRegistry.get(controllerName)

	// If a controller exists but its GVR differs from the resolved GVR, tear it down
	// completely. The old informer's watch stream is already dead (that is what triggered
	// re-discovery). A full restart is clean and avoids stale queue keys.
	if controller != nil {
		if activeGVR := controller.CurrentGVR(); activeGVR != gvr {
			logger.Info("preferred version changed, restarting replication controller",
				"old", activeGVR.Version, "new", gvr.Version)
			r.controllerRegistry.unregister(controllerName)
			r.localDiscoveringDynamicKcpInformers.ForgetResource(activeGVR)
			r.globalDiscoveringDynamicKcpInformers.ForgetResource(activeGVR)
			controller = nil
		}
	}

	// Track the current version in status.ReplicatedVersions.
	if gvr.Version != "" && !slices.Contains(clusterCachedResource.Status.StoredVersions, gvr.Version) {
		clusterCachedResource.Status.StoredVersions = append(clusterCachedResource.Status.StoredVersions, gvr.Version)
	}

	// We setup controller even if we are deleting. This is to ensure that we can purge the cache.
	// If for some reason was dead, we will recreate it.
	danglingResources := clusterCachedResource.Status.ResourceCounts != nil && clusterCachedResource.Status.ResourceCounts.Cache > 0
	if controller == nil {
		if clusterCachedResource.DeletionTimestamp != nil && !danglingResources {
			clusterCachedResource.Status.Phase = cachev1alpha1.ClusterCachedResourcePhaseDeleted
			return reconcileStatusStopAndRequeue, nil
		}

		controllerCtx, cancel := context.WithCancel(ctx)
		global, err := r.globalDiscoveringDynamicKcpInformers.ForResource(gvr)
		if err != nil {
			logger.Error(err, "Failed to get global informer for resource", "resource", gvr)
			cancel()
			return reconcileStatusStopAndRequeue, err
		}

		// Local informer is based on the specific types we want to replicate.
		local, err := r.localDiscoveringDynamicKcpInformers.ForResource(gvr)
		if err != nil {
			logger.Error(err, "Failed to get local informer for resource", "resource", gvr)
			cancel()
			return reconcileStatusStopAndRequeue, err
		}
		replicatedKind, err := r.dynRESTMapper.ForCluster(cluster).KindFor(gvr)
		if err != nil {
			logger.Error(err, "Failed to get Kind for resource", "resource", gvr)
			cancel()
			return reconcileStatusStopAndRequeue, err
		}
		replicated := &replicationcontroller.ReplicatedGVR{
			Identity: clusterCachedResource.Status.IdentityHash,
			Kind:     replicatedKind.Kind,
			Local:    local.Informer(),
			Global:   global.Informer(),
		}
		replicationcontroller.InstallIndexers(replicated)
		requeueSelf := func() {
			r.requeueSelf(clusterCachedResource)
		}

		c, err := replicationcontroller.NewController(
			r.shardName,
			r.localDynamicClusterClient,
			r.globalDynamicClusterClient,
			r.kcpCacheClient,
			r.cacheApiExtensionsClusterClient,
			cluster,
			gvr,
			replicated,
			requeueSelf,
			resourceLabelSelector,
		)
		if err != nil {
			cancel()
			// The informer may have been stopped by a previous controller teardown but not
			// yet removed from the factory (ForgetResource is called asynchronously by the
			// goroutine after c.Start returns). Remove it now so the next reconcile gets a
			// fresh informer instead of the already-stopped one.
			r.localDiscoveringDynamicKcpInformers.ForgetResource(gvr)
			r.globalDiscoveringDynamicKcpInformers.ForgetResource(gvr)
			return reconcileStatusContinue, err
		}

		// TODO(FIXME): This watch-error-driven requeue is likely wrong — see isGVRGoneError.
		watchErrHandler := func(_ context.Context, _ *cache.Reflector, err error) {
			gone := isGVRGoneError(err)
			fmt.Printf("### pkg/reconciler/cache/clustercachedresources/clustercachedresources_reconcile_replication.go watchErrHandler: gvr=%s err=%v gone=%v\n", gvr, err, gone)
			if gone {
				fmt.Printf("### pkg/reconciler/cache/clustercachedresources/clustercachedresources_reconcile_replication.go watchErrHandler: requeueing CCR %s\n", clusterCachedResource.Name)
				requeueSelf()
			}
		}
		// SetWatchErrorHandlerWithContext must be called before the informer is started.
		// Note: do NOT call cache.DefaultWatchErrorHandler here — the kcp apimachinery reflector
		// passes nil for the *cache.Reflector argument, which causes a nil-pointer panic inside
		// DefaultWatchErrorHandler when it accesses r.name.
		if err := replicated.Local.SetWatchErrorHandlerWithContext(func(ctx context.Context, r *cache.Reflector, err error) {
			fmt.Printf("### pkg/reconciler/cache/clustercachedresources/clustercachedresources_reconcile_replication.go local watch error handler: gvr=%s err=%v\n", gvr, err)
			watchErrHandler(ctx, r, err)
		}); err != nil {
			logger.Error(err, "failed to set watch error handler on local informer")
		}
		if err := replicated.Global.SetWatchErrorHandlerWithContext(func(ctx context.Context, r *cache.Reflector, err error) {
			fmt.Printf("### pkg/reconciler/cache/clustercachedresources/clustercachedresources_reconcile_replication.go global watch error handler: gvr=%s err=%v\n", gvr, err)
			watchErrHandler(ctx, r, err)
		}); err != nil {
			logger.Error(err, "failed to set watch error handler on global informer")
		}

		go replicated.Local.Run(controllerCtx.Done())
		go replicated.Global.Run(controllerCtx.Done())

		r.controllerRegistry.register(controllerName, c, cancel)
		if clusterCachedResource.Status.Phase != cachev1alpha1.ClusterCachedResourcePhaseDeleting {
			conditions.MarkTrue(clusterCachedResource, cachev1alpha1.ReplicationStarted)
			clusterCachedResource.Status.Phase = cachev1alpha1.ClusterCachedResourcePhaseReady
		}

		go func() {
			defer cancel()

			if !cache.WaitForCacheSync(controllerCtx.Done(), replicated.Local.HasSynced, replicated.Global.HasSynced) {
				logger.Error(nil, "Informers failed to sync, removing controller", "controller", controllerName)
				// Remove event handlers so the cancelled controller stops processing events.
				// Start's defers do the same cleanup, but Start is never called on this path.
				c.Shutdown()
				r.controllerRegistry.unregister(controllerName)
				// Remove stopped informers from the factory so the next reconcile gets fresh ones.
				// Without this, ForResource returns the stopped informer and AddEventHandler fails.
				r.localDiscoveringDynamicKcpInformers.ForgetResource(gvr)
				r.globalDiscoveringDynamicKcpInformers.ForgetResource(gvr)
				requeueSelf()
				return
			}

			c.Start(controllerCtx, 1)
		}()

		return reconcileStatusStopAndRequeue, nil // Once controller is started, we requeue to check if we need to delete it.
	}
	controller.SetLabelSelector(resourceLabelSelector)

	// Check if we need to wait for cleaning. This can be few cases:
	// 1. We are in deleting phase, but nothing to delete - we are good.
	// 2. We are in deleting phase, and there is something to delete - we need to wait.

	switch {
	case clusterCachedResource.Status.Phase == cachev1alpha1.ClusterCachedResourcePhaseDeleting && danglingResources:
		controller.SetDeleted(ctx)
		return reconcileStatusStopAndRequeue, nil
	case clusterCachedResource.Status.Phase == cachev1alpha1.ClusterCachedResourcePhaseDeleting && !danglingResources:
		r.controllerRegistry.unregister(controllerName) // cancels the controller context
		// Expel stopped informers from the factory so any immediate CCR re-creation gets
		// fresh informers rather than the now-stopped ones.
		r.localDiscoveringDynamicKcpInformers.ForgetResource(gvr)
		r.globalDiscoveringDynamicKcpInformers.ForgetResource(gvr)
		clusterCachedResource.Status.Phase = cachev1alpha1.ClusterCachedResourcePhaseDeleted
		return reconcileStatusStopAndRequeue, nil
	default:
		return reconcileStatusContinue, nil
	}
}

// TODO(FIXME): isGVRGoneError and the watch-error-handler approach below is almost certainly
// wrong. Watch errors are noisy, transient, and not a reliable signal for "this GVR has been
// permanently removed from the API". Driving version re-discovery off of them conflates
// network blips, server restarts, and actual API removal. The right mechanism is a GVR
// lifecycle event from the informer factory (like the RemovedFunc handler already wired in
// clustercachedresources_controller.go), not a heuristic on reflector error strings.
// This whole block needs to be rethought before it goes anywhere near production.
func isGVRGoneError(err error) bool {
	return apierrors.IsNotFound(err) || apierrors.IsGone(err) || meta.IsNoMatchError(err)
}
