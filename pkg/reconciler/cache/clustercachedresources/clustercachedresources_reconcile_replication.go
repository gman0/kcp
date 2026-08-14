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

	// Controller is keyed by name only — version is no longer part of the key.
	controllerName := fmt.Sprintf("%s.%s", cluster, clusterCachedResource.Name)

	var resourceLabelSelector labels.Selector
	if clusterCachedResource.Spec.LabelSelector != nil {
		resourceLabelSelector = labels.SelectorFromSet(clusterCachedResource.Spec.LabelSelector.MatchLabels)
	}

	controller := r.controllerRegistry.get(controllerName)

	// If a controller exists but its GVR differs from the resolved GVR, tear it down so we
	// restart with the new preferred version.
	if controller != nil {
		if activeGVR, ok := r.controllerRegistry.getGVR(controllerName); ok && activeGVR != gvr {
			logger.Info("preferred version changed, restarting replication controller",
				"old", activeGVR.Version, "new", gvr.Version)
			r.controllerRegistry.unregister(controllerName) // cancels the controller's context
			r.localDiscoveringDynamicKcpInformers.ForgetResource(activeGVR)
			r.globalDiscoveringDynamicKcpInformers.ForgetResource(activeGVR)
			controller = nil
		}
	}

	// Track the current version in status.ReplicatedVersions.
	if gvr.Version != "" && !slices.Contains(clusterCachedResource.Status.ReplicatedVersions, gvr.Version) {
		clusterCachedResource.Status.ReplicatedVersions = append(clusterCachedResource.Status.ReplicatedVersions, gvr.Version)
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

		go replicated.Local.Run(controllerCtx.Done())
		go replicated.Global.Run(controllerCtx.Done())

		r.controllerRegistry.register(controllerName, c, cancel, gvr)
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
		r.controllerRegistry.unregister(controllerName) // unregister will cancel the context. and things will
		clusterCachedResource.Status.Phase = cachev1alpha1.ClusterCachedResourcePhaseDeleted
		return reconcileStatusStopAndRequeue, nil
	default:
		return reconcileStatusContinue, nil
	}
}
