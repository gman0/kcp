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

package dynamicrestmapper

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"

	apiextensionshelpers "k8s.io/apiextensions-apiserver/pkg/apihelpers"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	kcpapiextensionsv1informers "github.com/kcp-dev/client-go/apiextensions/informers/apiextensions/v1"
	"github.com/kcp-dev/logicalcluster/v3"

	"github.com/kcp-dev/kcp/pkg/logging"
	"github.com/kcp-dev/kcp/pkg/reconciler/apis/apibinding"
	"github.com/kcp-dev/kcp/pkg/tombstone"
	builtinschemas "github.com/kcp-dev/kcp/pkg/virtual/apiexport/schemas/builtin"
	apisv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha1"
	apisv1alpha2 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha2"
	corev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/core/v1alpha1"
)

const (
	BuiltinTypesControllerName = "kcp-dynamicrestmapper-builtin-types"
)

var systemCRDClusterName = logicalcluster.Name("system:system-crds")

type BuiltinTypesController struct {
	// Built-in APIs are kcp-wide, we don't need a specific cluster.
	lock          sync.RWMutex
	state         *DefaultRESTMapper
	groupVersions map[string]string

	queue workqueue.TypedRateLimitingInterface[string]

	getLogicalCluster    func(clusterName logicalcluster.Name, name string) (*corev1alpha1.LogicalCluster, error)
	getAPIExportByPath   func(path logicalcluster.Path, name string) (*apisv1alpha2.APIExport, error)
	getAPIResourceSchema func(clusterName logicalcluster.Name, name string) (*apisv1alpha1.APIResourceSchema, error)
	getCRD               func(clusterName logicalcluster.Name, name string) (*apiextensionsv1.CustomResourceDefinition, error)
	getAPIBinding        func(clusterName logicalcluster.Name, name string) (*apibinding.APIBinding, error)
}

func NewBuiltinTypesController(
	ctx context.Context,
	crdInformer kcpapiextensionsv1informers.CustomResourceDefinitionClusterInformer,
) (*BuiltinTypesController, error) {
	c := &BuiltinTypesController{
		state: NewDefaultRESTMapper(nil),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: BuiltinTypesControllerName,
			},
		),
		groupVersions: make(map[string]string),
		getCRD: func(clusterName logicalcluster.Name, name string) (*apiextensionsv1.CustomResourceDefinition, error) {
			return crdInformer.Lister().Cluster(clusterName).Get(name)
		},
	}

	for i := range builtinschemas.BuiltInAPIs {
		group := builtinschemas.BuiltInAPIs[i].GroupVersion.Group
		version := builtinschemas.BuiltInAPIs[i].GroupVersion.Version
		if version > c.groupVersions[group] {
			c.groupVersions[group] = version
		}
		c.state.add(newTypeMeta(
			builtinschemas.BuiltInAPIs[i].GroupVersion.Group,
			builtinschemas.BuiltInAPIs[i].GroupVersion.Version,
			builtinschemas.BuiltInAPIs[i].Names.Kind,
			builtinschemas.BuiltInAPIs[i].Names.Singular,
			builtinschemas.BuiltInAPIs[i].Names.Plural,
			resourceScopeToRESTScope(builtinschemas.BuiltInAPIs[i].ResourceScope)),
		)
	}

	logger := logging.WithReconciler(klog.Background(), BuiltinTypesControllerName)

	// We are only interested in system CRDs.
	_, _ = crdInformer.Informer().Cluster("system:system-crds").AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueueCRD(tombstone.Obj[*apiextensionsv1.CustomResourceDefinition](obj), logger)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.enqueueCRD(tombstone.Obj[*apiextensionsv1.CustomResourceDefinition](newObj), logger)
		},
		DeleteFunc: func(obj interface{}) {
			c.enqueueCRD(tombstone.Obj[*apiextensionsv1.CustomResourceDefinition](obj), logger)
		},
	})

	return c, nil
}

func (c *BuiltinTypesController) enqueueCRD(crd *apiextensionsv1.CustomResourceDefinition, logger logr.Logger) {
	if !apiextensionshelpers.IsCRDConditionTrue(crd, apiextensionsv1.Established) {
		// The CRD is not ready yet. Nothing to do, we'll get notified on the next update event.
		return
	}

	key := fmt.Sprintf("%s.%s/%s", crd.Spec.Group, crd.Spec.Names.Plural, crd.Name)

	logger.V(4).Info("queueing system CRD")
	c.queue.Add(key)
}

func (c *BuiltinTypesController) Start(ctx context.Context, numThreads int) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	logger := logging.WithReconciler(klog.FromContext(ctx), BuiltinTypesControllerName)
	ctx = klog.NewContext(ctx, logger)
	logger.Info("Starting controller")
	defer logger.Info("Shutting down controller")

	for range numThreads {
		go wait.UntilWithContext(ctx, c.startWorker, time.Second)
	}

	<-ctx.Done()
}

func (c *BuiltinTypesController) startWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *BuiltinTypesController) processNextWorkItem(ctx context.Context) bool {
	// Wait until there is a new item in the working queue
	key, quit := c.queue.Get()
	if quit {
		return false
	}

	logger := logging.WithQueueKey(klog.FromContext(ctx), key)
	ctx = klog.NewContext(ctx, logger)
	logger.Info("processing key")

	// No matter what, tell the queue we're done with this key, to unblock
	// other workers.
	defer c.queue.Done(key)

	if err := c.process(ctx, key); err != nil {
		utilruntime.HandleError(fmt.Errorf("%q controller failed to sync %q, err: %w", ControllerName, key, err))
		c.queue.AddRateLimited(key)
		return true
	}
	c.queue.Forget(key)
	return true
}

func (c *BuiltinTypesController) gatherGVKRsForCRD(crd *apiextensionsv1.CustomResourceDefinition) []typeMeta {
	if crd == nil {
		return nil
	}
	gvkrs := make([]typeMeta, 0, len(crd.Spec.Versions))
	for _, version := range crd.Spec.Versions {
		if !version.Served {
			continue
		}

		gvkrs = append(gvkrs, newTypeMeta(
			crd.Spec.Group,
			version.Name,
			crd.Status.AcceptedNames.Kind,
			crd.Status.AcceptedNames.Singular,
			crd.Status.AcceptedNames.Plural,
			resourceScopeToRESTScope(crd.Spec.Scope),
		))
	}
	return gvkrs
}

func (c *BuiltinTypesController) gatherGVKRsForMappedGroupResource(gr schema.GroupResource) ([]typeMeta, error) {
	gvkrs, err := c.state.getGVKRs(gr)
	if err != nil {
		if meta.IsNoMatchError(err) {
			return nil, nil
		}
		return nil, err
	}
	return gvkrs, nil
}

func (c *BuiltinTypesController) process(ctx context.Context, key string) error {
	logger := logging.WithQueueKey(klog.FromContext(ctx), key)

	parts := strings.Split(key, "/")
	gr := schema.ParseGroupResource(parts[0])
	crdName := parts[1]

	crd, err := c.getCRD(systemCRDClusterName, crdName)
	if err != nil && apierrors.IsNotFound(err) {
		return err
	}

	// Retrieve type meta for all detected changes in bound resources.

	type gathererFunc func(resourceGroup string, boundResourceLock apibinding.Lock) ([]typeMeta, error)

	typeMetaToRemove, err := c.gatherGVKRsForMappedGroupResource(gr)
	if err != nil {
		return err
	}
	typeMetaToAdd := c.gatherGVKRsForCRD(crd)

	// Finally, store the new mappings in the RESTMapper for this LogicalCluster.

	logger.V(4).Info("applying mappings")

	c.lock.Lock()
	defer c.lock.Unlock()
	c.state.apply(typeMetaToRemove, typeMetaToAdd)
	return nil
}
