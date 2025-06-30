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

package cachedresourceendpointslice

import (
	"context"
	"fmt"
	"net/url"
	"path"

	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"

	"github.com/kcp-dev/logicalcluster/v3"

	virtualworkspacesoptions "github.com/kcp-dev/kcp/cmd/virtual-workspaces/options"
	"github.com/kcp-dev/kcp/pkg/logging"
	"github.com/kcp-dev/kcp/pkg/reconciler/apis/apibinding"
	apisv1alpha2 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha2"
	cachev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/cache/v1alpha1"
	corev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/core/v1alpha1"
	kcpclientset "github.com/kcp-dev/kcp/sdk/client/clientset/versioned/cluster"
)

type reconcileStatus int

const (
	reconcileStatusContinue reconcileStatus = iota
	reconcileStatusStopAndRequeue
	reconcileStatusStop
)

type reconciler interface {
	reconcile(ctx context.Context, endpoints *cachev1alpha1.CachedResourceEndpointSlice) (reconcileStatus, error)
}

// kcpClusterClient.Cluster(clusterName.Path()).CoreV1alpha1().LogicalClusters().Get(ctx, "cluster", metav1.GetOptions{})
func (c *controller) reconcile(ctx context.Context, endpoints *cachev1alpha1.CachedResourceEndpointSlice) (bool, error) {
	reconcilers := []reconciler{
		&endpointsReconciler{
			getLogicalCluster: c.getLogicalCluster,
			getAPIBinding:     c.getAPIBinding,
			getCachedResource: c.getCachedResource,
			getMyShard:        c.getMyShard,
		},
	}

	var errs []error

	requeue := false
	for _, r := range reconcilers {
		var err error
		var status reconcileStatus
		status, err = r.reconcile(ctx, endpoints)
		if err != nil {
			errs = append(errs, err)
		}
		if status == reconcileStatusStopAndRequeue {
			requeue = true
			break
		}
		if status == reconcileStatusStop {
			break
		}
	}

	return requeue, utilerrors.NewAggregate(errs)
}

type endpointsReconciler struct {
	getLogicalCluster func(clusterName logicalcluster.Name) (*corev1alpha1.LogicalCluster, error)
	getAPIBinding     func(clusterName logicalcluster.Name, bindingName string) (*apisv1alpha2.APIBinding, error)
	getCachedResource func(clusterName logicalcluster.Name, name string) (*cachev1alpha1.CachedResource, error)
	getMyShard        func() (*corev1alpha1.Shard, error)
}

type conditionsReconciler struct {
}

func getResourceBindingsAnnJSON(lc *corev1alpha1.LogicalCluster) string {
	const jsonEmptyObj = "{}"

	if lc == nil {
		return jsonEmptyObj
	}

	ann := lc.Annotations[apibinding.ResourceBindingsAnnotationKey]
	if ann == "" {
		ann = jsonEmptyObj
	}

	return ann
}

func (r *endpointsReconciler) getSourceAPIExportReferenceFor(
	ctx context.Context,
	kcpClusterClient kcpclientset.ClusterInterface,
	clusterName logicalcluster.Name,
	gvr schema.GroupVersionResource,
) (*apisv1alpha2.ExportBindingReference, error) {
	if gvr.Group == "" {
		// Assume built-in types.
		return nil, nil
	}

	lc, err := r.getLogicalCluster(clusterName)
	if err != nil {
		return nil, err
	}

	resBindingsAnnStr := getResourceBindingsAnnJSON(lc)
	resBindingsAnn, err := apibinding.UnmarshalResourceBindingsAnnotation(resBindingsAnnStr)

	bindingName := ""
	for gr, v := range resBindingsAnn {
		if v.CRD {
			continue
		}
		if gr == gvr.GroupResource().String() {
			bindingName = v.Name
		}
	}

	if bindingName == "" {
		return nil, fmt.Errorf("no binding for %s found in %s", gvr.GroupResource().String(), clusterName)
	}

	apiBinding, err := r.getAPIBinding(clusterName, bindingName)
	if err != nil {
		return nil, fmt.Errorf("failed to get APIBinding %s in %s", bindingName, clusterName)
	}

	return apiBinding.Spec.Reference.Export, nil
}

func (r *endpointsReconciler) reconcile(ctx context.Context, endpoints *cachev1alpha1.CachedResourceEndpointSlice) (reconcileStatus, error) {
	logger := klog.FromContext(ctx)

	shard, err := r.getMyShard()
	if err != nil {
		return reconcileStatusStopAndRequeue, err
	}

	cachedResource, err := r.getCachedResource(logicalcluster.From(endpoints), endpoints.Spec.CachedResource.Name)
	if err != nil {
		return reconcileStatusStopAndRequeue, err
	}

	addr, err := url.Parse(shard.Spec.VirtualWorkspaceURL)
	if err != nil {
		// Should never happen
		logger = logging.WithObject(logger, shard)
		logger.Error(
			err, "error parsing shard.spec.virtualWorkspaceURL",
			"VirtualWorkspaceURL", shard.Spec.VirtualWorkspaceURL,
		)
		return reconcileStatusStop, nil
	}

	exportRef, err := r.getSourceAPIExportReferenceFor(ctx, nil, logicalcluster.From(endpoints), schema.GroupVersionResource(cachedResource.Spec.GroupVersionResource))
	if err != nil {
		return reconcileStatusStopAndRequeue, err
	}

	addr.Path = path.Join(
		addr.Path,
		virtualworkspacesoptions.DefaultRootPathPrefix,
		"replication",
	)
	if exportRef != nil {
		addr.Path = path.Join(addr.Path, exportRef.Path, exportRef.Name)
	}

	addrUrl := addr.String()

	for _, endpoint := range endpoints.Status.CachedResourceEndpoints {
		if endpoint.URL == addrUrl {
			// Already in endpoints slice, nothing to do.
			return reconcileStatusContinue, nil
		}
	}

	endpoints.Status.CachedResourceEndpoints = append(endpoints.Status.CachedResourceEndpoints, cachev1alpha1.CachedResourceEndpoint{
		URL: addrUrl,
	})

	return reconcileStatusStop, nil
}

func (r *conditionsReconciler) reconcile(ctx context.Context, endpoints *cachev1alpha1.CachedResourceEndpointSlice) (reconcileStatus, error) {
	return reconcileStatusStop, nil
}
