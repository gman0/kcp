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

package virtualresources

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"

	"github.com/kcp-dev/logicalcluster/v3"

	cacheclient "github.com/kcp-dev/kcp/pkg/cache/client"
	"github.com/kcp-dev/kcp/pkg/cache/client/shard"
	"github.com/kcp-dev/kcp/pkg/crypto"
	"github.com/kcp-dev/kcp/pkg/logging"
	apisv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha1"
	apisv1alpha2 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha2"
)

func (c *Controller) reconcile(ctx context.Context, apiBinding *apisv1alpha2.APIBinding) (shouldRequeue bool, err error) {
	cluster := logicalcluster.From(apiBinding)

	logger := klog.FromContext(ctx)

	if apiBinding.DeletionTimestamp != nil {
		logger.V(4).Info("Binding is terminating, shutting down all its virtual resource APIs")
		// Remove all GRs from serving.
	}

	lc, err := c.getLogicalCluster(cluster)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.V(4).Info("LogicalCluster has been deleted, shutting down all its virtual resource APIs")
			// Remove the whole cluster from serving.
		}
		return true, err
	}
	if lc.DeletionTimestamp != nil {
		logger.V(4).Info("LogicalCluster is terminating, shutting down all its virtual resource APIs")
		// Remove the whole cluster from serving.
	}

	// Filter all virtual GRs.

	exportPath := logicalcluster.NewPath(apiBinding.Spec.Reference.Export.Path)
	if exportPath.Empty() {
		exportPath = logicalcluster.From(apiBinding).Path()
	}
	export, err := c.getAPIExport(exportPath, apiBinding.Spec.Reference.Export.Name)
	if err != nil {
		return true, err
	}

	thisShard, err := c.getMyShard()
	if err != nil {
		return true, err
	}

	for _, resource := range export.Spec.Resources {
		if resource.Storage.Virtual == nil {
			continue
		}

		resourceGR := schema.GroupResource{
			Group:    resource.Group,
			Resource: resource.Name,
		}

		vrURL, err := c.getVirtualResourceURL(ctx, thisShard.Spec.VirtualWorkspaceURL, logicalcluster.From(export), resource.Storage.Virtual)
		if err != nil {
			return true, err
		}

		if err := c.server.addHandlerFor(clusterName, resourceGR, vrURL, export.Status.IdentityHash); err != nil {
			return true, err
		}
	}

	return false, nil
}
