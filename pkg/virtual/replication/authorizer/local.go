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

package authorizer

import (
	"context"
	"fmt"

	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"

	kcpkubeclientset "github.com/kcp-dev/client-go/kubernetes"
	"github.com/kcp-dev/logicalcluster/v3"

	"github.com/kcp-dev/kcp/pkg/authorization/delegated"
	dynamiccontext "github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/context"
	"github.com/kcp-dev/kcp/pkg/virtual/replication/apidomainkey"
)

type localAuthorizer struct {
	newDelegatedAuthorizer func(clusterName logicalcluster.Name) (authorizer.Authorizer, error)
}

// NewLocalAuthorizer creates an authorizer that checks that the user has suitable permissions
// to the resource in the target cluster. Wildcard requests are skipped with NoOpinion.
func NewLocalAuthorizer(kubeClusterClient kcpkubeclientset.ClusterInterface) authorizer.Authorizer {
	return &localAuthorizer{
		newDelegatedAuthorizer: func(clusterName logicalcluster.Name) (authorizer.Authorizer, error) {
			return delegated.NewDelegatedAuthorizer(clusterName, kubeClusterClient, delegated.Options{})
		},
	}
}

func (a *localAuthorizer) Authorize(ctx context.Context, attr authorizer.Attributes) (authorizer.Decision, string, error) {
	targetCluster, err := genericapirequest.ValidClusterFrom(ctx)
	if err != nil {
		return authorizer.DecisionNoOpinion, "", fmt.Errorf("error getting valid cluster from context: %w", err)
	}

	if targetCluster.Wildcard {
		// Skipping checks for wildcard requests.
		return authorizer.DecisionAllow, "", nil
	}

	parsedKey, err := apidomainkey.Parse(dynamiccontext.APIDomainKeyFrom(ctx))
	if err != nil {
		return authorizer.DecisionNoOpinion, "",
			fmt.Errorf("invalid API domain key")
	}

	authz, err := a.newDelegatedAuthorizer(targetCluster.Name)
	if err != nil {
		return authorizer.DecisionNoOpinion, "",
			fmt.Errorf("error creating delegated authorizer for CachedResource %s|%s in workspace %s: %w", parsedKey.CachedResourceCluster, parsedKey.CachedResourceName, targetCluster.Name, err)
	}

	dec, reason, err := authz.Authorize(ctx, attr)
	if err != nil {
		return authorizer.DecisionNoOpinion, "",
			fmt.Errorf("error authorizing RBAC for CachedResource %s|%s in workspace %s: %w", parsedKey.CachedResourceCluster, parsedKey.CachedResourceName, targetCluster.Name, err)
	}
	fmt.Printf("### localAuthorizer dec=%v reason=%q err=%v attr=%#v\n", dec, reason, err, attr)
	if dec != authorizer.DecisionAllow {
		return authorizer.DecisionDeny, reason, nil
	}

	return authorizer.DecisionAllow, "", nil
}
