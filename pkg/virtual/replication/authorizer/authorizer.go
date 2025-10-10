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
	"runtime/debug"
	"slices"

	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"

	kcpkubeclientset "github.com/kcp-dev/client-go/kubernetes"
	"github.com/kcp-dev/logicalcluster/v3"

	"github.com/kcp-dev/kcp/pkg/authorization/delegated"
	dynamiccontext "github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/context"
	"github.com/kcp-dev/kcp/pkg/virtual/replication/apidomainkey"
)

type wrappedResourceAuthorizer struct {
	newDelegatedAuthorizer func(clusterName logicalcluster.Name) (authorizer.Authorizer, error)
}

var readOnlyVerbs = []string{"get", "list", "watch"}

func NewWrappedResourceAuthorizer(kubeClusterClient kcpkubeclientset.ClusterInterface) authorizer.Authorizer {
	return &wrappedResourceAuthorizer{
		newDelegatedAuthorizer: func(clusterName logicalcluster.Name) (authorizer.Authorizer, error) {
			return delegated.NewDelegatedAuthorizer(clusterName, kubeClusterClient, delegated.Options{})
		},
	}
}

func (a *wrappedResourceAuthorizer) Authorize(ctx context.Context, attr authorizer.Attributes) (authorizer.Decision, string, error) {
	fmt.Printf("### wrappedResourceAuthorizer 0\n")
	debug.PrintStack()

	targetCluster, err := genericapirequest.ValidClusterFrom(ctx)
	if err != nil {
		fmt.Printf("### wrappedResourceAuthorizer 1\n")
		return authorizer.DecisionNoOpinion, "", fmt.Errorf("error getting valid cluster from context: %w", err)
	}

	parsedKey, err := apidomainkey.Parse(dynamiccontext.APIDomainKeyFrom(ctx))
	if err != nil {
		fmt.Printf("### wrappedResourceAuthorizer 2\n")
		return authorizer.DecisionNoOpinion, "",
			fmt.Errorf("invalid API domain key")
	}

	if !slices.Contains(readOnlyVerbs, attr.GetVerb()) {
		fmt.Printf("### wrappedResourceAuthorizer 3\n")
		return authorizer.DecisionDeny, "write access to CachedResource is not allowed from virtual workspace", nil
	}

	if targetCluster.Wildcard || attr.GetResource() == "" {
		fmt.Printf("### wrappedResourceAuthorizer 4\n")
		// If the target is the wildcard cluster or it's a non-resource URL request,
		// we can skip checking the APIBinding in the target cluster.
		return authorizer.DecisionAllow, fmt.Sprintf("CachedResource: %s|%s, workspace: %q allowed for wildcard or non-resource requests",
			parsedKey.CachedResourceCluster.String(), parsedKey.CachedResourceName, targetCluster.Name), nil
	}

	authz, err := a.newDelegatedAuthorizer(targetCluster.Name)
	if err != nil {
		fmt.Printf("### wrappedResourceAuthorizer 5\n")
		return authorizer.DecisionNoOpinion, "", err
	}

	fmt.Printf("### wrappedResourceAuthorizer attr=%#v, attr.User=%#v\n", attr, attr.GetUser())

	dec, reason, err := authz.Authorize(ctx, attr)
	if err != nil {
		fmt.Printf("### wrappedResourceAuthorizer 6\n")
		return authorizer.DecisionNoOpinion, "", fmt.Errorf("error authorizing RBAC in workspace %q for CachedResource %s|%s: %w",
			targetCluster.Name, parsedKey.CachedResourceCluster.String(), parsedKey.CachedResourceName, err)
	}

	if dec == authorizer.DecisionAllow || dec == authorizer.DecisionNoOpinion {
		fmt.Printf("### wrappedResourceAuthorizer 7 reason=%q\n", reason)
		return authorizer.DecisionAllow, fmt.Sprintf("CachedResource: %s|%s, workspace: %q RBAC decision: %v",
			parsedKey.CachedResourceCluster.String(), parsedKey.CachedResourceName, targetCluster.Name, reason), nil
	}

	fmt.Printf("### wrappedResourceAuthorizer 8\n")
	return authorizer.DecisionDeny, fmt.Sprintf("CachedResource: %s|%s, workspace: %q RBAC decision: %v",
		parsedKey.CachedResourceCluster.String(), parsedKey.CachedResourceName, targetCluster.Name, reason), nil
}
