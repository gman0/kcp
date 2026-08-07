# Enhancement: Fine-Grained RBAC for ClusterCachedResources

## Status

Draft

## Summary

Introduce a `replicate` RBAC verb that controls which subjects may start replicating a particular resource type into the cache. Creating a `ClusterCachedResource` or `CachedResourceDefinition` (see the [identity-split enhancement](./enhancement-cached-resource-identity-split.md)) is currently governed only by the subject's permission to create that API object — nothing checks whether the subject is allowed to read or access the underlying resource being replicated. This enhancement closes that gap.

## Motivation

### Current behavior

The only permission check before replication begins is whether the user has RBAC permission to `create` a `ClusterCachedResource` object. Any user with that right can instruct kcp to begin replicating any cluster-scoped resource type, regardless of whether they have any access to those resources.

Concretely: a user with `create clustercachedresources` can replicate `secrets` — even if they have no right to `list` or `get` `secrets` directly.

### Problems

**Privilege escalation via the cache.** The cache server exposes replicated objects to any workspace that binds the corresponding `APIExport`. A user who triggers replication of a resource they cannot directly read may be able to consume that data indirectly through a cache-backed virtual resource in another workspace.

**No per-resource-type gating.** There is no way to express "users in this workspace may replicate `cpuflavors` but not `secrets`." The only lever is the binary `create clustercachedresources` permission which is all-or-nothing.

**Audit gap.** Replication is a data movement operation. Today it leaves no trace in RBAC policy of which subjects were authorized to move which data — only that they were allowed to create the wrapper object.

### Goals

- Allow administrators to grant or deny the right to replicate a specific resource type independently from the right to create `ClusterCachedResource` objects.
- Require no new API types; leverage standard Kubernetes `ClusterRole` / `ClusterRoleBinding` primitives.
- Apply the check at admission time, before any controller acts on the object.
- Default to restrictive: if no `replicate` grant exists, the request is denied.

### Non-goals

- Per-object replication filtering (label selectors already cover that use case).
- Controlling which *consumers* may read cached data (that is governed by the `APIExport` permission model).
- Retrofitting `replicate` onto namespace-scoped resources (namespace-scoped caching is not yet supported).

## Design

### The `replicate` verb

A new non-standard RBAC verb `replicate` is defined for resource types, analogous to the existing `use` verb used by `APIExport` permission claims. It is checked against the target resource's group and resource name — not against the `ClusterCachedResource` object itself.

Example `ClusterRole` granting replication of `cpuflavors.cloud.example.com`:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: replicate-cpuflavors
rules:
- apiGroups: ["cloud.example.com"]
  resources: ["cpuflavors"]
  verbs: ["replicate"]
```

A wildcard grant is also supported through normal RBAC expansion:

```yaml
rules:
- apiGroups: ["cloud.example.com"]
  resources: ["*"]
  verbs: ["replicate"]
```

### Admission check

The existing `ClusterCachedResource` admission plugin (at `pkg/admission/clustercachedresource/admission.go`) is extended with a pre-create/pre-update `SubjectAccessReview` check:

```
SubjectAccessReview{
  User:   <request user info>,
  ResourceAttributes: {
    Verb:     "replicate",
    Group:    <spec.group>,
    Resource: <spec.resource>,
  },
}
```

Neither the definition nor the consumer carries a `version` field (see the [identity-split enhancement](./enhancement-cached-resource-identity-split.md)); accordingly the SAR omits version as well.

The review is performed against the workspace where the object is being created — the same workspace that will perform replication. If the review returns `Allowed: false`, the admission plugin returns `403 Forbidden` with a message indicating which verb and resource was missing.

For the identity-split model, this check applies to `CachedResourceDefinition` creation (the resource type is known there), and it is re-evaluated when a `ClusterCachedResource` references a definition, using the **consumer workspace's** RBAC (since that is the workspace where replication occurs).

### Interaction with system:masters and bootstrapping

System-level components that set up built-in caching (e.g., kcp's own shard coordination) are exempt via the standard `system:masters` bypass. This ensures that kcp's internal replication bootstrap is not blocked on RBAC policy.

During initial cluster setup, a `ClusterRole` and `ClusterRoleBinding` granting `replicate` on appropriate resource groups can be pre-installed, mirroring how `system:authenticated` RBAC bootstrapping works for other verbs.

### Default posture

When the feature gate `CachedAPIsRBAC` is enabled, `replicate` is denied by default for all non-system subjects unless explicitly granted. A migration utility (see below) can audit existing `ClusterCachedResource` objects and emit the `ClusterRole` / `ClusterRoleBinding` objects required to preserve current behavior.

While the feature gate is disabled (the default for the initial release), the admission plugin skips the `replicate` check entirely, preserving backward compatibility.

### Audit trail

Because `replicate` is a standard RBAC verb evaluated through `SubjectAccessReview`, it appears in the kube-apiserver audit log alongside the SAR decision. This gives operators a clear record of which subjects were authorized to replicate which resource types, without any additional audit infrastructure.

## Example walkthrough

**Setup (administrator):**

```yaml
# Grant workspace "root:provider" the right to replicate cpuflavors.
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: replicate-cpuflavors
rules:
- apiGroups: ["cloud.example.com"]
  resources: ["cpuflavors"]
  verbs: ["replicate"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: provider-replicate-cpuflavors
subjects:
- kind: User
  name: alice@example.com
roleRef:
  kind: ClusterRole
  name: replicate-cpuflavors
  apiGroup: rbac.authorization.k8s.io
```

**User action (alice, in workspace `root:provider`):**

```yaml
# Succeeds: alice has 'replicate' on cpuflavors.
apiVersion: cache.kcp.io/v1alpha1
kind: CachedResourceDefinition
metadata:
  name: cpuflavors
spec:
  group: cloud.example.com
  resource: cpuflavors
```

```yaml
# Fails with 403: alice does not have 'replicate' on secrets.
apiVersion: cache.kcp.io/v1alpha1
kind: CachedResourceDefinition
metadata:
  name: secrets
spec:
  group: ""
  resource: secrets
```

## Implementation notes

### SubjectAccessReview mechanics

The admission webhook already has access to request user info via `admission.Attributes.GetUserInfo()`. The SAR is issued against the local `kube-apiserver` (same workspace) using the existing `authorizer` injected into the admission handler — no new client is required.

The check is a pure authorization decision and does not involve any watch or cache lookup, keeping admission latency impact minimal.

### Interaction with identity-split enhancement

When both enhancements are active:

- `CachedResourceDefinition` admission checks `replicate` against the group-resource at definition-creation time (this is where the resource type is first declared).
- `ClusterCachedResource` admission re-checks `replicate` in the **consumer's** workspace (where replication will actually run).

This two-step check ensures that neither workspace can be used as a replication vector without explicit `replicate` grants in that workspace's RBAC policy, even if the other workspace has already been granted permission.

### Feature gate

| Gate | Default | Effect |
|---|---|---|
| `CachedAPIsRBAC` | `false` | `replicate` check is skipped; behavior is unchanged from today |
| `CachedAPIsRBAC` | `true` | `replicate` check is enforced at admission |

## Alternatives considered

**Require `list`+`watch` on the target resource as a proxy for replication permission.** This is simpler but does not allow an administrator to say "you may list secrets but you may not replicate them." The `replicate` verb provides independent, explicit control.

**Validate permissions inside the controller rather than at admission.** Moving the check to the controller means an admitted object may still fail silently, making the failure mode harder to observe. Admission provides immediate, synchronous feedback to the user.

**Add a dedicated `ReplicationPolicy` resource.** An explicit policy object could grant replication rights with richer selectors (e.g., "only replicate objects with label X"). This is more expressive but significantly more complex. The `replicate` verb can be introduced now and a policy resource added later if the simpler model proves insufficient.
