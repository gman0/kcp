# Enhancement: Fine-Grained RBAC for ClusterCachedResources (v2)

## Status

Draft.

## Implementation status

Not yet implemented. The following changes are pending:

- **`list`+`watch` SAR checks.** The `ClusterCachedResource` admission plugin (`pkg/admission/clustercachedresource/admission.go`) does not yet issue `SubjectAccessReview` calls for the target resource on pre-create.
- **`CachedAPIsRBAC` feature gate.** The gate does not yet exist; no gating logic is wired into the admission path.

## Summary

At admission time, verify that the subject creating a `ClusterCachedResource` has `list` and `watch` on the resource being replicated. This closes a privilege escalation gap — today any user who can create a `ClusterCachedResource` can trigger replication of any resource type, regardless of whether they can read it — using only existing RBAC primitives and requiring no new verbs or additional operator grants.

This document is aligned with the [v2 ClusterCachedResource design](./enhancement-clustercachedresource-v2.md): the check is against the group-resource pair only; no version is involved.

## Motivation

### Current behavior

The only permission check before replication begins is whether the user has RBAC permission to `create` a `ClusterCachedResource` object. Any user with that right can instruct kcp to begin replicating any cluster-scoped resource type, regardless of whether they have any access to those resources.

Concretely: a user with `create clustercachedresources` can replicate `secrets` — even if they have no right to `list` or `get` `secrets` directly.

### Problems

**Privilege escalation via the cache.** The cache server exposes replicated objects to any workspace that binds the corresponding `APIExport`. A user who triggers replication of a resource they cannot directly read may be able to consume that data indirectly through a cache-backed virtual resource in another workspace.

**No per-resource-type gating.** There is no way to express "users in this workspace may replicate `cpuflavors` but not `secrets`." The only lever is the binary `create clustercachedresources` permission, which is all-or-nothing.

### Goals

- Ensure a user can only replicate resources they are already allowed to read.
- Require no new API types, no new verbs, and no additional operator grants beyond what already exists.
- Apply the check at admission time, before any controller acts on the object.

### Non-goals

- Per-object replication filtering (label selectors already cover that use case).
- Controlling which consumers may read cached data (governed by the `APIExport` permission model).
- Retrofitting this check onto namespace-scoped resources (not yet supported).

## Design

### Admission check

The `ClusterCachedResource` admission plugin (`pkg/admission/clustercachedresource/admission.go`) is extended with two `SubjectAccessReview` checks on pre-create, one for `list` and one for `watch`:

```
SubjectAccessReview{
  User:   <request user info>,
  ResourceAttributes: {
    Verb:     "list",   // and separately "watch"
    Group:    <spec.group>,
    Resource: <spec.resource>,
  },
}
```

Both must return `Allowed: true`. If either is denied, the admission plugin returns `403 Forbidden` indicating which verb and resource was missing.

The check is not repeated on update, since `spec.group` and `spec.resource` are immutable — changing the target resource requires a new object.

The rationale is direct: the replication controller issues a `list` and then a `watch` of the resource on the user's behalf. Requiring the user to hold those verbs themselves ensures the controller acts within the bounds of what the user is already permitted to read.

### Interaction with system:masters and bootstrapping

System-level components that set up built-in caching (e.g., kcp's own shard coordination) are exempt via the standard `system:masters` bypass.

### Default posture

When the feature gate `CachedAPIsRBAC` is enabled, the `list`+`watch` check is enforced. While the gate is disabled (the default for the initial release), the admission plugin skips the check entirely, preserving backward compatibility.

### Audit trail

Because the checks are `SubjectAccessReview` calls, both the `list` and `watch` decisions appear in the API server audit log, giving operators a record of which subjects were authorized to replicate which resource types without any additional audit infrastructure.

## Example walkthrough

Alice has `list` and `watch` on `cpuflavors.cloud.example.com` — granted as part of her normal read access to that API — and can therefore create a `ClusterCachedResource` for it without any additional grants:

```yaml
# Succeeds: alice has list+watch on cpuflavors.
apiVersion: cache.kcp.io/v1alpha1
kind: ClusterCachedResource
metadata:
  name: cpuflavors
spec:
  group: cloud.example.com
  resource: cpuflavors
```

```yaml
# Fails with 403: alice does not have list+watch on secrets.
apiVersion: cache.kcp.io/v1alpha1
kind: ClusterCachedResource
metadata:
  name: secrets
spec:
  group: ""
  resource: secrets
```

No extra ClusterRole or ClusterRoleBinding is needed. The existing read grants are sufficient.

## Implementation notes

### SubjectAccessReview mechanics

The admission plugin already has access to request user info via `admission.Attributes.GetUserInfo()`. Both SARs are issued against the local `kube-apiserver` using the existing `authorizer` injected into the admission handler — no new client is required. Both checks are pure authorization decisions with no watch or cache lookup.

### Forward compatibility with identity sharing (v3)

If cross-workspace identity sharing is introduced in a future v3, the `list`+`watch` check runs in each workspace where a `ClusterCachedResource` is created. No changes to the check are needed.

### Feature gate

| Gate | Default | Effect |
|---|---|---|
| `CachedAPIsRBAC` | `false` | check skipped; behavior unchanged from today |
| `CachedAPIsRBAC` | `true` | `list`+`watch` SAR enforced at admission |

## Alternatives considered

**A dedicated `replicate` verb.** Would allow expressing "you may list a resource but not replicate it." In practice this distinction is rarely meaningful: if a user can already `list` a resource, the objects are accessible to them directly; caching moves them to a different store, still gated by the `APIExport` permission model. A separate verb adds operator friction — new ClusterRoles on top of existing read grants — for a distinction that yields little practical benefit.

**Validate permissions inside the controller rather than at admission.** An admitted object may then fail silently, making the failure mode harder to observe. Admission gives immediate, synchronous feedback.
