# Enhancement: ClusterCachedResource v2

## Status

Draft

## Summary

This document describes the v2 API for `ClusterCachedResource`. It makes three targeted changes to the existing design:

1. **Removes `spec.version`** — identity ownership is a property of a group-resource pair; the cache server always stores the storage version, so version is not a meaningful axis.
2. **Makes `spec.identity` immutable** — identity rotation is not a spec edit; the API is structured so a future rotation mechanism (modelled on [KEP 0005](../../../../kcp-enhancements/keps/core/0005-apiexport-identity-rotation.md)) can be layered on top without API changes.
3. **Adds `status.identityHashes` (plural)** — a set field that tracks every identity hash currently present in the cache for this resource; normally a singleton, it grows to two entries during an in-flight rotation and shrinks back when the drain completes.

No other behaviour changes. The design also documents the forward path to optional cross-workspace identity sharing (v3), which becomes a feasible API version bump on top of v2 without any etcd path changes.

## Changes from v1

### Removed: `spec.version`

`spec.group` and `spec.resource` remain. `spec.version` is dropped.

**Why.** The etcd key for cached objects is `/<prefix>/<group>/<resource>:<identityHash>/<shard>/...` — version has never appeared in this path. Only the storage version of a resource lands in the cache. Requiring a version field forces owners to reason about a dimension that has no storage-level effect and would require one `ClusterCachedResource` per version for no benefit.

**Uniqueness.** The admission uniqueness constraint changes from one object per group-version-resource per workspace to one object per **group-resource-identity** tuple per workspace. Multiple `ClusterCachedResource` objects for the same group-resource are permitted if they carry different identities, since each produces a distinct identity hash and therefore distinct etcd paths.

### Changed: `spec.identity` is immutable

Once the object reaches `Ready` phase, `spec.identity.secretRef` is immutable. Rotation is a separate, future operation — not a spec edit. This matches the pattern established by `APIExport` (KEP 0005) and prevents accidental storage migrations triggered by a `kubectl patch`.

If no `secretRef` is provided, the controller auto-generates an identity secret as today. Once generated, that secret reference is written back into the spec and becomes immutable from that point on.

### Added: `status.identityHashes`

```go
// IdentityHashes lists every identity hash currently present in the cache
// for this group-resource. Normally contains exactly [IdentityHash].
// During an in-flight identity rotation it also contains the old hash(es)
// being drained; the list shrinks back to one entry when the drain completes.
// Mirrors BoundAPIResource.identityHashes from KEP 0005.
// +listType=set
IdentityHashes []string `json:"identityHashes,omitempty"`
```

The existing `status.identityHash` scalar is preserved for compatibility. `identityHashes` is always a superset of `{identityHash}`.

The cache server's `ByIdentityAndGroupResource` index is updated to index all entries in `identityHashes`, not only `identityHash`. Today this makes no difference; during a rotation drain it allows the replication virtual workspace to serve from both old and new prefixes simultaneously.

## Full API shape

```yaml
apiVersion: cache.kcp.io/v1alpha1
kind: ClusterCachedResource
metadata:
  name: cpuflavors
spec:
  group: cloud.example.com   # required
  resource: cpuflavors       # required
  # Optional: bring-your-own identity key. Immutable once the object is Ready.
  identity:
    secretRef:
      name: my-cpuflavors-identity
      namespace: kcp-system
  # Optional: filter which objects are replicated.
  labelSelector:
    cloud.example.com/visibility: Public
status:
  identityHash: cd2eb0837...       # current (target) hash
  identityHashes: [cd2eb0837...]   # all hashes in cache; normally a singleton
  phase: Ready
  resourceCounts:
    cache: 8
    local: 8
  conditions: [...]
```

## Admission changes

| Check | v1 | v2 |
|---|---|---|
| Uniqueness | one per group-version-resource per workspace | one per group-resource-identity tuple per workspace |
| `spec.identity` immutability | not enforced | enforced once `phase == Ready` |
| Resource must be cluster-scoped | unchanged | unchanged |

## Controller changes

| Controller | Change |
|---|---|
| Identity reconciler | After writing the auto-generated `secretRef` back into spec, marks it immutable via admission. Populates both `status.identityHash` and `status.identityHashes`. |
| Replication controller | No change in behaviour; reads `status.identityHash` as today. |
| Cache server index | Updated to index all entries in `status.identityHashes`; no behavioural change today. |

## Forward path: optional identity sharing (v3)

Cross-workspace identity sharing — multiple workspaces replicating under a single shared identity for aggregated list/watch — has no confirmed practical use case at the time of writing. It is explicitly a **future, optional** extension, not a goal of v2. This section records why v2 is designed so that sharing can be added without etcd path changes if the use case emerges.

### What v3 would add

A new `CachedResourceDefinition` type would own the group-resource-identity tuple without triggering replication. `ClusterCachedResource` would gain a `definitionRef` field and lose its GVR and identity fields. Multiple consumer objects across workspaces could reference the same definition, contributing their objects to the same cache prefix.

### Why the conversion from v2 to v3 is feasible

- **No etcd path changes.** The identity hash — and therefore the cache prefix — is determined by the secret, which doesn't change during conversion.
- **Uniqueness semantics are compatible.** Both v2 and v3 enforce one object per group-resource-identity tuple per workspace; the invariant is preserved across the conversion.
- **Additive migration.** The conversion creates a new `CachedResourceDefinition` object in the same workspace (same GVR, same identity secret reference) and adds a `definitionRef` to the existing `ClusterCachedResource`. No objects are deleted.

### What v3 conversion requires

- **A formal API version bump** (`v1alpha1` → `v1alpha2`). The v3 `ClusterCachedResource` spec shape (only `definitionRef`, no GVR or identity fields) is structurally incompatible with v2; a conversion webhook is needed.
- **A migration step before the webhook.** The conversion webhook cannot lazily synthesize a `CachedResourceDefinition` — the object must exist first. A migration controller (or CLI tool) creates a `CachedResourceDefinition` for each existing v2 `ClusterCachedResource` before the version bump is activated.
- **Secret ownership transfer.** In v2, the `ClusterCachedResource` controller manages the identity secret. In v3, the `CachedResourceDefinition` controller does. The migration must transfer ownership (via finalizer/annotation handoff) so the secret is not double-managed or orphaned.

These are implementation costs, not design obstacles. None of them require touching etcd or interrupting replication.
