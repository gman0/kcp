# Enhancement: Splitting ClusterCachedResource into Definition and Consumer

## Status

Draft

## Summary

Split the current `ClusterCachedResource` into two separate API objects: a **`CachedResourceDefinition`** that holds the group-resource-identity tuple without triggering replication, and a **`ClusterCachedResource`** (repurposed) that references a definition and activates replication within its own workspace. The `Cluster` prefix on the consumer type reflects that it caches cluster-scoped resources; a future `CachedResource` (no prefix) would handle namespaced resources by the same model.

`CachedResourceDefinition` is version-agnostic by design. Identity ownership is a property of a group-resource pair, not of any particular version. Only the storage version of a resource ends up in etcd anyway, so version is not a meaningful axis for either ownership or replication.

This mirrors the `APIExport`/`APIBinding` relationship and introduces a clear ownership boundary that prevents identity spoofing across workspaces.

## Motivation

### Current behavior

A single `ClusterCachedResource` object does two things at once:

1. It establishes a group-resource ↔ identity binding (the identity key is a secret whose SHA-256 hash appears in every etcd path for objects replicated under that resource).
2. It immediately triggers replication of matching objects into the cache.

The identity hash is the public discriminator: two workspaces that independently cache `cpuflavors.cloud.example.com` will store their copies under different paths in etcd precisely because they have different identities. The identity **key** (the private secret) must never leak, but there is currently no mechanism for one workspace to assert "only I may replicate under this identity hash."

### Problems

**No cross-workspace identity delegation.** When a service provider wants multiple workspaces to replicate the same resource under a shared, well-known identity hash (e.g., a provider shard and a DR shard), they must copy the raw identity secret. There is no first-class way to grant another workspace the right to replicate under a given identity without exposing the private key.

**Coupling of definition and activation.** Creating the current object immediately starts replication. There is no way to define the identity up front (e.g., as part of API-definition bootstrapping) and let consumers opt in to replication separately, with auditable per-workspace lifecycle.

**No revocability.** Because activation and identity ownership are fused into one object, revoking one workspace's access requires the definition owner to rotate the identity key, which disrupts all other workspaces that rely on the same identity hash.

**Version as a false axis.** The current spec includes `version`, but only the storage version of a resource lands in etcd. Requiring owners to create one `ClusterCachedResource` per version is unnecessary ceremony with no storage-level effect.

### Goals

- Separate identity ownership from replication activation.
- Allow a workspace to grant other workspaces the right to replicate under its identity without sharing the identity key.
- Replication is only triggered in workspaces where an explicit, permitted `ClusterCachedResource` exists.
- Multiple `ClusterCachedResource` objects across different workspaces may reference the same definition simultaneously with no exclusivity constraints. Within a single workspace, at most one `ClusterCachedResource` may exist per group-resource-identity tuple (enforced by resolving the identity hash from the definition at admission time).
- Neither the definition nor the consumer object carries a version field.
- Existing single-workspace usage remains as a trivial definition + `ClusterCachedResource` co-located in the same workspace.

### Non-goals

- Cross-workspace replication topology changes (direction of replication is unchanged).
- Exposing the identity key to consumer workspaces.
- Namespaced resource caching (`CachedResource`, no `Cluster` prefix) — this is a future extension that follows the same model.
- Replacing the existing `ClusterCachedResourceEndpointSlice` → `APIExport` export path; that layer is orthogonal.

## Design

### New resource: `CachedResourceDefinition`

The definition is a new cluster-scoped API object that lives in the provider workspace. It is responsible for:

- Holding the group-resource pair.
- Creating and owning the identity secret (same mechanism as today's `ClusterCachedResource`).
- Computing and publishing the identity hash in its status.
- **Not** triggering any replication itself.

```yaml
apiVersion: cache.kcp.io/v1alpha1
kind: CachedResourceDefinition
metadata:
  name: cpuflavors
spec:
  group: cloud.example.com
  resource: cpuflavors
  # Optional: bring-your-own identity key.
  identity:
    secretRef:
      name: my-cpuflavors-identity
      namespace: kcp-system
status:
  identityHash: cd2eb0837...
  phase: Ready          # Ready once identity is resolved; no replication phase.
  conditions: [...]
```

Constraints:

- At most one `CachedResourceDefinition` per group-resource-identity tuple per workspace. Multiple definitions for the same group-resource are permitted if they carry different identities, as each produces a distinct identity hash and therefore distinct etcd paths.
- Must reference a cluster-scoped resource (for now; namespaced support is a future extension).
- The identity secret is managed entirely by the definition's controller; consumer workspaces never receive or see it.

### Updated resource: `ClusterCachedResource` (the consumer)

The existing `ClusterCachedResource` is repurposed as the consumer object. Its spec gains a required `definitionRef` that points to a `CachedResourceDefinition` by workspace path and name, and loses its GVR fields entirely. The label selector and all status fields remain as today.

```yaml
apiVersion: cache.kcp.io/v1alpha1
kind: ClusterCachedResource
metadata:
  name: cpuflavors
spec:
  definitionRef:
    path: "root:provider"   # Logical cluster path of the definition workspace.
    name: cpuflavors        # Name of the CachedResourceDefinition.
  # LabelSelector filters which objects in this workspace are replicated.
  labelSelector:
    cloud.example.com/visibility: Public
status:
  identityHash: cd2eb0837...   # Resolved from the definition; used for etcd key construction.
  phase: Ready
  resourceCounts:
    cache: 8
    local: 8
  conditions: [...]
```

The controller resolves the definition, copies `identityHash` into status, and proceeds with replication exactly as today. Group and resource are derived from the definition; no version appears anywhere in the pipeline because the cache server always stores the resource's storage version.

#### Same-workspace shorthand

When the definition and `ClusterCachedResource` are in the same workspace, `definitionRef.path` may be omitted. This is the common case and matches current single-workspace behavior. Admission automatically fills in the path when absent.

#### Future: `CachedResource` for namespaced resources

When namespaced caching is introduced, `CachedResource` (no `Cluster` prefix) will follow the same model: it references a `CachedResourceDefinition` via `definitionRef` and activates replication of namespace-scoped objects. No changes to `CachedResourceDefinition` are anticipated; the definition is already scope-agnostic.

### Identity rotation readiness

Identity rotation is not implemented by this enhancement, but the design is laid in so that a future iteration can add it without API breaks. See also [KEP 0005 — APIExport Identity Rotation](../../../../kcp-enhancements/keps/core/0005-apiexport-identity-rotation.md), which solves the same problem for APIExport and is the direct analogue.

**`spec.identity` is immutable.** Rotation is not a spec edit. This preserves the invariant that a definition's identity can only change through a controlled, auditable procedure — preventing accidental storage migrations triggered by a `kubectl patch`.

**`status.identityHashes` (plural).** A new list field is added to `CachedResourceDefinitionStatus` alongside the existing `identityHash` scalar. Normally it contains exactly one entry — the current hash — and is redundant with the scalar. During rotation it contains both the old and new hash; the new hash is the drain target (canonical), old hashes are drain sources. This mirrors `BoundAPIResource.identityHashes` in KEP 0005, which uses the same set-semantics: the current `identityHash` is the target, every other entry in the list is a source to drain. A drain is complete when the list shrinks back to `[identityHash]`.

**Cache server index must tolerate multiple hashes.** The `ByIdentityAndGroupResource` index in the cache server currently maps one identity hash to one group-resource. During rotation both hashes must be indexed so the cache server can serve reads from both prefixes until the drain completes. The `identityHashes` list drives this: the index is populated from the list, not only from the scalar.

**Rotation is simpler than KEP 0005.** The two structural differences that make rotation tractable without the full KEP 0005 machinery:

1. *Data lives in one place.* Replicated objects are stored in the cache server, not spread across per-workspace etcd stores. Rotation is a single prefix-to-prefix copy inside the cache server, not a fan-out across every consumer workspace with per-workspace fencing.
2. *Consumers reference the definition, not the hash.* `ClusterCachedResource.spec.definitionRef` points to the definition by name; `status.identityHash` is derived. When the definition's hash changes, every consumer picks up the new hash from status automatically — no consumer spec updates, no alias/normalization layer, no cross-export permission claim breakage.

### Etcd key format (unchanged)

No change to the storage format. The consumer controller resolves the definition's identity hash and constructs the etcd key as today:

```
/<prefix>/<group>/<resource>:<identityHash>/<shard>/...
```

Version has never appeared in this path; the cache server stores whichever version is the storage version of the resource. This is consistent with removing version from the API surface entirely.

The security property is preserved: replicating under a given identity hash requires the controller to have fetched the raw secret from the **definition's** workspace, which is only possible after the consumer object has been admitted (see below). The consumer workspace never touches the key directly.

### Access policy

Admission of a new `ClusterCachedResource` performs two checks:

1. **Definition exists.** The referenced `CachedResourceDefinition` is resolved across workspaces.
2. **Consumer is permitted.** The user creating the `ClusterCachedResource` must have been granted the `use` verb on the `CachedResourceDefinition` object in the definition's workspace (via a standard RBAC `ClusterRoleBinding` in that workspace).

The check is performed as a `SubjectAccessReview` against the definition workspace, meaning the provider's RBAC controls who may activate replication under its identity. Revoking access requires only revoking the RBAC binding — no identity key rotation needed.

When access is revoked, the `ClusterCachedResource` enters a `Forbidden` phase and replication is suspended. Objects already in cache are not purged until the consumer object is deleted.

### Controller changes

| Controller | Change |
|---|---|
| `CachedResourceDefinition` reconciler (new) | Mirrors identity reconciler from today's CCR. Resolves/creates identity secret, computes hash, sets status. No replication. |
| `ClusterCachedResource` reconciler (updated) | Removes self-contained identity creation and GVR fields. Resolves definition via cross-workspace watch, copies `identityHash`, proceeds to replication. Adds `Forbidden` phase transition. |
| Admission plugin | Adds cross-workspace definition resolution and `use`-verb SAR. Uniqueness is enforced on the group-resource-identity tuple per workspace for both `CachedResourceDefinition` and `ClusterCachedResource`. For the consumer, the identity hash is resolved from the referenced definition at admission time. |

## API type sketches

```go
// CachedResourceDefinition owns a group-resource-identity tuple.
// It does not trigger replication and carries no version.
type CachedResourceDefinition struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`
    Spec   CachedResourceDefinitionSpec   `json:"spec"`
    Status CachedResourceDefinitionStatus `json:"status,omitempty"`
}

type CachedResourceDefinitionSpec struct {
    GroupResource `json:",inline"`
    Identity      *Identity `json:"identity,omitempty"`
}

// GroupResource identifies a resource independently of version.
type GroupResource struct {
    // group is the API group. Empty string for the core group.
    // +optional
    Group string `json:"group,omitempty"`
    // resource is the resource name.
    // +required
    Resource string `json:"resource"`
}

type CachedResourceDefinitionStatus struct {
    // IdentityHash is the current (target) identity hash, used in etcd key construction.
    IdentityHash string `json:"identityHash,omitempty"`
    // IdentityHashes lists every identity hash currently present in the cache for this
    // group-resource. Normally contains exactly [IdentityHash]. During rotation it also
    // contains the old hash(es) being drained; the list shrinks back to one entry when
    // the drain completes. Drives the cache server's ByIdentityAndGroupResource index.
    // +listType=set
    IdentityHashes []string                       `json:"identityHashes,omitempty"`
    Phase          ClusterCachedResourcePhaseType `json:"phase,omitempty"`
    Conditions     conditionsv1alpha1.Conditions  `json:"conditions,omitempty"`
}

// ClusterCachedResource (updated) is the consumer object for cluster-scoped resources.
// Group, resource, and identity hash are all resolved from the referenced definition.
type ClusterCachedResourceSpec struct {
    // DefinitionRef resolves the CachedResourceDefinition that governs this object.
    DefinitionRef ClusterCachedResourceReference `json:"definitionRef"`
    // LabelSelector filters objects in this workspace for replication.
    LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty"`
}
```

`ClusterCachedResourceReference` already exists in the API package with `Path` and `Name` fields, so no new cross-workspace reference type is required.

## Alternatives considered

**Keep version in `CachedResourceDefinition`.** Rejected: the etcd storage path never includes version, so tying identity ownership to a version forces owners to create redundant definitions with no storage-level effect.

**Let consumers specify a version.** Rejected for the same reason, and additionally because it would allow two consumers to reference the same definition with different versions — an incoherent state, since both would replicate to the same etcd paths under the same identity hash.

**Keep the single object, add an access allowlist.** This avoids adding a new type but does not decouple the replication lifecycle from identity ownership: you still cannot define an identity without immediately replicating.

**Use a separate `Secret` transfer mechanism.** The definition could grant another workspace read access to the identity secret. This leaks the private key and is explicitly a non-goal.

**Require an explicit approval object in the definition workspace (like `APIExportPermissionClaim`).** This is more auditable but adds ceremony. The `use`-verb RBAC check achieves the same access control with standard tooling and no additional resource type.

**Name the consumer type something other than `ClusterCachedResource`.** Reusing the existing name preserves backward compatibility and keeps the naming convention consistent: the `Cluster` prefix signals cluster-scoped resource caching, leaving `CachedResource` (no prefix) as a natural future slot for namespaced caching.
