# Enhancement: CachedResourceEndpointSlice

## Status

Draft

## Summary

Rename `ClusterCachedResourceEndpointSlice` to **`CachedResourceEndpointSlice`** and retarget its `spec.clusterCachedResource` reference from the consumer (`ClusterCachedResource`) to the new **`CachedResourceDefinition`**. Because the definition is the authoritative holder of the identity hash, the explicit `identityHash` field on the APIExport's virtual storage spec becomes derivable and is removed, eliminating a manual sync requirement and a class of misconfiguration. The endpoint slice itself remains identity-agnostic: it is purely a routing object. Identity resolution is the replication virtual workspace handler's responsibility, via the definition reference carried in the slice's spec.

## Motivation

### Current behavior

`ClusterCachedResourceEndpointSlice` references a `ClusterCachedResource` (the old combined owner+consumer object) and an `APIExport`. The APIExport's virtual storage spec must additionally carry an explicit `identityHash` that matches `ClusterCachedResource.status.identityHash`:

```yaml
# APIExport (current)
storage:
  virtual:
    reference:
      apiGroup: cache.kcp.io
      kind: ClusterCachedResourceEndpointSlice
      name: cpuflavors-v1
    identityHash: cd2eb0837...   # must be kept in sync manually
```

### Problems

**Wrong anchor.** The endpoint slice is about serving cached data from the cache server via the replication virtual workspace. That is an identity-level concern, not a per-consumer concern. In the new design, the consumer (`ClusterCachedResource`) may live in a different workspace than the provider, and many consumers may replicate under the same identity. The endpoint slice has no meaningful relationship with any individual consumer.

**Manual identity hash sync.** The `identityHash` in the APIExport spec must match the `ClusterCachedResource`'s status exactly. This is error-prone: a typo, a stale copy-paste, or an identity rotation leaves the APIExport serving the wrong prefix with no automatic detection.

### Goals

- Retarget the endpoint slice to `CachedResourceDefinition` as the definition reference; keep the slice itself identity-agnostic.
- Eliminate the explicit `identityHash` from the APIExport virtual storage spec; the replication VW resolves it from the definition directly.
- Maintain all existing endpoint URL population, partition, and shard-selector behavior.

### Non-goals

- Changing how the replication virtual workspace routes or serves requests beyond the identity resolution.
- Implementing identity rotation (deferred; see the identity-split enhancement).
- Changing the `APIExport` → `APIBinding` relationship or the consumer-side projection of cached resources.

## Design

### Renamed and retargeted resource: `CachedResourceEndpointSlice`

The `Cluster` prefix is dropped because the endpoint slice is no longer tied to a cluster-scoped consumer; it is tied to a `CachedResourceDefinition`, which is itself scope-agnostic (the resource being cached must currently be cluster-scoped, but that is a definition constraint, not a property of the endpoint slice).

```yaml
apiVersion: cache.kcp.io/v1alpha1
kind: CachedResourceEndpointSlice
metadata:
  name: cpuflavors
spec:
  cachedResourceDefinition:
    path: "root:provider"   # Logical cluster path; omit if same workspace as the slice.
    name: cpuflavors        # Name of the CachedResourceDefinition.
  export:
    path: "root:provider"   # Logical cluster path; omit if same workspace as the slice.
    name: vm-provider       # Name of the APIExport.
  # Optional: restrict endpoint population to a named Partition.
  partition: eu-west
status:
  endpoints:
  - url: https://shard-1.kcp.example.com/services/replication/root:provider/cpuflavors
  - url: https://shard-2.kcp.example.com/services/replication/root:provider/cpuflavors
  shardSelector: "region=eu-west"
  conditions: [...]
```

Both `cachedResourceDefinition` and `export` references are immutable once set, as today.

### Dropped field: `identityHash` on the APIExport virtual storage spec

`ResourceSchemaStorageVirtual.IdentityHash` is removed. The replication virtual workspace handler resolves the identity hash(es) by following the endpoint slice reference to the `CachedResourceDefinition` and reading `status.identityHashes`. The APIExport spec simplifies to:

```yaml
# APIExport (new)
spec:
  resources:
  - group: cloud.example.com
    name: cpuflavors
    schema: v250801.cpuflavors.cloud.example.com
    storage:
      virtual:
        reference:
          apiGroup: cache.kcp.io
          kind: CachedResourceEndpointSlice
          name: cpuflavors
```

### Identity resolution in the replication virtual workspace

The virtual workspace handler (currently in `pkg/virtual/replication/builder/build.go`) is updated to:

1. Resolve the `CachedResourceEndpointSlice` from the request URL.
2. Follow `spec.cachedResourceDefinition` to the `CachedResourceDefinition`.
3. Read `status.identityHashes` from the definition — normally one entry, multiple during an in-flight rotation.
4. Fan reads across all live identity prefixes and merge results.

The endpoint slice is not involved in steps 2–4; it only provides the definition reference. Identity is the definition's concern, not the slice's.

### Controller changes

| Component | Change |
|---|---|
| `CachedResourceEndpointSlice` reconciler | `spec.clusterCachedResource` → `spec.cachedResourceDefinition`; validates definition exists and is Ready. No identity handling. |
| URL population reconciler | Unchanged in structure and behavior. |
| Replication VW handler | Follows `spec.cachedResourceDefinition` to resolve identity from the definition's `status.identityHashes`; drops use of `ResourceSchemaStorageVirtual.IdentityHash`. |
| Indexers | `IndexByClusterCachedResource` renamed to `IndexByCachedResourceDefinition`; key format updated to match the new reference type. |

### Admission

The existing admission on `ClusterCachedResourceEndpointSlice` (immutability of `clusterCachedResource` and `export`) carries over unchanged, applied to the renamed type and the renamed field.

## API type sketches

```go
type CachedResourceEndpointSlice struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`
    Spec   CachedResourceEndpointSliceSpec   `json:"spec"`
    Status CachedResourceEndpointSliceStatus `json:"status,omitempty"`
}

type CachedResourceEndpointSliceSpec struct {
    // CachedResourceDefinition is the definition whose identity governs this slice.
    // Immutable once set.
    CachedResourceDefinition ClusterCachedResourceReference `json:"cachedResourceDefinition"`
    // Export is the APIExport that exposes the cached resource to consumers.
    // Immutable once set.
    Export ExportBindingReference `json:"export"`
    // Partition optionally restricts endpoint population to a named Partition.
    // +optional
    Partition string `json:"partition,omitempty"`
}

type CachedResourceEndpointSliceStatus struct {
    Endpoints     []CachedResourceEndpoint      `json:"endpoints,omitempty"`
    ShardSelector string                        `json:"shardSelector,omitempty"`
    Conditions    conditionsv1alpha1.Conditions `json:"conditions,omitempty"`
}

// CachedResourceEndpoint is an alias for corev1alpha1.Endpoint, as today.
type CachedResourceEndpoint = corev1alpha1.Endpoint
```

`ClusterCachedResourceReference` and `ExportBindingReference` already exist in the API package; no new reference types are required.

## Alternatives considered

**Keep referencing `ClusterCachedResource` (the consumer).** Rejected: in the new design the consumer may live in a different workspace, and multiple consumers replicate under the same identity. The endpoint slice has no meaningful per-consumer relationship; tying it to one consumer arbitrarily would force providers to pick one or create one dummy consumer per slice.

**Keep the explicit `identityHash` in the APIExport spec.** Rejected: it is always derivable by the VW handler from the definition, and manual maintenance creates a sync hazard. Removing it shrinks the error surface. Existing APIExport objects with the field set can be migrated by dropping the field; behavior is unchanged since the value is now sourced from the definition at request time.

**Keep the `Cluster` prefix.** Rejected: `CachedResourceEndpointSlice` is associated with a `CachedResourceDefinition`, which is scope-agnostic. The prefix would be misleading and would need revisiting when namespaced caching is introduced.
