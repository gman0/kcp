# Enhancement: ClusterCachedResourceEndpointSlice v2

## Status

Draft

## Summary

Remove `ResourceSchemaStorageVirtual.IdentityHash` from the APIExport spec entirely. Replace it as a lookup key throughout the system — in the bound CRD annotation and in the `aggregatingcrdversiondiscovery` indexer — with the endpoint slice reference (`<apiGroup>/<kind>/<name>`) that is already present in `ResourceSchemaStorageVirtual.Reference`. The replication virtual workspace handler derives the identity hash at request time by following the endpoint slice reference to the `ClusterCachedResource` and reading `status.identityHashes`. The endpoint slice type itself carries no identity information.

This document is aligned with the [v2 ClusterCachedResource design](./enhancement-clustercachedresource-v2.md).

## Motivation

### Current behavior

`ResourceSchemaStorageVirtual.IdentityHash` currently serves two distinct roles:

1. **Lookup key for `aggregatingcrdversiondiscovery`.** At bind time, `apiextensions.go` writes the annotation `apis.kcp.io/schema-storage = "virtual:<identityHash>"` onto the bound CRD. The discovery handler reads this annotation, extracts the hash, and uses `IndexAPIExportByVirtualResourceIdentities` to find the matching APIExport and its `storage.virtual` spec.

2. **Routing value for the replication VW handler.** The handler uses the hash to construct the correct etcd prefix path on the cache server (`/<prefix>/<group>/<resource>:<identityHash>/...`).

### Problems

**Redundancy with the reference.** `ResourceSchemaStorageVirtual.Reference` already uniquely identifies the endpoint slice. The hash is derived from the `ClusterCachedResource` that the endpoint slice points to — it is always recoverable by following that chain. Requiring the APIExport author to also state it manually is duplication that creates a sync hazard: a stale or wrong hash silently breaks both discovery and VW routing.

**Identity does not belong in the APIExport spec.** The `ClusterCachedResource` owns the identity. The APIExport's job is to declare which endpoint slice to use for virtual storage; the identity is an internal property of that endpoint slice's backing resource, not something the APIExport author should reason about.

### Goals

- Remove `ResourceSchemaStorageVirtual.IdentityHash` from the APIExport type.
- Replace the identity hash as the lookup key in `aggregatingcrdversiondiscovery` with the endpoint slice reference.
- The endpoint slice type remains a pure routing object with no identity fields.
- The replication VW handler resolves identity internally by following the endpoint slice → `ClusterCachedResource` → `status.identityHashes` chain.

### Non-goals

- Changes to the endpoint slice type or its controller.
- Implementing identity rotation (handled by the v2 ClusterCachedResource design).

## Design

### Removed field: `ResourceSchemaStorageVirtual.IdentityHash`

The field is removed from the APIExport type. The virtual storage spec reduces to:

```yaml
# APIExport (v2)
spec:
  resources:
  - group: cloud.example.com
    name: cpuflavors
    schema: v250801.cpuflavors.cloud.example.com
    storage:
      virtual:
        reference:
          apiGroup: cache.kcp.io
          kind: ClusterCachedResourceEndpointSlice
          name: cpuflavors
```

### Changed: bound CRD annotation format

`apiextensions.go` currently writes:

```go
out.Annotations[apisv1alpha1.AnnotationSchemaStorageKey] = fmt.Sprintf("virtual:%s", resourceStorage.Virtual.IdentityHash)
```

This changes to encode the endpoint slice reference instead:

```go
ref := resourceStorage.Virtual.Reference
out.Annotations[apisv1alpha1.AnnotationSchemaStorageKey] = fmt.Sprintf("virtual:%s/%s/%s",
    ptr.Deref(ref.APIGroup, ""), ref.Kind, ref.Name)
```

Example annotation value: `"virtual:cache.kcp.io/ClusterCachedResourceEndpointSlice/cpuflavors"`

### Changed: indexer and discovery handler

**New indexer** (`pkg/indexers/apiexport.go`):

```go
// IndexAPIExportByVirtualResourceReference indexes an APIExport by the
// "<apiGroup>/<kind>/<name>" key of each virtual storage reference.
func IndexAPIExportByVirtualResourceReference(obj interface{}) ([]string, error) {
    apiExport := obj.(*apisv1alpha2.APIExport)
    var keys []string
    for _, res := range apiExport.Spec.Resources {
        if res.Storage.Virtual == nil {
            continue
        }
        ref := res.Storage.Virtual.Reference
        keys = append(keys, VirtualResourceReferenceKey(ref))
    }
    return keys, nil
}

func VirtualResourceReferenceKey(ref corev1.TypedLocalObjectReference) string {
    return fmt.Sprintf("%s/%s/%s", ptr.Deref(ref.APIGroup, ""), ref.Kind, ref.Name)
}
```

The existing `IndexAPIExportByVirtualResourceIdentities` and `IndexAPIExportByVirtualResourceIdentitiesAndGRs` are removed.

**Updated discovery handler** (`pkg/server/aggregatingcrdversiondiscovery/verbs_provider.go`):

```go
// Previously extracted vrIdentity from the annotation and looked up by identity hash.
// Now extracts the reference key and looks up by reference.
refKey := strings.TrimPrefix(annotation, "virtual:")
apiExports := indexer.ByIndex(IndexAPIExportByVirtualResourceReference, refKey)
```

**Updated virtual storage verbs provider** (`verbs_provider_virtualstorage.go`):

The match condition changes from:

```go
resourceSchema.Storage.Virtual.IdentityHash == vrIdentity
```

to matching on the reference key:

```go
VirtualResourceReferenceKey(resourceSchema.Storage.Virtual.Reference) == refKey
```

### Identity resolution in the replication virtual workspace

The VW handler (`pkg/virtual/replication/builder/build.go`) resolves the identity at request time, independently of the discovery path:

1. Resolve the `ClusterCachedResourceEndpointSlice` from the request URL.
2. Follow `spec.clusterCachedResource` to the `ClusterCachedResource` (informer-cached lookup).
3. Read `status.identityHashes` — normally one entry; multiple during an in-flight rotation.
4. Fan reads across all live identity prefixes and merge results.

No identity hash appears anywhere in the discovery handler or the APIExport spec; the two concerns (discovery routing and cache read routing) are now fully separated.

## Summary of changes

| Location | Current | v2 |
|---|---|---|
| `ResourceSchemaStorageVirtual` | Has `IdentityHash string` field | Field removed |
| `apiextensions.go` annotation | `"virtual:<identityHash>"` | `"virtual:<apiGroup>/<kind>/<name>"` |
| Indexer | `IndexAPIExportByVirtualResourceIdentities` (by hash) | `IndexAPIExportByVirtualResourceReference` (by reference) |
| Discovery handler match | `Storage.Virtual.IdentityHash == vrIdentity` | `VirtualResourceReferenceKey(ref) == refKey` |
| VW handler identity source | `ResourceSchemaStorageVirtual.IdentityHash` | `ClusterCachedResource.status.identityHashes` via endpoint slice reference |

## Alternatives considered

**Keep `IdentityHash` but make it system-derived.** A controller or admission plugin could read `ClusterCachedResource.status.identityHash` and fill the field in automatically, removing the manual sync requirement while preserving the existing lookup mechanism. Rejected: the field would still exist as a confusing redundant identity field on the APIExport, the sync logic adds complexity, and the reference is already a sufficient and more stable lookup key.

**Use the endpoint slice name alone as the annotation value.** Simpler annotation format, but endpoint slice names are only unique within a kind. Using the full `<apiGroup>/<kind>/<name>` key avoids ambiguity if other endpoint slice types are introduced.
