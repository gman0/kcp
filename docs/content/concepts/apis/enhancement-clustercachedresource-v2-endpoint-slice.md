# Enhancement: ClusterCachedResourceEndpointSlice v2

## Status

Implemented.

## Implementation status

All changes described in this document are implemented:

- `ResourceSchemaStorageVirtual.IdentityHash` removed from the type, CRD YAML, OpenAPI schema, and apply-configuration.
- `Fingerprint(*APIExport)` method added to `ResourceSchemaStorageVirtual` (`staging/src/github.com/kcp-dev/sdk/apis/apis/v1alpha2/types_apiexport.go`).
- `IndexAPIExportByVirtualResourceFingerprint` indexer replaces the identity-based indexers (`pkg/indexers/apiexport.go`).
- `apiextensions.go` annotation uses `Fingerprint()` at bind time.
- `verbs_provider.go` looks up by fingerprint; `verbs_provider_virtualstorage.go` matches on group+resource only.
- Replication VW handler unchanged — it already read identity from `ClusterCachedResource.status.identityHash` independently.

## Summary

Remove `ResourceSchemaStorageVirtual.IdentityHash` from the APIExport spec entirely. Replace it as a lookup key throughout the `aggregatingcrdversiondiscovery` path with a **fingerprint** — a composite of the APIExport's own identity hash (read from `status.identityHash`, not the spec) and the endpoint slice reference already present in `ResourceSchemaStorageVirtual.Reference`. The fingerprint is computed at bind time and stored in the bound CRD annotation; no identity information is manually authored in the APIExport spec.

The replication virtual workspace handler is unaffected: it already resolves identity independently by reading `ClusterCachedResource.status.identityHash` through its own informer, and has never depended on `ResourceSchemaStorageVirtual.IdentityHash`.

This document is aligned with the [v2 ClusterCachedResource design](./enhancement-clustercachedresource-v2.md).

## Motivation

### Current behavior

`ResourceSchemaStorageVirtual.IdentityHash` currently serves one role in the discovery path and is otherwise unused at runtime:

1. **Lookup key for `aggregatingcrdversiondiscovery`.** At bind time, `apiextensions.go` writes the annotation `apis.kcp.io/schema-storage = "virtual:<identityHash>"` onto the bound CRD. The discovery handler reads this annotation, extracts the hash, and uses `IndexAPIExportByVirtualResourceIdentities` to find the matching APIExport and its `storage.virtual` spec.

The replication VW handler constructs the correct etcd prefix path (`/<prefix>/<group>/<resource>:<identityHash>/...`) from `ClusterCachedResource.status.identityHash` directly — it has never read `ResourceSchemaStorageVirtual.IdentityHash`.

### Problems

**Manual sync hazard.** `ResourceSchemaStorageVirtual.IdentityHash` must match the `ClusterCachedResource.status.identityHash` that backs the referenced endpoint slice. This is not enforced by admission or any controller. A stale or wrong hash silently breaks discovery for all workspaces that have bound the APIExport.

**Identity does not belong in the APIExport spec.** The `ClusterCachedResource` owns the identity. The APIExport's job is to declare which endpoint slice to use for virtual storage; the identity hash is an internal status property of that resource, not something the APIExport author should reason about or maintain.

### Goals

- Remove `ResourceSchemaStorageVirtual.IdentityHash` from the APIExport type.
- Derive the annotation lookup key at bind time from the system-owned `APIExport.status.identityHash` and the endpoint slice reference, with no manual authoring required.
- The endpoint slice type remains a pure routing object with no identity fields.
- The replication VW handler requires no changes.

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

This changes to write a **fingerprint** computed from the APIExport's own identity hash and the endpoint slice reference:

```go
out.Annotations[apisv1alpha1.AnnotationSchemaStorageKey] = fmt.Sprintf("virtual:%s", resourceStorage.Virtual.Fingerprint(apiExport))
```

The `Fingerprint` method is defined on `ResourceSchemaStorageVirtual`:

```go
func (virtual *ResourceSchemaStorageVirtual) Fingerprint(o *APIExport) string {
    gk := schema.GroupKind{Group: ptr.Deref(virtual.Reference.APIGroup, ""), Kind: virtual.Reference.Kind}
    return fmt.Sprintf("%s|%s/%s", o.Status.IdentityHash, virtual.Reference.Name, gk)
}
```

Example annotation values:
- `"virtual:cd2eb083...|cpuflavors/ClusterCachedResourceEndpointSlice.cache.kcp.io"`
- `"virtual:cd2eb083...|pods/EndpointSlice"` (core group — no dot suffix)

The format is `<apiExportIdentityHash>|<endpointSliceName>/<Kind>.<group>`, using `schema.GroupKind.String()` for the type portion. The `|` separator cannot appear in any component (hex hash, DNS subdomain name, PascalCase kind, DNS subdomain group). Core group produces a bare `Kind` with no trailing dot.

The fingerprint encodes the APIExport's identity alongside the endpoint slice reference, so two APIExports with different identities referencing the same endpoint slice produce distinct fingerprints and are unambiguous in the index.

### Changed: indexer and discovery handler

**New indexer** (`pkg/indexers/apiexport.go`):

```go
// IndexAPIExportByVirtualResourceFingerprint indexes an APIExport by the fingerprint of each
// virtual storage resource (identity hash + endpoint slice reference).
func IndexAPIExportByVirtualResourceFingerprint(obj interface{}) ([]string, error) {
    apiExport, ok := obj.(*apisv1alpha2.APIExport)
    if !ok {
        return []string{}, fmt.Errorf("obj %T is not an APIExport", obj)
    }
    if apiExport.Status.IdentityHash == "" {
        return []string{}, nil
    }
    keys := sets.New[string]()
    for _, res := range apiExport.Spec.Resources {
        if res.Storage.Virtual != nil {
            keys.Insert(res.Storage.Virtual.Fingerprint(apiExport))
        }
    }
    return sets.List[string](keys), nil
}
```

The existing `IndexAPIExportByVirtualResourceIdentities` and `IndexAPIExportByVirtualResourceIdentitiesAndGRs` are removed.

**Updated discovery handler** (`pkg/server/aggregatingcrdversiondiscovery/verbs_provider.go`):

```go
fingerprint := strings.TrimPrefix(crd.Annotations[apisv1alpha1.AnnotationSchemaStorageKey], "virtual:")
apiExports, err := f.getAPIExportsByVirtualResourceFingerprint(fingerprint)
if err != nil {
    return nil, err
}
if len(apiExports) == 0 {
    return nil, fmt.Errorf("no matching APIExport for virtual resource fingerprint %q", fingerprint)
}
// Pick a deterministic export. Multiple exports in different logical clusters
// may share the same fingerprint; sort for stable selection.
slices.SortFunc(apiExports, func(a, b *apisv1alpha2.APIExport) int {
    return cmp.Or(
        cmp.Compare(logicalcluster.From(a), logicalcluster.From(b)),
        cmp.Compare(a.Name, b.Name),
    )
})
return newVirtualStorageVerbsProvider(ctx, gvr, apiExports[0], opts)
```

**Updated virtual storage verbs provider** (`verbs_provider_virtualstorage.go`):

The fingerprint already uniquely resolved the APIExport; within it, the resource is matched on group and name only:

```go
for _, resourceSchema := range apiExport.Spec.Resources {
    if resourceSchema.Storage.Virtual != nil &&
        resourceSchema.Group == vrResource.Group &&
        resourceSchema.Name == vrResource.Resource {
        virtualStorage = resourceSchema.Storage.Virtual
        break
    }
}
```

The previous `resourceSchema.Storage.Virtual.IdentityHash == vrIdentity` condition is removed.

### Replication virtual workspace handler

No changes are required. The VW handler resolves identity independently by reading `ClusterCachedResource.status.identityHash` through its own informer-backed reconciler. It has never depended on `ResourceSchemaStorageVirtual.IdentityHash`, and the two concerns — discovery routing and cache read routing — were already fully separated.

## Summary of changes

| Location | Current | v2 |
|---|---|---|
| `ResourceSchemaStorageVirtual` | Has `IdentityHash string` field | Field removed; `Fingerprint(*APIExport)` method added |
| `apiextensions.go` annotation | `"virtual:<identityHash>"` | `"virtual:<hash>\|<name>/<Kind>.<group>"` |
| Indexer | `IndexAPIExportByVirtualResourceIdentities` (by spec hash) | `IndexAPIExportByVirtualResourceFingerprint` (by computed fingerprint) |
| Discovery handler lookup | by identity hash extracted from annotation | by fingerprint extracted from annotation |
| `verbs_provider_virtualstorage.go` match | `Storage.Virtual.IdentityHash == vrIdentity` | group + resource name only (APIExport already resolved via fingerprint) |
| VW handler identity source | `ClusterCachedResource.status.identityHash` (unchanged) | `ClusterCachedResource.status.identityHash` (unchanged) |

## Alternatives considered

**Use the endpoint slice reference alone as the lookup key (no identity hash in fingerprint).** A pure `<apiGroup>/<kind>/<name>` key is simpler, but two APIExports with different identities may legitimately reference the same endpoint slice — the lookup would return both, requiring a secondary filter. Including the APIExport's own identity hash in the fingerprint makes the lookup precise with no secondary disambiguation step.

**Keep `IdentityHash` but make it system-derived.** A controller or admission plugin could read `ClusterCachedResource.status.identityHash` and fill the field in automatically, removing the manual sync requirement while preserving the existing lookup mechanism. Rejected: the field would still exist as a confusing redundant identity field on the APIExport, and it creates an additional reconciliation loop for no net benefit over computing the fingerprint at bind time.

**Use the endpoint slice name alone as the annotation value.** Simpler, but endpoint slice names are only unique within a kind. The full fingerprint avoids ambiguity if other endpoint slice types or multiple APIExports with different identities are introduced.
