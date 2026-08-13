# Plan: Remove `version` from ClusterCachedResource, pivot replication to group+resource

## Goal

Remove the `version` field from `ClusterCachedResourceSpec`. The replication mechanism pivots
exclusively on group+resource. The controller autonomously discovers the preferred API version via
the REST mapper, so users never need to update or recreate a `ClusterCachedResource` when a CRD
migrates versions (e.g. v1beta1 → v1 with no conversion webhook).

## Decisions

| # | Question | Decision |
|---|---|---|
| 1 | Which version to use | Preferred version from REST mapper (`KindFor` with empty version) |
| 2 | Controller registry key | CCR name only — drop version from key |
| 3 | Trigger for version changes | Informer watch failure → `requeueSelf` only (no CRD/APIService watcher) |
| 4 | Old informer cleanup | Add `ForgetResource(gvr)` to `DiscoveringDynamicSharedInformerFactory` |
| 5a | Cache server version serving | `synthesizeCRDForClusterCachedResources` reads versions from `status.replicatedVersions` |
| 5b | Purge / drain old version | `status.replicatedVersions []string` (mirrors `CRD.status.storedVersions`); drain before removing |
| 6 | Index keys | Drop version; rename `GVRAndLogicalClusterKey` → `GRAndLogicalClusterKey` |
| 7 | Reconcile chain | Discovered GVR threaded as a shared struct through the reconcile chain |

## Limitation accepted

If the preferred version changes but the old version is still served (both versions are simultaneously
active), no re-discovery is triggered. The controller only re-discovers when the old informer's watch
stream fails (i.e. the old version is actively removed from the API). This covers the primary use
case (CRD migration that drops the old version). A future periodic-resync mechanism can address
the subtle preference-change-only case.

---

## Implementation plan

### Step 1 — API type changes

File: `staging/src/github.com/kcp-dev/sdk/apis/cache/v1alpha1/types_clustercachedresource.go`

1. Remove `Version string` from `GroupVersionResource`.
2. Rename `GroupVersionResource` → `GroupResource` (the type no longer carries version).
3. Add `ReplicatedVersions []string` to `ClusterCachedResourceStatus`, with a godoc comment
   linking it conceptually to `CRD.status.storedVersions`.

After codegen: regenerate deepcopy, client, apply-configuration, and CRD YAML.

---

### Step 2 — Reconcile chain: thread discovered GVR

File: `pkg/reconciler/cache/clustercachedresources/clustercachedresources_reconcile.go`

Introduce a `reconcileContext` struct passed through the chain:

```go
type reconcileContext struct {
    resolvedGVR schema.GroupVersionResource // populated by versionResolver, consumed by all downstream reconcilers
}
```

Add `versionResolver` as the **first** reconciler in the chain (before `validSchema`):

```go
type versionResolver struct {
    getPreferredVersion func(cluster logicalcluster.Name, gr schema.GroupResource) (schema.GroupVersionResource, error)
}
```

`getPreferredVersion` calls `dynRESTMapper.ForCluster(cluster).KindFor(partialGVR)` where
`partialGVR` has an empty version, which causes the REST mapper to return the preferred
(first-listed) mapping. Extracts the version from the returned GVK and constructs the full GVR.

Returns `reconcileStatusStopAndRequeue` if the resource is not yet discoverable (not registered).

All downstream reconcilers (`validSchema`, `reconcileResourceMetadata`, `replication`, `counter`,
`purge`) receive `reconcileContext` and use `ctx.resolvedGVR` instead of building GVR from
`clusterCachedResource.Spec`.

Adjust the reconciler interface signature:

```go
type reconciler interface {
    reconcile(ctx context.Context, rctx *reconcileContext, ccr *cachev1alpha1.ClusterCachedResource) (reconcileStatus, error)
}
```

---

### Step 3 — Controller registry: key by name only

File: `pkg/reconciler/cache/clustercachedresources/clustercachedresources_reconcile_replication.go`

Change:
```go
// before
controllerName := fmt.Sprintf("%s.%s.%s.%s.%s", clusterName, gvr.Version, gvr.Resource, gvr.Group, clusterCachedResource.Name)

// after
controllerName := fmt.Sprintf("%s.%s", clusterName, clusterCachedResource.Name)
```

On each reconcile, when a controller is found in the registry, compare its active GVR against
`resolvedGVR`. If they differ (version changed):

1. Call `r.controllerRegistry.unregister(controllerName)` — cancels the old controller's context.
2. Call `r.localDiscoveringDynamicKcpInformers.ForgetResource(oldGVR)` and same for global.
3. Fall through to create a new controller with `resolvedGVR`.

Store the active GVR in the registry entry alongside the controller so it can be compared on the
next reconcile.

---

### Step 4 — Informer watch failure → requeueSelf

File: `pkg/reconciler/cache/clustercachedresources/clustercachedresources_reconcile_replication.go`

Extend the goroutine that waits for informer sync to also watch for watch stream errors after
initial sync. When the informer's list-watch fails (the GVR no longer exists), call `requeueSelf`
so the CCR re-reconciles and re-discovers the preferred version.

Concretely: after `WaitForCacheSync` succeeds, add a goroutine that monitors the informer's
`LastSyncResourceVersion` or uses a `cache.ResourceEventHandlerFuncs` error callback. A simpler
approach: subscribe to informer `Run` returning — when the informer goroutine exits (watch broken),
call `requeueSelf`.

---

### Step 5 — Add `ForgetResource` to the informer factory

File: `pkg/informer/informer.go`

Add to `GenericDiscoveringDynamicSharedInformerFactory`:

```go
// ForgetResource stops and removes the informer for gvr. A no-op if no informer exists.
// The caller is responsible for having cancelled the informer's context before calling this.
func (d *GenericDiscoveringDynamicSharedInformerFactory[...]) ForgetResource(gvr schema.GroupVersionResource)
```

Implementation: acquire write lock on `d.informersLock`, delete `d.informers[gvr]`. The informer's
context was already cancelled by `controllerRegistry.unregister` in Step 3, so the goroutines
backing it will stop.

Expose on both the cluster-scoped and scoped wrapper types.

---

### Step 6 — status.replicatedVersions lifecycle

File: `pkg/reconciler/cache/clustercachedresources/clustercachedresources_reconcile_replication.go`

When a new version starts replicating, append it to `status.replicatedVersions` if not already
present.

Add a `versionDrainer` reconciler (runs after `replication`) that, for each version in
`status.replicatedVersions` that is **not** the current `resolvedGVR.Version`:

1. Lists cache objects under `<oldVersion>:<identityHash>` GVR.
2. Deletes them.
3. Once count reaches 0, removes the version from `status.replicatedVersions`.

This mirrors the CRD `status.storedVersions` drain pattern.

---

### Step 7 — Cache server: synthesize CRD from status.replicatedVersions

File: `pkg/cache/server/crd_lister.go`

In `synthesizeCRDForClusterCachedResources`, change the version set construction:

```go
// before
for _, cr := range crs {
    gvr := schema.GroupVersionResource(cr.Spec.GroupVersionResource)
    versionSet[gvr.Version] = struct{}{}
}

// after
for _, cr := range crs {
    for _, v := range cr.Status.ReplicatedVersions {
        versionSet[v] = struct{}{}
    }
}
```

This ensures the synthetic CRD continues to serve old versions while the version drainer (Step 6)
is running, keeping old cache objects reachable for deletion.

The content-addressable UID (`syntheticCRDUID`) already covers this: it changes whenever the
version set changes, forcing the apiextensions handler to rebuild serving info.

---

### Step 8 — Index keys: drop version

File: `pkg/reconciler/cache/clustercachedresources/indexers.go`

- Rename `GVRAndLogicalClusterKey` → `GRAndLogicalClusterKey`.
- Remove the `gvr.Version` component from the key string.
- Update all callers.

---

### Step 9 — purge and counter: use resolvedGVR

Files:
- `pkg/reconciler/cache/clustercachedresources/clustercachedresources_reconcile.go`
  (`listSelectedLocalResources`, `deleteSelectedCacheResources`, `listSelectedCacheResources`)

Replace all `clusterCachedResource.Spec.Version` references with `rctx.resolvedGVR.Version`.

For `deleteSelectedCacheResources`, iterate over `status.replicatedVersions` to ensure all stored
versions are purged (not just the current preferred one).

---

### Step 10 — Tests and generated code

1. Update unit tests in:
   - `clustercachedresources_reconcile_schema_test.go`
   - `clustercachedresources_reconcile_identity_test.go`
   - `replication/replication_reconcile_unstructured_test.go`
2. Update `client/applyconfiguration/cache/v1alpha1/groupversionresource.go` (or rename to
   `groupresource.go`).
3. Regenerate CRD YAML for `ClusterCachedResource`.
4. Update any e2e or integration tests that create CCRs with a `version:` field.

---

## Affected files (summary)

| File | Change |
|---|---|
| `staging/.../types_clustercachedresource.go` | Remove `Version` from spec; add `ReplicatedVersions` to status |
| `staging/.../zz_generated.deepcopy.go` | Regenerate |
| `client/applyconfiguration/.../groupversionresource.go` | Remove `Version` field / rename |
| `pkg/informer/informer.go` | Add `ForgetResource(gvr)` |
| `pkg/reconciler/cache/clustercachedresources/clustercachedresources_reconcile.go` | Add `reconcileContext`; add `versionResolver` first in chain; update GVR construction throughout |
| `pkg/reconciler/cache/clustercachedresources/clustercachedresources_reconcile_replication.go` | Key by name only; detect GVR change; call `ForgetResource`; update `status.replicatedVersions` |
| `pkg/reconciler/cache/clustercachedresources/clustercachedresources_reconcile_schema.go` | Use `rctx.resolvedGVR` |
| `pkg/reconciler/cache/clustercachedresources/clustercachedresources_reconcile_annotations.go` | Use `rctx.resolvedGVR` |
| `pkg/reconciler/cache/clustercachedresources/indexers.go` | Rename key function; drop version from key |
| `pkg/reconciler/cache/replication/replication_controller.go` | Drop version from gvrKey if needed |
| `pkg/cache/server/crd_lister.go` | Read versions from `status.replicatedVersions` |
| CRD YAML | Regenerate |
| Tests | Update per above |
