# Plan: CCR Version Migration — Fixing Drain and Replication Handoff

## What is wrong today

### 1. The drain is wrong in both directions

`versionDrainer.reconcile` calls `listCacheResourcesForVersion(ctx, "v1", ccr)` and
`deleteCacheResourcesForVersion(ctx, "v1", ccr)`. Both go through the cache server's
apiextensions handler.

The cache server's synthetic CRD has no schema and no conversion webhook. Apiextensions serves
objects at any version listed in the CRD via **loose passthrough**: the stored object is returned
as-is with `apiVersion` rewritten to match the requested version. There is no concept of
"v1 objects" vs "v2 objects" as separable sets — listing v1 returns all objects regardless of
which version the replication controller wrote them as.

Consequently:
- **The list check is always wrong.** Listing v1 never returns empty as long as any objects
  exist. The drain can never conclude that migration is complete.
- **The delete is always wrong.** `DeleteCollection` via v1 GVR deletes objects by their etcd
  key (version is not part of the key). It deletes the same objects the new v2 replication
  controller just wrote. The drain actively destroys freshly-replicated data.

### 2. `UpdateGVR` leaves stale queue keys

When a version change is detected, the replication reconciler calls `controller.UpdateGVR(v2,
newLocal, newGlobal)` in-place without stopping the controller. The work queue still holds
entries encoded as `v1.foos.example.com::cluster/ns/name`. Workers fail to resolve these
against the new v2 informer store and requeue with backoff until they drain. Full tear-down +
recreate avoids this entirely.

### 3. No ongoing watch failure detection

The goroutine in `replication.reconcile` calls `WaitForCacheSync`, then `c.Start`, then exits.
If the informer's watch stream dies after initial sync — which is exactly what happens when an
API version is removed — nothing detects this and calls `requeueSelf`. The replication
controller silently stops receiving events and the CCR is never re-reconciled.

The `UpdateGVR` path has the same gap: it installs no failure monitor on the new informers.

Importantly: `Reflector.RunWithContext` **never exits due to errors**. Its `delayHandler.Until`
loop always returns `false, nil` and only exits on context cancellation. The correct hook is
`SharedIndexInformer.SetWatchErrorHandlerWithContext`, which the reflector calls on every failed
`ListAndWatchWithContext` before retrying.

### 4. Purge can get stuck during deletion

`versionResolver` falls back to `StoredVersions[0]` when the REST mapper can't find the
resource during deletion. This is wrong: if `StoredVersions = [v1, v2]` and
`StorageVersion = v2`, the fallback downgrades `StorageVersion` to v1 and returns
stop+requeue, blocking the reconcile chain from reaching `purge` for an extra cycle with no
benefit.

More critically, if a CCR is deleted before `StorageVersion` is ever set (deleted before first
successful reconcile), the chain errors out and `purge` never runs. The finalizer is never
removed and the CCR is stuck forever — even though there is nothing in the cache to purge.

---

## Decisions

| # | Question | Decision |
|---|---|---|
| O1 | Watch failure detection | `SetWatchErrorHandlerWithContext` installed before `informer.Run`; no channel approach |
| O2 | storedVersions — keep or remove? | **Keep.** Needed to protect the migration window (passthrough semantics require both versions in the synthetic CRD during migration) |
| O2b | storedVersions cleanup | Left to the user (manual). Old versions accumulate; they serve all objects via passthrough so are not empty, just redundant |
| O3 | UpdateGVR — keep or remove? | **Remove.** Full tear-down + recreate on every version change |
| O4 | versionResolver deletion fallback | Use `StorageVersion` directly (already persisted). Drop `StoredVersions[0]` fallback. Also handle empty `StorageVersion` case |
| O5 | Synthetic CRD migration window behavior | Resolved by passthrough semantics: objects always accessible at any listed version; confirms storedVersions must stay |
| O6 | requeueSelf flood via `Add()` | Not broken (work queue deduplicates), but ugly. Defer for redesign |

---

## Proposed approach

### Core principle

The new replication controller writing v2 objects **is** the migration. Each v2 write lands at
the same etcd key as the old v1 object (version is not part of the key). No explicit deletion
of old-version objects is needed or possible without risking newly-replicated data.
**Remove the `versionDrainer` reconciler entirely.**

---

### Step 1 — Watch failure detection via `SetWatchErrorHandlerWithContext`

After `local, err := r.localDiscoveringDynamicKcpInformers.ForResource(gvr)` and before
`go replicated.Local.Run(controllerCtx.Done())`, install a custom watch error handler:

```go
_ = local.Informer().SetWatchErrorHandlerWithContext(func(ctx context.Context, r *cache.Reflector, err error) {
    cache.DefaultWatchErrorHandler(ctx, r, err)
    if isGVRGoneError(err) {
        requeueSelf()
    }
})
```

`isGVRGoneError` should match `apierrors.IsNotFound`, `apierrors.IsGone`, and
`meta.IsNoMatchError` — errors that indicate the GVR is no longer available, as opposed to
transient connection issues which the reflector already retries internally.

`SetWatchErrorHandlerWithContext` must be called before the informer is started (`started ==
false` guard). `requeueSelf` calls `queue.Add()` which is deduplicated by the work queue, so
repeated calls per reflector retry cycle are absorbed. See O6 note for future redesign.

Do the same for the global informer.

---

### Step 2 — Remove `UpdateGVR`; always do full tear-down on version change

Remove the `UpdateGVR` branch from the replication reconciler. Remove the `UpdateGVR` method
and `getCtx` from the controller and registry respectively.

When the replication reconciler detects that `clusterCachedResource.Status.StorageVersion`
differs from the running controller's GVR:

```
1. r.controllerRegistry.unregister(controllerName) — cancels controllerCtx
2. r.localDiscoveringDynamicKcpInformers.ForgetResource(oldGVR)
3. r.globalDiscoveringDynamicKcpInformers.ForgetResource(oldGVR)
4. Fall through to controller == nil branch → create new controller with new GVR
```

The old informer's watch is already dead (that is what triggered re-discovery). Full restart is
clean, avoids stale queue keys, and the latency cost is acceptable for a rare event.

---

### Step 3 — storedVersions bookkeeping in the replication reconciler

When a new version starts replicating, append it to `status.storedVersions` if not already
present. Both the old and new version now appear in `storedVersions`, which means the synthetic
CRD serves both during the migration window — protecting readers from a gap between "old objects
still in etcd" and "new replication controller has rewritten them all."

Old versions are never removed automatically. Cleanup is left to the user.

---

### Step 4 — Remove `versionDrainer`

Delete `clustercachedresources_reconcile_drain.go`. Remove `versionDrainer` from the reconcile
chain. Remove `listCacheResourcesForVersion` and `deleteCacheResourcesForVersion` from the
controller (only used by the drainer).

---

### Step 5 — Fix `versionResolver` deletion fallback and `deleteSelectedCacheResources` guard

**`versionResolver`:** When the REST mapper errors during deletion, use the already-persisted
`StorageVersion` and let the chain continue. Do not fall back to `StoredVersions[0]`.

```go
if err != nil {
    if !clusterCachedResource.DeletionTimestamp.IsZero() {
        // StorageVersion is persisted from the last successful reconcile; use it.
        // If it is empty the CCR was never reconciled and there is nothing to purge.
        return reconcileStatusContinue, nil
    }
    return reconcileStatusStopAndRequeue, err
}
```

**`deleteSelectedCacheResources`:** Guard against the never-reconciled case to avoid issuing
`DeleteCollection` with a malformed (empty version + empty identity) GVR:

```go
func (c *Controller) deleteSelectedCacheResources(...) error {
    if clusterCachedResource.Status.StorageVersion == "" || clusterCachedResource.Status.IdentityHash == "" {
        return nil  // nothing was ever replicated; nothing to purge
    }
    // ... existing logic
}
```

The purge reconciler talks exclusively to the cache server (via `globalDynamicClient` with
`cacheclient.WithShardInContext`). The source API being gone is irrelevant.

---

## O6 — deferred: requeueSelf trigger mechanism needs redesign

Two separate mechanisms currently trigger CCR re-reconciliation on GVR removal:
1. `GVRLifecycleHandler.RemovedFunc` in the controller (line 222 of `clustercachedresources_controller.go`)
2. The watch error handler installed per-informer (Step 1 above)

Both call `enqueue` → `queue.Add()` (unthrottled). The work queue deduplication prevents
flooding in practice (at most 2 reconcile runs regardless of call count), but the overall
trigger design is convoluted. Revisit as a unit rather than patching each callsite.

---

## Files affected

| File | Change |
|---|---|
| `clustercachedresources_reconcile_drain.go` | **Delete** |
| `clustercachedresources_reconcile.go` | Remove `versionDrainer` from chain; remove `listCacheResourcesForVersion`, `deleteCacheResourcesForVersion`; add guard in `deleteSelectedCacheResources` |
| `clustercachedresources_reconcile_version.go` | Fix deletion fallback: use existing `StorageVersion`, drop `StoredVersions[0]` path |
| `clustercachedresources_reconcile_replication.go` | Remove `UpdateGVR` branch; add full tear-down on GVR change; install watch error handler on informers before `Run` |
| `replication/replication_controller.go` | Remove `UpdateGVR` method; simplify registry entry (drop `getCtx` if only used for `UpdateGVR`) |
| `pkg/cache/server/crd_lister.go` | No change — already reads `StoredVersions` |
