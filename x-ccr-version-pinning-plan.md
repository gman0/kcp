# Plan: CCR Explicit Version Field

## Context

The storage version used for replication must be set explicitly by the user via `spec.version`.
There is no automatic version selection or migration. The controller validates that the requested
version is currently served and surfaces the result through the `StorageVersionAvailable` condition.

This plan supersedes the auto-migration portion of `x-ccr-storage-version-unavailable-plan.md`.
The `getServedGVKs` callback refactor and condition type definitions from that plan are
prerequisites and should land first (or together).

---

## Problem Statement

Today the storage version is resolved entirely by the controller (REST mapper preferred version).
Users have no say, no visibility into the choice, and no way to guarantee stability. When
versions change in the source workspace the controller silently switches, potentially causing
replication disruptions without any operator signal.

Making `spec.version` required and authoritative gives operators full control and makes version
changes an explicit, observable action.

---

## New Spec Field

Add to `ClusterCachedResourceSpec` in
`staging/src/github.com/kcp-dev/sdk/apis/cache/v1alpha1/types_clustercachedresource.go`:

```go
// version is the API version used for storage and replication. The controller replicates
// objects at exactly this version and does not migrate to a different version automatically.
//
// The version must be currently served by the source workspace. If it is not, the
// StorageVersionAvailable condition is set to False and replication does not start (or stops).
// Update this field when you want to migrate to a different version.
//
// +kubebuilder:validation:Required
// +kubebuilder:validation:Pattern=`^v[0-9]+(alpha[0-9]+|beta[0-9]+)?$`
Version string `json:"version"`
```

### Why required

There is no meaningful default the controller can choose without user intent. Defaulting to the
REST mapper's preferred version would recreate the current opaque behavior under a different
name. An empty `spec.version` would need special handling in the controller, condition messages,
and docs — net cost higher than asking users to set the field once.

### Why not `spec.storageVersion`

`status.storageVersion` reflects what the controller is actually using. A parallel `spec.storageVersion`
would invite "is this desired or observed?" confusion. `spec.version` is shorter, unambiguous in
intent, and consistent with `spec.group` / `spec.resource`.

### Kubebuilder validation

The pattern `^v[0-9]+(alpha[0-9]+|beta[0-9]+)?$` accepts `v1`, `v1alpha1`, `v1beta1`, `v2`, etc.
It rejects arbitrary strings at admission time. Whether the version is *served* by the source
workspace is runtime state and is handled by the controller via the condition.

### Migration of existing CCRs

`spec.version` is a new required field on a v1alpha1 type. Existing CCRs without it will fail
validation after the API update. Operators must set `spec.version` on all existing CCRs before
or immediately after upgrading.

Two options to ease migration:
1. **Defaulting webhook**: default to `status.storageVersion` (current value) so existing CCRs
   survive admission unchanged. Remove the webhook once all CCRs are migrated.
2. **Bump the API version**: if a breaking change is already planned, fold this in.

If a defaulting webhook is added, it must be careful not to overwrite an already-set value.

---

## Condition

`StorageVersionAvailable` (from `x-ccr-storage-version-unavailable-plan.md`):

```go
StorageVersionAvailable conditionsv1alpha1.ConditionType = "StorageVersionAvailable"

// RequestedVersionNotServedReason: spec.version is not currently served by the source workspace.
// Check that the version exists in the source workspace and that the REST mapper has picked it up.
RequestedVersionNotServedReason = "RequestedVersionNotServed"
```

`StorageVersionUnavailableReason` from the companion plan was for auto-migration and is not
needed. Only `RequestedVersionNotServedReason` is required here.

Severity: `Error` — replication is not running. User action required.

---

## Reconciler Chain

```
finalizer → versionResolver → validSchema → identityReconciler → purge → counter → replication
```

No new reconciler is added. Teardown of the running replication controller is handled inside
`versionResolver` using the same `getServedGVKs` result, eliminating a second REST mapper call
and the zombie-goroutine problem that a separate pre-gate teardown reconciler could not fully
solve (it can't see served versions without its own mapper call).

---

## `versionResolver` — Struct and Logic

`versionResolver` gains direct access to the controller registry and informer factories so it
can tear down the running controller as part of the same pass where it evaluates served versions.

```go
type versionResolver struct {
    getServedGVKs func(cluster logicalcluster.Name, gr schema.GroupResource) ([]schema.GroupVersionKind, error)

    controllerRegistry                   *controllerRegistry
    localDiscoveringDynamicKcpInformers  *informer.DiscoveringDynamicSharedInformerFactory
    globalDiscoveringDynamicKcpInformers *informer.DiscoveringDynamicSharedInformerFactory
}
```

### Reconcile logic

```
// During deletion, skip all version checking and teardown.
// status.storageVersion is already persisted from the last successful reconcile.
// The chain must reach purge and replication to drain the cache, advance the deletion
// phase, and eventually remove the finalizer.
if !ccr.DeletionTimestamp.IsZero():
    return continue, nil

desired  := ccr.Spec.Version
cluster  := logicalcluster.From(ccr)
gr       := schema.GroupResource{Group: ccr.Spec.Group, Resource: ccr.Spec.Resource}

gvks, err := getServedGVKs(cluster, gr)
if err:
    stop+requeue, err   // API entirely gone (non-deletion); no fallback needed

// len(gvks) == 0 without error is theoretically possible but treated the same as
// "version not served" — served set is empty, served.Has(desired) is false.
served = set{gvk.Version for gvk in gvks}

// Tear down the running controller if:
//   (a) its GVR version differs from spec.version (user changed the pin), or
//   (b) spec.version is no longer served (version removed from source).
// Both facts come from the single getServedGVKs call above — no extra mapper round-trip.
controllerName := fmt.Sprintf("%s.%s.%s", cluster, gr.Group, gr.Resource)
if controller := controllerRegistry.get(controllerName); controller != nil {
    activeGVR := controller.CurrentGVR()
    if activeGVR.Version != desired || !served.Has(desired) {
        controllerRegistry.unregister(controllerName)
        localDiscoveringDynamicKcpInformers.ForgetResource(activeGVR)
        globalDiscoveringDynamicKcpInformers.ForgetResource(activeGVR)
    }
}

if served.Has(desired):
    MarkTrue(StorageVersionAvailable)
    if status.StorageVersion != desired:
        status.StorageVersion = desired
        stop+requeue    // commit status alone; replication reconciler starts next pass
    else:
        continue        // everything aligned; full chain runs

else:
    MarkFalse(StorageVersionAvailable, RequestedVersionNotServedReason, Error,
              "version %s is not served by the source workspace", desired)
    stop+requeue        // replication reconciler does not run
```

**Teardown condition covers both failure modes in one check:**
- `activeGVR.Version != desired` — spec.version was changed by the user
- `!served.Has(desired)` — the desired version disappeared from the source workspace

The early deletion return also removes the need for the old `if err { if deleting → continue }`
fallback that existed in the original `versionResolver`. During deletion, `status.storageVersion`
is persisted and the downstream reconcilers use it directly.

**`status.storageVersion` when `spec.version` is not served**: left at its previous value
(or empty if never set). Only updated when the requested version is confirmed served.
Preserves the last-known-good value for the deletion/purge path.

**The GVR-mismatch check in `replication.reconcile` (lines 75–84)** becomes a safety net
for concurrent reconciles, not the primary mechanism.

---

## Teardown Scenarios

### spec.version changed, new version IS served

Pass 1:
- `versionResolver`: getServedGVKs → v2 served; running v1 controller, desired=v2, v2≠v1 → **tear down v1**; v2 ∈ served, storageVersion=v1≠v2 → set storageVersion=v2, stop+requeue

Pass 2:
- `versionResolver`: no controller; v2 ∈ served, storageVersion=v2 → MarkTrue, continue
- `replication`: no controller → start v2

### spec.version changed, new version NOT YET served

Pass 1:
- `versionResolver`: getServedGVKs → v2 not served; running v1, desired=v2, v2≠v1 → **tear down v1**; v2 ∉ served → MarkFalse, stop+requeue

Passes 2..N (v2 still not served):
- `versionResolver`: no controller; v2 ∉ served → MarkFalse, stop+requeue

Pass N+1 (v2 now served):
- `versionResolver`: no controller; v2 ∈ served, storageVersion=v1≠v2 → set storageVersion=v2, stop+requeue

Pass N+2:
- `versionResolver`: no controller; v2 ∈ served, storageVersion=v2 → MarkTrue, continue
- `replication`: start v2

### spec.version unchanged, version removed from source

Pass 1 (after watch error handler re-enqueues):
- `versionResolver`: getServedGVKs → v1 not served; running v1, desired=v1, `!served.Has(v1)` → **tear down v1**; v1 ∉ served → MarkFalse, stop+requeue

No zombie goroutine. The running controller is cancelled in the same pass where the REST mapper
confirms the version is gone. User must update `spec.version` to resume replication.

---

## Simplified Behavior

| `spec.version` | `status.storageVersion` | `spec.version` served? | Action |
|---|---|---|---|
| `"v2"` | `""` | yes | Tear down nothing; set `storageVersion=v2`, requeue |
| `"v2"` | `""` | no | Tear down nothing; `StorageVersionAvailable=False`, requeue |
| `"v2"` | `"v2"` | yes | No teardown; `StorageVersionAvailable=True`, continue |
| `"v2"` | `"v2"` | no | **Tear down v2 controller**; `StorageVersionAvailable=False`, requeue |
| `"v2"` | `"v1"` | yes | **Tear down v1 controller**; set `storageVersion=v2`, requeue |
| `"v2"` | `"v1"` | no | **Tear down v1 controller**; `StorageVersionAvailable=False`, requeue |

---

## Edge Cases

### E1: User changes `spec.version` from `"v1"` to `"v2"`, v2 served

- Pass 1: v1 controller torn down, storageVersion=v2, requeue.
- Pass 2: v2 ∈ served, MarkTrue, continue → `replication` starts v2 controller.
- `v1` stays in `StoredVersions` (passthrough protection).

### E2: User changes `spec.version` to `"v2"`, v2 not yet served

- Pass 1: v1 controller torn down, MarkFalse, requeue. Replication gap begins.
- When v2 appears: MarkTrue, storageVersion=v2, requeue → replication starts.
- Gap is expected: user chose a version not yet available.

### E3: Source workspace removes the pinned version

- `versionResolver` tears down the controller in the same pass it detects the removal.
- No zombie goroutine. No auto-migration. User must update `spec.version` to resume.

### E4: Downgrade

User sets `spec.version = "v1alpha1"` after previously replicating `v1beta1`. If `v1alpha1`
is served: v1beta1 controller torn down, storageVersion updated, v1alpha1 controller starts.
`v1beta1` remains in `StoredVersions`. Version ordering is not enforced by the controller.

### E5: Deletion with `StorageVersionAvailable=False`

`versionResolver` returns `continue` immediately when `DeletionTimestamp` is set, regardless
of whether `spec.version` is served or the condition state. No teardown fires. The full chain
runs: `purge` issues `DeleteCollection` using `status.storageVersion` (persisted from last
successful reconcile); `replication` handles drain and phase transition via its existing
deletion logic.

If `storageVersion` was never set (CCR deleted before first successful reconcile),
`deleteSelectedCacheResources` guards on `storageVersion == ""` and skips the purge —
nothing was ever replicated. The finalizer is removed cleanly.

---

## Open Questions

### OQ4: Tight reconcile loop when spec.version is not served

`reconcileStatusStopAndRequeue` with no error causes `queue.Add(key)` (unthrottled) in the
controller's `processNextWorkItem`. While `spec.version` is not served, the CCR reconciles in
a tight loop, calling `getServedGVKs` on every pass. The `AddedFunc` handler means the CCR
is promptly re-enqueued once the version appears, but between discovery poll cycles the tight
loop hammers the REST mapper unnecessarily.

The root design issue (OQ6 in `x-ccr-version-migration-plan.md`) is that `stop+requeue` with
no error is unthrottled system-wide. A targeted fix would be to return a small error (causing
`AddRateLimited`) or introduce a distinct `reconcileStatusStopAndRequeueRateLimited` status.
Deferred pending the broader OQ6 redesign.

### OQ1: Cache coverage gap when spec.version is changed to an unserved version

Changing `spec.version` to an unserved version tears down the existing controller immediately,
creating a replication gap. An alternative: keep replicating the old version until the new one
is confirmed served, then cut over atomically. This requires a "pending version" field in status
and blurs the "spec is authoritative" principle. Current decision: gap is acceptable; the
condition makes it visible.

### OQ2: Defaulting webhook for migration of existing CCRs

A defaulting webhook that reads `status.storageVersion` and writes it into `spec.version` on
first admission would make the migration seamless. However, a webhook that reads status is
unusual. The alternative is a one-time migration script. Decision deferred to the team
implementing this.

### OQ3: `StoredVersions` cleanup

When a user migrates from v1 to v2, `v1` stays in `StoredVersions` indefinitely. A future
GC mechanism could remove versions once all objects at that version have been overwritten by
the new replication controller. Out of scope for this plan.

---

## Triggering Reconcile When a Version Becomes Available

When `spec.version` is not served, `versionResolver` returns `stop+requeue`. The CCR must
be re-reconciled when the requested version eventually appears in the source workspace.

`GVRLifecycleHandlerFuncs.AddedFunc` already exists in the informer package and fires from
`updateInformers()` whenever a GVR appears in fresh CRD discovery that was not previously
tracked. This includes new **versions** of an existing group+resource — exactly the event we
need.

The current controller only wires `RemovedFunc`. Adding the symmetric `AddedFunc` in
`clustercachedresources_controller.go` covers this:

```go
c.localDiscoveringDynamicKcpInformers.AddGVRLifecycleHandler(ctx, informer.GVRLifecycleHandlerFuncs{
    AddedFunc: func(gvr schema.GroupVersionResource) {
        ccrs, err := indexers.ByIndex[*cachev1alpha1.ClusterCachedResource](
            c.ClusterCachedResourceIndexer,
            ByGroupResource,
            GroupResourceKey(gvr.GroupResource()),
        )
        if err != nil {
            utilruntime.HandleError(fmt.Errorf("..."))
            return
        }
        for _, ccr := range ccrs {
            c.enqueue(ccr)
        }
    },
    RemovedFunc: func(gvr schema.GroupVersionResource) { /* existing */ },
})
```

The existing `ByGroupResource` index (keyed by group+resource, ignoring version) is reused.
This over-triggers slightly — CCRs whose `spec.version` was already served get a no-op
reconcile — but the check is idempotent and the event is rare.

A version-filtered index (`ByGroupVersionResource`) would be more precise but is not worth
the extra complexity: the over-triggering is bounded, and the CCR doesn't have a `spec.version`
field in the current index setup anyway.

---

## Files Affected

| File | Change |
|---|---|
| `staging/src/github.com/kcp-dev/sdk/apis/cache/v1alpha1/types_clustercachedresource.go` | Add `Version string` to `ClusterCachedResourceSpec`; add `RequestedVersionNotServedReason` |
| `config/` CRD manifests | Regenerate (`make generate manifests`) |
| `clustercachedresources_reconcile_version.go` | Add registry/informer fields to `versionResolver`; replace reconcile logic with pseudocode above |
| `clustercachedresources_reconcile.go` | Wire new `versionResolver` fields; closure rename (`getPreferredGVR` → `getServedGVKs`); remove `fmt.Printf` |
| `clustercachedresources_controller.go` | Add `AddedFunc` to `GVRLifecycleHandler` (alongside existing `RemovedFunc`) |
