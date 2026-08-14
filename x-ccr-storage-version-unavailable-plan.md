# Plan: CCR StorageVersionAvailable Condition — Prerequisites

## Status

This plan covers the prerequisite refactoring that `x-ccr-version-pinning-plan.md` depends on.
It does NOT include auto-migration logic — that concept was dropped in favor of fully explicit
user control via `spec.version`.

---

## What This Plan Covers

1. Rename the `getPreferredGVR` callback to `getServedGVKs`, changing its return type so that
   the `versionResolver` has access to all currently-served versions (not just the preferred one).
2. Add the `StorageVersionAvailable` condition type and `RequestedVersionNotServedReason` constant
   to the API types.
3. Remove the stray debug `fmt.Printf` from the callback closure.

The actual reconcile logic using these primitives is specified in `x-ccr-version-pinning-plan.md`.

---

## Callback Rename and Signature Change

In `clustercachedresources_reconcile_version.go`, replace:

```go
type versionResolver struct {
    getPreferredGVR func(cluster logicalcluster.Name, gr schema.GroupResource) (schema.GroupVersionResource, error)
}
```

With:

```go
type versionResolver struct {
    // getServedGVKs returns all GVKs currently served for the given group+resource, sorted
    // by REST mapper preference (index 0 is preferred). Returns an error only if the
    // group+resource itself is unknown — not if a specific version is absent.
    getServedGVKs func(cluster logicalcluster.Name, gr schema.GroupResource) ([]schema.GroupVersionKind, error)

    // The following fields are added by x-ccr-version-pinning-plan.md to support
    // in-place controller teardown. Listed here for completeness; see that plan for details.
    controllerRegistry                   *controllerRegistry
    localDiscoveringDynamicKcpInformers  *informer.DiscoveringDynamicSharedInformerFactory
    globalDiscoveringDynamicKcpInformers *informer.DiscoveringDynamicSharedInformerFactory
}
```

In `clustercachedresources_reconcile.go`, update the closure:

```go
// Before
getPreferredGVR: func(cluster logicalcluster.Name, gr schema.GroupResource) (schema.GroupVersionResource, error) {
    kinds, err := c.dynRESTMapper.ForCluster(cluster).KindsFor(gr.WithVersion(""))
    if err != nil {
        return schema.GroupVersionResource{}, err
    }
    if len(kinds) == 0 {
        return schema.GroupVersionResource{}, fmt.Errorf("no kind found for %v", gr)
    }
    gvr := gr.WithVersion(kinds[0].Version)
    fmt.Printf("### getPreferredGVR: selecting %#v from %#v\n", gvr, kinds)
    return schema.GroupVersionResource{Group: kinds[0].Group, Version: kinds[0].Version, Resource: gr.Resource}, nil
},

// After
getServedGVKs: func(cluster logicalcluster.Name, gr schema.GroupResource) ([]schema.GroupVersionKind, error) {
    return c.dynRESTMapper.ForCluster(cluster).KindsFor(gr.WithVersion(""))
},
```

The `len(kinds) == 0` guard and preferred-version extraction move into `versionResolver.reconcile`
(specified in `x-ccr-version-pinning-plan.md`).

---

## New API Constants

In `staging/src/github.com/kcp-dev/sdk/apis/cache/v1alpha1/types_clustercachedresource.go`:

```go
// StorageVersionAvailable indicates that the version in spec.version is currently served
// by the source workspace. When False, the replication controller is not running for this
// version and will not start until spec.version is updated to a served version.
// Not evaluated during deletion — the condition may be stale while a CCR is terminating.
StorageVersionAvailable conditionsv1alpha1.ConditionType = "StorageVersionAvailable"

// RequestedVersionNotServedReason is set on StorageVersionAvailable=False when the version
// in spec.version is not currently served by the source workspace.
RequestedVersionNotServedReason = "RequestedVersionNotServed"
```

---

## Interaction with `x-ccr-version-migration-plan.md`

The `x-ccr-version-migration-plan.md` (watch failure detection, UpdateGVR removal, versionDrainer
removal) is independent and can land in any order relative to this plan.

---

## Files Affected

| File | Change |
|---|---|
| `staging/src/github.com/kcp-dev/sdk/apis/cache/v1alpha1/types_clustercachedresource.go` | Add `StorageVersionAvailable` + `RequestedVersionNotServedReason` |
| `clustercachedresources_reconcile_version.go` | Rename struct field to `getServedGVKs`; update signature |
| `clustercachedresources_reconcile.go` | Update closure; remove `fmt.Printf` |
