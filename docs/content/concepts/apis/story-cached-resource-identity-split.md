# Conclusion

All four questions point the same direction: **`CachedResourceDefinition` does not have compelling independent motivations.**

- Cross-workspace identity sharing (the primary driver) has no practical use case.
- "Identity up front" is already achievable today via `spec.identity.secretRef`.
- Revocability is a circular problem — it only exists because of shared identities.
- The `identityRef` alternative would still require a separate type, but is moot since the sharing use case is not real.

Identity rotation (Story 1) can be implemented directly on `ClusterCachedResource` without a split. The existing single-object design is sufficient.

---

# User story 1

> I am a company that offers multiple services that share a common meta API (e.g. InstanceType, InstanceImage).
> I want to be able to rotate the identity of the cached resources of that meta API.

- We can just implement identity rotation on CachedResource-basis. We don't need CachedResourceDefinition.

# Question: is revocability (revoking a workspace's ability to replicate without rotating the key) an independent motivation?

- No, and the argument is circular. Without shared identities, each workspace owns its own `ClusterCachedResource` and its own identity. Revoking replication means deleting that object — trivially solved. The "disrupting all other workspaces" scenario requires multiple workspaces sharing one identity, which only exists if `CachedResourceDefinition` is already present. Revocability is not an independent motivation; it is a derived problem of the cross-workspace sharing feature itself.

# Question: is "identity up front" (decoupling identity establishment from replication activation) an independent motivation?

- No, it's a side effect of the split. You can already pre-create the identity secret and reference it via `spec.identity.secretRef` on a `ClusterCachedResource` before replication is needed. Not a motivation for `CachedResourceDefinition`.

# User story 2

> Platform provides a common meta API (InstanceType.platform.org, InstanceImage.platform.org).
> Service providers are encouraged to use that, and publish the objects of those respective APIs under a single identity.

> Two service providers (A, B) that offer VM services.
> Consumer binds compute of service A, and binds to the images offered by service B.

- There should be a common list of images anyway. Platform should enforce that if they are the ones providing the API. Why would VM service providers also provide images?
- **Cross-workspace identity sharing (N workspaces replicate under one shared identity for aggregated list/watch): the idea is sound, but no practical use case found. This removes the primary motivation for `CachedResourceDefinition`.**

# User story 3

> Service provider exposes a set of InstanceImages.
> User wants to re-export that set, with custom images added to that.

- This is very difficult to implement at the moment:
  - Somehow aggregate two sets of a resource, `/<prefix>/<group>/<resource>/<identityHash>/<shard-1>/<cluster-a>/<rest>` and `/<prefix>/<group>/<resource>/<identityHash>/<shard-2>/<cluster-b>/<rest>`, where shard-1 and shard-2 are distinct API servers with their own etcd.

# User story 4

> Hierarchical distributed cache setup (touches on ./enhancement-kcp-distributed-cache.md). There are multiple levels of cache. Certain service providers are discoverable only in certain region(s). Global providers need to be discoverable from all regions.
> CachedResourceDefinition could have a selector of cache servers. Replication of the resources would be done to these cache servers.

- Not sure if CachedResource is the correct tool for this at all. If I want to make a service globally discoverable, I want to replicate only that particular APIExport+APIResourceSchema to the global cache server; this could be done using special annotation for example. Yes, if the service also relies on service metadata, that also requires a CachedResource.
- So in this case I can see a tiny benefit of having CachedResource be the driver for replicating arbitrary resources into different caches.
- The problem is that this is a bit incosistent with how kcp-builtin resources are being replicated:
  - There is InstallIndexers in @kcp/pkg/reconciler/cache/replication/replication_controller.go that compiles a hard-coded list of GVRs to be replicated into the region-local cache.
  - Now we want to add an additional mechanism for replicating these builtin resources, into another instance of the cache. The identity of those resources is defined on the root shard. I don't want to have create ADDITIONAL mechanism of bridging these two (yes, one could create the CachedResourceDefinition in :root and use the same secret, but what if the definition would need to be stored elsewhere?)
