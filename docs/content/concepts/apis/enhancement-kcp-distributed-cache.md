# Design: distributed kcp cache

The current cache server design assumes a single global cache server per kcp installation. Every shard pushes its replication-marked objects to the same cache (configured via a single --cache-kubeconfig, see pkg/cache/client/options/cache.go), and every controller reads from it.

This works for small/medium installations but the cache server is a single apiextensions-apiserver backed by etcd, so the cardinality of replicated objects across all shards must fit in one apiserver. As the number of shards and replicated objects grows (APIExports, webhook configurations, replicated RBAC, CachedResources, …), the cache becomes a hard scaling ceiling for the whole installation.

Why it matters:

* Single point of failure for cross-shard visibility.
* Single write fan-in (N shards → 1 cache) bounds write throughput.
* Cardinality limit applies installation-wide, not per region/tenant.

The current limitation is documented in kcp/docs/content/concepts/sharding/shards.md under "Partitioning strategies" and explicitly flagged as future work.

## What exists

Controllers in kcp/pkg/reconciler/cache/ do shard-to-cache replication.

In kcp-operator/sdk/apis/operator/v1alpha1/cacheserver_types.go, CacheServer describes the type for deploying a cache server. It supports multi-replica deployments, with an external etcd cluster (i.e. one can point the apiextensions-apiserver to a specific etcd instance). There currently may be only a single etcd instance for the whole cache server deployment.

## What we want

### Level 1

A platform owner deploys many CacheServers through kcp-operator.

> It must be possible those are deployed across many separate runtime clusters (kcp-operator installations).

One set of shards replicates to CacheServer `A`. Another set of shards replicates to CacheServer `B`. This handles shard->cache replication.

> There must be a way to list/watch data across all cache servers (replication / aggregation)

### Level 2

If CacheServer `A` goes down, shards needs to know that there is also CacheServer `B` and should direct traffic there as a fallback. There may be many levels of fallback.

> There must be a way to perform CacheServer discovery, and handle switching to the fallback transparently (i.e. watches & informers should continue to function).

There may be a set of resources that are local to the regions, with other regions not needing those. Then there may be a set of global resources shared by all regions.

> There must be a way to define cache hierarchies.

### General remarks

As it currently stands, kcp does not have a concrete definition of a "region". As such, the work for distributed cache servers must not require such a definition to materialize, and should depend only minimal set of APIs to carry out its tasks.

Existing patterns need to be reused. Bespoke components may be created; one that comes to mind is kcp-cache-syncagent, similar to the existing kcp-api-syncagent/.

### Cache->Cache replication (peering)

The resource-hubs that kcp cache servers are, they already are expected to be under a lot of load. The intra-cache replication therefore needs to be as efficient as possible, and the result having distributed caches MUST improve performance of the system: performance SHOULD scale with number of caches (if hierarchies are configured correctly, etc.).

## Topology options

### Option B — Explicit hierarchy (tree)

Shards replicate to a regional cache. Regional caches replicate upward to a root cache. Reads that miss locally walk up the tree.

```
         [root cache]
        /             \
  [cache-eu]       [cache-us]
   /     \            |
[shard1][shard2]   [shard3]
```

A `CacheServer` knows its parent via a cross-cluster reference (kubeconfig secret). The parent relationship implies replication: data flows upward (and potentially downward for globally-scoped objects). Depth is bounded in practice (2–3 levels) but the model should not assume it.

Open questions:
- Does data flow bottom-up, top-down, or both? Global configuration may need to propagate downward.
- Who decides what is "global": the object itself (label/annotation), the CacheServer spec, or the edge between parent and child?
- On root failure, cross-region visibility breaks. Level 2 failover must account for this.

### Option C — Partition + selective peering

No hierarchy. Shards are statically assigned to a cache. Caches declare explicit peer relationships with resource filters.

```
[cache-A] <--replicates APIExports--> [cache-B]
    |                                      |
[shard1,2]                           [shard3,4]
```

A peer relationship names a local cache, a remote cache (cross-cluster reference), and a replication filter (by resource type, workspace, label selector, etc.). Direction is explicit: A pulls from B, B pulls from A, or both — declared independently.

Open questions:
- Do controllers on shard1 query only cache-A (requiring cache-A to hold full copies from cache-B), or do they query cache-B directly? Fully local reads are cleaner but require full replication across peers.
- What is the right filter granularity? Resource type is coarse; workspace is fine-grained. The common case likely needs both.

### Option D — Flat with frontproxy aggregation (preferred for Level 1)

A flat structure: every `CacheServer` is directly associated with a set of shards. Shards replicate to their assigned `CacheServer` — existing behavior, unchanged. A `cache-frontproxy` aggregates reads across all `CacheServer` instances, giving controllers a single unified endpoint. No cache-to-cache replication is needed; aggregation replaces it.

```
       [cache-frontproxy]
      /         |         \
 [cache-A]  [cache-B]  [cache-C]
  /    \        |        /    \
[s1]  [s2]    [s3]    [s4]  [s5]
```

A hierarchical topology (Options B, C) was considered. Internal nodes in a tree are not associated with any shards and exist purely for routing — a concern already handled by the frontproxy. The flat model is simpler. Options B and C remain relevant as extension points for installations that require cache-to-cache replication (e.g. for locality, compliance, or disconnected-region scenarios), driven by a `kcp-cache-syncagent`.

The two components are complementary and independently scoped:
- **`kcp-cache-syncagent`** — handles cache-to-cache replication (Options B, C).
- **`cache-frontproxy`** — handles read aggregation and write routing (all options).

### Comparison of options B, C, D

Both B and C share the same structural needs:

1. A cross-cluster remote reference (kubeconfig secret, same pattern as kcp-api-syncagent).
2. A replication filter spec — what resources/objects flow across an edge.
3. An **edge object** representing "replicate from X to Y with filter F"; implicit in B via `parentRef`, explicit in C as a standalone CR.
4. Status on the edge (lag, last-synced, health).

Option B's `parentRef` can be viewed as syntactic sugar over Option C's explicit edge with a fixed filter of "everything". Both could unify into a single model: a directed graph of `CacheServer` nodes connected by replication edges, where a hierarchy is a constrained (acyclic, rooted) graph shape. Option D drops the replication graph entirely in favour of frontproxy aggregation.

### cache-frontproxy

A `cache-frontproxy` component can sit in front of a set of cache servers and present a single aggregated virtual view to clients. This separates two concerns that would otherwise be conflated:

- **Replication topology** — how data flows between caches (the edge graph above).
- **Read path aggregation** — what endpoint clients actually watch.

With a frontproxy, individual caches no longer need to be complete local replicas. The frontproxy owns the unified view.

```
          [cache-frontproxy]
         /                  \
   [cache-A]            [cache-B]
    /     \                  |
[shard1][shard2]         [shard3]
```

**Client watch invariant:**

kcp controllers open two watches per resource: one local (against their own shard) and one global (against the cache, which today contains objects from all shards). This invariant must be preserved. The frontproxy is the global watch endpoint — clients point at it and get a single unified view. The internal structure of how many caches back it is transparent to clients.

**What the frontproxy must handle:**

- **Synthetic resourceVersion**: the frontproxy mints its own monotonic RV, mapping each upstream event `(cache-id, etcd-rv)` to a synthetic sequence. Clients resuming with a stale synthetic RV receive `410 Gone` and relist — standard Kubernetes behavior.
- **Fan-out**: List fans out to all backing caches, merges results, and returns under the synthetic RV. Watch fans out and multiplexes event streams.
- **Topology awareness**: which caches to aggregate is driven by the same CacheServer edge objects, not hardcoded.

**Interaction with topology options:**

- **Option C + frontproxy**: caches are fully partitioned by shard set; zero cache-to-cache replication is needed. The frontproxy provides the unified view.
- **Option B + frontproxy**: regional caches hold local shard data; the frontproxy aggregates across the subtree, eliminating or reducing the need for downward replication of globals into each leaf.

Like `kcp-cache-syncagent`, the frontproxy is replaceable: the contract is the set of CacheServer objects it is configured to aggregate, not the implementation itself.

#### Memory management

Caches may hold large volumes of data. The frontproxy must not hold all of it in memory. Options, from simplest to most complex:

1. **Pure passthrough (no local state)**: no in-memory store; List fans out to backends and streams the merged response directly to clients; Watch proxies the event stream without storing objects. Does not work for multi-backend aggregation — without a synthetic RV the frontproxy cannot aggregate watches across independent etcd RV domains, and clients cannot resume a watch after disconnect.

2. **Shared watch cache (one copy, N clients)**: one in-memory object store shared across all clients (the kube-apiserver model). Eliminates per-client duplication but memory is still O(total objects).

3. **Ring buffer watch + passthrough List** *(preferred)*: keep only a bounded event ring buffer (recent deltas), not full object snapshots. List requests fan out to backends and are streamed directly to clients without buffering — both options 1 and 3 face the same List aggregation challenge (fan-out, merge, chunked pagination across different continue tokens). Watch clients resuming within the ring receive deltas; clients resuming from before the compaction point receive `410 Gone` and relist. Memory is O(ring size × event size), not O(total objects). This is how the kube-apiserver watch cache works internally.

4. **Metadata-only watch + lazy fetch**: watch only object metadata (name, RV, labels) from backends — much smaller than full objects. Full objects are fetched on demand. Useful for the `kcp-cache-syncagent` too: the agent only needs the full object when something actually changed.

5. **Demand-paged cache**: only load objects into memory when they have active watchers; evict after a TTL. Memory is proportional to the active watch set, not total objects. Complex eviction bookkeeping; risk of thundering herd on simultaneous cold watches.

6. **Off-heap / external state**: push aggregated state to an external store (local etcd, Redis). Process memory is bounded regardless of data volume; adds an operational dependency.

#### Write routing

The frontproxy can also serve as a unified write endpoint, routing incoming writes to whichever cache owns the originating shard.

The writer (shard replication controller) knows which shard it is on. The shard name is embedded in the request path, passed through context and parsed via a client roundtripper. The frontproxy extracts the shard name directly from the path, consults a shard→cache ownership mapping, and forwards the request to the owning cache. From the writer's perspective there is one endpoint regardless of how many caches back it.

```
shard1 replication controller
  → POST /shards/shard1/clusters/cluster1/apis/.../apiexports/foo
  → frontproxy: shard1 → cache-A
  → forwarded to cache-A
```

The shard→cache mapping is derived from the same shard assignment CRs that drive the replication topology. The frontproxy watches them and maintains an in-memory routing table.

Open questions:
- **Mid-reassignment writes**: if shard1 is moving from cache-A to cache-B, the frontproxy must either buffer/retry or return a retriable error to the writer. The transition window needs a defined policy.

**Level 1 vs Level 2 split**: write routing through the frontproxy adds complexity. For Level 1, writes can bypass the frontproxy entirely — shards write directly to their assigned cache using static config. The frontproxy handles reads only. Write routing becomes a Level 2 concern, once the frontproxy and shard assignment APIs are stable.

## Goals

* Develop a design document with concrete APIs and integration points to the system.
* Consider different configuration options these cache servers co-exist and iteract with each other.
* Outline a roadmap with concrete tasks to be done.
