# PartitionedEts

A distributed, partitioned ETS table for Elixir.

`PartitionedEts` exposes the full `:ets` API, but routes every operation to a
single `(node, partition)` shard picked by [rendezvous hashing (HRW)][hrw] over
the cluster. On membership changes it physically moves the affected entries
between shards so that the data and the routing stay in sync.

[hrw]: https://en.wikipedia.org/wiki/Rendezvous_hashing

Written the same way you'd write `:ets`:

```elixir
children = [
  {PartitionedEts,
   name: :my_cache, table_opts: [:named_table, :public], partitions: 16}
]

PartitionedEts.insert(:my_cache, {:key, :value})
PartitionedEts.lookup(:my_cache, :key)
#=> [{:key, :value}]
```

## Status

Pre-1.0, unreleased, API may still change. CI green at every commit across
format, credo, 85 tests, and dialyzer.

## Why this exists

There are several mature distributed data stores for BEAM — Mnesia, Khepri,
Riak Core, Nebulex — but none of them fills exactly the spot `PartitionedEts`
targets. The pitch is narrow:

> **A distributed sharded ETS table that preserves the `:ets` API verbatim and
> physically moves data when the cluster changes. Drop-in when you outgrow one
> node. Drop-out when you don't need it anymore.**

- **Nebulex's `Partitioned` adapter** uses consistent hashing but [does not
  rebalance data when nodes join or leave][nebulex-partitioned] — the docs are
  explicit that failed-node data is unavailable and the recommended recovery
  path is "back it up to a database". Treat-as-cache semantics. `PartitionedEts`
  moves data on graceful events; crashes still lose data.
- **Mnesia** has fragmented tables but a clunky API, heavy ceremony around
  schema management, and famously brittle split-brain recovery. `PartitionedEts`
  is `:ets` with distribution — no new mental model.
- **`shards`** (the original inspiration for sharding `:ets`) is local-only,
  with no distribution story. `PartitionedEts` *is* distributed; for single-node
  sharding it has the same effect with a consistent API across both.
- **Horde** distributes *processes*, not data. Different tool, different
  problem.

[nebulex-partitioned]: https://hexdocs.pm/nebulex/Nebulex.Adapters.Partitioned.html

If your use case is an in-memory sharded table that (a) spans a small-ish BEAM
cluster, (b) uses the `:ets` API you already know, (c) automatically rebalances
on graceful joins/leaves, and (d) doesn't need replication — this is the
library.

## Installation

Not yet published. Use a Git dep:

```elixir
def deps do
  [
    {:partitioned_ets, github: "twinn/partitioned_ets"}
  ]
end
```

Requires Elixir `~> 1.19` and OTP 28+.

## Quick start

### Module form (`:ets`-shaped)

Every function takes the table name as its first argument, mirroring `:ets`
exactly:

```elixir
# In a supervision tree
children = [
  {PartitionedEts,
   name: :my_cache, table_opts: [:named_table, :public], partitions: 16}
]

# Operations
PartitionedEts.insert(:my_cache, {:key, :value})
PartitionedEts.lookup(:my_cache, :key)
#=> [{:key, :value}]

PartitionedEts.delete(:my_cache, :key)
PartitionedEts.match(:my_cache, {:"$1", :"$2"})
PartitionedEts.foldl(:my_cache, fn {_k, v}, acc -> v + acc end, 0)
```

### Macro form (`use` sugar)

One module per table, with wrapper functions that drop the table argument:

```elixir
defmodule MyApp.Cache do
  use PartitionedEts, table_opts: [:named_table, :public], partitions: 16

  # Optional overrides — see "Callbacks" below
  def hash(key, shards), do: ...
  def node_added(node), do: ...
  def node_removed(node), do: ...
end

# In your supervision tree
children = [MyApp.Cache]

# Operations — same names as :ets, no table argument
MyApp.Cache.insert({:key, :value})
MyApp.Cache.lookup(:key)
```

Both forms share the same GenServer + partition layout under the hood; the
macro is a ~30-line shim of `def insert(obj), do: PartitionedEts.insert(__MODULE__, obj)`
one-liners. You can mix and match.

## How it works

### Shards

A *shard* is a `{node, partition_table_atom}` pair. On each node hosting the
table, `PartitionedEts.start_link/1` creates `N` named ETS tables — with
`partitions: 1` (the default) the single ETS table is named after the table
itself, with `partitions: N > 1` they are named `:"<table>_p0"` through
`:"<table>_p<N-1>"`. The cluster-wide shard set is the cross product of all
nodes × all partition tables.

### Routing

Every operation that targets a single key picks the one shard whose HRW weight
is highest:

```elixir
hash = fn key, shards ->
  Enum.max_by(shards, fn shard -> :erlang.phash2({key, shard}) end)
end
```

The owning node is then either the local node (direct `:ets` call, no GenServer
hop, no `:erpc`) or a remote node (`:erpc.call` to a tiny internal dispatcher
that runs the `:ets` op against the partition table). Fan-out operations
(`match/2`, `select/2`, `tab2list/1`, `delete_all_objects/1`, `foldl/3`, etc.)
iterate every shard and merge results.

HRW has the property that **adding or removing one shard remaps only
`~1/total_shards` of keys**; everything else stays where it was. This is what
makes handoff cheap.

### Fast path

On the hot read/write path there's no GenServer call involved:

1. Read the shard config from `:persistent_term` (lock-free, O(1)).
2. Run HRW over the shard list (O(total_shards), cheap — just `:erlang.phash2/2`
   per shard).
3. Dispatch to `:ets` directly (local) or via one `:erpc.call` (remote).

The owner GenServer exists for lifecycle (`init/1` creates the tables,
`terminate/2` runs handoff) and for `:pg` membership. It does not serve reads
or writes.

### Handoff

On **graceful join**, every existing node iterates its local entries in
response to the `:pg` `:join` event, finds the keys whose new HRW shard is on
the joiner, and ships them via `:erpc`. The user-visible `node_added/1`
callback fires *after* handoff on that node has completed, so application code
can use `node_added` as a "handoff done" signal.

On **graceful leave** (`terminate/2` with reason `:normal`, `:shutdown`, or
`{:shutdown, _}`), the leaving node snapshots membership, leaves `:pg`, then
ships every local entry to its new owner under the shard set excluding self.

The owner GenServer traps exits so `terminate/2` actually runs on supervisor
shutdown.

## API overview

`PartitionedEts` exposes the full `:ets` surface. Each function mirrors the
corresponding `:ets` function; see the `:ets` docs for semantics. The key
distinction is that the table argument is the logical table name, and the
library routes to the right shard internally.

**Key-based (single-shard, via HRW):**

`insert/2`, `insert_new/2`, `lookup/2`, `lookup_element/3`, `member/2`,
`delete/2`, `delete_object/2`, `take/2`, `update_counter/3`, `update_counter/4`,
`update_element/3`.

**Fan-out (every shard, results merged):**

`match/2`, `select/2`, `match_object/2`, `select_reverse/2`, `select_count/2`,
`tab2list/1`, `delete_all_objects/1`, `match_delete/2`, `select_delete/2`,
`select_replace/2`, `foldl/3`, `foldr/3`.

**Paginated (see "Limitations" for the in-memory simplification):**

`match/3`, `match/2` (continuation), `select/3`, `select/2` (continuation),
`match_object/3`, `match_object/2` (continuation), `select_reverse/3`,
`select_reverse/2` (continuation).

**Ordered iteration (best-effort, no cluster-wide order):**

`first/1`, `last/1`, `next/2`, `prev/2`. These iterate shards in a deterministic
order but the result is *not* globally ordered across shards. Use them if you
need "some next key", not "the next key by `:ordered_set` semantics."

## Configuration

`PartitionedEts.start_link/1` options:

| Option | Type | Default | Description |
|---|---|---|---|
| `:name` | `atom` | *required* | ETS table name (at `partitions: 1`) or prefix (`partitions: N > 1`). Also the `:pg` group key and `:via` registration name. |
| `:table_opts` | `list` | *required* | Passed to `:ets.new/2`. Must include `:named_table` and `:public`. |
| `:partitions` | `pos_integer` | `1` | Number of ETS tables per node. Splits write locks; higher reduces contention but increases fan-out cost. |
| `:callbacks` | `module or nil` | `nil` | Optional module exporting any subset of `hash/2`, `node_added/1`, `node_removed/1`. With the macro form, the using module *is* the callbacks module and overrides are declared via `defoverridable`. |

## Performance tradeoffs

Rough cost model, assuming an N-node cluster with P partitions per node.

**Single-key ops (`insert`, `lookup`, `delete`, etc.):**

- 1× `:persistent_term.get` (lock-free)
- HRW: N×P × `:erlang.phash2` (microseconds)
- If target is local: 1× `:ets` call (nanoseconds)
- If target is remote: 1× `:erpc.call` (milliseconds, network-bound)

**Fan-out ops (`match`, `select`, `tab2list`, etc.):**

- HRW not needed (we iterate every shard)
- N × (`:erpc.call` to remote + P × `:ets` call locally per node)
- Result merge: `Enum.concat` (lists) or `Enum.sum` (integers) or `hd` (scalars)

Fan-out is O(cluster-wide total shards) per call. For clusters in the 2-20
node range this is fine; for hundreds of nodes you'd want to reshape the use
case (fan-out semantics don't scale regardless of implementation).

**Paginated ops (`match/3`, `select/3`, `match_object/3`, `select_reverse/3`):**

Phase 3 simplification: the first call materializes the *full* cluster-wide
result set in memory, then hands out chunks of `limit` entries. Subsequent
`match(continuation)` calls just walk the in-memory tail.

This is correct but wasteful for large result sets — a better implementation
would stream `:ets` continuations per shard. The reason we don't: `:ets`
continuation tuples contain magic refs to compiled match-spec NIF resources
that are local to the originating VM and don't round-trip cleanly via `:erpc`.
A per-node scan-session process that holds the continuation locally and
streams chunks over `:erpc` is the right fix and is on the roadmap.

**Partition count (`:partitions: N`):**

Each partition is its own ETS table and thus has its own write lock. For a
hot table with concurrent writers, splitting into `N` partitions reduces
contention by roughly a factor of `N` (the same reason `Registry` has a
`:partitions` option). Cost: N×P fan-out instead of N per call, and N ETS
tables to own per node (negligible overhead each).

A reasonable default for contention-heavy workloads is
`System.schedulers_online()`. For read-mostly workloads `1` is fine.

**Handoff blocks the owner GenServer.**

Iterate-and-ship runs inline in `handle_info` (join) or `terminate` (leave),
so during the handoff the GenServer doesn't process other messages. For large
tables (millions of entries) this can exceed the default supervisor shutdown
timeout of 5 seconds. Bump `shutdown:` in the child spec for the leave path,
or accept the brief pause in `handle_info` for the join path.

## Limitations

These are the things we *haven't* solved; the docs and tests lock them in so
they don't regress quietly.

### Hard crashes lose data

If a node dies without running `terminate/2` (SIGKILL, panic, hardware fault)
the entries it owned are gone. Replication is a separate feature and is not
implemented here. The test suite has a `hard stop` test that explicitly
locks in this behaviour — if it ever starts passing with "data reachable",
that's a signal that replication landed.

### Concurrent-write race on join

While handoff on existing node X is shipping key K to the new joiner, a
concurrent write from node Y for K that HRW now routes to the joiner will land
on the joiner. X's older shipped value may overwrite the newer write. Closing
this window requires symmetric sync-blocking on the destination during the
handoff, which is a future improvement.

### Gossip propagation race on leave

Between `:pg.leave` on the leaving node and the gossip propagating to peers,
other nodes may briefly still route writes to the leaving node. Those writes
land on ETS tables that are about to be destroyed. Stagehand has the same race
window on its own leave path and documents it the same way.

### Homogeneous partition count assumed

Every node hosting the same logical table must use the same `:partitions`
value. Heterogeneous per-node counts (for weighted capacity) require gossiping
per-node layouts, which is a future improvement.

### Ordered-set semantics don't survive partitioning

`first/1`, `last/1`, `next/2`, `prev/2` iterate shards in a deterministic but
not globally-ordered way. If you need ordered iteration across an entire
partitioned table, use a single-node `:ordered_set`.

### Pagination is in-memory

As described above — the first call to `match/3`/`select/3`/etc. materializes
the full result set. Fine for small-to-medium scans; bad for result sets that
don't fit in memory.

## Roadmap

Numbered phases that have landed:

- [x] Phase 1: project scaffolding, CI, license
- [x] Phase 2: `:ets`-shaped canonical API + `use` macro shim
- [x] Phase 3: local multi-partition support
- [x] Phase 4: HRW routing across shards
- [x] Phase 5: handoff on graceful membership changes

Plausible next steps, in rough order of payoff:

- Sync-blocking on the destination to close the join-race window
- Per-node scan-session processes for true streaming pagination
- Heterogeneous per-node partition counts
- Optional replication (moves the library into "sharded + HA" territory and
  is the biggest scope change)
- Hex publishing + HexDocs

## Running the tests

```bash
epmd -daemon    # required for the :peer cluster
mix test
```

The suite has 85 tests: 31 on a 2-node `:peer` cluster, 23 single-node
module-form, 20 multi-partition, 8 HRW pure-function, 3 handoff end-to-end.

## License

MIT — see [LICENSE](LICENSE).
