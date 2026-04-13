# PartitionedEts

[![Hex.pm](https://img.shields.io/hexpm/v/partitioned_ets.svg)](https://hex.pm/packages/partitioned_ets)
[![HexDocs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/partitioned_ets)

A distributed, partitioned ETS table for Elixir.

`PartitionedEts` exposes the full `:ets` API, but routes every operation to a
single `{node, partition}` shard selected by
[rendezvous hashing (HRW)](https://en.wikipedia.org/wiki/Rendezvous_hashing)
over the cluster. On membership changes it transfers the affected entries
between shards so that data and routing remain in sync.

## Installation

Add `:partitioned_ets` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:partitioned_ets, "~> 0.1.0"}
  ]
end
```

Requires Elixir `~> 1.15` and OTP 26+.

## Usage

### Module form

Every function takes the table name as its first argument, mirroring `:ets`:

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

### Macro form

Defines one module per table with wrapper functions that omit the table argument:

```elixir
defmodule MyApp.Cache do
  use PartitionedEts, table_opts: [:named_table, :public], partitions: 16
end

# In a supervision tree
children = [MyApp.Cache]

# Operations
MyApp.Cache.insert({:key, :value})
MyApp.Cache.lookup(:key)
```

Both forms share the same GenServer and partition layout. The macro generates
thin delegation functions.

## How it works

### Shards

A shard is a `{node, partition_table_atom}` pair. On each node hosting the
table, `PartitionedEts.start_link/1` creates `N` named ETS tables. With
`partitions: 1` (the default) the single ETS table is named after the table
itself; with `partitions: N > 1` they are named `:"<table>_p0"` through
`:"<table>_p<N-1>"`. The cluster-wide shard set is the cross product of all
nodes and all partition tables.

### Routing

Single-key operations use rendezvous hashing to select a shard. Local
operations go directly to ETS; remote operations are dispatched via
`:erpc.call`. Fan-out operations (`match/2`, `select/2`, `tab2list/1`,
`foldl/3`, etc.) iterate every shard and merge results.

Adding or removing a node remaps only approximately `1/total_shards` of keys.

### Handoff

On graceful join, every existing node iterates its local entries, identifies
keys whose new HRW shard is on the joining node, and ships them via `:erpc`.
On graceful leave, the leaving node ships every local entry to its new owner
using a two-pass strategy that avoids read misses during the transfer.

### Consistency model

PartitionedEts is **eventually consistent** with **last-writer-wins**
semantics during topology changes. In steady state (no nodes joining or
leaving), every operation is immediately consistent.

During a join or leave handoff, there is a brief window where concurrent
writes to the same key on different nodes can conflict. Conflicts are resolved
by a monotonic clock: the write with the higher clock value wins. There are no
locks or blocking during handoff — reads and writes continue to be served
throughout.

## Configuration

| Option | Type | Default | Description |
|---|---|---|---|
| `:name` | `atom` | required | Logical table name. Used as the ETS table name (when `partitions: 1`) or prefix, the `:pg` group key, and the `:via` registration name. |
| `:table_opts` | `list` | required | Passed to `:ets.new/2`. Must include `:named_table` and `:public`. |
| `:partitions` | `pos_integer` | `1` | Number of ETS tables per node. Higher values reduce write contention at the cost of increased fan-out. |
| `:callbacks` | `module \| nil` | `nil` | Optional module exporting any subset of `hash/2`, `node_added/1`, `node_removed/1`. |
| `:distributed` | `boolean` | `true` | When `false`, the table runs on a single node with no cluster registration or handoff. Useful for write-contention relief without distribution overhead. |

## Limitations

- **Hard crashes lose data.** If a node terminates without running `terminate/2`,
  the entries it owned are lost. Replication is not implemented.
- **Handoff blocks the owner GenServer.** For large tables, handoff may exceed
  the default supervisor shutdown timeout. Increase the `shutdown:` value in
  the child spec if needed.
- **Homogeneous partition count assumed.** All nodes hosting the same logical
  table must use the same `:partitions` value.
- **Ordered-set semantics are not preserved across shards.** `first/1`,
  `last/1`, `next/2`, and `prev/2` iterate shards in a deterministic but not
  globally ordered sequence.
- **Pagination is in-memory.** The first call to `match/3` or `select/3`
  materializes the full cluster-wide result set before returning chunks.

## Running the tests

```bash
epmd -daemon    # required for the :peer cluster tests
mix test
```

## License

MIT -- see [LICENSE](LICENSE).
