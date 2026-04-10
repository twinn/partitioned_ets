# PartitionedEts

[![Hex.pm](https://img.shields.io/hexpm/v/partitioned_ets.svg)](https://hex.pm/packages/partitioned_ets)
[![HexDocs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/partitioned_ets)

A distributed, partitioned ETS table for Elixir.

`PartitionedEts` exposes the full `:ets` API, but routes every operation to a
single `{node, partition}` shard selected by
[rendezvous hashing (HRW)](https://en.wikipedia.org/wiki/Rendezvous_hashing)
over the cluster. On membership changes it physically moves the affected entries
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

Requires Elixir `~> 1.19` and OTP 28+.

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

Every operation targeting a single key selects the shard whose HRW weight is
highest:

```elixir
Enum.max_by(shards, fn shard -> :erlang.phash2({key, shard}) end)
```

When the target shard is local, the operation is a direct `:ets` call with no
GenServer hop. When the target is remote, the operation is dispatched via a
single `:erpc.call`. Fan-out operations (`match/2`, `select/2`, `tab2list/1`,
`foldl/3`, etc.) iterate every shard and merge results.

HRW guarantees that adding or removing one shard remaps only approximately
`1/total_shards` of keys.

### Handoff

On graceful join, every existing node iterates its local entries, identifies
keys whose new HRW shard is on the joining node, and ships them via `:erpc`.
On graceful leave (`terminate/2` with a normal shutdown reason), the leaving
node ships every local entry to its new owner under the shard set excluding
itself.

## Configuration

| Option | Type | Default | Description |
|---|---|---|---|
| `:name` | `atom` | required | Logical table name. Used as the ETS table name (when `partitions: 1`) or prefix, the `:pg` group key, and the `:via` registration name. |
| `:table_opts` | `list` | required | Passed to `:ets.new/2`. Must include `:named_table` and `:public`. |
| `:partitions` | `pos_integer` | `1` | Number of ETS tables per node. Higher values reduce write contention at the cost of increased fan-out. |
| `:callbacks` | `module \| nil` | `nil` | Optional module exporting any subset of `hash/2`, `node_added/1`, `node_removed/1`. |

## Limitations

- **Hard crashes lose data.** If a node terminates without running `terminate/2`,
  the entries it owned are lost. Replication is not implemented.
- **Brief race windows during membership changes.** Concurrent writes during
  handoff may be overwritten by shipped older values. Gossip propagation delay
  on leave may briefly route writes to the departing node.
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
