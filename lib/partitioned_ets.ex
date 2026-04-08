defmodule PartitionedEts do
  @moduledoc """
  Distributed, partitioned ETS table for Elixir.

  `PartitionedEts` exposes the same API surface as `:ets`, but routes each
  operation to the cluster node responsible for the key — and on each
  node, to one of `N` local ETS tables sharded by `:erlang.phash2/2`. The
  canonical API is module-shaped: every function takes the table name as
  its first argument, mirroring `:ets` exactly.

      children = [
        {PartitionedEts,
         name: :my_cache, table_opts: [:named_table, :public], partitions: 16}
      ]

      PartitionedEts.insert(:my_cache, {:key, :value})
      PartitionedEts.lookup(:my_cache, :key)
      #=> [{:key, :value}]

  ## Partitions

  The `:partitions` option (default `1`) controls how many ETS tables
  back the logical table on each node. With `partitions: 1`, the single
  ETS table is named `name` itself; with `partitions: N > 1`, the
  partition tables are named `:"\#{name}_p0"` through
  `:"\#{name}_p\#{N - 1}"`. Routing within a node is done by
  `:erlang.phash2(key, N)` and goes directly to the chosen partition's
  ETS table — no GenServer hop on the read or write path.

  Splitting one logical table across multiple ETS tables on a single
  node reduces write contention (each ETS table has its own write
  lock). On a multi-node cluster, the same `:partitions` setting must
  be used on every node hosting the table — heterogeneous partition
  counts arrive in a later phase.

  ## `use` macro

  An optional `use PartitionedEts, table_opts: [...], partitions: N`
  macro generates a one-module-per-table wrapper for users who prefer
  that style. The generated functions delegate to the canonical module
  API:

      defmodule MyApp.Cache do
        use PartitionedEts, table_opts: [:named_table, :public], partitions: 16
      end

      MyApp.Cache.insert({:key, :value})

  Override callbacks (`hash/2`, `node_added/1`, `node_removed/1`) by
  defining them on the using module — the macro marks them
  `defoverridable`. For the module-form API, pass an optional
  `:callbacks` module to `start_link/1` that exports any subset.
  """

  use GenServer

  alias PartitionedEts.Registry

  @callback hash(term(), [Node.t()]) :: Node.t()
  @callback node_added(Node.t()) :: any()
  @callback node_removed(Node.t()) :: any()
  @optional_callbacks hash: 2, node_added: 1, node_removed: 1

  @typedoc """
  Opaque continuation token returned from `match/3`, `select/3`, and the
  other paginated ETS functions. Cluster-aware: encodes which shard
  ((node, partition) pair) the scan should resume on as well as the
  underlying `:ets` continuation, if any.

  `:ets` declares its own `continuation/0` as `typep` (private), so we
  carry our own opaque alias instead of referring to it from outside the
  module.
  """
  @opaque continuation :: tuple()

  defstruct [:name, :callbacks, :monitor_ref]

  defguardp is_continuation(value) when is_tuple(value) and elem(value, 0) == :continue

  # ── Lifecycle ────────────────────────────────────────────────────────

  @doc """
  Returns a child specification suitable for use in a supervision tree.

  Required keys: `:name` (atom), `:table_opts` (list).
  Optional: `:partitions` (positive integer, default 1), `:callbacks` (module).
  """
  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 5_000
    }
  end

  @doc """
  Starts a partitioned ETS table.

  ## Options

    * `:name` — atom, required. Used as the ETS table name (when
      `:partitions` is 1) or as the prefix for the partition table
      names (when greater), the `:pg` group key, and the `:via`
      registration name.
    * `:table_opts` — list, required. Passed to `:ets.new/2`. Must
      include `:named_table` and `:public`.
    * `:partitions` — positive integer, optional, default `1`. Number
      of ETS tables to create on this node. Routing within the node is
      done by `:erlang.phash2(key, partitions)`.
    * `:callbacks` — module, optional. May export `hash/2`,
      `node_added/1`, `node_removed/1`. Each is optional individually;
      defaults are used for any not exported.
  """
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    table_opts = opts |> Keyword.fetch!(:table_opts) |> List.wrap()
    partitions = Keyword.get(opts, :partitions, 1)
    callbacks = Keyword.get(opts, :callbacks)

    validate_table_opts!(table_opts)
    validate_partitions!(partitions)

    GenServer.start_link(
      __MODULE__,
      {name, table_opts, partitions, callbacks},
      name: {:via, Registry, name}
    )
  end

  defp validate_table_opts!(opts) do
    Enum.each(opts, fn
      {:keypos, num} when num != 1 ->
        raise "#{inspect(__MODULE__)} only supports `keypos: 1`, include it in your list of options"

      opt when opt in [:private, :protected] ->
        raise "#{inspect(__MODULE__)} does not support `:#{opt}` it only supports `:public`, include it in your list of options"

      _ ->
        :ok
    end)

    if !Enum.member?(opts, :named_table) do
      raise "#{inspect(__MODULE__)} only supports `:named_table`, include it in your list of options"
    end

    if !Enum.member?(opts, :public) do
      raise "#{inspect(__MODULE__)} only supports `:public`, include it in your list of options"
    end
  end

  defp validate_partitions!(n) when is_integer(n) and n > 0, do: :ok

  defp validate_partitions!(n) do
    raise ArgumentError, "#{inspect(__MODULE__)} :partitions must be a positive integer, got: #{inspect(n)}"
  end

  # ── GenServer callbacks ──────────────────────────────────────────────

  @impl GenServer
  def init({name, table_opts, partitions, callbacks}) do
    partition_tables = build_partition_tables(name, partitions)

    Enum.each(partition_tables, fn pt ->
      :ets.new(pt, table_opts)
    end)

    if callbacks, do: Code.ensure_loaded(callbacks)

    hash_module =
      if callbacks && function_exported?(callbacks, :hash, 2),
        do: callbacks,
        else: __MODULE__

    config = %{
      partition_count: partitions,
      partition_tables: List.to_tuple(partition_tables),
      hash: hash_module
    }

    :persistent_term.put({__MODULE__, name, :config}, config)

    # Monitor only this table's group; raw :pg.monitor avoids the
    # cluster-wide event fan-out from monitor_scope/1.
    {monitor_ref, _initial_pids} = :pg.monitor(Registry, name)

    {:ok,
     %__MODULE__{
       name: name,
       callbacks: callbacks,
       monitor_ref: monitor_ref
     }}
  end

  defp build_partition_tables(name, 1), do: [name]

  defp build_partition_tables(name, n) do
    for i <- 0..(n - 1), do: :"#{name}_p#{i}"
  end

  @impl GenServer
  def terminate(_reason, %{name: name}) do
    :persistent_term.erase({__MODULE__, name, :config})
    :ok
  end

  @impl GenServer
  def handle_info({ref, :join, group, pids}, %{monitor_ref: ref, name: group, callbacks: callbacks} = state) do
    if callbacks && function_exported?(callbacks, :node_added, 1) do
      node = pids |> hd() |> :erlang.node()
      # Run user callbacks asynchronously so a slow callback can't block
      # the GenServer from accepting routing-state queries.
      Task.start(fn -> callbacks.node_added(node) end)
    end

    {:noreply, state}
  end

  def handle_info({ref, :leave, group, pids}, %{monitor_ref: ref, name: group, callbacks: callbacks} = state) do
    if callbacks && function_exported?(callbacks, :node_removed, 1) do
      node = pids |> hd() |> :erlang.node()
      Task.start(fn -> callbacks.node_removed(node) end)
    end

    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ── Default hash ─────────────────────────────────────────────────────

  @doc """
  Default cluster-level hash function: `:erlang.phash2(key, length(nodes))`
  mapped onto the sorted node list. Picks which node owns the key.

  Override by passing a `:callbacks` module to `start_link/1` that
  exports `hash/2`, or by defining `hash/2` on a `use PartitionedEts`
  module.
  """
  @spec hash(term(), [Node.t()]) :: Node.t()
  def hash(key, nodes) do
    index = :erlang.phash2(key, length(nodes))
    Enum.at(nodes, index)
  end

  # ── ETS-shaped API ───────────────────────────────────────────────────

  @spec insert(atom(), tuple() | [tuple()]) :: true
  def insert(table, objs) do
    for obj <- List.wrap(objs), is_tuple(obj) do
      partitioned_call(table, elem(obj, 0), :insert, [table, obj])
    end

    true
  end

  @spec lookup(atom(), term()) :: [tuple()]
  def lookup(table, key), do: partitioned_call(table, key, :lookup, [table, key])

  @spec insert_new(atom(), tuple() | [tuple()]) :: boolean()
  def insert_new(table, objects) when is_list(objects) do
    Enum.all?(objects, &insert_new(table, &1))
  end

  def insert_new(table, object) when is_tuple(object) do
    partitioned_call(table, elem(object, 0), :insert_new, [table, object])
  end

  @spec member(atom(), term()) :: boolean()
  def member(table, key), do: partitioned_call(table, key, :member, [table, key])

  @spec delete(atom(), term()) :: true
  def delete(table, key), do: partitioned_call(table, key, :delete, [table, key])

  @spec delete_object(atom(), tuple()) :: true
  def delete_object(table, obj), do: partitioned_call(table, elem(obj, 0), :delete_object, [table, obj])

  @spec delete_all_objects(atom()) :: true
  def delete_all_objects(table), do: all_call(table, :delete_all_objects, [table])

  @spec lookup_element(atom(), term(), pos_integer()) :: term() | [term()]
  def lookup_element(table, key, pos), do: partitioned_call(table, key, :lookup_element, [table, key, pos])

  @spec match(atom(), :ets.match_pattern() | continuation()) ::
          [term()] | {[term()], continuation()} | :"$end_of_table"
  def match(table, continuation) when is_continuation(continuation) do
    resume_paginated(table, continuation)
  end

  def match(table, spec), do: all_call(table, :match, [table, spec])

  @spec match(atom(), :ets.match_pattern(), pos_integer()) ::
          {[term()], continuation()} | :"$end_of_table"
  def match(table, spec, limit) do
    start_paginated(table, :match, spec, limit, :forward)
  end

  @spec select(atom(), :ets.match_spec() | continuation()) ::
          [term()] | {[term()], continuation()} | :"$end_of_table"
  def select(table, continuation) when is_continuation(continuation) do
    resume_paginated(table, continuation)
  end

  def select(table, spec), do: all_call(table, :select, [table, spec])

  @spec select(atom(), :ets.match_spec(), pos_integer()) ::
          {[term()], continuation()} | :"$end_of_table"
  def select(table, spec, limit) do
    start_paginated(table, :select, spec, limit, :forward)
  end

  @spec select_count(atom(), :ets.match_spec()) :: non_neg_integer()
  def select_count(table, spec), do: all_call(table, :select_count, [table, spec])

  @spec tab2list(atom()) :: [tuple()]
  def tab2list(table), do: all_call(table, :tab2list, [table])

  @spec first(atom()) :: term() | :"$end_of_table"
  def first(table), do: find_shards(shards(table, :forward), :first)

  @spec last(atom()) :: term() | :"$end_of_table"
  def last(table), do: find_shards(shards(table, :reverse), :last)

  @spec next(atom(), term()) :: term() | :"$end_of_table"
  def next(table, key) do
    {key_node, _} = key_node_with_list(table, key)
    pt = local_partition_table(table, key)

    case shard_call(key_node, :next, [pt, key]) do
      :"$end_of_table" -> find_shards(shards_after(table, :forward, {key_node, pt}), :first)
      value -> value
    end
  end

  @spec prev(atom(), term()) :: term() | :"$end_of_table"
  def prev(table, key) do
    {key_node, _} = key_node_with_list(table, key)
    pt = local_partition_table(table, key)

    case shard_call(key_node, :prev, [pt, key]) do
      :"$end_of_table" -> find_shards(shards_after(table, :reverse, {key_node, pt}), :last)
      value -> value
    end
  end

  @spec foldl(atom(), (term(), term() -> term()), term()) :: term()
  def foldl(table, fun, acc), do: fold_shards(shards(table, :forward), :foldl, fun, acc)

  @spec foldr(atom(), (term(), term() -> term()), term()) :: term()
  def foldr(table, fun, acc), do: fold_shards(shards(table, :reverse), :foldr, fun, acc)

  @spec match_delete(atom(), :ets.match_pattern()) :: true
  def match_delete(table, spec), do: all_call(table, :match_delete, [table, spec])

  @spec match_object(atom(), :ets.match_pattern() | continuation()) ::
          [tuple()] | {[tuple()], continuation()} | :"$end_of_table"
  def match_object(table, continuation) when is_continuation(continuation) do
    resume_paginated(table, continuation)
  end

  def match_object(table, spec), do: all_call(table, :match_object, [table, spec])

  @spec match_object(atom(), :ets.match_pattern(), pos_integer()) ::
          {[tuple()], continuation()} | :"$end_of_table"
  def match_object(table, spec, limit) do
    start_paginated(table, :match_object, spec, limit, :forward)
  end

  @spec select_delete(atom(), :ets.match_spec()) :: non_neg_integer()
  def select_delete(table, spec), do: all_call(table, :select_delete, [table, spec])

  @spec select_reverse(atom(), :ets.match_spec() | continuation()) ::
          [term()] | {[term()], continuation()} | :"$end_of_table"
  def select_reverse(table, continuation) when is_continuation(continuation) do
    resume_paginated(table, continuation)
  end

  def select_reverse(table, spec), do: all_call(table, :select_reverse, [table, spec], :reverse)

  @spec select_reverse(atom(), :ets.match_spec(), pos_integer()) ::
          {[term()], continuation()} | :"$end_of_table"
  def select_reverse(table, spec, limit) do
    start_paginated(table, :select_reverse, spec, limit, :reverse)
  end

  @spec update_counter(atom(), term(), term()) :: integer()
  def update_counter(table, key, update_op) do
    partitioned_call(table, key, :update_counter, [table, key, update_op])
  end

  @spec update_counter(atom(), term(), term(), tuple()) :: integer()
  def update_counter(table, key, update_op, default) do
    partitioned_call(table, key, :update_counter, [table, key, update_op, default])
  end

  @spec update_element(atom(), term(), {pos_integer(), term()}) :: boolean()
  def update_element(table, key, update_op) do
    partitioned_call(table, key, :update_element, [table, key, update_op])
  end

  @spec take(atom(), term()) :: [tuple()]
  def take(table, key), do: partitioned_call(table, key, :take, [table, key])

  @spec select_replace(atom(), :ets.match_spec()) :: non_neg_integer()
  def select_replace(table, spec), do: all_call(table, :select_replace, [table, spec])

  # ── Internal: remote-callable dispatchers ────────────────────────────
  #
  # These are public so :erpc.call can target them, but are not part of
  # the user API. They run on the *target* node and resolve the local
  # partition layout from that node's persistent_term, which lets each
  # node manage its own partition count independently.

  @doc false
  def __remote_dispatch__(table, key, fun, args) do
    pt = local_partition_table(table, key)
    apply(:ets, fun, [pt | tl(args)])
  end

  @doc false
  def __remote_fanout__(table, fun, args) do
    local_fanout(table, fun, args)
  end

  # ── Routing helpers ──────────────────────────────────────────────────

  defp config(table) do
    :persistent_term.get({__MODULE__, table, :config})
  end

  defp local_partition_table(table, key) do
    cfg = config(table)
    partition_id = :erlang.phash2(key, cfg.partition_count)
    elem(cfg.partition_tables, partition_id)
  end

  defp partitioned_call(table, key, fun, args) do
    {key_node, _nodes} = key_node_with_list(table, key)

    if key_node == node() do
      pt = local_partition_table(table, key)
      apply(:ets, fun, [pt | tl(args)])
    else
      :erpc.call(key_node, __MODULE__, :__remote_dispatch__, [table, key, fun, args])
    end
  end

  defp all_call(table, fun, args, direction \\ :forward) do
    nodes = fetch_nodes(table, direction)

    per_node_results =
      Enum.map(nodes, fn node ->
        if node == node() do
          local_fanout(table, fun, args)
        else
          :erpc.call(node, __MODULE__, :__remote_fanout__, [table, fun, args])
        end
      end)

    merge_results(per_node_results, table, fun)
  end

  defp local_fanout(table, fun, args) do
    cfg = config(table)
    partition_tables = Tuple.to_list(cfg.partition_tables)

    results =
      Enum.map(partition_tables, fn pt ->
        apply(:ets, fun, [pt | tl(args)])
      end)

    merge_results(results, table, fun)
  end

  defp merge_results([], table, fun), do: raise("no shards available for #{inspect(fun)} on #{inspect(table)}")

  defp merge_results([single], _table, _fun), do: single

  defp merge_results(results, _table, _fun) do
    cond do
      Enum.all?(results, &is_list/1) -> Enum.concat(results)
      Enum.all?(results, &is_integer/1) -> Enum.sum(results)
      true -> hd(results)
    end
  end

  defp key_node_with_list(table, key) do
    nodes = fetch_nodes(table)
    cfg = config(table)
    {cfg.hash.hash(key, nodes), nodes}
  end

  defp fetch_nodes(table) do
    # Exclude the local node when its named ETS tables no longer exist.
    # There is a brief window between the local owner GenServer exiting
    # (which destroys all of its named partition ETS tables) and `:pg`
    # processing the resulting process EXIT and removing the dead pid
    # from the group. During that window, `Registry.members/1` still
    # returns the dead local pid, and routing to the local node would
    # fail with `:badarg` because the named tables are gone.
    local_node = node()
    local_present? = local_present?(table)

    table
    |> Registry.members()
    |> Enum.map(&:erlang.node/1)
    |> Enum.uniq()
    |> Enum.reject(fn n -> n == local_node and not local_present? end)
    |> Enum.sort()
  end

  defp local_present?(table) do
    case :persistent_term.get({__MODULE__, table, :config}, nil) do
      nil ->
        false

      cfg ->
        cfg.partition_tables
        |> Tuple.to_list()
        |> Enum.all?(&(:ets.whereis(&1) != :undefined))
    end
  end

  defp fetch_nodes(tab, :reverse), do: tab |> fetch_nodes() |> Enum.reverse()
  defp fetch_nodes(tab, :forward), do: fetch_nodes(tab)

  # ── Shard iteration ──────────────────────────────────────────────────
  #
  # A "shard" is a {node, partition_table} pair. Iterating shards is
  # what powers fan-out reads, paginated scans, find/fold, and
  # next/prev. The shard order is nodes-major (sorted), partitions-
  # minor (in their stored order). Phase 3 assumes a homogeneous
  # partition layout across the cluster — each node has the same
  # `:partitions` count and therefore the same partition table names.

  defp shards(table, direction) do
    nodes = fetch_nodes(table, direction)
    pts = config(table).partition_tables |> Tuple.to_list() |> maybe_reverse(direction)

    for n <- nodes, pt <- pts, do: {n, pt}
  end

  defp maybe_reverse(list, :forward), do: list
  defp maybe_reverse(list, :reverse), do: Enum.reverse(list)

  defp shards_after(table, direction, current_shard) do
    table
    |> shards(direction)
    |> Enum.drop_while(&(&1 != current_shard))
    |> case do
      [] -> []
      [_current | rest] -> rest
    end
  end

  defp shard_call(node, fun, args) do
    if node == node() do
      apply(:ets, fun, args)
    else
      :erpc.call(node, :ets, fun, args)
    end
  end

  defp find_shards(shards, fun) do
    Enum.reduce_while(shards, :"$end_of_table", fn {node, pt}, acc ->
      case shard_call(node, fun, [pt]) do
        :"$end_of_table" -> {:cont, acc}
        value -> {:halt, value}
      end
    end)
  end

  defp fold_shards(shards, remote_fn, fun, acc) do
    Enum.reduce(shards, acc, fn {node, pt}, acc ->
      try do
        shard_call(node, remote_fn, [fun, acc, pt])
      rescue
        e in ErlangError ->
          # :erpc surfaces a remote `:undef` (e.g. an anonymous fn that
          # only exists on the calling node) as ErlangError with
          # `original: {:exception, :undef, _}`. Translate it back to
          # UndefinedFunctionError so callers see a meaningful exception
          # type instead of a generic ErlangError tuple.
          case e.original do
            {:exception, :undef, _} ->
              reraise UndefinedFunctionError,
                      [message: "Function: #{inspect(fun)} is not defined on a remote node"],
                      __STACKTRACE__

            _ ->
              reraise e, __STACKTRACE__
          end
      end
    end)
  end

  # ── Paginated scans (match/3, select/3, …) ───────────────────────────
  #
  # Phase 3 limitation: rather than streaming chunks via `:ets`
  # continuations across nodes (which doesn't actually work — the
  # internal magic refs in an `:ets` continuation point to compiled
  # match-spec resources that are local to the originating VM and
  # round-trip incorrectly via :erpc), we materialize the full result
  # set on the first call by fanning out unbounded across all shards,
  # then paginate the in-memory list.
  #
  # The continuation tuple shape is opaque to callers but holds:
  #
  #   {:continue, fun, spec, limit, leftover}
  #
  # where `leftover` is the unread tail of the materialized result
  # list. This is wasteful for large tables — Phase 4/5 will introduce
  # per-node scan-session processes that hold the `:ets` continuation
  # locally and stream chunks back, sidestepping the cross-node refs
  # problem.

  defp start_paginated(table, fun, spec, limit, direction) do
    full_args =
      if direction == :reverse,
        do: [table, spec],
        else: [table, spec]

    full_results = all_call(table, unbounded_fun(fun), full_args, direction)
    paginate(full_results, fun, spec, limit)
  end

  defp resume_paginated(_table, {:continue, fun, spec, limit, leftover}) do
    paginate(leftover, fun, spec, limit)
  end

  # The 3-arity (limited) ETS calls and the 2-arity (unbounded) ETS
  # calls share the same name. We always invoke the 2-arity flavour for
  # fan-out and paginate on the materialized list.
  defp unbounded_fun(:match), do: :match
  defp unbounded_fun(:select), do: :select
  defp unbounded_fun(:match_object), do: :match_object
  defp unbounded_fun(:select_reverse), do: :select_reverse

  defp paginate([], _fun, _spec, _limit), do: :"$end_of_table"

  defp paginate(results, fun, spec, limit) do
    {taken, rest} = Enum.split(results, limit)
    # Always return a `{:continue, ...}` tuple even when `rest == []`
    # so the caller can pass the continuation back through `match/2`,
    # `select/2`, etc. without it being misinterpreted as a match spec.
    # An empty leftover triggers the `paginate([], …)` clause on the
    # next call and returns `:"$end_of_table"`.
    {taken, {:continue, fun, spec, limit, rest}}
  end

  # ── `use` macro (sugar) ──────────────────────────────────────────────

  defmacro __using__(opts) do
    table_opts = Keyword.fetch!(opts, :table_opts)
    partitions = Keyword.get(opts, :partitions, 1)

    quote location: :keep do
      @behaviour PartitionedEts

      @partitioned_ets_table_opts unquote(table_opts)
      @partitioned_ets_partitions unquote(partitions)

      def child_spec(_opts) do
        PartitionedEts.child_spec(
          name: __MODULE__,
          table_opts: @partitioned_ets_table_opts,
          partitions: @partitioned_ets_partitions,
          callbacks: __MODULE__
        )
      end

      def start_link(_opts \\ []) do
        PartitionedEts.start_link(
          name: __MODULE__,
          table_opts: @partitioned_ets_table_opts,
          partitions: @partitioned_ets_partitions,
          callbacks: __MODULE__
        )
      end

      def insert(obj), do: PartitionedEts.insert(__MODULE__, obj)
      def lookup(key), do: PartitionedEts.lookup(__MODULE__, key)
      def insert_new(obj), do: PartitionedEts.insert_new(__MODULE__, obj)
      def member(key), do: PartitionedEts.member(__MODULE__, key)
      def delete(key), do: PartitionedEts.delete(__MODULE__, key)
      def delete_object(obj), do: PartitionedEts.delete_object(__MODULE__, obj)
      def delete_all_objects, do: PartitionedEts.delete_all_objects(__MODULE__)
      def lookup_element(key, pos), do: PartitionedEts.lookup_element(__MODULE__, key, pos)
      def match(arg), do: PartitionedEts.match(__MODULE__, arg)
      def match(spec, limit), do: PartitionedEts.match(__MODULE__, spec, limit)
      def select(arg), do: PartitionedEts.select(__MODULE__, arg)
      def select(spec, limit), do: PartitionedEts.select(__MODULE__, spec, limit)
      def select_count(spec), do: PartitionedEts.select_count(__MODULE__, spec)
      def tab2list, do: PartitionedEts.tab2list(__MODULE__)
      def first, do: PartitionedEts.first(__MODULE__)
      def last, do: PartitionedEts.last(__MODULE__)
      def next(key), do: PartitionedEts.next(__MODULE__, key)
      def prev(key), do: PartitionedEts.prev(__MODULE__, key)
      def foldl(fun, acc), do: PartitionedEts.foldl(__MODULE__, fun, acc)
      def foldr(fun, acc), do: PartitionedEts.foldr(__MODULE__, fun, acc)
      def match_delete(spec), do: PartitionedEts.match_delete(__MODULE__, spec)
      def match_object(arg), do: PartitionedEts.match_object(__MODULE__, arg)
      def match_object(spec, limit), do: PartitionedEts.match_object(__MODULE__, spec, limit)
      def select_delete(spec), do: PartitionedEts.select_delete(__MODULE__, spec)
      def select_reverse(arg), do: PartitionedEts.select_reverse(__MODULE__, arg)
      def select_reverse(spec, limit), do: PartitionedEts.select_reverse(__MODULE__, spec, limit)
      def update_counter(key, op), do: PartitionedEts.update_counter(__MODULE__, key, op)
      def update_counter(key, op, default), do: PartitionedEts.update_counter(__MODULE__, key, op, default)
      def update_element(key, op), do: PartitionedEts.update_element(__MODULE__, key, op)
      def take(key), do: PartitionedEts.take(__MODULE__, key)
      def select_replace(spec), do: PartitionedEts.select_replace(__MODULE__, spec)

      @impl PartitionedEts
      def hash(key, nodes), do: PartitionedEts.hash(key, nodes)

      @impl PartitionedEts
      def node_added(_node), do: :ok

      @impl PartitionedEts
      def node_removed(_node), do: :ok

      defoverridable child_spec: 1,
                     start_link: 1,
                     hash: 2,
                     node_added: 1,
                     node_removed: 1
    end
  end
end
