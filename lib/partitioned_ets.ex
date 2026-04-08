defmodule PartitionedEts do
  @moduledoc """
  Distributed, partitioned ETS table for Elixir.

  `PartitionedEts` exposes the same API surface as `:ets`, but routes each
  operation to the cluster node responsible for the key. The canonical API
  is module-shaped — every function takes the table name as its first
  argument, mirroring `:ets` exactly:

      children = [
        {PartitionedEts, name: :my_cache, table_opts: [:named_table, :public]}
      ]

      PartitionedEts.insert(:my_cache, {:key, :value})
      PartitionedEts.lookup(:my_cache, :key)
      #=> [{:key, :value}]

  ## `use` macro

  An optional `use PartitionedEts, table_opts: [...]` macro generates a
  one-module-per-table wrapper for users who prefer that style. The
  generated functions delegate to the canonical module API:

      defmodule MyApp.Cache do
        use PartitionedEts, table_opts: [:named_table, :public]
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
  other paginated ETS functions. Cluster-aware: encodes which node the
  scan should resume on as well as the underlying `:ets` continuation.

  `:ets` declares its own `continuation/0` as `typep` (private), so we
  carry our own opaque alias instead of referring to it from outside the
  module.
  """
  @opaque continuation :: tuple()

  defstruct [:name, :table_ref, :callbacks, :monitor_ref]

  defguardp is_continuation(value) when is_tuple(value) and elem(value, 0) == :continue

  # ── Lifecycle ────────────────────────────────────────────────────────

  @doc """
  Returns a child specification suitable for use in a supervision tree.

  Required keys: `:name` (atom), `:table_opts` (list).
  Optional: `:callbacks` (module).
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

    * `:name` — atom, required. Used as the ETS table name, the `:pg`
      group key, and the `:via` registration name.
    * `:table_opts` — list, required. Passed to `:ets.new/2`. Must
      include `:named_table` and `:public`.
    * `:callbacks` — module, optional. May export `hash/2`, `node_added/1`,
      `node_removed/1`. Each is optional individually; defaults are used
      for any not exported.
  """
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    table_opts = opts |> Keyword.fetch!(:table_opts) |> List.wrap()
    callbacks = Keyword.get(opts, :callbacks)

    validate_table_opts!(table_opts)

    GenServer.start_link(
      __MODULE__,
      {name, table_opts, callbacks},
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

  # ── GenServer callbacks ──────────────────────────────────────────────

  @impl GenServer
  def init({name, table_opts, callbacks}) do
    table_ref = :ets.new(name, table_opts)

    if callbacks, do: Code.ensure_loaded(callbacks)

    hash_module =
      if callbacks && function_exported?(callbacks, :hash, 2),
        do: callbacks,
        else: __MODULE__

    :persistent_term.put({__MODULE__, name, :hash}, hash_module)

    # Monitor only this table's group; raw :pg.monitor avoids the
    # cluster-wide event fan-out from monitor_scope/1.
    {monitor_ref, _initial_pids} = :pg.monitor(Registry, name)

    {:ok,
     %__MODULE__{
       name: name,
       table_ref: table_ref,
       callbacks: callbacks,
       monitor_ref: monitor_ref
     }}
  end

  @impl GenServer
  def terminate(_reason, %{name: name}) do
    :persistent_term.erase({__MODULE__, name, :hash})
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
  Default hash function: `:erlang.phash2(key, length(nodes))` mapped onto
  the sorted node list.

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
    handle_continuation(table, continuation, :match)
  end

  def match(table, spec), do: all_call(table, :match, [table, spec])

  @spec match(atom(), :ets.match_pattern(), pos_integer()) ::
          {[term()], continuation()} | :"$end_of_table"
  def match(table, spec, limit) do
    handle_continuation(table, {:continue, nil, limit, [table, spec, limit]}, :match)
  end

  @spec select(atom(), :ets.match_spec() | continuation()) ::
          [term()] | {[term()], continuation()} | :"$end_of_table"
  def select(table, continuation) when is_continuation(continuation) do
    handle_continuation(table, continuation, :select)
  end

  def select(table, spec), do: all_call(table, :select, [table, spec])

  @spec select(atom(), :ets.match_spec(), pos_integer()) ::
          {[term()], continuation()} | :"$end_of_table"
  def select(table, spec, limit) do
    handle_continuation(table, {:continue, nil, limit, [table, spec, limit]}, :select)
  end

  @spec select_count(atom(), :ets.match_spec()) :: non_neg_integer()
  def select_count(table, spec), do: all_call(table, :select_count, [table, spec])

  @spec tab2list(atom()) :: [tuple()]
  def tab2list(table), do: all_call(table, :tab2list, [table])

  @spec first(atom()) :: term() | :"$end_of_table"
  def first(table) do
    table
    |> fetch_nodes()
    |> find(:first, [table])
  end

  @spec last(atom()) :: term() | :"$end_of_table"
  def last(table) do
    table
    |> fetch_nodes(:reverse)
    |> find(:last, [table])
  end

  @spec next(atom(), term()) :: term() | :"$end_of_table"
  def next(table, key) do
    {key_node, nodes} = key_node_with_list(table, key)

    case :erpc.call(key_node, :ets, :next, [table, key]) do
      :"$end_of_table" ->
        nodes
        |> remaining_nodes(key_node)
        |> find(:first, [table])

      value ->
        value
    end
  end

  @spec prev(atom(), term()) :: term() | :"$end_of_table"
  def prev(table, key) do
    {key_node, nodes} = key_node_with_list(table, key)

    case :erpc.call(key_node, :ets, :prev, [table, key]) do
      :"$end_of_table" ->
        nodes
        |> Enum.reverse()
        |> remaining_nodes(key_node)
        |> find(:last, [table])

      value ->
        value
    end
  end

  @spec foldl(atom(), (term(), term() -> term()), term()) :: term()
  def foldl(table, fun, acc) do
    table
    |> fetch_nodes()
    |> fold(:foldl, fun, table, acc)
  end

  @spec foldr(atom(), (term(), term() -> term()), term()) :: term()
  def foldr(table, fun, acc) do
    table
    |> fetch_nodes(:reverse)
    |> fold(:foldr, fun, table, acc)
  end

  @spec match_delete(atom(), :ets.match_pattern()) :: true
  def match_delete(table, spec), do: all_call(table, :match_delete, [table, spec])

  @spec match_object(atom(), :ets.match_pattern() | continuation()) ::
          [tuple()] | {[tuple()], continuation()} | :"$end_of_table"
  def match_object(table, continuation) when is_continuation(continuation) do
    handle_continuation(table, continuation, :match_object)
  end

  def match_object(table, spec), do: all_call(table, :match_object, [table, spec])

  @spec match_object(atom(), :ets.match_pattern(), pos_integer()) ::
          {[tuple()], continuation()} | :"$end_of_table"
  def match_object(table, spec, limit) do
    handle_continuation(table, {:continue, nil, limit, [table, spec, limit]}, :match_object)
  end

  @spec select_delete(atom(), :ets.match_spec()) :: non_neg_integer()
  def select_delete(table, spec), do: all_call(table, :select_delete, [table, spec])

  @spec select_reverse(atom(), :ets.match_spec() | continuation()) ::
          [term()] | {[term()], continuation()} | :"$end_of_table"
  def select_reverse(table, continuation) when is_continuation(continuation) do
    handle_continuation(table, continuation, :select_reverse, :reverse)
  end

  def select_reverse(table, spec), do: all_call(table, :select_reverse, [table, spec], :reverse)

  @spec select_reverse(atom(), :ets.match_spec(), pos_integer()) ::
          {[term()], continuation()} | :"$end_of_table"
  def select_reverse(table, spec, limit) do
    handle_continuation(
      table,
      {:continue, nil, limit, [table, spec, limit]},
      :select_reverse,
      :reverse
    )
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

  # ── Routing helpers ──────────────────────────────────────────────────

  defp find(nodes, fun, args) do
    Enum.reduce_while(nodes, :"$end_of_table", fn node, acc ->
      case :erpc.call(node, :ets, fun, args) do
        :"$end_of_table" -> {:cont, acc}
        value -> {:halt, value}
      end
    end)
  end

  defp fold(nodes, remote_fn, fun, tab, acc) do
    Enum.reduce(nodes, acc, fn node, acc ->
      try do
        :erpc.call(node, :ets, remote_fn, [fun, acc, tab])
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

  defp key_node_with_list(tab, key) do
    nodes = fetch_nodes(tab)
    hash_module = :persistent_term.get({__MODULE__, tab, :hash}, __MODULE__)
    {hash_module.hash(key, nodes), nodes}
  end

  defp fetch_nodes(tab) do
    # Exclude the local node when its ETS table no longer exists. There
    # is a brief window between the local owner GenServer exiting (which
    # destroys its named ETS table) and `:pg` processing the resulting
    # process EXIT and removing the dead pid from the group. During that
    # window, `Registry.members/1` still returns the dead local pid, and
    # routing to the local node would fail with `:badarg` because the
    # named table is gone.
    local_node = node()
    local_present? = :ets.whereis(tab) != :undefined

    tab
    |> Registry.members()
    |> Enum.map(&:erlang.node/1)
    |> Enum.uniq()
    |> Enum.reject(fn n -> n == local_node and not local_present? end)
    |> Enum.sort()
  end

  defp fetch_nodes(tab, :reverse), do: tab |> fetch_nodes() |> Enum.reverse()
  defp fetch_nodes(tab, :forward), do: fetch_nodes(tab)

  defp partitioned_call(tab, key, fun, args) do
    {key_node, _nodes} = key_node_with_list(tab, key)
    :erpc.call(key_node, :ets, fun, args)
  end

  defp all_call(tab, fun, args, direction \\ :forward) do
    nodes = fetch_nodes(tab, direction)
    results = Enum.map(nodes, fn node -> :erpc.call(node, :ets, fun, args) end)

    cond do
      results == [] -> raise "no nodes available for #{inspect(fun)} on #{inspect(tab)}"
      Enum.all?(results, &is_list/1) -> Enum.concat(results)
      Enum.all?(results, &is_integer/1) -> Enum.sum(results)
      true -> hd(results)
    end
  end

  # The cross-node continuation merge logic is incomplete and known to
  # have edge-case bugs (negative limits, accumulator never returned on
  # the less-than-limit branch, ordering on reverse). It is scheduled
  # for a full rewrite in Phase 3 alongside multi-partition support.
  defp handle_continuation(tab, continuation, fun, direction \\ :forward) do
    case continuation do
      {:continue, _prev_node, 0, _args} ->
        :"$end_of_table"

      {:continue, prev_node, limit, args} ->
        tab
        |> fetch_nodes(direction)
        |> remaining_nodes(prev_node)
        |> Enum.reduce_while({limit, []}, fn node, {limit, acc} ->
          handle_continuation_step(node, fun, args, limit, acc)
        end)
    end
  end

  defp handle_continuation_step(node, fun, args, limit, acc) do
    case :erpc.call(node, :ets, fun, args) do
      :"$end_of_table" ->
        {:cont, {limit, acc}}

      {results, :"$end_of_table"} when length(results) < limit ->
        {:cont, {limit - length(results), results ++ acc}}

      {results, continuation} ->
        {:halt, {results ++ acc, {:continue, node, limit - length(results), [continuation]}}}
    end
  end

  defp remaining_nodes(nodes, nil), do: nodes

  defp remaining_nodes(nodes, node) do
    index = Enum.find_index(nodes, &Kernel.==(&1, node)) + 1
    nodes |> Enum.split(index) |> elem(1)
  end

  # ── `use` macro (sugar) ──────────────────────────────────────────────

  defmacro __using__(opts) do
    table_opts = Keyword.fetch!(opts, :table_opts)

    quote location: :keep do
      @behaviour PartitionedEts

      @partitioned_ets_table_opts unquote(table_opts)

      def child_spec(_opts) do
        PartitionedEts.child_spec(
          name: __MODULE__,
          table_opts: @partitioned_ets_table_opts,
          callbacks: __MODULE__
        )
      end

      def start_link(_opts \\ []) do
        PartitionedEts.start_link(
          name: __MODULE__,
          table_opts: @partitioned_ets_table_opts,
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
