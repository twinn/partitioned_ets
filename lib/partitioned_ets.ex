defmodule PartitionedEts do
  @moduledoc """
  Documentation for `PartitionedEts`.
  """
  @callback hash(term(), [Node.t()]) :: Node.t()
  @callback node_added(Node.t()) :: any()

  # The `use` macro is intentionally large for now: it generates the full
  # ETS API surface (insert, lookup, match, select, foldl, continuations,
  # update_counter, take, etc.) on the using module. Phase 2 of the project
  # replaces this with a thin `defdelegate` shim over module-based functions
  # on `PartitionedEts`, at which point this quote block will collapse to a
  # few dozen lines. Until then we silence Credo's LongQuoteBlocks check.
  defmacro __using__(table_opts) do
    # credo:disable-for-next-line Credo.Check.Refactor.LongQuoteBlocks
    quote do
      use GenServer

      alias PartitionedEts.Registry

      def start_link(_opts) do
        opts = List.wrap(unquote(table_opts))

        validate_supported_opts!(opts)
        GenServer.start_link(__MODULE__, opts, name: {:via, Registry, __MODULE__})
      end

      def init(opts) do
        ref = :ets.new(__MODULE__, opts)
        {:ok, ref}
      end

      defp validate_supported_opts!(opts) do
        Enum.each(opts, fn
          {:keypos, num} when num != 1 ->
            raise "#{unquote(__MODULE__)} only supports `keypos: 1`, include it in your list of options"

          opt when opt in [:private, :protected] ->
            raise "#{unquote(__MODULE__)} does not support `:#{opt}` it only supports `:public`, include it in your list of options"

          _ ->
            :ok
        end)

        if !Enum.member?(opts, :named_table) do
          raise "#{unquote(__MODULE__)} only supports `:named_table`, include it in your list of options"
        end

        if !Enum.member?(opts, :public) do
          raise "#{unquote(__MODULE__)} only supports `:public`, include it in your list of options"
        end
      end

      @spec insert(tuple() | [tuple()]) :: true
      def insert(objs) do
        objs
        |> List.wrap()
        |> Enum.each(fn obj when is_tuple(obj) ->
          partitioned_call(__MODULE__, elem(obj, 0), :insert, [__MODULE__, obj])
        end)
      end

      @spec lookup(term()) :: [tuple()]
      def lookup(key) do
        partitioned_call(__MODULE__, key, :lookup, [__MODULE__, key])
      end

      @spec insert_new(tuple() | [tuple()]) :: true
      def insert_new(objects) when is_list(objects) do
        for object <- objects do
          insert_new(object)
        end

        true
      end

      def insert_new(object) when is_tuple(object) do
        key = elem(object, 0)
        partitioned_call(__MODULE__, key, :insert_new, [__MODULE__, object])
      end

      @spec member(term()) :: boolean()
      def member(key) do
        partitioned_call(__MODULE__, key, :member, [__MODULE__, key])
      end

      @spec delete(term) :: true
      def delete(key) do
        partitioned_call(__MODULE__, key, :delete, [__MODULE__, key])
      end

      @spec delete_object(term) :: true
      def delete_object(obj) do
        key = elem(obj, 0)
        partitioned_call(__MODULE__, key, :delete_object, [__MODULE__, obj])
      end

      @spec delete_all_objects() :: true
      def delete_all_objects do
        all_call(__MODULE__, :delete_all_objects, [__MODULE__])
      end

      @spec lookup_element(term, integer()) :: term() | [term()]
      def lookup_element(key, pos) do
        partitioned_call(__MODULE__, key, :lookup_element, [__MODULE__, key, pos])
      end

      defguard is_continuation?(maybe_continuation) when elem(maybe_continuation, 0) == :continue
      @spec match(:ets.continuation()) :: {[term()], :ets.continuation()} | :"$end_of_table"
      def match(continuation) when is_continuation?(continuation) do
        handle_continuation(continuation, :match)
      end

      @spec match(:ets.match_pattern()) :: [term()]
      def match(spec) do
        all_call(__MODULE__, :match, [__MODULE__, spec])
      end

      @spec match(:ets.match_pattern(), integer()) ::
              {[term()], :ets.continuation()} | :"$end_of_table"
      def match(spec, limit) do
        handle_continuation({:continue, nil, limit, [__MODULE__, spec, limit]}, :match)
      end

      @spec select(:ets.continuation()) :: {[term()], :ets.continuation()} | :"$end_of_table"
      def select(continuation) when is_continuation?(continuation) do
        handle_continuation(continuation, :select)
      end

      @spec select(:ets.match_pattern()) :: [term()]
      def select(spec) do
        all_call(__MODULE__, :select, [__MODULE__, spec])
      end

      @spec select(:ets.match_pattern(), integer()) :: [term()]
      def select(spec, limit) do
        handle_continuation({:continue, nil, limit, [__MODULE__, spec, limit]}, :select)
      end

      @spec select_count(:ets.match_pattern()) :: integer()
      def select_count(spec) do
        all_call(__MODULE__, :select_count, [__MODULE__, spec])
      end

      @spec tab2list() :: [term()]
      def tab2list do
        all_call(__MODULE__, :tab2list, [__MODULE__])
      end

      @spec first() :: term() | :"$end_of_table"
      def first do
        __MODULE__
        |> fetch_nodes()
        |> find(:first, [__MODULE__])
      end

      @spec last() :: term() | :"$end_of_table"
      def last do
        __MODULE__
        |> fetch_nodes(:reverse)
        |> find(:last, [__MODULE__])
      end

      @spec next(term()) :: term() | :"$end_of_table"
      def next(key) do
        {key_node, nodes} = key_node_with_list(__MODULE__, key)

        case :rpc.call(key_node, :ets, :next, [__MODULE__, key]) do
          :"$end_of_table" ->
            nodes
            |> remaining_nodes(key_node)
            |> find(:first, [__MODULE__])

          value ->
            value
        end
      end

      @spec prev(term()) :: term() | :"$end_of_table"
      def prev(key) do
        {key_node, nodes} = key_node_with_list(__MODULE__, key)

        case :rpc.call(key_node, :ets, :prev, [__MODULE__, key]) do
          :"$end_of_table" ->
            nodes
            |> Enum.reverse()
            |> remaining_nodes(key_node)
            |> find(:last, [__MODULE__])

          value ->
            value
        end
      end

      @spec foldl((term(), term() -> term()), term()) :: term()
      def foldl(fun, acc) do
        __MODULE__
        |> fetch_nodes()
        |> fold(:foldl, fun, __MODULE__, acc)
      end

      @spec foldr((term(), term() -> term()), term()) :: term()
      def foldr(fun, acc) do
        __MODULE__
        |> fetch_nodes(:reverse)
        |> fold(:foldr, fun, __MODULE__, acc)
      end

      @spec match_delete(term()) :: true
      def match_delete(spec) do
        all_call(__MODULE__, :match_delete, [__MODULE__, spec])
      end

      @spec match_object(:ets.continuation()) ::
              {[term()], :ets.continuation()} | :"$end_of_table"
      def match_object(continuation) when is_continuation?(continuation) do
        handle_continuation(continuation, :match_object)
      end

      @spec match_object(:ets.match_pattern()) :: [term()]
      def match_object(spec) do
        all_call(__MODULE__, :match_object, [__MODULE__, spec])
      end

      @spec match_object(:ets.match_pattern(), integer()) ::
              {[term()], :ets.continuation()} | :"$end_of_table"
      def match_object(spec, limit) do
        handle_continuation({:continue, nil, limit, [__MODULE__, spec, limit]}, :match_object)
      end

      @spec select_delete(:ets.match_pattern()) :: term()
      def select_delete(spec) do
        all_call(__MODULE__, :select_delete, [__MODULE__, spec])
      end

      @spec select_reverse(:ets.continuation()) ::
              {[term()], :ets.continuation()} | :"$end_of_table"
      def select_reverse(continuation) when is_continuation?(continuation) do
        handle_continuation(continuation, :select_reverse, :reverse)
      end

      @spec select_reverse(:ets.match_pattern()) :: [term()]
      def select_reverse(spec) do
        all_call(__MODULE__, :select_reverse, [__MODULE__, spec], :reverse)
      end

      @spec select_reverse(:ets.match_pattern()) ::
              {[term()], :ets.continuation()} | :"$end_of_table"
      def select_reverse(spec, limit) do
        handle_continuation(
          {:continue, nil, limit, [__MODULE__, spec, limit]},
          :select_reverse,
          :reverse
        )
      end

      @spec update_counter(
              term(),
              {integer(), integer()} | {integer(), integer(), integer(), integer()}
            ) :: integer()
      def update_counter(key, update_op) do
        partitioned_call(__MODULE__, key, :update_counter, [__MODULE__, key, update_op])
      end

      @spec update_counter(
              term(),
              {integer(), integer()} | {integer(), integer(), integer(), integer()},
              integer()
            ) :: integer()
      def update_counter(key, update_op, default) do
        partitioned_call(__MODULE__, key, :update_counter, [__MODULE__, key, update_op, default])
      end

      @spec update_element(term(), {integer(), term()}) :: boolean()
      def update_element(key, update_op) do
        partitioned_call(__MODULE__, key, :update_element, [__MODULE__, key, update_op])
      end

      @spec take(term()) :: [tuple()]
      def take(key) do
        partitioned_call(__MODULE__, key, :take, [__MODULE__, key])
      end

      @spec select_replace(:ets.match_pattern()) :: integer()
      def select_replace(spec) do
        all_call(__MODULE__, :select_replace, [__MODULE__, spec])
      end

      defp find(nodes, fun, args) do
        Enum.reduce_while(nodes, :"$end_of_table", fn node, acc ->
          case :rpc.call(node, :ets, fun, args) do
            :"$end_of_table" -> {:cont, acc}
            value -> {:halt, value}
          end
        end)
      end

      defp fold(nodes, remote_fn, fun, tab, acc) do
        Enum.reduce(nodes, acc, fn node, acc ->
          case :rpc.call(node, :ets, remote_fn, [fun, acc, tab]) do
            {:badrpc, {:EXIT, {:undef, _}}} ->
              raise %UndefinedFunctionError{
                message: "Function: #{inspect(fun)} is not defined on node: #{inspect(node)}"
              }

            result ->
              result
          end
        end)
      end

      defp key_node_with_list(tab, key) do
        nodes = fetch_nodes(tab)
        {hash(key, nodes), nodes}
      end

      @spec hash(term(), [Node.t()]) :: Node.t()
      def hash(key, nodes) do
        index = :erlang.phash2(key, length(nodes))
        Enum.at(nodes, index)
      end

      defp fetch_nodes(tab) do
        tab
        |> Registry.members()
        |> Enum.map(&:erlang.node(&1))
        |> Enum.sort()
      end

      defp fetch_nodes(tab, :reverse) do
        tab
        |> fetch_nodes()
        |> Enum.reverse()
      end

      defp fetch_nodes(tab, :forward) do
        fetch_nodes(tab)
      end

      defp partitioned_call(tab, key, fun, args) do
        {key_node, _nodes} = key_node_with_list(tab, key)
        :rpc.call(key_node, :ets, fun, args)
      end

      defp all_call(tab, fun, args, direction \\ :forward) do
        tab
        |> fetch_nodes(direction)
        |> :rpc.multicall(:ets, fun, args)
        |> case do
          {[list | _] = results, []} when is_list(list) ->
            Enum.concat(results)

          {[int | _] = results, []} when is_integer(int) ->
            Enum.sum(results)

          {[result | _] = _other, []} ->
            result

          err ->
            # Surface the raw multicall shape before raising so the
            # un-formatted result is visible in stdout in addition to
            # the exception message. The exhaustive shape handling is
            # tracked for the Phase 2 cleanup.
            # credo:disable-for-next-line Credo.Check.Warning.IoInspect
            IO.inspect(err, label: "PartitionedEts unexpected multicall result")
            raise "unexpected multicall result from #{inspect(fun)}"
        end
      end

      # The cross-node continuation merge logic is incomplete and known to
      # have edge-case bugs (negative limits, accumulator never returned,
      # ordering on reverse). It is scheduled for a full rewrite in Phase 3
      # alongside multi-partition support, which is why we don't refactor
      # the nesting cosmetically here — the inner case is going to move to
      # a per-partition function shortly.
      # credo:disable-for-next-line Credo.Check.Refactor.Nesting
      defp handle_continuation(continuation, fun, direction \\ :forward) do
        case continuation do
          {:continue, _prev_node, 0, _args} ->
            :"$end_of_table"

          {:continue, prev_node, limit, [tab | _] = args} ->
            all_nodes = fetch_nodes(tab, direction)

            all_nodes
            |> remaining_nodes(prev_node)
            |> Enum.reduce_while({limit, []}, fn node, {limit, acc} ->
              handle_continuation_step(node, fun, args, limit, acc)
            end)
        end
      end

      defp handle_continuation_step(node, fun, args, limit, acc) do
        case :rpc.call(node, :ets, fun, args) do
          :"$end_of_table" ->
            {:cont, {limit, acc}}

          {results, :"$end_of_table"} when length(results) < limit ->
            {:cont, {limit - length(results), results ++ acc}}

          {results, continuation} ->
            {:halt, {results ++ acc, {:continue, node, limit - length(results), [continuation]}}}
        end
      end

      defp remaining_nodes(nodes, nil) do
        nodes
      end

      defp remaining_nodes(nodes, node) do
        index = Enum.find_index(nodes, &Kernel.==(&1, node)) + 1

        nodes
        |> Enum.split(index)
        |> elem(1)
      end

      def node_added(_node) do
        :ok
      end

      defoverridable hash: 2, node_added: 1
    end
  end
end
