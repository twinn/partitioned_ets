defmodule PartitionedEts.EtsContinuationTest do
  @moduledoc """
  Tests whether `:ets` continuation tuples survive distribution.

  The original assumption was that continuations from `ets:select/3`
  contain compiled match-spec NIF resources that are local to the
  originating VM and cannot be shipped across nodes. These tests
  verify whether that holds on the current OTP version.
  """

  use ExUnit.Case

  alias PartitionedEts.Cluster

  @remote :"cont_test@127.0.0.1"
  @match_spec [{{:"$1", :"$2"}, [], [:"$$"]}]

  setup_all do
    Cluster.spawn([@remote])
    :ok
  end

  setup do
    table = :"cont_#{:erlang.unique_integer([:positive])}"
    :rpc.block_call(@remote, :ets, :new, [table, [:set, :public, :named_table]])
    for i <- 1..100, do: :rpc.block_call(@remote, :ets, :insert, [table, {i, "val_#{i}"}])
    on_exit(fn -> :rpc.block_call(@remote, :ets, :delete, [table]) end)
    %{table: table}
  end

  test "continuation created and resumed on the remote node", %{table: table} do
    {first, cont} = :rpc.block_call(@remote, :ets, :select, [table, @match_spec, 10])
    assert length(first) == 10

    {second, _cont2} = :rpc.block_call(@remote, :ets, :select, [cont])
    assert length(second) == 10
  end

  test "continuation round-trips through distribution", %{table: table} do
    # Step 1: continuation travels remote → local via rpc return value.
    {first, cont} = :rpc.block_call(@remote, :ets, :select, [table, @match_spec, 10])
    assert length(first) == 10

    # Step 2: send the continuation back to the remote node.
    {second, _cont2} = :rpc.block_call(@remote, :ets, :select, [cont])
    assert length(second) == 10
  end

  test "continuation can be fully drained across distribution", %{table: table} do
    {batch, cont} = :rpc.block_call(@remote, :ets, :select, [table, @match_spec, 10])
    all = drain_continuation(@remote, cont, batch)

    assert length(all) == 100
    assert length(Enum.uniq(all)) == 100
  end

  test "match/3 continuation round-trips through distribution", %{table: table} do
    pattern = {:"$1", :"$2"}
    {first, cont} = :rpc.block_call(@remote, :ets, :match, [table, pattern, 10])
    assert length(first) == 10

    {second, _} = :rpc.block_call(@remote, :ets, :match, [cont])
    assert length(second) == 10
  end

  test "match_object/3 continuation round-trips through distribution", %{table: table} do
    pattern = {:"$1", :"$2"}
    {first, cont} = :rpc.block_call(@remote, :ets, :match_object, [table, pattern, 10])
    assert length(first) == 10

    {second, _} = :rpc.block_call(@remote, :ets, :match_object, [cont])
    assert length(second) == 10
  end

  defp drain_continuation(_node, :"$end_of_table", acc), do: acc

  defp drain_continuation(node, cont, acc) do
    case :rpc.block_call(node, :ets, :select, [cont]) do
      {batch, next_cont} -> drain_continuation(node, next_cont, acc ++ batch)
      :"$end_of_table" -> acc
    end
  end
end
