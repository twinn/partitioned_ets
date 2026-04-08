defmodule PartitionedEts.MultiPartitionTest do
  @moduledoc """
  Tests for the local multi-partition path: a single node owning N
  ETS tables for one logical table, with intra-node routing by
  `:erlang.phash2(key, N)`.

  Single-node — no `:peer` cluster needed.
  """

  use ExUnit.Case, async: false

  describe "validation" do
    test "rejects partitions: 0" do
      assert_raise ArgumentError, ~r/partitions must be a positive integer/, fn ->
        PartitionedEts.start_link(
          name: :mp_v0,
          table_opts: [:named_table, :public],
          partitions: 0
        )
      end
    end

    test "rejects partitions: -1" do
      assert_raise ArgumentError, ~r/partitions must be a positive integer/, fn ->
        PartitionedEts.start_link(
          name: :mp_vneg,
          table_opts: [:named_table, :public],
          partitions: -1
        )
      end
    end

    test "rejects partitions: :foo" do
      assert_raise ArgumentError, ~r/partitions must be a positive integer/, fn ->
        PartitionedEts.start_link(
          name: :mp_vbad,
          table_opts: [:named_table, :public],
          partitions: :foo
        )
      end
    end
  end

  describe "partition table creation" do
    test "partitions: 1 creates a single ETS table named after the table" do
      table = unique_name()
      start_supervised!({PartitionedEts, name: table, table_opts: [:named_table, :public]})

      assert :ets.whereis(table) != :undefined
      assert :ets.whereis(:"#{table}_p0") == :undefined
    end

    test "partitions: 4 creates four ETS tables named with _p<i> suffixes" do
      table = unique_name()

      start_supervised!({PartitionedEts, name: table, table_opts: [:named_table, :public], partitions: 4})

      # Original name is NOT a real table when partitions > 1
      assert :ets.whereis(table) == :undefined

      for i <- 0..3 do
        assert :ets.whereis(:"#{table}_p#{i}") != :undefined
      end

      # No fifth partition
      assert :ets.whereis(:"#{table}_p4") == :undefined
    end

    test "terminating the owner removes all partitions and config" do
      table = unique_name()

      pid =
        start_supervised!({PartitionedEts, name: table, table_opts: [:named_table, :public], partitions: 4})

      assert :ets.whereis(:"#{table}_p0") != :undefined
      stop_supervised!({PartitionedEts, table})

      refute Process.alive?(pid)
      assert :ets.whereis(:"#{table}_p0") == :undefined
      assert :ets.whereis(:"#{table}_p3") == :undefined
    end
  end

  describe "routing across local partitions" do
    setup do
      table = unique_name()

      start_supervised!({PartitionedEts, name: table, table_opts: [:named_table, :public], partitions: 8})

      {:ok, table: table}
    end

    test "insert/lookup round-trips many keys", %{table: t} do
      keys = for i <- 1..200, do: {:"key_#{i}", i}
      Enum.each(keys, fn obj -> PartitionedEts.insert(t, obj) end)

      Enum.each(keys, fn {k, v} ->
        assert [{^k, ^v}] = PartitionedEts.lookup(t, k)
      end)
    end

    test "keys are spread across multiple partitions", %{table: t} do
      for i <- 1..200 do
        PartitionedEts.insert(t, {:"k_#{i}", i})
      end

      partition_sizes =
        for i <- 0..7 do
          :ets.info(:"#{t}_p#{i}", :size)
        end

      # All 8 partitions should have at least one entry with this many keys.
      assert Enum.all?(partition_sizes, &(&1 > 0)),
             "expected all partitions to be non-empty, got #{inspect(partition_sizes)}"

      assert Enum.sum(partition_sizes) == 200
    end

    test "delete removes from the correct partition", %{table: t} do
      PartitionedEts.insert(t, {:k, :v})
      assert PartitionedEts.member(t, :k)
      PartitionedEts.delete(t, :k)
      refute PartitionedEts.member(t, :k)
    end

    test "delete_all_objects clears every partition", %{table: t} do
      for i <- 1..50, do: PartitionedEts.insert(t, {:"k#{i}", i})
      assert PartitionedEts.delete_all_objects(t)

      for i <- 0..7 do
        assert :ets.info(:"#{t}_p#{i}", :size) == 0
      end
    end
  end

  describe "fan-out reads across local partitions" do
    setup do
      table = unique_name()

      start_supervised!({PartitionedEts, name: table, table_opts: [:named_table, :public], partitions: 4})

      for i <- 1..20, do: PartitionedEts.insert(table, {:"k#{i}", i})
      {:ok, table: table}
    end

    test "match returns rows from every partition", %{table: t} do
      results = PartitionedEts.match(t, {:"$1", :"$2"})
      assert length(results) == 20
    end

    test "select returns rows from every partition", %{table: t} do
      results = PartitionedEts.select(t, [{{:"$1", :"$2"}, [], [:"$$"]}])
      assert length(results) == 20
    end

    test "select_count sums across partitions", %{table: t} do
      assert 20 == PartitionedEts.select_count(t, [{{:"$1", :"$2"}, [], [true]}])
    end

    test "tab2list concatenates across partitions", %{table: t} do
      list = PartitionedEts.tab2list(t)
      assert length(list) == 20
    end

    test "foldl visits every entry", %{table: t} do
      sum = PartitionedEts.foldl(t, fn {_, n}, acc -> n + acc end, 0)
      assert sum == Enum.sum(1..20)
    end
  end

  describe "paginated scans across local partitions" do
    setup do
      table = unique_name()

      start_supervised!({PartitionedEts, name: table, table_opts: [:named_table, :public], partitions: 4})

      for i <- 1..10, do: PartitionedEts.insert(table, {:"k#{i}", i})
      {:ok, table: table}
    end

    test "match with limit returns at most `limit` results then a continuation",
         %{table: t} do
      assert {first_batch, cont} = PartitionedEts.match(t, {:"$1", :"$2"}, 3)
      assert length(first_batch) == 3

      assert {second_batch, cont2} = PartitionedEts.match(t, cont)
      assert length(second_batch) == 3

      # Keep paginating until exhausted, accumulate everything
      all = collect_paginated(first_batch ++ second_batch, cont2, &PartitionedEts.match(t, &1))
      assert length(all) == 10
    end

    test "select with limit terminates at end-of-table", %{table: t} do
      {batch, cont} = PartitionedEts.select(t, [{{:"$1", :"$2"}, [], [:"$$"]}], 100)
      assert length(batch) == 10

      assert :"$end_of_table" == PartitionedEts.select(t, cont)
    end

    test "select on empty table returns end-of-table immediately", %{table: t} do
      PartitionedEts.delete_all_objects(t)
      assert :"$end_of_table" == PartitionedEts.select(t, [{{:"$1", :"$2"}, [], [:"$$"]}], 5)
    end
  end

  describe "use macro with :partitions option" do
    defmodule SixteenPartitionTable do
      @moduledoc false
      use PartitionedEts, table_opts: [:named_table, :public], partitions: 16
    end

    test "the macro propagates :partitions to the underlying GenServer" do
      start_supervised!(SixteenPartitionTable)

      for i <- 0..15 do
        assert :ets.whereis(:"Elixir.PartitionedEts.MultiPartitionTest.SixteenPartitionTable_p#{i}") != :undefined
      end
    end

    test "macro-form insert/lookup routes through the partition layer" do
      start_supervised!(SixteenPartitionTable)
      assert SixteenPartitionTable.insert({:k, :v})
      assert [{:k, :v}] == SixteenPartitionTable.lookup(:k)
    end
  end

  defp unique_name do
    :"mptest_#{:erlang.unique_integer([:positive])}"
  end

  defp collect_paginated(acc, :"$end_of_table", _resume), do: acc

  defp collect_paginated(acc, cont, resume) do
    case resume.(cont) do
      :"$end_of_table" -> acc
      {batch, next_cont} -> collect_paginated(acc ++ batch, next_cont, resume)
    end
  end
end
