defmodule PartitionedEts.LocalOnlyTest do
  @moduledoc """
  Tests for `distributed: false` mode — partitioned ETS tables that
  run on a single node without cluster registration or handoff.
  """

  use ExUnit.Case, async: true

  defmodule MacroTable do
    @moduledoc false
    use PartitionedEts, table_opts: [:named_table, :public], partitions: 4, distributed: false
  end

  test "use macro with distributed: false" do
    {:ok, _} = MacroTable.start_link()

    MacroTable.insert({:key, :value})
    assert [{:key, :value}] = MacroTable.lookup(:key)

    for i <- 1..50, do: MacroTable.insert({:"k_#{i}", i})
    results = MacroTable.match({:"$1", :"$2"})
    assert length(results) == 51

    assert PgRegistry.lookup(PartitionedEts.Registry, MacroTable) == []
  end

  test "basic insert and lookup" do
    {:ok, _} =
      PartitionedEts.start_link(
        name: :"local_basic_#{:erlang.unique_integer([:positive])}",
        table_opts: [:named_table, :public],
        distributed: false
      )

    table = :"local_basic_#{:erlang.unique_integer([:positive])}"

    {:ok, _} =
      PartitionedEts.start_link(
        name: table,
        table_opts: [:named_table, :public],
        distributed: false
      )

    assert PartitionedEts.insert(table, {:key, :value})
    assert [{:key, :value}] = PartitionedEts.lookup(table, :key)
  end

  test "multiple partitions work locally" do
    table = :"local_parts_#{:erlang.unique_integer([:positive])}"

    {:ok, _} =
      PartitionedEts.start_link(
        name: table,
        table_opts: [:named_table, :public],
        partitions: 4,
        distributed: false
      )

    for i <- 1..100 do
      PartitionedEts.insert(table, {:"key_#{i}", i})
    end

    for i <- 1..100 do
      key = :"key_#{i}"
      assert [{^key, ^i}] = PartitionedEts.lookup(table, key)
    end

    assert PartitionedEts.select_count(table, [{{:_, :_}, [], [true]}]) == 100
  end

  test "does not register with PgRegistry" do
    table = :"local_noreg_#{:erlang.unique_integer([:positive])}"

    {:ok, _} =
      PartitionedEts.start_link(
        name: table,
        table_opts: [:named_table, :public],
        distributed: false
      )

    assert PgRegistry.lookup(PartitionedEts.Registry, table) == []
  end

  test "fan-out operations work" do
    table = :"local_fanout_#{:erlang.unique_integer([:positive])}"

    {:ok, _} =
      PartitionedEts.start_link(
        name: table,
        table_opts: [:named_table, :public],
        partitions: 4,
        distributed: false
      )

    for i <- 1..50, do: PartitionedEts.insert(table, {:"k_#{i}", i})

    assert PartitionedEts.select_count(table, [{{:_, :_}, [], [true]}]) == 50
    results = PartitionedEts.match(table, {:"$1", :"$2"})
    assert length(results) == 50

    PartitionedEts.delete_all_objects(table)
    assert PartitionedEts.select_count(table, [{{:_, :_}, [], [true]}]) == 0
  end

  test "shutdown does not attempt handoff" do
    Process.flag(:trap_exit, true)

    table = :"local_shutdown_#{:erlang.unique_integer([:positive])}"

    {:ok, pid} =
      PartitionedEts.start_link(
        name: table,
        table_opts: [:named_table, :public],
        distributed: false
      )

    Process.unlink(pid)
    PartitionedEts.insert(table, {:key, :value})

    # Should shut down cleanly without any handoff or :pg errors
    ref = Process.monitor(pid)
    Process.exit(pid, :shutdown)
    assert_receive {:DOWN, ^ref, :process, ^pid, :shutdown}, 5000
  end
end
