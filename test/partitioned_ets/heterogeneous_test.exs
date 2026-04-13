defmodule PartitionedEts.HeterogeneousTest do
  @moduledoc """
  Tests that nodes with different partition counts can coexist in the
  same cluster. PgRegistry metadata carries each node's partition
  table names, so shards/2 builds the correct shard set.
  """

  use ExUnit.Case

  alias PartitionedEts.Cluster

  @table :hetero_table

  setup_all do
    Cluster.spawn([:"hetero_n1@127.0.0.1"])
    :ok
  end

  setup do
    # Start with 2 partitions locally
    {:ok, local_pid} =
      PartitionedEts.start_link(
        name: @table,
        table_opts: [:named_table, :public],
        partitions: 2
      )

    # Start with 4 partitions on the peer
    peer_node = :"hetero_n1@127.0.0.1"

    {:ok, _remote_pid} =
      :rpc.block_call(peer_node, PartitionedEts, :start_link, [
        [name: @table, table_opts: [:named_table, :public], partitions: 4]
      ])

    # Wait for PgRegistry gossip
    wait_for_members(2)

    on_exit(fn ->
      try do
        GenServer.stop(local_pid)
      catch
        :exit, _ -> :ok
      end

      try do
        remote_pid = :erpc.call(peer_node, PgRegistry, :whereis_name, [{PartitionedEts.Registry, @table}])
        if is_pid(remote_pid), do: :erpc.call(peer_node, GenServer, :stop, [remote_pid])
      catch
        _, _ -> :ok
      end

      for node <- [node(), peer_node] do
        for pt <- partition_tables(node) do
          try do
            :erpc.call(node, :ets, :delete_all_objects, [pt])
          catch
            _, _ -> :ok
          end
        end
      end
    end)

    %{peer_node: peer_node}
  end

  test "nodes with different partition counts can insert and lookup" do
    for i <- 1..100 do
      PartitionedEts.insert(@table, {:"key_#{i}", i})
    end

    for i <- 1..100 do
      key = :"key_#{i}"
      assert [{^key, ^i}] = PartitionedEts.lookup(@table, key)
    end
  end

  test "shards include both nodes' partition tables" do
    members = PgRegistry.lookup(PartitionedEts.Registry, @table)

    local_member = Enum.find(members, fn {pid, _} -> :erlang.node(pid) == node() end)
    remote_member = Enum.find(members, fn {pid, _} -> :erlang.node(pid) != node() end)

    {_, %{partition_tables: local_pts}} = local_member
    {_, %{partition_tables: remote_pts}} = remote_member

    assert length(local_pts) == 2
    assert length(remote_pts) == 4
  end

  test "fan-out operations span both nodes" do
    for i <- 1..100, do: PartitionedEts.insert(@table, {:"k_#{i}", i})

    results = PartitionedEts.match(@table, {:"$1", :"$2"})
    assert length(results) == 100

    PartitionedEts.delete_all_objects(@table)
    assert PartitionedEts.match(@table, {:"$1", :"$2"}) == []
  end

  test "data is distributed across both nodes", %{peer_node: peer_node} do
    for i <- 1..200, do: PartitionedEts.insert(@table, {:"dist_#{i}", i})

    local_size =
      node()
      |> partition_tables()
      |> Enum.map(&:ets.info(&1, :size))
      |> Enum.sum()

    remote_size =
      peer_node
      |> partition_tables()
      |> Enum.map(fn pt ->
        :erpc.call(peer_node, :ets, :info, [pt, :size])
      end)
      |> Enum.sum()

    assert local_size + remote_size == 200
    # Both nodes should have a meaningful share
    assert local_size > 20
    assert remote_size > 20
  end

  defp wait_for_members(expected, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_members(expected, deadline)
  end

  defp do_wait_members(expected, deadline) do
    members = PgRegistry.lookup(PartitionedEts.Registry, @table)

    cond do
      length(members) >= expected ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("timed out waiting for #{expected} members")

      true ->
        Process.sleep(20)
        do_wait_members(expected, deadline)
    end
  end

  defp partition_tables(target_node) do
    members = PgRegistry.lookup(PartitionedEts.Registry, @table)

    case Enum.find(members, fn {pid, _} -> :erlang.node(pid) == target_node end) do
      {_, %{partition_tables: pts}} -> pts
      _ -> []
    end
  end
end
