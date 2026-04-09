defmodule PartitionedEts.HandoffTest do
  @moduledoc """
  End-to-end handoff tests using a `:peer` cluster.

  These tests verify the two halves of Phase 5's rebalancing work:

    * **Join handoff**: data inserted before a new node joins the
      cluster remains reachable after the join, because every
      existing node iterates its local entries and ships the keys
      whose new HRW shard lives on the joiner.

    * **Leave handoff**: data on a node that is shut down gracefully
      is reachable from the surviving nodes, because `terminate/2`
      on the leaving node ships every local entry to its new owner
      before the ETS tables are destroyed.

  Synchronization between the test process and the cluster is done
  via `:sys.get_state/2` on each PartitionedEts GenServer pid. A
  GenServer processes messages in FIFO order, so a `:sys.get_state`
  call that lands *after* a `:pg` `:join` event in the mailbox is
  guaranteed to see the handoff completed — handoff runs inline in
  `handle_info`.
  """

  use ExUnit.Case

  alias PartitionedEts.Cluster

  defmodule HandoffTable do
    @moduledoc false

    use PartitionedEts, table_opts: [:named_table, :public]

    @after_compile {Cluster, :inject}
  end

  setup_all do
    Cluster.spawn([:"handoff_n1@127.0.0.1", :"handoff_n2@127.0.0.1"])

    Enum.each(Node.list(), fn node ->
      Cluster.start(node, [HandoffTable])
    end)

    :ok
  end

  setup do
    start_supervised!(HandoffTable)
    on_exit(fn -> HandoffTable.delete_all_objects() end)
    :ok
  end

  # ──────────────────────────────────────────────────────────────────────

  test "join handoff: inserted data is reachable after a new node joins" do
    # Fill the 3-node cluster (test node + node1 + node2) with data.
    keys = for i <- 1..200, do: :"handoff_k#{i}"
    Enum.each(keys, fn k -> HandoffTable.insert({k, :v}) end)

    assert Enum.all?(keys, fn k -> HandoffTable.lookup(k) == [{k, :v}] end),
           "data not reachable before handoff"

    # Spawn a 4th node and start the table on it.
    new_node = :"handoff_join_#{:erlang.unique_integer([:positive])}@127.0.0.1"
    [{:ok, peer_pid, ^new_node}] = Cluster.spawn([new_node])

    try do
      Cluster.start(new_node, [HandoffTable])

      wait_for_node_in_pg(HandoffTable, new_node)
      sync_all_genservers(HandoffTable)

      # Every key should still be reachable after the join handoff.
      for k <- keys do
        assert HandoffTable.lookup(k) == [{k, :v}],
               "key #{inspect(k)} lost after join handoff"
      end

      # And the new node should actually own a meaningful share of
      # the data — with 4 shards of 1 partition each, HRW puts
      # ~200/4 = 50 keys on the new node.
      new_node_size = :erpc.call(new_node, :ets, :info, [HandoffTable, :size])

      assert new_node_size > 20,
             "new node has only #{new_node_size} keys; expected ~50"

      assert new_node_size < 100,
             "new node has #{new_node_size} keys; HRW shouldn't have moved >50%"
    after
      :peer.stop(peer_pid)
    end
  end

  test "leave handoff: data on a gracefully-stopped node is reachable afterward" do
    leave_node = :"handoff_leave_#{:erlang.unique_integer([:positive])}@127.0.0.1"
    [{:ok, peer_pid, ^leave_node}] = Cluster.spawn([leave_node])

    try do
      # Capture the supervisor pid so we can stop the whole subtree
      # gracefully later — stopping just the child would trigger an
      # automatic restart (the default :permanent restart spec).
      {:ok, leave_sup_pid} = Cluster.start(leave_node, [HandoffTable])

      wait_for_node_in_pg(HandoffTable, leave_node)
      sync_all_genservers(HandoffTable)

      # Insert data — some will hash to leave_node under HRW.
      keys = for i <- 1..200, do: :"leave_k#{i}"
      Enum.each(keys, fn k -> HandoffTable.insert({k, :v}) end)

      assert Enum.all?(keys, fn k -> HandoffTable.lookup(k) == [{k, :v}] end)

      # Confirm leave_node actually owns some data before we stop it.
      pre_size = :erpc.call(leave_node, :ets, :info, [HandoffTable, :size])
      assert pre_size > 20, "leave_node has only #{pre_size} keys before leave"

      # Gracefully stop the supervisor on leave_node. This propagates
      # `:shutdown` to every child including PartitionedEts, so
      # `terminate/2` runs with reason `:shutdown` and triggers
      # `do_leave_handoff`.
      :ok = :erpc.call(leave_node, Supervisor, :stop, [leave_sup_pid, :shutdown])

      wait_for_node_not_in_pg(HandoffTable, leave_node)
      sync_all_genservers(HandoffTable)

      # Every key — including the ones that had lived on leave_node
      # — should now be reachable from the remaining nodes.
      for k <- keys do
        assert HandoffTable.lookup(k) == [{k, :v}],
               "key #{inspect(k)} lost after graceful leave of #{inspect(leave_node)}"
      end
    after
      :peer.stop(peer_pid)
    end
  end

  test "hard stop of a peer does NOT transfer data (documented limitation)" do
    # This test locks in the Phase 5 limitation: hard VM crashes
    # (:peer.stop, which kills the VM abruptly) bypass terminate/2
    # entirely. Data on the crashed node is lost. If this test ever
    # starts passing with "data reachable", it means we added
    # replication or some form of crash-time handoff — worth
    # revisiting the docs at that point.
    crash_node = :"handoff_crash_#{:erlang.unique_integer([:positive])}@127.0.0.1"
    [{:ok, peer_pid, ^crash_node}] = Cluster.spawn([crash_node])

    Cluster.start(crash_node, [HandoffTable])
    wait_for_node_in_pg(HandoffTable, crash_node)
    sync_all_genservers(HandoffTable)

    # Keep re-inserting until at least one key lands on the crash
    # node — we can't predict HRW placement without recomputing it,
    # and flakiness on "oh no, none of the 10 random keys ended up
    # on the right shard" isn't useful.
    key = find_key_on_node(HandoffTable, crash_node)
    HandoffTable.insert({key, :value})
    assert [{^key, :value}] = HandoffTable.lookup(key)

    pre_size = :erpc.call(crash_node, :ets, :info, [HandoffTable, :size])
    assert pre_size >= 1

    # Hard-stop the peer: the VM dies immediately, no terminate.
    :peer.stop(peer_pid)
    wait_for_node_not_in_pg(HandoffTable, crash_node)
    sync_all_genservers(HandoffTable)

    # The key that lived on the crash node should now be unreachable.
    assert HandoffTable.lookup(key) == [],
           "key unexpectedly survived a hard crash — did we add replication?"
  end

  # ──────────────────────────────────────────────────────────────────────

  defp wait_for_node_in_pg(table, node, timeout \\ 5_000) do
    # Every node in the cluster must see `node` as a member. Polling
    # only the local node's :pg state isn't enough — :pg is gossiped
    # and other nodes may lag behind briefly.
    wait_until(timeout, fn ->
      Enum.all?(all_known_nodes(node), fn n ->
        members = :erpc.call(n, :pg, :get_members, [PartitionedEts.Registry, table])
        node in Enum.map(members, &:erlang.node/1)
      end)
    end)
  end

  defp wait_for_node_not_in_pg(table, node, timeout \\ 5_000) do
    wait_until(timeout, fn ->
      Enum.all?([Node.self() | Node.list()], fn n ->
        members =
          try do
            :erpc.call(n, :pg, :get_members, [PartitionedEts.Registry, table])
          catch
            # If the node we're asking about is already gone, that's
            # obviously "not in :pg".
            :error, _ -> []
          end

        node not in Enum.map(members, &:erlang.node/1)
      end)
    end)
  end

  # Includes `extra` in case it's the newly-joined node we want to
  # probe — Node.list() might not see it yet at the instant we start
  # polling.
  defp all_known_nodes(extra) do
    [Node.self() | Node.list()]
    |> Enum.uniq()
    |> Enum.concat([extra])
    |> Enum.uniq()
  end

  defp wait_until(timeout, fun) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until(deadline, fun)
  end

  defp do_wait_until(deadline, fun) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("wait_until/2 deadline exceeded")

      true ->
        Process.sleep(20)
        do_wait_until(deadline, fun)
    end
  end

  # Block until every :pg-registered PartitionedEts GenServer has
  # drained its mailbox up to the current moment. Since handoff runs
  # inline in handle_info, a :sys.get_state call that lands in the
  # mailbox *after* a :pg :join event is guaranteed to return only
  # after the join's handoff is complete.
  defp sync_all_genservers(table) do
    for pid <- :pg.get_members(PartitionedEts.Registry, table) do
      try do
        :sys.get_state(pid, 5_000)
      catch
        :exit, _ -> :ok
      end
    end
  end

  defp find_key_on_node(table, target_node) do
    Enum.find_value(1..1000, fn i ->
      k = :"find_k#{i}"

      if key_owning_node(table, k) == target_node do
        k
      end
    end) || flunk("no key hashes to #{inspect(target_node)} in 1000 attempts")
  end

  defp key_owning_node(table, key) do
    for_result =
      for pid <- :pg.get_members(PartitionedEts.Registry, table),
          node <- [:erlang.node(pid)],
          into: [] do
        {node, table}
      end

    shards =
      for_result
      |> Enum.uniq()
      |> Enum.sort()

    {node, _} = PartitionedEts.hash(key, shards)
    node
  end
end
