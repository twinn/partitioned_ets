defmodule PartitionedEts.HRWTest do
  @moduledoc """
  Tests for the rendezvous-hashing (HRW) routing layer introduced in
  Phase 4.

  These tests are pure-function tests against `PartitionedEts.hash/2`
  and don't need any cluster, ETS table, or peer setup. The point is
  to lock in the two properties that distinguish HRW from the
  modulo-N hashing the library used previously:

    1. **Determinism** — the same key + the same shard list always
       picks the same shard, regardless of order or call site.
    2. **Minimal disruption on membership change** — adding a shard
       remaps roughly `1/(N+1)` of keys; the rest stay put.
  """

  use ExUnit.Case, async: true

  defp shards_for(nodes, partitions) do
    for n <- nodes, i <- 0..(partitions - 1), do: {n, :"t_p#{i}"}
  end

  describe "determinism" do
    test "same key + same shards always returns the same shard" do
      shards = shards_for([:n1, :n2, :n3], 4)

      first = PartitionedEts.hash(:my_key, shards)

      for _ <- 1..100 do
        assert PartitionedEts.hash(:my_key, shards) == first
      end
    end

    test "shard order does not affect the result" do
      shards = shards_for([:n1, :n2, :n3], 4)
      shuffled = Enum.shuffle(shards)

      assert PartitionedEts.hash(:k1, shards) == PartitionedEts.hash(:k1, shuffled)
      assert PartitionedEts.hash(:k2, shards) == PartitionedEts.hash(:k2, shuffled)

      assert PartitionedEts.hash({:complex, :key, [1, 2, 3]}, shards) ==
               PartitionedEts.hash({:complex, :key, [1, 2, 3]}, shuffled)
    end

    test "result is one of the input shards" do
      shards = shards_for([:n1, :n2, :n3], 4)

      for i <- 1..100 do
        result = PartitionedEts.hash({:k, i}, shards)
        assert result in shards
      end
    end
  end

  describe "distribution" do
    test "1000 keys spread roughly evenly across 12 shards" do
      shards = shards_for([:n1, :n2, :n3], 4)
      counts = count_shards(1..1000, shards)

      assert map_size(counts) == 12
      avg = 1000 / 12

      # Every shard should be within 50% of the average — generous
      # bound for a small sample of phash2.
      for {shard, count} <- counts do
        assert count >= avg * 0.5,
               "shard #{inspect(shard)} got only #{count} of #{1000} keys (avg=#{avg})"

        assert count <= avg * 1.5,
               "shard #{inspect(shard)} got #{count} of #{1000} keys (avg=#{avg})"
      end
    end
  end

  describe "minimal disruption on shard set change" do
    test "adding a node remaps ~1/(N+1) of keys" do
      keys = for i <- 1..2000, do: {:k, i}

      old_shards = shards_for([:n1, :n2, :n3], 4)
      new_shards = shards_for([:n1, :n2, :n3, :n4], 4)

      moved = count_moved(keys, old_shards, new_shards)
      moved_pct = moved / length(keys) * 100

      # Adding a 4th node to a 3-node cluster (each with 4 partitions)
      # adds 4 new shards out of 16 total. HRW should remap ~4/16 = 25%
      # of keys. Allow a generous +/- 5pp window for sampling noise.
      assert_in_delta moved_pct, 25.0, 5.0
    end

    test "removing a node remaps only the keys that lived on it" do
      keys = for i <- 1..2000, do: {:k, i}

      old_shards = shards_for([:n1, :n2, :n3, :n4], 4)
      new_shards = shards_for([:n1, :n2, :n3], 4)

      # Identify which keys lived on the removed node BEFORE the change
      keys_on_removed =
        Enum.count(keys, fn k ->
          {n, _} = PartitionedEts.hash(k, old_shards)
          n == :n4
        end)

      moved = count_moved(keys, old_shards, new_shards)

      # All moved keys should be exactly the ones that lived on n4 —
      # no other key should change shard.
      assert moved == keys_on_removed
    end

    test "growing the cluster many times never bulk-remaps existing keys" do
      keys = for i <- 1..1000, do: {:k, i}

      # Start with 2 nodes, grow one node at a time to 6, measure
      # cumulative remapping. Each step should remap only its share.
      sizes = [2, 3, 4, 5, 6]

      [first | rest] = sizes
      shard_lists = Enum.map(sizes, fn n -> shards_for(node_names(n), 4) end)

      total_moves =
        Enum.reduce(Enum.zip(shard_lists, tl(shard_lists)), 0, fn {old, new}, acc ->
          acc + count_moved(keys, old, new)
        end)

      # The sum of incremental moves should be far less than the
      # `length(keys) * (length(sizes) - 1)` worst case (which is
      # what modulo hashing would give).
      worst_case = length(keys) * (length(sizes) - 1)

      assert total_moves < worst_case * 0.5,
             "total moves #{total_moves} is more than half of worst-case #{worst_case}"

      # Suppress unused-binding warnings
      _ = first
      _ = rest
    end
  end

  describe "user-overridable hash callback" do
    defmodule WeightedHash do
      @moduledoc false
      # Demonstrates that an override receives the shards list and
      # must return a member of it.
      def hash(_key, shards), do: hd(shards)
    end

    test "an override that always returns the first shard does so" do
      shards = shards_for([:a, :b, :c], 2)
      assert WeightedHash.hash(:anything, shards) == hd(shards)
    end
  end

  defp count_shards(keys, shards) do
    Enum.reduce(keys, %{}, fn k, acc ->
      shard = PartitionedEts.hash(k, shards)
      Map.update(acc, shard, 1, &(&1 + 1))
    end)
  end

  defp count_moved(keys, old_shards, new_shards) do
    Enum.count(keys, fn k ->
      PartitionedEts.hash(k, old_shards) != PartitionedEts.hash(k, new_shards)
    end)
  end

  defp node_names(n) do
    for i <- 1..n, do: :"n#{i}"
  end
end
