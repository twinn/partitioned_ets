defmodule PartitionedEts.HandoffClockTest do
  @moduledoc """
  Tests for clock-mode handoff. During leave handoff, the leaving node
  enters clock mode — every write is stamped with a monotonic counter.
  Pass 2 only ships entries that changed during pass 1, using clock
  values for conflict resolution on the destination.
  """

  use ExUnit.Case, async: true

  alias PartitionedEts.Handoff

  describe "clock mode on source" do
    test "record_write/3 stamps entries in the shadow table" do
      clock = :atomics.new(1, [])
      shadow = :ets.new(:shadow, [:set, :public])

      Handoff.record_write(shadow, clock, :key1)
      Handoff.record_write(shadow, clock, :key2)
      Handoff.record_write(shadow, clock, :key1)

      # key1 was written twice — should have the latest clock
      [{:key1, c1}] = :ets.lookup(shadow, :key1)
      [{:key2, c2}] = :ets.lookup(shadow, :key2)

      assert c1 > c2, "key1's second write should have a higher clock"
      assert c2 > 0
    end

    test "only writes after a checkpoint have clock values above it" do
      clock = :atomics.new(1, [])
      shadow = :ets.new(:shadow, [:set, :public])

      Handoff.record_write(shadow, clock, :before1)
      Handoff.record_write(shadow, clock, :before2)

      checkpoint = :atomics.get(clock, 1)

      Handoff.record_write(shadow, clock, :after1)
      Handoff.record_write(shadow, clock, :after2)
      # Update a key that was written before the checkpoint
      Handoff.record_write(shadow, clock, :before1)

      # Verify clock values directly from the shadow table
      [{:before1, c_before1}] = :ets.lookup(shadow, :before1)
      [{:before2, c_before2}] = :ets.lookup(shadow, :before2)
      [{:after1, c_after1}] = :ets.lookup(shadow, :after1)
      [{:after2, c_after2}] = :ets.lookup(shadow, :after2)

      assert c_after1 > checkpoint
      assert c_after2 > checkpoint
      assert c_before1 > checkpoint, "updated key should have a clock above checkpoint"
      assert c_before2 <= checkpoint
    end
  end

  describe "destination conflict resolution" do
    setup do
      data = :ets.new(:data, [:set, :public])
      meta = :ets.new(:meta, [:set, :public])
      %{data: data, meta: meta}
    end

    test "accepts entry when no metadata exists (new key)", %{data: data, meta: meta} do
      assert Handoff.clock_insert(data, meta, {:k, :v1}, 5) == true
      assert :ets.lookup(data, :k) == [{:k, :v1}]
      assert :ets.lookup(meta, {:k, :erlang.phash2({:k, :v1})}) == [{{:k, :erlang.phash2({:k, :v1})}, 5}]
    end

    test "accepts entry with higher clock than existing", %{data: data, meta: meta} do
      # Pass 1 shipped v1 at clock 5
      :ets.insert(data, {:k, :v1})
      :ets.insert(meta, {{:k, :erlang.phash2({:k, :v1})}, 5})

      # Pass 2 ships v2 at clock 8 — should win
      assert Handoff.clock_insert(data, meta, {:k, :v2}, 8) == true
      assert :ets.lookup(data, :k) == [{:k, :v2}]
    end

    test "rejects entry with lower clock than existing", %{data: data, meta: meta} do
      # Direct write at clock 10
      :ets.insert(data, {:k, :v3})
      :ets.insert(meta, {{:k, :erlang.phash2({:k, :v3})}, 10})

      # Pass 2 ships v2 at clock 7 — should lose
      assert Handoff.clock_insert(data, meta, {:k, :v2}, 7) == false
      assert :ets.lookup(data, :k) == [{:k, :v3}]
    end

    test "accepts entry with equal clock (tiebreak: accept)", %{data: data, meta: meta} do
      :ets.insert(data, {:k, :v1})
      :ets.insert(meta, {{:k, :erlang.phash2({:k, :v1})}, 5})

      # Same clock — accept (leaving node is authoritative for ties)
      assert Handoff.clock_insert(data, meta, {:k, :v2}, 5) == true
      assert :ets.lookup(data, :k) == [{:k, :v2}]
    end
  end

  describe "bag table conflict resolution" do
    setup do
      data = :ets.new(:data, [:bag, :public])
      meta = :ets.new(:meta, [:set, :public])
      %{data: data, meta: meta}
    end

    test "adds new entry for existing key", %{data: data, meta: meta} do
      :ets.insert(data, {:k, :v1})
      :ets.insert(meta, {{:k, :erlang.phash2({:k, :v1})}, 5})

      # Different value for same key — should be added (bag semantics)
      assert Handoff.clock_insert(data, meta, {:k, :v2}, 6) == true
      assert length(:ets.lookup(data, :k)) == 2
    end

    test "skips duplicate entry (same fingerprint exists)", %{data: data, meta: meta} do
      :ets.insert(data, {:k, :v1})
      :ets.insert(meta, {{:k, :erlang.phash2({:k, :v1})}, 5})

      # Same value, same key — already exists, skip
      assert Handoff.clock_insert(data, meta, {:k, :v1}, 6) == false
      assert :ets.lookup(data, :k) == [{:k, :v1}]
    end
  end

  describe "duplicate_bag table conflict resolution" do
    setup do
      data = :ets.new(:data, [:duplicate_bag, :public])
      meta = :ets.new(:meta, [:set, :public])
      %{data: data, meta: meta}
    end

    test "skips entry whose fingerprint is already tracked", %{data: data, meta: meta} do
      :ets.insert(data, {:k, :v1})
      :ets.insert(meta, {{:k, :erlang.phash2({:k, :v1})}, 5})

      # Fingerprint exists — already shipped, don't duplicate
      assert Handoff.clock_insert(data, meta, {:k, :v1}, 6) == false
      assert :ets.lookup(data, :k) == [{:k, :v1}]
    end

    test "adds entry with new fingerprint", %{data: data, meta: meta} do
      :ets.insert(data, {:k, :v1})
      :ets.insert(meta, {{:k, :erlang.phash2({:k, :v1})}, 5})

      assert Handoff.clock_insert(data, meta, {:k, :v2}, 6) == true
      assert length(:ets.lookup(data, :k)) == 2
    end
  end

  describe "multi-element tuples" do
    setup do
      data = :ets.new(:data, [:set, :public])
      meta = :ets.new(:meta, [:set, :public])
      %{data: data, meta: meta}
    end

    test "works with wide tuples", %{data: data, meta: meta} do
      entry = {:user, 42, "alice", :active, %{role: :admin}}
      :ets.insert(data, entry)
      :ets.insert(meta, {{:user, :erlang.phash2(entry)}, 5})

      updated = {:user, 42, "alice", :inactive, %{role: :admin}}
      assert Handoff.clock_insert(data, meta, updated, 8) == true
      assert :ets.lookup(data, :user) == [updated]
    end
  end
end
