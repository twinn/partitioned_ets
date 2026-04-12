defmodule PartitionedEts.HandoffFingerprintTest do
  @moduledoc """
  Tests the fingerprint-based conditional update used during leave handoff.

  The two-pass leave handoff works as follows:

    1. Pass 1: ship all entries via `insert`, record fingerprints on the
       destination (still registered, no read misses).
    2. Leave :pg, wait for monitor confirmation.
    3. Pass 2: for each local entry, conditionally update the destination —
       only overwrite if the destination's current value still matches the
       fingerprint from pass 1 (meaning no direct write landed).

  These tests exercise the conditional update function directly to verify
  the conflict resolution logic.
  """

  use ExUnit.Case, async: true

  alias PartitionedEts.Handoff

  describe "conditional_insert/3" do
    setup do
      data_table = :ets.new(:data, [:set, :public])
      fp_table = :ets.new(:fingerprints, [:set, :public])
      %{data: data_table, fp: fp_table}
    end

    test "overwrites when destination value matches fingerprint (stale from pass 1)", %{
      data: data,
      fp: fp
    } do
      # Pass 1 shipped {k, :v1} and recorded its fingerprint.
      :ets.insert(data, {:k, :v1})
      :ets.insert(fp, {:k, :erlang.phash2({:k, :v1})})

      # Pass 2: local value was updated to :v2 during pass 1.
      # Destination still has :v1 (fingerprint matches), so overwrite.
      assert Handoff.conditional_insert(data, fp, {:k, :v2}) == true
      assert :ets.lookup(data, :k) == [{:k, :v2}]
    end

    test "skips when destination value differs from fingerprint (direct write landed)", %{
      data: data,
      fp: fp
    } do
      # Pass 1 shipped {k, :v1} and recorded its fingerprint.
      :ets.insert(fp, {:k, :erlang.phash2({:k, :v1})})

      # After leaving :pg, a direct write updated k to :v3.
      :ets.insert(data, {:k, :v3})

      # Pass 2: tries to ship :v2, but fingerprint doesn't match :v3.
      # The direct write is newer — keep it.
      assert Handoff.conditional_insert(data, fp, {:k, :v2}) == false
      assert :ets.lookup(data, :k) == [{:k, :v3}]
    end

    test "inserts when key has no fingerprint (new entry, not in pass 1)", %{
      data: data,
      fp: fp
    } do
      # Key didn't exist during pass 1, so no fingerprint.
      # Pass 2 should insert it.
      assert Handoff.conditional_insert(data, fp, {:k, :new}) == true
      assert :ets.lookup(data, :k) == [{:k, :new}]
    end

    test "inserts when key doesn't exist on destination at all", %{data: data, fp: fp} do
      # Fingerprint exists from pass 1, but destination entry was deleted
      # (shouldn't happen normally, but defensive).
      :ets.insert(fp, {:k, :erlang.phash2({:k, :v1})})

      assert Handoff.conditional_insert(data, fp, {:k, :v2}) == true
      assert :ets.lookup(data, :k) == [{:k, :v2}]
    end

    test "works with multi-element tuples", %{data: data, fp: fp} do
      tuple = {:user, 42, "alice", :active}
      :ets.insert(data, tuple)
      :ets.insert(fp, {:user, :erlang.phash2(tuple)})

      # Same fingerprint — overwrite with updated tuple.
      updated = {:user, 42, "alice", :inactive}
      assert Handoff.conditional_insert(data, fp, updated) == true
      assert :ets.lookup(data, :user) == [updated]
    end

    test "preserves direct write for multi-element tuples", %{data: data, fp: fp} do
      original = {:user, 42, "alice", :active}
      :ets.insert(fp, {:user, :erlang.phash2(original)})

      # Direct write changed the value.
      direct = {:user, 42, "bob", :active}
      :ets.insert(data, direct)

      # Pass 2 tries to ship a different update — should be skipped.
      stale = {:user, 42, "alice", :inactive}
      assert Handoff.conditional_insert(data, fp, stale) == false
      assert :ets.lookup(data, :user) == [direct]
    end
  end
end
