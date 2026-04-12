defmodule PartitionedEts.Handoff do
  @moduledoc false

  # Helpers for the fingerprint-based conditional update used during
  # two-pass leave handoff.
  #
  # Pass 1 ships entries via `insert` and records a fingerprint
  # (phash2 of the full tuple) on the destination. Pass 2 calls
  # `conditional_insert` which only overwrites the destination value
  # if its fingerprint still matches (no direct write landed).

  @doc """
  Conditionally inserts `obj` into `data_table` using fingerprints
  from `fp_table` to detect direct writes.

  Returns `true` if the insert was performed, `false` if skipped.

  Rules:
    * No fingerprint for this key → insert (new entry, wasn't in pass 1)
    * Key missing from data_table → insert (entry was deleted)
    * Fingerprint matches current value → insert (still stale from pass 1)
    * Fingerprint differs from current value → skip (direct write landed)
  """
  @spec conditional_insert(:ets.table(), :ets.table(), tuple()) :: boolean()
  def conditional_insert(data_table, fp_table, obj) do
    key = elem(obj, 0)

    case :ets.lookup(fp_table, key) do
      [] ->
        # No fingerprint — new entry that wasn't in pass 1.
        :ets.insert(data_table, obj)
        true

      [{^key, fingerprint}] ->
        case :ets.lookup(data_table, key) do
          [] ->
            # Entry was deleted from destination — safe to insert.
            :ets.insert(data_table, obj)
            true

          [current] ->
            if :erlang.phash2(current) == fingerprint do
              # Value hasn't changed since pass 1 — overwrite with our update.
              :ets.insert(data_table, obj)
              true
            else
              # A direct write changed the value — preserve it.
              false
            end
        end
    end
  end
end
