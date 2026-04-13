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
    * No fingerprint for this key → `insert_new` (missed by pass 1;
      only insert if no direct write landed)
    * Key missing from data_table → insert (entry was deleted)
    * Fingerprint matches current value → insert (still stale from pass 1)
    * Fingerprint differs from current value → skip (direct write landed)
  """
  @spec conditional_insert(:ets.table(), :ets.table(), tuple()) :: boolean()
  def conditional_insert(data_table, fp_table, obj) do
    key = elem(obj, 0)

    case action(data_table, fp_table, key) do
      :insert ->
        :ets.insert(data_table, obj)
        true

      :insert_new ->
        :ets.insert_new(data_table, obj)

      :skip ->
        false
    end
  end

  defp action(data_table, fp_table, key) do
    case :ets.lookup(fp_table, key) do
      [] ->
        # No fingerprint — pass 1 missed this key. Use insert_new so
        # we don't clobber a direct write that may have arrived at the
        # destination after we left :pg.
        :insert_new

      [{^key, fingerprint}] ->
        case :ets.lookup(data_table, key) do
          [] -> :insert
          [current] -> if :erlang.phash2(current) == fingerprint, do: :insert, else: :skip
        end
    end
  end
end
