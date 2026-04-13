defmodule PartitionedEts.Handoff do
  @moduledoc false

  # Clock-mode handoff helpers.
  #
  # During leave handoff, the leaving node enters "clock mode" — a
  # monotonic `:atomics` counter stamps every write. This allows pass 2
  # to identify entries that changed during pass 1 and ship only those.
  #
  # On the destination, a metadata table `{key, fingerprint, clock}`
  # tracks every entry that arrived (via pass 1, pass 2, or direct
  # writes). Conflict resolution uses the clock for `:set` tables and
  # the fingerprint for `:bag`/`:duplicate_bag` tables.

  # ── Source-side: clock mode ──────────────────────────────────────────

  @doc """
  Records a write in the shadow table with the next clock value.
  Called from `partitioned_call` when clock mode is active.
  """
  @spec record_write(:ets.table(), :atomics.atomics_ref(), term()) :: :ok
  def record_write(shadow_table, clock_ref, key) do
    tick = :atomics.add_get(clock_ref, 1, 1)
    :ets.insert(shadow_table, {key, tick})
    :ok
  end

  @doc """
  Returns entries from the shadow table with clock > `since`.
  These are the keys that changed after the given checkpoint.
  """
  @spec changed_since(:ets.table(), non_neg_integer()) :: [{term(), non_neg_integer()}]
  def changed_since(shadow_table, since) do
    # Match spec: select entries where clock > since
    :ets.select(shadow_table, [{{:"$1", :"$2"}, [{:>, :"$2", since}], [{{:"$1", :"$2"}}]}])
  end

  # ── Destination-side: clock-based conflict resolution ────────────────
  #
  # The metadata table is a :set keyed by `{key, fingerprint}`.
  # Each entry stores `{{key, fingerprint}, clock}`.
  #
  # For :set tables: a new value for the same key has a different
  # fingerprint. We check if any entry for this key has a higher clock.
  #
  # For :bag/:duplicate_bag: if the exact fingerprint exists, the entry
  # was already shipped — skip to avoid duplicates.

  @doc """
  Inserts `obj` into `data_table` using clock-based conflict resolution.

  Returns `true` if the insert was performed, `false` if skipped.

  The metadata table tracks `{{key, phash2(entry)}, clock}` for every
  entry that has been shipped or written during the handoff window.

  For `:set` tables:
    * If no metadata for this key exists → insert
    * If the incoming clock >= the highest clock for this key → insert
    * If the incoming clock < the highest clock → skip (newer write exists)

  For `:bag`/`:duplicate_bag` tables:
    * If the exact `{key, fingerprint}` exists in metadata → skip (duplicate)
    * Otherwise → insert
  """
  @spec clock_insert(:ets.table(), :ets.table(), tuple(), non_neg_integer()) :: boolean()
  def clock_insert(data_table, meta_table, obj, clock) do
    key = elem(obj, 0)
    fp = :erlang.phash2(obj)
    meta_key = {key, fp}

    case :ets.info(data_table, :type) do
      type when type in [:bag, :duplicate_bag] ->
        bag_insert(data_table, meta_table, obj, meta_key, clock)

      _set_or_ordered_set ->
        set_insert(data_table, meta_table, obj, key, meta_key, clock)
    end
  end

  defp bag_insert(data_table, meta_table, obj, meta_key, clock) do
    case :ets.lookup(meta_table, meta_key) do
      [] ->
        # New entry — insert and track
        :ets.insert(data_table, obj)
        :ets.insert(meta_table, {meta_key, clock})
        true

      [{^meta_key, _existing_clock}] ->
        # Exact entry already exists — skip to avoid duplicates
        false
    end
  end

  defp set_insert(data_table, meta_table, obj, key, meta_key, clock) do
    # Find the highest clock for any entry with this key.
    # Match spec: select clock values where the key component of the
    # compound key matches.
    highest =
      meta_table
      |> :ets.select([
        {{{key, :_}, :"$1"}, [], [:"$1"]}
      ])
      |> Enum.max(fn -> -1 end)

    if clock >= highest do
      :ets.insert(data_table, obj)
      # Remove old metadata for this key, insert new
      :ets.match_delete(meta_table, {{key, :_}, :_})
      :ets.insert(meta_table, {meta_key, clock})
      true
    else
      false
    end
  end
end
