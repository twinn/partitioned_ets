defmodule PartitionedEts.Handoff do
  @moduledoc false

  # Handoff orchestration and conflict resolution.
  #
  # Join: existing nodes iterate local partitions, ship keys whose new
  # HRW shard lives on the joiner via insert_new, delete locally.
  #
  # Leave (two-pass with clock-based conflict resolution):
  #   1. Enter clock mode — stamp every write in a shadow table.
  #   2. Pass 1: ship all entries via insert while still registered.
  #      Record {fingerprint, clock} metadata on each destination.
  #   3. Leave PgRegistry, wait for local monitor confirmation.
  #   4. Pass 2: ship only entries that changed during pass 1.
  #      Destination uses clock-based resolution for :set tables
  #      and fingerprint-based dedup for :bag/:duplicate_bag.
  #   5. Exit clock mode, clean up metadata tables.
  #
  # All cross-node calls go through a versioned protocol
  # (receive_handoff/1) for rolling deploy compatibility.

  @handoff_version 1

  # ── Join handoff ─────────────────────────────────────────────────────

  def join(partition_tables, joiner_node, current_shards, hash_module) do
    Enum.each(partition_tables, fn local_pt ->
      handoff_partition_to_joiner(local_pt, joiner_node, current_shards, hash_module)
    end)
  end

  defp handoff_partition_to_joiner(local_pt, joiner_node, current_shards, hash_module) do
    to_move =
      :ets.foldl(
        &collect_if_owned_by(&1, &2, joiner_node, current_shards, hash_module),
        [],
        local_pt
      )

    Enum.each(to_move, fn {key, obj, target_pt} ->
      case ship_entry(joiner_node, target_pt, obj, :insert_new) do
        :ok -> :ets.delete(local_pt, key)
        {:error, _} -> :ok
      end
    end)
  end

  defp collect_if_owned_by(obj, acc, target_node, shards, hash_module) do
    key = elem(obj, 0)
    {owner_node, target_pt} = hash_module.hash(key, shards)

    if owner_node == target_node do
      [{key, obj, target_pt} | acc]
    else
      acc
    end
  end

  # ── Leave handoff ────────────────────────────────────────────────────

  def leave(name, monitor_ref, current_shards, cfg) do
    new_shards = Enum.reject(current_shards, fn {n, _} -> n == node() end)

    if new_shards != [] do
      hash_module = cfg.hash
      partition_tables = Tuple.to_list(cfg.partition_tables)

      # Enter clock mode
      shadow = :ets.new(cfg.shadow_name, [:set, :public, :named_table])
      pass1_clock = :atomics.get(cfg.clock_ref, 1)

      meta_tables = create_meta_tables(name, new_shards)

      # Pass 1: ship while still registered (no read misses)
      ship_pass1(partition_tables, new_shards, hash_module, meta_tables, pass1_clock)

      # Leave PgRegistry
      PgRegistry.unregister_name({PartitionedEts.Registry.scope(), name})

      # Wait for local monitor confirmation
      receive do
        {^monitor_ref, :leave, ^name, _pids} -> :ok
      after
        5_000 -> :ok
      end

      # Pass 2: ship entries that changed during pass 1
      ship_pass2(shadow, pass1_clock, partition_tables, new_shards, hash_module, meta_tables)

      # Exit clock mode and clean up
      :ets.delete(shadow)
      cleanup_meta_tables(meta_tables)
    end

    :ok
  end

  # ── Clock mode helpers (source side) ─────────────────────────────────

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

  @spec changed_since(:ets.table(), non_neg_integer()) :: [{term(), non_neg_integer()}]
  defp changed_since(shadow_table, since) do
    :ets.select(shadow_table, [{{:"$1", :"$2"}, [{:>, :"$2", since}], [{{:"$1", :"$2"}}]}])
  end

  # ── Conflict resolution (destination side) ───────────────────────────

  @doc """
  Inserts `obj` into `data_table` using clock-based conflict resolution.

  Returns `true` if the insert was performed, `false` if skipped.

  For `:set` tables: higher clock wins.
  For `:bag`/`:duplicate_bag` tables: fingerprint-based dedup.
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

  # ── Versioned protocol ──────────────────────────────────────────────

  @doc false
  def receive_handoff(%{version: 1, op: :insert, table: table, obj: obj}) do
    :ets.insert(table, obj)
    :ok
  end

  def receive_handoff(%{version: 1, op: :insert_new, table: table, obj: obj}) do
    :ets.insert_new(table, obj)
    :ok
  end

  def receive_handoff(%{version: 1, op: :clock_insert, table: table, meta_table: meta_table, obj: obj, clock: clock}) do
    clock_insert(table, meta_table, obj, clock)
  end

  def receive_handoff(%{version: 1, op: :record_meta, meta_table: meta_table, entry: entry}) do
    :ets.insert(meta_table, entry)
    :ok
  end

  def receive_handoff(%{version: 1, op: :create_meta_table, name: name}) do
    {:ok, :ets.new(name, [:set, :public])}
  end

  def receive_handoff(%{version: 1, op: :delete_table, table: table}) do
    :ets.delete(table)
    :ok
  end

  def receive_handoff(%{version: v} = msg) when v > @handoff_version do
    require Logger

    Logger.warning(
      "PartitionedEts: received handoff version #{v}, max supported is #{@handoff_version}. Ignoring: #{inspect(msg)}"
    )

    {:error, :unsupported_version}
  end

  # ── Private ─────────────────────────────────────────────────────────

  defp create_meta_tables(name, shards) do
    nodes = shards |> Enum.map(fn {n, _} -> n end) |> Enum.uniq()
    Map.new(nodes, &create_meta_table(name, &1))
  end

  defp create_meta_table(name, target_node) do
    meta_name = :"#{name}_handoff_meta_#{:erlang.unique_integer([:positive])}"

    if target_node == node() do
      {target_node, :ets.new(meta_name, [:set, :public])}
    else
      case send_handoff(target_node, %{version: @handoff_version, op: :create_meta_table, name: meta_name}) do
        {:ok, table} -> {target_node, table}
        _ -> {target_node, nil}
      end
    end
  end

  defp cleanup_meta_tables(meta_tables) do
    Enum.each(meta_tables, &cleanup_meta_table/1)
  end

  defp cleanup_meta_table({_target_node, nil}), do: :ok

  defp cleanup_meta_table({target_node, table}) do
    if target_node == node() do
      :ets.delete(table)
    else
      send_handoff(target_node, %{version: @handoff_version, op: :delete_table, table: table})
    end
  end

  defp ship_pass1(partition_tables, shards, hash_module, meta_tables, clock) do
    Enum.each(partition_tables, fn local_pt ->
      :ets.foldl(
        fn obj, :ok ->
          key = elem(obj, 0)
          {target_node, target_pt} = hash_module.hash(key, shards)
          ship_entry(target_node, target_pt, obj, :insert)
          record_meta(target_node, meta_tables, obj, clock)
          :ok
        end,
        :ok,
        local_pt
      )
    end)
  end

  defp record_meta(target_node, meta_tables, obj, clock) do
    case Map.get(meta_tables, target_node) do
      nil -> :ok
      meta_table -> do_record_meta(target_node, meta_table, obj, clock)
    end
  end

  defp do_record_meta(target_node, meta_table, obj, clock) do
    key = elem(obj, 0)
    fp = :erlang.phash2(obj)
    entry = {{key, fp}, clock}

    if target_node == node() do
      :ets.insert(meta_table, entry)
    else
      send_handoff(target_node, %{version: @handoff_version, op: :record_meta, meta_table: meta_table, entry: entry})
    end

    :ok
  end

  defp ship_pass2(shadow, pass1_clock, partition_tables, shards, hash_module, meta_tables) do
    changed_map =
      shadow
      |> changed_since(pass1_clock)
      |> Map.new()

    Enum.each(partition_tables, fn local_pt ->
      :ets.foldl(&ship_changed(&1, &2, changed_map, shards, hash_module, meta_tables), :ok, local_pt)
    end)
  end

  defp ship_changed(obj, :ok, changed_map, shards, hash_module, meta_tables) do
    key = elem(obj, 0)

    case Map.fetch(changed_map, key) do
      {:ok, clock} ->
        {target_node, target_pt} = hash_module.hash(key, shards)
        clock_ship(target_node, target_pt, obj, Map.get(meta_tables, target_node), clock)

      :error ->
        :ok
    end

    :ok
  end

  defp clock_ship(_target_node, _target_pt, _obj, nil, _clock), do: :ok

  defp clock_ship(target_node, target_pt, obj, meta_table, clock) do
    if target_node == node() do
      clock_insert(target_pt, meta_table, obj, clock)
    else
      send_handoff(target_node, %{
        version: @handoff_version,
        op: :clock_insert,
        table: target_pt,
        meta_table: meta_table,
        obj: obj,
        clock: clock
      })
    end

    :ok
  end

  defp ship_entry(target_node, target_table, obj, insert_fn) do
    if target_node == node() do
      apply(:ets, insert_fn, [target_table, obj])
      :ok
    else
      send_handoff(target_node, %{version: @handoff_version, op: insert_fn, table: target_table, obj: obj})
    end
  end

  defp send_handoff(target_node, msg) do
    :erpc.call(target_node, __MODULE__, :receive_handoff, [msg])
  rescue
    _ -> {:error, :erpc_failed}
  end

  defp bag_insert(data_table, meta_table, obj, meta_key, clock) do
    case :ets.lookup(meta_table, meta_key) do
      [] ->
        :ets.insert(data_table, obj)
        :ets.insert(meta_table, {meta_key, clock})
        true

      [{^meta_key, _existing_clock}] ->
        false
    end
  end

  defp set_insert(data_table, meta_table, obj, key, meta_key, clock) do
    highest =
      meta_table
      |> :ets.select([{{{key, :_}, :"$1"}, [], [:"$1"]}])
      |> Enum.max(fn -> -1 end)

    if clock >= highest do
      :ets.insert(data_table, obj)
      :ets.match_delete(meta_table, {{key, :_}, :_})
      :ets.insert(meta_table, {meta_key, clock})
      true
    else
      false
    end
  end
end
