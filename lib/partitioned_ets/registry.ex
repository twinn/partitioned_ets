defmodule PartitionedEts.Registry do
  @moduledoc false
  use GenServer

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok)
  end

  def members(name) do
    :pg.get_members(__MODULE__, name)
  end

  def init(_opts) do
    :pg.start_link(__MODULE__)
    :pg.monitor_scope(__MODULE__)
    {:ok, :ok}
  end

  def handle_info({_ref, :join, table, pids}, state) do
    if function_exported?(table, :node_added, 1) do
      node = pids |> hd() |> :erlang.node()
      table.node_added(node)
    end

    {:noreply, state}
  end

  def handle_info({_ref, :leave, table, pids}, state) do
    if function_exported?(table, :node_removed, 1) do
      node = pids |> hd() |> :erlang.node()
      table.node_removed(node)
    end

    {:noreply, state}
  end

  # Catch-all for any :pg messages we don't yet handle. We intentionally
  # surface these via IO.inspect so unknown event shapes are visible during
  # development — silent dropping would hide protocol changes in :pg.
  def handle_info(msg, state) do
    # credo:disable-for-next-line Credo.Check.Warning.IoInspect
    IO.inspect(msg, label: "PartitionedEts.Registry unhandled message")
    {:noreply, state}
  end

  def whereis_name(name) do
    __MODULE__
    |> :pg.get_local_members(name)
    |> case do
      [pid | _] -> pid
      _ -> :undefined
    end
  end

  def register_name(name, pid) do
    :pg.join(__MODULE__, name, pid)
    :yes
  end

  def unregister_name(name) do
    :pg.leave(__MODULE__, name, self())
    :ok
  end

  # def send(name, msg) do
  #   name
  #   |> whereis_name()
  #   |> Kernel.send(msg)
  # end
end
