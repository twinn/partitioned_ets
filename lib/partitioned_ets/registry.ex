defmodule PartitionedEts.Registry do
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
      node =
        pids
        |> hd()
        |> :erlang.node()

      apply(table, :node_added, [node])
    end

    {:noreply, state}
  end

  def handle_info({_ref, :leave, table, pids}, state) do
    if function_exported?(table, :node_removed, 1) do
      node =
        pids
        |> hd()
        |> :erlang.node()

      apply(table, :node_removed, [node])
    end

    {:noreply, state}
  end

  def handle_info(msg, state) do
    IO.inspect(msg)
    {:noreply, state}
  end

  def whereis_name(name) do
    :pg.get_local_members(__MODULE__, name)
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
