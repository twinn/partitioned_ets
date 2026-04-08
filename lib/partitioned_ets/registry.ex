defmodule PartitionedEts.Registry do
  @moduledoc false

  # Wrapper around the :pg scope used by PartitionedEts. Provides the
  # `:via` integration so PartitionedEts GenServers can register
  # themselves with `name: {:via, PartitionedEts.Registry, name}`.
  #
  # The :pg scope itself is started as a child of `PartitionedEts.Application`,
  # not from inside this module.

  def members(name), do: :pg.get_members(__MODULE__, name)

  def whereis_name(name) do
    case :pg.get_local_members(__MODULE__, name) do
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

  def send(name, msg) do
    case whereis_name(name) do
      :undefined -> :erlang.error(:badarg, [{name, msg}])
      pid -> Kernel.send(pid, msg)
    end
  end
end
