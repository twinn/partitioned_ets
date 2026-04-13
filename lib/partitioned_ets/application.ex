defmodule PartitionedEts.Application do
  @moduledoc false

  use Application

  @impl Application
  def start(_type, _args) do
    children = [
      {PgRegistry, name: PartitionedEts.Registry, keys: :unique},
      {Registry, name: PartitionedEts.LocalRegistry, keys: :unique}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: PartitionedEts.Supervisor)
  end
end
