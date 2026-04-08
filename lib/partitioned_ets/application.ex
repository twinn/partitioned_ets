defmodule PartitionedEts.Application do
  @moduledoc false

  use Application

  @impl Application
  def start(_type, _args) do
    children = [
      %{
        id: PartitionedEts.Registry.Pg,
        start: {:pg, :start_link, [PartitionedEts.Registry]},
        type: :worker
      }
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: PartitionedEts.Supervisor)
  end
end
