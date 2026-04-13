defmodule PartitionedEts.Registry do
  @moduledoc false

  # Thin wrapper around PgRegistry for cluster membership queries.
  # The PgRegistry scope is started by `PartitionedEts.Application`
  # with `keys: :unique` — one owner GenServer per table name per node.

  @scope __MODULE__

  def scope, do: @scope

  def members(name), do: PgRegistry.lookup(@scope, name)

  def monitor(name), do: PgRegistry.monitor(@scope, name)
end
