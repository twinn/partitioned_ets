defmodule PartitionedEts.ShutdownRaceTest do
  @moduledoc """
  Verifies that stopping the PartitionedEts GenServer while operations
  are in-flight produces a clear ArgumentError instead of a cryptic
  persistent_term or :badarg crash.
  """

  use ExUnit.Case

  alias PartitionedEts.Cluster

  defmodule RaceTable do
    @moduledoc false
    use PartitionedEts, table_opts: [:named_table, :public]
    @after_compile {Cluster, :inject}
  end

  test "operations raise ArgumentError when GenServer is stopped mid-flight" do
    Process.flag(:trap_exit, true)
    {:ok, sup} = Supervisor.start_link([RaceTable], strategy: :one_for_one)
    Process.unlink(sup)
    Process.sleep(100)

    RaceTable.insert({:warmup, 1})
    assert [{:warmup, 1}] = RaceTable.lookup(:warmup)

    collector = self()

    for i <- 1..50 do
      spawn(fn ->
        result =
          try do
            for j <- 1..10_000 do
              key = :"race_#{i}_#{j}"
              RaceTable.insert({key, j})
              RaceTable.lookup(key)
            end

            :ok
          rescue
            e in ArgumentError -> {:error, :argument_error, e.message}
          catch
            kind, reason -> {:unexpected, kind, reason}
          end

        send(collector, {:done, i, result})
      end)
    end

    Process.sleep(5)
    Supervisor.stop(sup, :shutdown)

    results =
      for _ <- 1..50 do
        receive do
          {:done, _i, result} -> result
        after
          5_000 -> :timeout
        end
      end

    errors = Enum.reject(results, &(&1 == :ok))
    assert errors != [], "expected some tasks to hit the shutdown race"

    # Every error should be a clean ArgumentError, not a cryptic crash.
    for error <- errors do
      assert {:error, :argument_error, msg} = error,
             "expected ArgumentError, got: #{inspect(error)}"

      assert msg =~ "is not running on this node"
    end
  end
end
