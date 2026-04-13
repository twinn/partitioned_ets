defmodule PartitionedEts.Cluster do
  @moduledoc """
  Vast majority lifted from https://github.com/phoenixframework/phoenix_pubsub/blob/v2.1.3/test/support/cluster.ex
  A helper module for testing distributed code.
  Requires `epmd` to be running in order to work:
  `$ epmd -daemon`
  """

  def inject(%{module: module}, binary) do
    :persistent_term.put(module, binary)
  end

  def spawn(nodes) do
    # Turn node into a distributed node with the given long name
    :net_kernel.start([:"primary@127.0.0.1"])

    # Allow spawned nodes to fetch all code from this node
    :erl_boot_server.start([])
    allow_boot(to_charlist("127.0.0.1"))

    nodes
    |> Enum.map(&Task.async(fn -> spawn_node(&1) end))
    |> Enum.map(&Task.await(&1, 30_000))
  end

  def start(node, services) do
    Enum.each(services, fn service ->
      export_module(node, service)
    end)

    # PartitionedEts.Application has already been started on the remote
    # node by ensure_applications_started/1, so the :pg scope is up.
    # We just supervise the user's table modules here.
    args = [services, [strategy: :one_for_one]]

    rpc(node, Supervisor, :start_link, args)
  end

  defp spawn_node(node_host) do
    {:ok, pid, node} =
      :peer.start(%{
        host: to_charlist("127.0.0.1"),
        name: node_name(node_host),
        args: [inet_loader_args()]
      })

    add_code_paths(node)
    transfer_configuration(node)
    ensure_applications_started(node)
    {:ok, pid, node}
  end

  defp rpc(node, module, function, args) do
    :rpc.block_call(node, module, function, args)
  end

  defp inet_loader_args do
    to_charlist("-loader inet -hosts 127.0.0.1 -setcookie #{:erlang.get_cookie()} -kernel prevent_overlapping_partitions false")
  end

  defp allow_boot(host) do
    {:ok, ipv4} = :inet.parse_ipv4_address(host)
    :erl_boot_server.add_slave(ipv4)
  end

  defp add_code_paths(node) do
    rpc(node, :code, :add_paths, [:code.get_path()])
  end

  defp transfer_configuration(node) do
    for {app_name, _, _} <- Application.loaded_applications() do
      for {key, val} <- Application.get_all_env(app_name) do
        rpc(node, Application, :put_env, [app_name, key, val])
      end
    end
  end

  @skip_apps [:dialyxir, :credo, :styler]

  defp ensure_applications_started(node) do
    rpc(node, Application, :ensure_all_started, [:mix])
    rpc(node, Mix, :env, [Mix.env()])

    for {app_name, _, _} <- Application.loaded_applications(),
        app_name not in @skip_apps do
      rpc(node, Application, :ensure_all_started, [app_name])
    end
  end

  defp export_module(node, module) do
    case :persistent_term.get(module, false) do
      binary when is_binary(binary) ->
        filename = ~c"./Elixir.PartitionedEts.Example.beam"
        :rpc.call(node, :code, :load_binary, [module, filename, binary])

      _ ->
        :ok
    end
  end

  defp node_name(node_host) do
    node_host
    |> to_string()
    |> String.split("@")
    |> Enum.at(0)
    |> String.to_atom()
  end
end
