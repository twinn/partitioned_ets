defmodule PartitionedEts.MixProject do
  use Mix.Project

  @source_url "https://github.com/twinn/partitioned_ets"
  @version "0.1.0"

  def project do
    [
      app: :partitioned_ets,
      version: @version,
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      description: description(),
      package: package(),
      source_url: @source_url,
      dialyzer: [plt_add_apps: [:ex_unit]]
    ]
  end

  def cli do
    [
      preferred_envs: [ci: :test, precommit: :test]
    ]
  end

  def application do
    [
      mod: {PartitionedEts.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp description do
    "A distributed, partitioned ETS table for Elixir."
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp aliases do
    [
      ci: ["format --check-formatted", "credo --strict", "test"],
      precommit: ["format", "credo --strict", "test"]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:styler, "~> 1.11", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end
end
