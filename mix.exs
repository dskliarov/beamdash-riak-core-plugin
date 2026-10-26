defmodule RiakCorePlugin.MixProject do
  use Mix.Project

  @version "0.0.1"
  @source_url "https://github.com/dskliarov/beamdash-riak-core-plugin"

  def project do
    [
      app: :riak_core_plugin,
      version: @version,
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      description:
        "A beamdash plugin with Ring, Members, Vnodes, Handoffs and Admin pages for riak_core.",
      package: package(),
      source_url: @source_url,
      deps: deps()
    ]
  end

  def application, do: [extra_applications: [:logger]]

  defp elixirc_paths(_env), do: ["lib"]

  # beamdash reads these deps as data. `riak_core` is the application this
  # plugin supports, with its versions (riak_core_lite's app is riak_core).
  defp deps do
    [
      {:riak_core, "~> 0.10", hex: :riak_core_lite, runtime: false},
      {:phoenix_live_view, "~> 1.0"},
      {:beamdash, path: "../beamdash", only: [:dev, :test], runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["GPL-3.0-or-later"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib priv mix.exs README.md LICENSE .formatter.exs)
    ]
  end
end
