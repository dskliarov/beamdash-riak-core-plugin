defmodule RiakCorePlugin do
  @moduledoc """
  The beamdash plugin for riak_core (and riak_core_lite): Ring, Members,
  Vnodes, Handoffs and Admin pages. It is active while `riak_core ~> 0.10` runs on
  the attached node (see `mix.exs`).
  """
  use Beamdash.Plugin,
    name: "riak_core",
    pages: [
      {"ring", "Ring", RiakCorePlugin.RingLive},
      {"members", "Members", RiakCorePlugin.MembersLive},
      {"vnodes", "Vnodes", RiakCorePlugin.VnodesLive},
      {"handoffs", "Handoffs", RiakCorePlugin.HandoffsLive},
      {"admin", "Admin", RiakCorePlugin.AdminLive}
    ]
end
