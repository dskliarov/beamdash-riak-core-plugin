defmodule RiakCorePlugin do
  @moduledoc """
  The beamdash plugin for riak_core (and riak_core_lite): Ring, Members,
  Vnodes, Load, Handoffs and Admin pages. It is active while `riak_core ~> 0.10` runs on
  the attached node (see `mix.exs`). The Ring page is its main page, also shown
  on beamdash's Apps tab for riak_core.
  """
  use Beamdash.Plugin,
    name: "riak_core",
    pages: [
      {"ring", "Ring", RiakCorePlugin.RingLive},
      {"members", "Members", RiakCorePlugin.MembersLive},
      {"vnodes", "Vnodes", RiakCorePlugin.VnodesLive},
      {"load", "Load", RiakCorePlugin.LoadLive},
      {"handoffs", "Handoffs", RiakCorePlugin.HandoffsLive},
      {"admin", "Admin", RiakCorePlugin.AdminLive}
    ],
    main: "ring",
    mcp_tools: [
      {"ring",
       "The ring: size, claimant, members, partitions per node, down owners, pending changes",
       %{type: "object", properties: %{}}, {RiakCorePlugin.Tools, :ring}},
      {"members", "Members with their status, partitions, share, what they say about themselves",
       %{type: "object", properties: %{}}, {RiakCorePlugin.Tools, :members}},
      {"handoffs", "Active handoffs and pending ownership changes",
       %{type: "object", properties: %{}}, {RiakCorePlugin.Tools, :handoffs}},
      {"load",
       "Scheduler busy share, I/O per second, memory and the busiest processes, over a 1 s sample",
       %{type: "object", properties: %{}}, {RiakCorePlugin.Tools, :load}}
    ]
end
