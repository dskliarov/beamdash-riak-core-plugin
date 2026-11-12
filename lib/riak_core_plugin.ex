defmodule RiakCorePlugin do
  @moduledoc """
  The beamdash plugin for riak_core (and riak_core_lite): Ring, Members,
  Vnodes, Load, Handoffs, History and Admin pages. It is active while `riak_core ~> 0.10` runs on
  the attached node (see `mix.exs`). The Ring page is its main page, also shown
  on beamdash's Apps tab for riak_core.

  Its collectors keep the cluster's history in beamdash's event store (see
  `RiakCorePlugin.History`).
  """
  use Beamdash.Plugin,
    name: "riak_core",
    pages: [
      {"ring", "Ring", RiakCorePlugin.RingLive},
      {"health", "Health", RiakCorePlugin.HealthLive},
      {"members", "Members", RiakCorePlugin.MembersLive},
      {"vnodes", "Vnodes", RiakCorePlugin.VnodesLive},
      {"load", "Load", RiakCorePlugin.LoadLive},
      {"handoffs", "Handoffs", RiakCorePlugin.HandoffsLive},
      {"history", "History", RiakCorePlugin.HistoryLive},
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
       %{type: "object", properties: %{}}, {RiakCorePlugin.Tools, :load}},
      {"history",
       "The cluster's history: ended handoffs, member and ring changes, load samples or Admin actions",
       %{
         type: "object",
         properties: %{
           kind: %{
             type: "string",
             enum: ["handoffs", "members", "ring", "load", "admin"],
             description: "Which history"
           },
           minutes: %{type: "integer", description: "How far back, in minutes (default 1440)"},
           limit: %{
             type: "integer",
             description: "The most rows, newest first (default 50, at most 500)"
           }
         },
         required: ["kind"]
       }, {RiakCorePlugin.Tools, :history}}
    ],
    collectors: [
      {"handoffs", 5_000, {RiakCorePlugin.Collect, :handoffs}},
      {"cluster", 10_000, {RiakCorePlugin.Collect, :cluster}},
      {"load", 60_000, {RiakCorePlugin.Collect, :load}}
    ]
end
