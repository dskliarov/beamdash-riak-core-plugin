defmodule RiakCorePlugin.ToolsTest do
  # The MCP tools call the attached node through the global Beamdash.Target,
  # so each test starts it and attaches it to a :peer node (test/support/peer.exs).
  use ExUnit.Case, async: false

  alias RiakCorePlugin.Test.Peer
  alias RiakCorePlugin.Tools

  setup_all do
    {peer, node} = Peer.start()
    on_exit(fn -> Peer.stop(peer) end)
    Peer.put(node, :rcp_ring, Peer.fresh_ring(node, 8))
    %{node: node}
  end

  setup %{node: node} do
    start_supervised!(Beamdash.Target)
    {:ok, _} = Beamdash.Target.connect(Atom.to_string(node), Node.get_cookie())
    :ok
  end

  test "the plugin declares its five tools, and its collectors" do
    plugin = RiakCorePlugin.__beamdash_plugin__()

    assert Enum.map(plugin.mcp_tools, & &1.name) == [
             "ring",
             "members",
             "handoffs",
             "load",
             "history"
           ]

    assert Enum.map(plugin.collectors, &{&1.name, &1.interval}) ==
             [{"handoffs", 5_000}, {"cluster", 10_000}, {"load", 60_000}]
  end

  test "ring summarises the attached node's ring", %{node: node} do
    assert {:ok, ring} = Tools.ring(%{})
    assert ring.size == 8
    assert ring.claimant == node
    assert ring.members == [%{node: node, status: :valid}]
    assert ring.partitions == [%{node: node, partitions: 8}]
    assert node in ring.up
    assert ring.down == []
    assert ring.pending == 0
    assert {:ok, _} = Jason.encode(ring)
  end

  test "members lists each member with its partitions", %{node: node} do
    assert {:ok, [member]} = Tools.members(%{})
    assert member.node == node
    assert member.partitions == 8
    assert member.claimant
    assert member.up
    assert member.info.vnodes == 0
    assert {:ok, _} = Jason.encode(member)
  end

  test "a member that does not answer shows its error as JSON" do
    {peer, node} = Peer.start()
    {bare_peer, bare} = Peer.start(riak: false)

    on_exit(fn ->
      Peer.stop(peer)
      Peer.stop(bare_peer)
    end)

    Peer.put(
      node,
      :rcp_ring,
      :erpc.call(node, :riak_core_ring, :add_member, [node, Peer.fresh_ring(node, 8), bare])
    )

    true = :erpc.call(node, :net_kernel, :connect_node, [bare])
    {:ok, _} = Beamdash.Target.connect(Atom.to_string(node), Node.get_cookie())

    assert {:ok, members} = Tools.members(%{})
    assert %{info: %{error: text}} = Enum.find(members, &(&1.node == bare))
    assert is_binary(text)
    assert {:ok, _} = Jason.encode(members)
  end

  test "handoffs has no active handoffs or pending changes" do
    assert {:ok, %{active: [], pending: []}} = Tools.handoffs(%{})
  end

  test "an active handoff's size is a map that encodes as JSON", %{node: node} do
    on_exit(fn -> Peer.put(node, :rcp_handoffs, []) end)

    Peer.put(node, :rcp_handoffs, [
      {:status_v2,
       [
         type: :ownership,
         mod: :a_vnode,
         src_partition: 0,
         target_partition: 1,
         src_node: node,
         target_node: :b@h,
         direction: :outbound,
         status: :awaiting,
         start_ts: {1_700, 0, 0},
         stats: [size: {2048, :bytes}, pct_done_decimal: 0.5]
       ]}
    ])

    assert {:ok, %{active: [handoff]} = data} = Tools.handoffs(%{})
    assert handoff.size == %{n: 2048, unit: :bytes}
    assert handoff.progress == 0.5
    assert handoff.from == node and handoff.to == :b@h
    assert {:ok, _} = Jason.encode(data)
  end

  test "load samples the attached node and encodes as JSON", %{node: node} do
    assert {:ok, load} = Tools.load(%{})
    assert length(load.schedulers) == :erpc.call(node, :erlang, :system_info, [:schedulers])

    assert Enum.map(load.memory, & &1.label) == [
             "Total",
             "Processes",
             "System",
             "Binaries",
             "ETS",
             "Code"
           ]

    assert {:ok, _} = Jason.encode(load)
  end

  test "a tool says so when no node is attached" do
    :ok = Beamdash.Target.disconnect()
    assert {:error, "no node is attached"} = Tools.ring(%{})
  end
end
