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

  test "the plugin declares its three tools" do
    assert Enum.map(RiakCorePlugin.__beamdash_plugin__().mcp_tools, & &1.name) ==
             ["ring", "members", "handoffs"]
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
    assert ring.fallbacks == 0
  end

  test "members lists each member with its partitions", %{node: node} do
    assert {:ok, [member]} = Tools.members(%{})
    assert member.node == node
    assert member.partitions == 8
    assert member.claimant
    assert member.up
  end

  test "handoffs has no active handoffs or pending changes" do
    assert {:ok, %{active: [], pending: []}} = Tools.handoffs(%{})
  end

  test "a tool says so when no node is attached" do
    :ok = Beamdash.Target.disconnect()
    assert {:error, "no node is attached"} = Tools.ring(%{})
  end
end
