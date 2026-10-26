defmodule RiakCorePlugin.RiakTest do
  # RiakCorePlugin.Riak against a :peer node with riak_core_lite's ring
  # modules and fakes of its servers (test/support/peer.exs). The plugin's
  # own modules are never loaded on the peer.
  use ExUnit.Case, async: true

  alias RiakCorePlugin.{Data, Riak}
  alias RiakCorePlugin.Test.{FakeTarget, Peer}

  setup_all do
    {peer, node} = Peer.start()
    {bare_peer, bare} = Peer.start(riak: false)

    on_exit(fn ->
      Peer.stop(peer)
      Peer.stop(bare_peer)
    end)

    ring = Peer.fresh_ring(node, 8)
    Peer.put(node, :rcp_ring, ring)
    %{node: node, bare: bare, ring: ring}
  end

  defp target(node) do
    {:ok, target} = FakeTarget.start_link(node)
    target
  end

  test "the plugin's modules are not on the peer", %{node: node} do
    refute :erpc.call(node, :code, :is_loaded, [RiakCorePlugin.Riak])
    refute :erpc.call(node, :code, :is_loaded, [RiakCorePlugin.Data])
  end

  test "ring/1 reads the five ring accessors in one call", %{node: node} do
    assert {:ok, ring} = Riak.ring(target(node))
    assert ring.size == 8
    assert ring.claimant == node
    assert length(ring.owners) == 8
    assert Enum.all?(ring.owners, &match?({idx, ^node} when is_integer(idx), &1))
    assert ring.pending == []
    assert ring.members == [{node, :valid}]
    assert node in ring.up
    # no owner is down: no fallbacks, and no rpc for them
    assert ring.fallbacks == %{}
    # riak_core's target_n_val (unset here: riak_core's default, 4) is the N
    assert ring.n_val == 4
  end

  test "ring/1: N is riak_core's target_n_val on the attached node", %{node: node} do
    :ok = :erpc.call(node, :application, :set_env, [:riak_core, :target_n_val, 2])
    on_exit(fn -> :erpc.call(node, :application, :unset_env, [:riak_core, :target_n_val]) end)

    assert {:ok, %{n_val: 2}} = Riak.ring(target(node))
  end

  test "members/1: statuses, partitions per node and the up nodes", %{node: node} do
    assert {:ok, %{members: [{^node, :valid}], counts: [{^node, 8}], up: up}} =
             Riak.members(target(node))

    assert node in up
  end

  test "members/1: each up member says what it runs, its ring and versions", %{node: node} do
    assert {:ok, %{nodes: %{^node => info}, ring: ring, pending: []}} = Riak.members(target(node))
    assert info.ring == ring
    assert info.vnodes == 0 and info.not_owned == 0 and info.handoffs == 0
    # no riak_core_node_watcher on the test node: unknown, not an error
    assert info.services == nil
    assert info.otp == System.otp_release()
    assert is_integer(info.uptime_ms) and is_integer(info.memory)

    [row] =
      Data.member_details(%{
        members: [{node, :valid}],
        counts: [{node, 8}],
        up: [node],
        nodes: %{node => info},
        ring: ring
      })

    assert row.ring_agrees and row.share == 100.0
  end

  test "up_nodes/1 is the target plus the nodes it sees", %{node: node} do
    assert {:ok, up} = Riak.up_nodes(target(node))
    assert node in up and node() in up
  end

  test "handoffs/1: status/0 and the pending changes", %{node: node} do
    assert Riak.handoffs(target(node)) == {:ok, %{active: [], pending: []}}
  end

  test "ring_ready/2: the result as text, a timeout as readable text", %{node: node} do
    assert Riak.ring_ready(target(node)) == {:ok, {:ok, "ready (1 node)"}}

    {peer, slow} = Peer.start()
    on_exit(fn -> Peer.stop(peer) end)
    Peer.put(slow, :rcp_ready_sleep, 1_000)

    assert Riak.ring_ready(target(slow), 100) ==
             {:error, "timed out (a member may be unreachable)"}
  end

  test "without riak_core: riak_core is not running", %{bare: bare} do
    target = target(bare)
    assert Riak.ring(target) == {:error, "riak_core is not running"}
    assert Riak.members(target) == {:error, "riak_core is not running"}
    assert Riak.handoffs(target) == {:error, "riak_core is not running"}
    assert Riak.vnodes(target, "") == {:error, "riak_core is not running"}
    assert Riak.ring_ready(target) == {:error, "riak_core is not running"}
  end

  test "no ring yet: riak_core is not running" do
    {peer, node} = Peer.start()
    on_exit(fn -> Peer.stop(peer) end)
    assert Riak.ring(target(node)) == {:error, "riak_core is not running"}
  end

  test "no node attached" do
    assert Riak.ring(target(nil)) == {:error, "no node is attached"}
  end

  describe "vnodes/2" do
    setup do
      {peer, vnode_node} = Peer.start()
      on_exit(fn -> Peer.stop(peer) end)

      vnodes =
        for {mod, n} <- [a_vnode: 300, b_vnode: 3],
            idx <- 1..n,
            do: {mod, idx, Peer.spawn_idle(vnode_node)}

      Peer.put(vnode_node, :rcp_vnodes, Enum.shuffle(vnodes))
      %{vnode_node: vnode_node}
    end

    test "the module list comes from all vnodes; the cap applies after the filter",
         %{vnode_node: node} do
      target = target(node)

      assert {:ok, all} = Riak.vnodes(target, "")
      assert all.modules == [:a_vnode, :b_vnode]
      assert all.module == ""
      assert all.total == 303
      assert length(all.fetched) == Riak.max_vnodes()
      # the first 256 by module and index: all a_vnode
      assert Enum.all?(all.fetched, &match?({{:a_vnode, _, _}, _}, &1))

      assert {:ok, b} = Riak.vnodes(target, "b_vnode")
      assert b.module == "b_vnode"
      assert b.total == 3
      assert [{{:b_vnode, 1, _}, info} | _] = b.fetched
      assert length(b.fetched) == 3
      assert info[:message_queue_len] == 0
      assert info[:memory] > 0
    end

    test "a filter that matches no module is dropped", %{vnode_node: node} do
      assert {:ok, %{module: "", total: 303, modules: [_, _]}} =
               Riak.vnodes(target(node), "gone_vnode")
    end
  end

  describe "admin" do
    setup do
      {peer, node} = Peer.start()
      on_exit(fn -> Peer.stop(peer) end)
      Peer.put(node, :rcp_ring, Peer.fresh_ring(node, 8))
      %{node: node}
    end

    test "cluster/1: size, claimant, members, counts and the riak nodes that could join",
         %{node: node} do
      {joiner_peer, joiner} = Peer.start()
      {bare_peer, bare} = Peer.start(riak: false)

      on_exit(fn ->
        Peer.stop(joiner_peer)
        Peer.stop(bare_peer)
      end)

      # a joining node runs riak_core: its ring manager is registered
      :erpc.call(joiner, :erlang, :register, [
        :riak_core_ring_manager,
        Peer.spawn_idle(joiner)
      ])

      for n <- [joiner, bare], do: true = :erpc.call(node, :net_kernel, :connect_node, [n])

      assert {:ok, c} = Riak.cluster(target(node))
      assert c.size == 8
      assert c.claimant == node
      assert c.members == [{node, :valid}]
      assert c.counts == [{node, 8}]
      assert bare in c.up
      assert c.candidates == [joiner]
    end

    test "plan/1 sums up each transition on the target", %{node: node} do
      ring = Peer.fresh_ring(node, 8)
      other = :other@host
      # after: half the partitions move to `other`
      moved =
        ring
        |> then(&:erpc.call(node, :riak_core_ring, :all_owners, [&1]))
        |> Enum.take_every(2)
        |> Enum.reduce(ring, fn {idx, _}, r ->
          :erpc.call(node, :riak_core_ring, :transfer_node, [idx, other, r])
        end)

      Peer.put(node, :rcp_plan, {:ok, [{other, :join}], [{ring, moved}]})

      assert {:ok, %{changes: [{^other, :join}], transitions: [{before, after_, 0}]}} =
               Riak.plan(target(node))

      assert before == [{node, 8}]
      assert after_ == Enum.sort([{node, 4}, {other, 4}])
    end

    test "plan/1: a refusal is data", %{node: node} do
      Peer.put(node, :rcp_plan, {:error, :ring_not_ready})
      assert Riak.plan(target(node)) == {:ok, {:error, :ring_not_ready}}
    end

    test "admin/3 calls the claimant or riak_core with the action's arguments",
         %{node: node} do
      t = target(node)

      for action <- [
            {:leave, :b@h},
            {:remove, :b@h},
            {:replace, :b@h, :c@h},
            {:force_replace, :b@h, :c@h},
            {:resize, 16},
            :abort_resize,
            :commit,
            :clear,
            {:down, :b@h}
          ] do
        assert Riak.admin(t, node, action) == :ok
      end

      assert Peer.calls(node) == [
               {:riak_core_claimant, {:leave_member, [:b@h]}},
               {:riak_core_claimant, {:remove_member, [:b@h]}},
               {:riak_core_claimant, {:replace, [:b@h, :c@h]}},
               {:riak_core_claimant, {:force_replace, [:b@h, :c@h]}},
               {:riak_core_claimant, {:resize_ring, [16]}},
               {:riak_core_claimant, {:abort_resize, []}},
               {:riak_core_claimant, {:commit, []}},
               {:riak_core_claimant, {:clear, []}},
               {:riak_core, {:down, [:b@h]}}
             ]
    end

    test "admin/3: a join is asked of the joining node, with the target as the cluster",
         %{node: node} do
      {joiner_peer, joiner} = Peer.start()
      on_exit(fn -> Peer.stop(joiner_peer) end)

      assert Riak.admin(target(node), node, {:join, joiner}) == :ok
      assert Peer.calls(joiner) == [{:riak_core, {:staged_join, [node]}}]
      assert Peer.calls(node) == []
    end

    test "admin/3: refusals in words", %{node: node} do
      t = target(node)
      Peer.put(node, :rcp_reply, {:error, :is_claimant})

      assert Riak.admin(t, node, {:remove, node}) ==
               {:error, Data.admin_error({:error, :is_claimant})}

      Peer.put(node, :rcp_reply, :error)
      assert {:error, "the claimant refused" <> _} = Riak.admin(t, node, :commit)

      assert {:error, "the node is not reachable" <> _} =
               Riak.admin(t, node, {:join, :"nobody@nowhere.invalid"})
    end
  end
end
