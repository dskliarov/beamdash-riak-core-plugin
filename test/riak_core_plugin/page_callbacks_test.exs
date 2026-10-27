defmodule RiakCorePlugin.PageCallbacksTest do
  # The pages' LiveView callbacks, called directly on a hand-built connected
  # socket, against a :peer node with riak_core's ring modules and fakes
  # (test/support/peer.exs) behind a fake Beamdash.Target.
  use ExUnit.Case, async: true

  alias Phoenix.LiveView.{Async, Lifecycle, Socket}
  alias RiakCorePlugin.{AdminLive, HandoffsLive, MembersLive, RingLive, VnodesLive}
  alias RiakCorePlugin.Test.{FakeTarget, Peer}

  setup_all do
    {peer, node} = Peer.start()
    on_exit(fn -> Peer.stop(peer) end)
    Peer.put(node, :rcp_ring, Peer.fresh_ring(node, 8))

    vnodes =
      for {mod, n} <- [a_vnode: 2, b_vnode: 1], idx <- 1..n, do: {mod, idx, Peer.spawn_idle(node)}

    Peer.put(node, :rcp_vnodes, vnodes)
    %{node: node}
  end

  setup %{node: node} do
    {:ok, target} = FakeTarget.start_link(node)
    %{target: target}
  end

  defp mount(page, target) do
    socket = %Socket{
      view: page,
      transport_pid: self(),
      private: %{lifecycle: %Lifecycle{}, live_temp: %{}}
    }

    {:ok, socket} = page.mount(%{}, %{"target" => target}, socket)
    socket
  end

  # Waits for the start_async result of `key` and hands it to the page, as
  # the LiveView channel does. The result of a load that was replaced by a
  # newer one is dropped (the channel drops it too).
  defp await(socket, key) do
    receive do
      {:phoenix, :async_result, {:start, {ref, nil, ^key, result}}} ->
        case socket.private.live_async do
          %{^key => {^ref, _pid, _kind}} ->
            Async.handle_async(socket, nil, :start, key, ref, result)

          _ ->
            await(socket, key)
        end
    after
      5_000 -> flunk("no async result for #{inspect(key)}")
    end
  end

  defp info(socket, msg) do
    {:noreply, socket} = socket.view.handle_info(msg, socket)
    socket
  end

  defp event(socket, name, params) do
    {:noreply, socket} = socket.view.handle_event(name, params, socket)
    socket
  end

  defp render(socket),
    do: socket.assigns |> socket.view.render() |> Phoenix.LiveViewTest.rendered_to_string()

  defp timers(socket), do: socket.assigns.rcp_timers
  defp loading(socket), do: socket.private |> Map.get(:live_async, %{}) |> Map.keys()

  defp timer_ms(socket, key) do
    {_token, tref} = Map.fetch!(timers(socket), key)
    Process.read_timer(tref)
  end

  test "with no node attached nothing is loaded and no timer runs" do
    {:ok, target} = FakeTarget.start_link(nil)

    for page <- [RingLive, MembersLive, VnodesLive, HandoffsLive, AdminLive] do
      socket = mount(page, target)
      assert timers(socket) == %{}
      assert loading(socket) == []
      assert render(socket) =~ "No node is attached"
    end

    refute_receive {:phoenix, :async_result, _}, 50
  end

  test "mount renders Loading… and only starts the load", %{target: target} do
    socket = mount(RingLive, target)
    assert render(socket) =~ "Loading…"
    assert Enum.sort(loading(socket)) == [:ready, :ring]
    # the refresh is scheduled once the load completes
    assert timers(socket) == %{}

    socket = await(socket, :ring)
    assert render(socket) =~ "8 partitions"
    assert timer_ms(socket, :ring) in 1..5_000
    # the only owner is the attached node: its name is not a link
    refute render(socket) =~ "rcp-node"
    refute Map.has_key?(timers(socket), :ready)

    # ringready is loaded on its own, less often
    socket = await(socket, :ready)
    assert render(socket) =~ "ready (1 node)"
    assert timer_ms(socket, :ready) in 5_001..30_000
  end

  test "the timer message loads again; the next timer starts after that load",
       %{target: target} do
    socket = MembersLive |> mount(target) |> await(:members)
    assert render(socket) =~ "valid"
    %{members: {token, tref}} = timers(socket)

    socket = info(socket, {:rcp_refresh, :members, token})
    assert timers(socket) == %{}
    assert loading(socket) == [:members]
    assert Process.read_timer(tref) == false

    socket = await(socket, :members)
    assert [:members] = Map.keys(timers(socket))
  end

  test "a stale timer message does nothing", %{target: target} do
    socket = HandoffsLive |> mount(target) |> await(:handoffs)
    before = timers(socket)

    socket = info(socket, {:rcp_refresh, :handoffs, make_ref()})
    assert timers(socket) == before
    assert loading(socket) == []
  end

  test "reconnecting keeps one timer per load", %{target: target, node: node} do
    socket = HandoffsLive |> mount(target) |> await(:handoffs)
    %{handoffs: {_, old}} = timers(socket)

    socket = info(socket, {:beamdash_target, {:connected, node}})
    assert Process.read_timer(old) == false
    assert timers(socket) == %{}
    assert loading(socket) == [:handoffs]

    socket = info(socket, {:beamdash_target, {:connected, node}})
    socket = await(socket, :handoffs)
    assert [:handoffs] = Map.keys(timers(socket))
    assert render(socket) =~ "No handoffs"
  end

  test "detaching stops the timers; a refresh with no node doesn't restart them",
       %{target: target, node: node} do
    socket = MembersLive |> mount(target) |> await(:members)
    %{members: {token, tref}} = timers(socket)

    FakeTarget.set_node(target, nil)
    socket = info(socket, {:beamdash_target, {:disconnected, :closed}})
    assert timers(socket) == %{}
    assert Process.read_timer(tref) == false
    assert render(socket) =~ "No node is attached"

    # a timer message already in the mailbox
    socket = info(socket, {:rcp_refresh, :members, token})
    assert timers(socket) == %{}
    assert loading(socket) == []

    FakeTarget.set_node(target, node)
    socket = info(socket, {:beamdash_target, {:connected, node}})
    assert render(socket) =~ "Loading…"
    socket = await(socket, :members)
    assert [:members] = Map.keys(timers(socket))
  end

  test "a load that finishes after the node detached schedules nothing", %{target: target} do
    socket = mount(HandoffsLive, target)

    # the result is in the mailbox before the detach (detaching cancels a
    # load still running, and this socket never hears of a cancelled one)
    assert_receive {:phoenix, :async_result, {:start, {_, nil, :handoffs, _}}} = result, 5_000
    send(self(), result)

    FakeTarget.set_node(target, nil)
    socket = socket |> info({:beamdash_target, {:disconnected, :closed}}) |> await(:handoffs)
    assert timers(socket) == %{}
  end

  test "ring: an owner filter that is no longer an option is reset", %{target: target} do
    socket = mount(RingLive, target)
    socket = event(socket, "filter", %{"node" => "gone@host"})
    assert socket.assigns.filter == "gone@host"
    socket = await(socket, :ring)
    assert socket.assigns.filter == ""
  end

  test "vnodes: the module list is from all vnodes; filtering fetches again",
       %{target: target} do
    socket = VnodesLive |> mount(target) |> await(:vnodes)
    html = render(socket)
    assert html =~ ~s(value="a_vnode")
    assert html =~ ~s(value="b_vnode")

    socket = event(socket, "filter", %{"module" => "b_vnode"})
    assert loading(socket) == [:vnodes]
    socket = await(socket, :vnodes)
    assert {:ok, %{total: 1, fetched: [{{:b_vnode, 1, _}, _}]}} = socket.assigns.vnodes
    # the other module is still an option
    assert render(socket) =~ ~s(value="a_vnode")

    # a module that is gone is reset
    socket = socket |> event("filter", %{"module" => "gone_vnode"}) |> await(:vnodes)
    assert socket.assigns.module == ""
    assert {:ok, %{total: 3}} = socket.assigns.vnodes
  end

  test "odd events don't crash the pages", %{target: target} do
    for page <- [RingLive, MembersLive, VnodesLive, HandoffsLive, AdminLive] do
      socket = mount(page, target)
      assert %Socket{} = event(socket, "nope", %{})
      assert %Socket{} = event(socket, "filter", %{"x" => "y"})
      assert %Socket{} = event(socket, "sort", %{})
    end

    socket = mount(VnodesLive, target)
    assert event(socket, "sort", %{"col" => "nope"}).assigns.sort == {:mailbox, :desc}
  end

  describe "admin" do
    # a node of its own: the fakes record the admin calls there
    setup do
      {peer, node} = Peer.start()
      on_exit(fn -> Peer.stop(peer) end)
      ring = Peer.fresh_ring(node, 8)
      Peer.put(node, :rcp_ring, ring)
      {:ok, target} = FakeTarget.start_link(node)
      %{node: node, target: target, ring: ring}
    end

    defp admin(target), do: AdminLive |> mount(target) |> await(:cluster) |> await(:plan)

    defp acted(socket), do: socket |> await(:action) |> await(:plan) |> await(:cluster)

    test "shows the members and the plan; nothing staged, nothing to commit",
         %{target: target, node: node} do
      html = target |> admin() |> render()
      assert html =~ Atom.to_string(node)
      assert html =~ "Nothing is staged."
      assert html =~ ~r/<button[^>]*phx-click="commit"[^>]*disabled/
    end

    test "leave is staged at once and the plan reloads", %{target: target, node: node} do
      socket = target |> admin() |> event("leave", %{"node" => Atom.to_string(node)})
      assert :action in loading(socket)
      assert socket.assigns.busy

      # one action at a time
      socket = event(socket, "leave", %{"node" => Atom.to_string(node)})

      socket = acted(socket)
      refute socket.assigns.busy
      assert render(socket) =~ "Leave of #{node} staged"
      assert Peer.calls(node) == [{:riak_core_claimant, {:leave_member, [node]}}]
    end

    test "a replacement that isn't in the cluster joins first, then replaces",
         %{target: target, node: node} do
      {joiner_peer, joiner} = Peer.start()
      on_exit(fn -> Peer.stop(joiner_peer) end)

      # the cluster as loaded, with the joiner among the nodes that could join
      socket = admin(target)
      {:ok, cluster} = socket.assigns.cluster
      socket = Phoenix.Component.assign(socket, cluster: {:ok, %{cluster | candidates: [joiner]}})

      html = render(socket)
      assert html =~ ~s|<optgroup label="Not in the cluster (joins first)">|
      assert html =~ ~s(<option value="#{joiner}">)

      params = %{"node" => Atom.to_string(node), "new" => Atom.to_string(joiner)}
      socket = socket |> event("replace", Map.put(params, "mode", "replace")) |> acted()
      assert render(socket) =~ "Join of #{joiner} and replace of #{node} staged"
      assert Peer.calls(joiner) == [{:riak_core, {:staged_join, [node]}}]
      assert Peer.calls(node) == [{:riak_core_claimant, {:replace, [node, joiner]}}]

      # a force replace asks first
      socket = Phoenix.Component.assign(socket, cluster: {:ok, %{cluster | candidates: [joiner]}})
      socket = event(socket, "replace", Map.put(params, "mode", "force_replace"))
      refute :action in loading(socket)
      assert render(socket) =~ "Join #{joiner} and stage a force replace of #{node} by it?"
    end

    test "force remove asks first; cancel does nothing, confirm runs it",
         %{target: target, node: node} do
      socket = target |> admin() |> event("remove", %{"node" => Atom.to_string(node)})
      refute :action in loading(socket)
      assert render(socket) =~ "Stage a force remove of #{node}?"

      socket = event(socket, "cancel", %{})
      assert socket.assigns.confirm == nil
      refute render(socket) =~ "force remove of"

      socket =
        socket
        |> event("remove", %{"node" => Atom.to_string(node)})
        |> event("confirm", %{})
        |> acted()

      assert Peer.calls(node) == [{:riak_core_claimant, {:remove_member, [node]}}]
      assert render(socket) =~ "Force remove of #{node} staged"
    end

    test "a plan with changes shows its transitions and commits after confirming",
         %{target: target, node: node, ring: ring} do
      Peer.put(node, :rcp_plan, {:ok, [{node, :leave}], [{ring, ring}]})
      socket = admin(target)
      html = render(socket)
      assert html =~ "leave"
      assert html =~ "Transition 1 of 1: 0 partition transfers"
      refute html =~ ~r/<button[^>]*phx-click="commit"[^>]*disabled/

      socket = event(socket, "commit", %{})
      assert render(socket) =~ "Commit the plan above?"
      socket = socket |> event("confirm", %{}) |> acted()
      assert Peer.calls(node) == [{:riak_core_claimant, {:commit, []}}]
      assert render(socket) =~ "Plan committed"
    end

    test "no Mark down on a member that responds", %{target: target} do
      refute target |> admin() |> render() =~ ~s(phx-click="down")
    end

    test "a refusal is shown in words", %{target: target, node: node} do
      Peer.put(node, :rcp_reply, {:error, :only_member})

      socket =
        target |> admin() |> event("leave", %{"node" => Atom.to_string(node)}) |> acted()

      assert render(socket) =~ "Leave of #{node} staged failed: it is the only member"
    end

    test "only members, and only well-formed names to join, are taken",
         %{target: target, node: node} do
      socket = admin(target)

      for {name, params} <- [
            {"leave", %{"node" => "stranger@host"}},
            {"down", %{"node" => "stranger@host"}},
            {"replace", %{"node" => Atom.to_string(node), "new" => "x@h", "mode" => "replace"}},
            {"join", %{"node" => "not a node"}},
            {"resize", %{"size" => "big"}}
          ] do
        socket = event(socket, name, params)
        refute :action in loading(socket)
        assert {:error, _} = socket.assigns.note
      end

      assert Peer.calls(node) == []

      # a well-formed new name is asked to join (and isn't reachable here)
      socket = socket |> event("join", %{"node" => "nobody@nowhere.invalid"}) |> acted()
      assert render(socket) =~ "failed: the node is not reachable"
    end
  end
end
