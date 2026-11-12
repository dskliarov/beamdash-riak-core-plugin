defmodule RiakCorePlugin.HistoryTest do
  # The collectors, the Admin page's records and the history MCP tool, with
  # beamdash's event store started on a scratch file and the global
  # Beamdash.Target attached to a :peer node (test/support/peer.exs).
  use ExUnit.Case, async: false

  alias Phoenix.LiveView.{Async, Lifecycle, Socket}
  alias RiakCorePlugin.{AdminLive, Collect, Data, History, Tools}
  alias RiakCorePlugin.Test.{FakeTarget, Peer}

  setup do
    {peer, node} = Peer.start()
    on_exit(fn -> Peer.stop(peer) end)
    ring = Peer.fresh_ring(node, 8)
    Peer.put(node, :rcp_ring, ring)

    dir = Path.join(System.tmp_dir!(), "rcp_history_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf(dir) end)

    start_supervised!(
      {Beamdash.Store, path: Path.join(dir, "events.sqlite3"), purge_interval: :infinity}
    )

    start_supervised!(Beamdash.Target)
    {:ok, _} = Beamdash.Target.connect(Atom.to_string(node), Node.get_cookie())

    name = :erpc.call(node, :riak_core_ring, :cluster_name, [ring])
    %{node: node, ring: ring, key: Data.cluster_key(name)}
  end

  defp handoff(node, partition, type \\ :hinted) do
    {:status_v2,
     [
       mod: :kv_vnode,
       src_partition: partition,
       target_partition: partition,
       src_node: node,
       target_node: :other@h,
       direction: :outbound,
       status: :running,
       start_ts: :os.timestamp(),
       sender_pid: self(),
       stats: [size: {10, :objects}],
       type: type
     ]}
  end

  test "history is kept while the event store runs" do
    assert History.status() == :ok
  end

  test "handoffs: a hinted handoff is stored once it ends", %{node: node, key: key} do
    Peer.put(node, :rcp_handoffs, [handoff(node, 0), handoff(node, 1)])
    assert {:ok, state} = Collect.handoffs(nil)
    assert map_size(state.tracked) == 2

    Peer.put(node, :rcp_handoffs, [handoff(node, 1)])
    assert {:ok, state} = Collect.handoffs(state)
    assert {:ok, [row]} = History.query("handoffs", key)

    assert %{node: from, key: ^key, data: %{"src_partition" => 0, "to" => "other@h"}} = row
    assert from == Atom.to_string(node)
    assert %{"size" => %{"n" => 10, "unit" => "objects"}, "duration_ms" => ms} = row.data
    assert is_integer(ms) and ms >= 0

    # one already stored isn't stored again
    assert {:ok, _} = Collect.handoffs(state)
    assert {:ok, [_]} = History.query("handoffs", key)
  end

  test "handoffs: a failed call stores nothing and the state it was given still works",
       %{node: node, key: key} do
    Peer.put(node, :rcp_handoffs, [handoff(node, 0)])
    assert {:ok, state} = Collect.handoffs(nil)

    Beamdash.Target.disconnect()
    assert {:error, _} = Collect.handoffs(state)
    assert {:ok, []} = History.query("handoffs", key)

    {:ok, _} = Beamdash.Target.connect(Atom.to_string(node), Node.get_cookie())
    Peer.put(node, :rcp_handoffs, [])
    assert {:ok, _} = Collect.handoffs(state)
    assert {:ok, [_]} = History.query("handoffs", key)
  end

  test "handoffs: a transfer the ring marks done is stored, though never seen running",
       %{node: node, ring: ring, key: key} do
    pending = fn mods -> [{0, node, :new@h, mods, :awaiting}] end

    set = fn mods ->
      Peer.put(
        node,
        :rcp_ring,
        :erpc.call(node, :riak_core_ring, :set_pending_changes, [ring, pending.(mods)])
      )
    end

    set.([])
    assert {:ok, state} = Collect.handoffs(nil)
    assert {:ok, state} = Collect.handoffs(state)

    # done between two looks, in far less time than a look takes to come round
    set.([:kv_vnode])
    assert {:ok, state} = Collect.handoffs(state)

    assert {:ok, [%{node: from, data: data}]} = History.query("handoffs", key)
    assert from == Atom.to_string(node)

    assert %{
             "type" => "ownership",
             "module" => "kv_vnode",
             "src_partition" => 0,
             "to" => "new@h",
             "seen_complete" => true
           } = data

    # and an ownership handoff caught running adds its size to it
    Peer.put(node, :rcp_ring, ring)
    {:ok, _} = Collect.handoffs(state)
    assert {:ok, [_]} = History.query("handoffs", key)
  end

  test "handoffs: an ownership handoff caught running gives the ring's record its size",
       %{node: node, ring: ring, key: key} do
    set = fn mods ->
      Peer.put(
        node,
        :rcp_ring,
        :erpc.call(node, :riak_core_ring, :set_pending_changes, [
          ring,
          [{0, node, :new@h, mods, :awaiting}]
        ])
      )
    end

    set.([])
    Peer.put(node, :rcp_handoffs, [handoff(node, 0, :ownership)])
    assert {:ok, state} = Collect.handoffs(nil)

    # gone from the live handoffs, not yet marked done in the ring: nothing yet
    Peer.put(node, :rcp_handoffs, [])
    assert {:ok, state} = Collect.handoffs(state)
    assert {:ok, []} = History.query("handoffs", key)

    set.([:kv_vnode])
    assert {:ok, _} = Collect.handoffs(state)
    assert {:ok, [%{data: data}]} = History.query("handoffs", key)
    assert %{"handoffs" => 1, "size" => %{"n" => 10, "unit" => "objects"}} = data
  end

  test "cluster: member and ring changes are stored, not the first look",
       %{node: node, ring: ring, key: key} do
    assert {:ok, state} = Collect.cluster(nil)
    assert {:ok, []} = History.query("members", key)

    joined = :erpc.call(node, :riak_core_ring, :add_member, [node, ring, :new@h])
    Peer.put(node, :rcp_ring, joined)
    assert {:ok, state} = Collect.cluster(state)

    assert {:ok, [%{node: "new@h", data: %{"from" => nil, "to" => "joining", "up" => false}}]} =
             History.query("members", key)

    # nothing changed: nothing more
    assert {:ok, _} = Collect.cluster(state)
    assert {:ok, [_]} = History.query("members", key)
    assert {:ok, []} = History.query("ring", key)
  end

  test "load: one sample per call", %{node: node, key: key} do
    assert {:ok, nil} = Collect.load(nil)

    assert {:ok, [%{node: n, data: %{"avg_busy" => busy, "schedulers" => [_ | _]}}]} =
             History.query("load", key)

    assert n == Atom.to_string(node)
    assert is_float(busy)
  end

  test "an Admin action is stored with its result", %{node: node, key: key} do
    {:ok, target} = FakeTarget.start_link(node)

    socket =
      %Socket{
        view: AdminLive,
        transport_pid: self(),
        private: %{lifecycle: %Lifecycle{}, live_temp: %{}}
      }
      |> then(fn s -> elem(AdminLive.mount(%{}, %{"target" => target}, s), 1) end)
      |> await(:cluster)
      |> await(:plan)

    {:noreply, socket} =
      AdminLive.handle_event("leave", %{"node" => Atom.to_string(node)}, socket)

    await(socket, :action)

    assert {:ok, [%{node: n, data: data}]} = History.query("admin", key)
    assert n == Atom.to_string(node)

    assert data == %{
             "action" => "Leave of #{node} staged",
             "ok" => true,
             "error" => nil,
             "changes" => []
           }
  end

  test "the history tool returns the cluster's rows of a kind", %{node: node, key: key} do
    :ok = History.record_admin(key, node, "Plan committed", {:error, "refused"}, [{node, :leave}])

    assert {:ok, %{cluster: ^key, kind: "admin", rows: [row]}} =
             Tools.history(%{"kind" => "admin"})

    assert %{"ok" => false, "error" => "refused", "changes" => [change]} = row.data
    assert change == "#{node}: leave"
    assert {:ok, _} = Jason.encode(row)

    assert {:ok, %{rows: []}} = Tools.history(%{"kind" => "handoffs", "minutes" => 5})
    assert {:error, "kind must be one of" <> _} = Tools.history(%{"kind" => "nope"})

    assert {:error, "limit must be a positive integer" <> _} =
             Tools.history(%{"kind" => "ring", "limit" => 0})
  end

  test "the History page loads the selected kind, and another when it is picked",
       %{node: node, key: key} do
    :ok = History.record_admin(key, node, "Plan cleared", :ok)
    {:ok, target} = FakeTarget.start_link(node)

    socket =
      %Socket{
        view: RiakCorePlugin.HistoryLive,
        transport_pid: self(),
        private: %{lifecycle: %Lifecycle{}, live_temp: %{}}
      }
      |> then(fn s ->
        elem(RiakCorePlugin.HistoryLive.mount(%{}, %{"target" => target}, s), 1)
      end)
      |> await(:history)

    assert {:ok, %{kind: "handoffs", cluster: ^key, rows: []}} = socket.assigns.history

    {:noreply, socket} =
      RiakCorePlugin.HistoryLive.handle_event("kind", %{"kind" => "admin"}, socket)

    assert socket.assigns.history == nil
    socket = await(socket, :history)

    assert {:ok, %{kind: "admin", rows: [%{action: "Plan cleared", result: "done"}]}} =
             socket.assigns.history

    # a kind that isn't one is ignored
    {:noreply, same} = RiakCorePlugin.HistoryLive.handle_event("kind", %{"kind" => "x"}, socket)
    assert same.assigns.kind == "admin"
  end

  test "with the event store off: nothing is stored, and queries say why", %{key: key} do
    stop_supervised!(Beamdash.Store)
    assert {:off, "beamdash's event store is switched off"} = History.status()
    assert :ok = History.record("admin", key, [%{data: %{}}])

    assert {:error, "history is off: beamdash's event store is switched off"} =
             History.query("admin", key)

    assert {:error, "history is off" <> _} = Tools.history(%{"kind" => "admin"})
  end

  # Waits for the start_async result of `key` and hands it to the page.
  defp await(socket, key) do
    receive do
      {:phoenix, :async_result, {:start, {ref, nil, ^key, result}}} ->
        Async.handle_async(socket, nil, :start, key, ref, result)
    after
      5_000 -> flunk("no async result for #{inspect(key)}")
    end
  end
end
