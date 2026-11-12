defmodule RiakCorePlugin.DataTest do
  # Data shaping from terms recorded from riak_core_lite 0.11 (no riak_core
  # running, no node attached).
  use ExUnit.Case, async: true

  alias RiakCorePlugin.Data

  # pids can't be module attributes
  defp pid1, do: :c.pid(0, 101, 0)
  defp pid2, do: :c.pid(0, 102, 0)
  defp pid3, do: :c.pid(0, 103, 0)

  describe "owners_by_node/1" do
    test "counts partitions per node, sorted by node" do
      assert Data.owners_by_node([{0, :b}, {1, :a}, {2, :b}]) == [{:a, 1}, {:b, 2}]
      assert Data.owners_by_node([{0, :a}, {1, :b}, {2, :a}]) == [{:a, 2}, {:b, 1}]
      assert Data.owners_by_node([]) == []
    end
  end

  describe "scheduler_busy/2" do
    test "is the active share of the time between two reads, by scheduler" do
      before = [{2, 0, 1_000}, {1, 500, 1_000}]
      later = [{1, 1_000, 2_000}, {2, 250, 2_000}]
      assert Data.scheduler_busy(before, later) == [%{id: 1, busy: 50.0}, %{id: 2, busy: 25.0}]
    end

    test "a scheduler with no time in the window is 0%" do
      assert Data.scheduler_busy([{1, 5, 10}], [{1, 5, 10}]) == [%{id: 1, busy: 0.0}]
    end
  end

  describe "io_rates/3" do
    test "bytes per second from two reads" do
      before = {{:input, 1_000}, {:output, 500}}
      later = {{:input, 3_000}, {:output, 1_500}}
      assert Data.io_rates(before, later, 2_000) == %{input: 1_000, output: 500}
    end
  end

  describe "load_view/1" do
    test "shapes one sample for the Load page" do
      sample = %{
        elapsed: 1_000,
        schedulers: {[{1, 0, 1_000}, {2, 0, 1_000}], [{1, 750, 2_000}, {2, 250, 2_000}]},
        io: {{{:input, 0}, {:output, 0}}, {{:input, 2_048}, {:output, 1_024}}},
        memory: [total: 4_096, processes: 1_024, binary: 512],
        top: [
          {pid1(), 500,
           [registered_name: [], initial_call: {:gen_server, :loop, 7}, message_queue_len: 3]},
          {pid2(), 10, :undefined}
        ]
      }

      view = Data.load_view(sample)
      assert view.window_ms == 1_000
      assert view.avg_busy == 50.0
      assert view.hottest == %{id: 1, busy: 75.0}
      assert view.coolest == %{id: 2, busy: 25.0}
      assert view.io == %{input: 2_048, output: 1_024}

      assert [{"Total", 4_096}, {"Processes", 1_024}, {"System", 0}, {"Binaries", 512}, _, _] =
               view.memory

      assert [busy, exited] = view.processes
      assert busy == %{pid: pid1(), name: ":gen_server.loop/7", mailbox: 3, reductions_per_s: 500}
      assert exited == %{pid: pid2(), name: "exited", mailbox: nil, reductions_per_s: 10}
    end
  end

  describe "partition_rows/2" do
    test "sets next where a change is pending, sorted by index" do
      owners = [{2, :a}, {0, :a}, {1, :b}]
      # riak_core_ring:pending_changes/1: {Idx, Owner, NextOwner, Mods, Status}
      pending = [{1, :b, :c, [:my_vnode], :awaiting}]

      assert Data.partition_rows(owners, pending) == [
               %{index: 0, owner: :a, next: nil},
               %{index: 1, owner: :b, next: :c},
               %{index: 2, owner: :a, next: nil}
             ]
    end

    test "ignores pending entries it doesn't understand" do
      assert Data.partition_rows([{0, :a}], [:weird]) == [%{index: 0, owner: :a, next: nil}]
    end
  end

  describe "member_rows/3" do
    test "combines statuses, owner counts and the up list" do
      statuses = [{:b@h, :leaving}, {:a@h, :valid}, {:c@h, :joining}]
      counts = [{:a@h, 40}, {:b@h, 24}]
      up = [:a@h, :c@h, :other@h]

      assert Data.member_rows(statuses, counts, up) == [
               %{node: :a@h, status: :valid, partitions: 40, up: true},
               %{node: :b@h, status: :leaving, partitions: 24, up: false},
               %{node: :c@h, status: :joining, partitions: 0, up: true}
             ]
    end
  end

  describe "vnodes" do
    test "select_vnodes/3 sorts by module and index and caps the list" do
      vnodes = [{:b_vnode, 0, pid1()}, {:a_vnode, 5, pid2()}, {:a_vnode, 1, pid3()}]

      assert Data.select_vnodes(vnodes, "", 2) == %{
               taken: [{:a_vnode, 1, pid3()}, {:a_vnode, 5, pid2()}],
               total: 3,
               modules: [:a_vnode, :b_vnode],
               module: ""
             }

      assert %{taken: [_, _, _], total: 3} = Data.select_vnodes(vnodes, "", 256)
    end

    test "select_vnodes/3 filters by module before the cap; modules come from all" do
      vnodes = [{:b_vnode, 0, pid1()}, {:a_vnode, 5, pid2()}, {:a_vnode, 1, pid3()}]

      assert Data.select_vnodes(vnodes, "b_vnode", 1) == %{
               taken: [{:b_vnode, 0, pid1()}],
               total: 1,
               modules: [:a_vnode, :b_vnode],
               module: "b_vnode"
             }

      # a module that has no vnodes any more: the filter is dropped
      assert %{total: 3, module: ""} = Data.select_vnodes(vnodes, "gone_vnode", 256)
    end

    test "vnode_rows/2 combines all_vnodes with process_info and sorts by a column" do
      fetched = [
        {{:a_vnode, 0, pid1()}, [message_queue_len: 3, memory: 1000, reductions: 50]},
        {{:a_vnode, 1, pid2()}, [message_queue_len: 9, memory: 500, reductions: 70]},
        # the vnode exited between the two calls
        {{:b_vnode, 2, pid3()}, :undefined}
      ]

      rows = Data.vnode_rows(fetched, {:mailbox, :desc})

      assert Enum.map(rows, & &1.pid) == [pid2(), pid1(), pid3()]

      assert hd(rows) == %{
               module: :a_vnode,
               index: 1,
               pid: pid2(),
               mailbox: 9,
               memory: 500,
               reductions: 70
             }

      assert List.last(rows).mailbox == nil

      assert Enum.map(Data.vnode_rows(fetched, {:memory, :asc}), & &1.pid) ==
               [pid2(), pid1(), pid3()]

      assert Enum.map(Data.vnode_rows(fetched, {:module, :desc}), & &1.index) == [2, 1, 0]
    end
  end

  describe "handoff_rows/1" do
    test "a riak_core_handoff_manager:status/0 entry becomes a row" do
      status =
        {:status_v2,
         [
           mod: :my_vnode,
           src_partition: 0,
           target_partition: 0,
           src_node: :a@h,
           target_node: :b@h,
           direction: :outbound,
           status: :running,
           start_ts: {1_761, 400_000, 0},
           sender_pid: pid1(),
           stats: [
             objs_total: 10,
             objs_per_s: 5,
             bytes_per_s: 100,
             last_update: {1_761, 400_002, 0},
             size: {1000, :objects},
             pct_done_decimal: 0.25
           ],
           type: :ownership
         ]}

      assert [row] = Data.handoff_rows([status])

      assert row == %{
               known: true,
               type: :ownership,
               module: :my_vnode,
               src_partition: 0,
               target_partition: 0,
               from: :a@h,
               to: :b@h,
               direction: :outbound,
               status: :running,
               start: "2025-10-25T13:46:40Z",
               sender: pid1(),
               size: {1000, :objects},
               progress: 0.25,
               objs_total: 10,
               objs_per_s: 5,
               bytes_per_s: 100,
               eta_ms: 150_000
             }
    end

    test "missing keys and no_stats give nil, an old mod_src_tgt is read" do
      assert [row] =
               Data.handoff_rows([
                 [mod_src_tgt: {:m, 1, 2}, type: :hinted, stats: :no_stats, size: {5, :bytes}]
               ])

      assert %{known: true, module: :m, src_partition: 1, target_partition: 2, type: :hinted} =
               row

      assert %{size: {5, :bytes}, progress: nil, start: nil, from: nil, status: nil} = row
    end

    test "an unknown term becomes an inspected row" do
      assert Data.handoff_rows([{:something, :else}]) == [
               %{known: false, text: "{:something, :else}"}
             ]
    end
  end

  describe "pending_rows/1" do
    test "pending changes become rows" do
      assert Data.pending_rows([{0, :a, :b, [:my_vnode], :awaiting}, :odd]) == [
               %{index: 0, owner: :a, next: :b, modules: [:my_vnode], status: :awaiting},
               %{index: nil, owner: nil, next: nil, modules: [], status: ":odd"}
             ]
    end
  end

  describe "ring_ready/1" do
    test "riak_core_status:ringready/0 results as text" do
      assert Data.ring_ready({:ok, [:a, :b]}) == {:ok, "ready (2 nodes)"}
      assert Data.ring_ready({:error, {:nodes_down, [:b]}}) == {:error, "nodes down: b"}

      assert Data.ring_ready({:error, {:different_owners, :a, :b}}) ==
               {:error, "a and b see different owners"}

      assert Data.ring_ready({:error, :nope}) == {:error, ":nope"}
    end
  end

  test "member_details/1: share, moves, the node's own answer and ring agreement" do
    m = %{
      members: [{:a@h, :valid}, {:b@h, :valid}, {:c@h, :down}],
      counts: [{:a@h, 3}, {:b@h, 1}],
      up: [:a@h, :b@h],
      pending: [{1, :a@h, :b@h, [:m], :awaiting}, {2, :a@h, :b@h, [:m], :awaiting}],
      ring: 42,
      claimant: :b@h,
      nodes: %{a@h: %{ring: 42}, b@h: %{ring: 7}}
    }

    [a, b, c] = Data.member_details(m)
    assert {a.share, a.moving_out, a.moving_in, a.ring_agrees} == {75.0, 2, 0, true}
    assert {b.share, b.moving_out, b.moving_in, b.ring_agrees} == {25.0, 0, 2, false}
    assert {c.share, c.info, c.ring_agrees} == {0.0, nil, nil}
    assert {a.claimant, b.claimant, c.claimant} == {false, true, false}
  end

  test "uptime/1" do
    assert Data.uptime(45_000) == "45s"
    assert Data.uptime(725_000) == "12m 5s"
    assert Data.uptime(12_000_000) == "3h 20m"
    assert Data.uptime(352_800_000) == "4d 2h"
  end

  test "down_owners/1: owners not up, none when up isn't known" do
    ring = %{owners: [{0, :a@h}, {1, :b@h}, {2, :c@h}], up: [:a@h, :c@h]}
    assert Data.down_owners(ring) == [:b@h]
    assert Data.down_owners(Map.delete(ring, :up)) == []
  end

  describe "error_message/1" do
    test "undef for a riak_core module means riak_core isn't running" do
      undef = {:error, {:exception, :undef, [{:riak_core_ring_manager, :get_my_ring, [], []}]}}
      assert Data.error_message(undef) == "riak_core is not running"
      assert Data.error_message(:no_ring) == "riak_core is not running"

      noproc =
        {:exit,
         {:exception,
          {:noproc,
           {:gen_server, :call, [:riak_core_handoff_manager, {:status, :none}, :infinity]}}}}

      assert Data.error_message(noproc) == "riak_core is not running"
    end

    test "other errors are shown as they are" do
      assert Data.error_message(:not_connected) == "no node is attached"

      other = {:error, {:exception, :undef, [{:lists, :nope, [], []}]}}
      assert Data.error_message(other) =~ "lists"
      assert Data.error_message({:exit, :timeout}) == "{:exit, :timeout}"
    end

    test "a claimant that is down: what it blocks and what to do" do
      down =
        {:exit,
         {:exception,
          {{:nodedown, :kv1@h},
           {:gen_server, :call, [{:riak_core_claimant, :kv1@h}, :plan, :infinity]}}}}

      text = Data.error_message(down)
      assert text =~ "the claimant, kv1@h, is down"
      assert text =~ "mark it down"

      other =
        {:exit,
         {:exception,
          {{:nodedown, :kv2@h}, {:gen_server, :call, [{:riak_core_vnode_master, :kv2@h}, :x]}}}}

      assert Data.error_message(other) == "kv2@h is down"

      noproc =
        {:exit,
         {:exception, {:noproc, {:gen_server, :call, [{:riak_core_claimant, :kv1@h}, :plan]}}}}

      assert Data.error_message(noproc) == "riak_core is not running"
    end

    test "erpc timeouts and lost connections as text" do
      assert Data.error_message({:error, {:erpc, :timeout}}) == "timed out"
      assert Data.error_message({:error, {:erpc, :noconnection}}) == "the node is not reachable"
    end
  end

  describe "Components.name/1" do
    test "text is kept as it is; other terms are inspected" do
      alias RiakCorePlugin.Components
      assert Components.name(":odd") == ":odd"
      assert Components.name(:a@h) == "a@h"
      assert Components.name(nil) == ""
      assert Components.name({1, 2}) == "{1, 2}"
    end
  end

  test "join_candidates/2: up nodes that aren't members, sorted" do
    assert Data.join_candidates([:c@h, :a@h, :b@h], [{:a@h, :valid}]) == [:b@h, :c@h]
  end

  test "change_text/1" do
    assert Data.change_text(:join) == "join"
    assert Data.change_text(:remove) == "force remove"
    assert Data.change_text({:replace, :b@h}) == "replace with b@h"
    assert Data.change_text({:force_replace, :b@h}) == "force replace with b@h"
    assert Data.change_text({:resize, 128}) == "resize the ring to 128 partitions"
    assert Data.change_text(:odd) == ":odd"
  end

  test "transition_rows/1: every node of before or after, sorted" do
    assert Data.transition_rows({[a@h: 4, b@h: 4], [a@h: 3, b@h: 3, c@h: 2], 2}) == [
             %{node: :a@h, before: 4, after: 3},
             %{node: :b@h, before: 4, after: 3},
             %{node: :c@h, before: 0, after: 2}
           ]

    assert Data.transition_rows({[a@h: 8, b@h: 0], [a@h: 8], 0}) == [
             %{node: :a@h, before: 8, after: 8},
             %{node: :b@h, before: 0, after: 0}
           ]
  end

  test "admin_error/1" do
    assert Data.admin_error({:error, :is_up}) =~ "only a node that is down"
    assert Data.admin_error({:error, :plan_changed}) =~ "plan changed"
    assert Data.admin_error(:error) =~ "refused to commit"
    assert Data.admin_error({:badrpc, :nodedown}) =~ "not reachable"
    assert Data.admin_error({:error, :new_reason}) == ":new_reason"
    assert Data.admin_error(:weird) == ":weird"
  end

  describe "claimant_note/3" do
    @members [{:a@h, :valid}, {:b@h, :valid}, {:c@h, :leaving}, {:d@h, :valid}, {:j@h, :joining}]

    test "names the next valid or leaving member by name" do
      assert Data.claimant_note(:a@h, @members, [:a@h, :b@h, :c@h, :d@h]) ==
               {:ok, "If a@h leaves or is marked down, b@h becomes the claimant."}

      assert {:ok, text} = Data.claimant_note(:d@h, @members, [:a@h, :d@h])
      assert text =~ "a@h becomes the claimant"
    end

    test "members ahead that are down have to be marked down too" do
      assert {:ok, text} = Data.claimant_note(:a@h, @members, [:a@h, :d@h])
      assert text =~ "the role goes to b@h, c@h, which are down"
      assert text =~ "mark them down too for d@h to become the claimant"

      assert {:ok, text} = Data.claimant_note(:a@h, @members, [:a@h])
      assert text =~ "no other member is up"
    end

    test "a down claimant: mark it down, or start it again" do
      assert {:bad, text} = Data.claimant_note(:a@h, @members, [:b@h, :c@h])
      assert text =~ "a@h, the claimant, is down"
      assert text =~ "Marking it down on the Admin page makes b@h the claimant."

      assert {:bad, text} = Data.claimant_note(:a@h, @members, [:d@h])
      assert text =~ "Marking it down (and b@h, c@h, down too) on the Admin page makes d@h"

      assert {:bad, text} = Data.claimant_note(:a@h, @members, [:j@h])
      assert text =~ "start a@h again"
    end

    test "nothing when no one else could take over, or there is no claimant" do
      assert Data.claimant_note(:a@h, [{:a@h, :valid}, {:j@h, :joining}], [:a@h, :j@h]) == nil
      assert Data.claimant_note(:undefined, @members, [:a@h]) == nil
      assert Data.claimant_note(:a@h, @members, nil) == nil
    end
  end

  describe "history" do
    defp handoff(opts) do
      {:status_v2,
       Keyword.merge(
         [
           mod: :kv_vnode,
           src_partition: 0,
           target_partition: 0,
           src_node: :a@h,
           target_node: :b@h,
           direction: :outbound,
           status: :running,
           start_ts: {1_761, 400_000, 0},
           sender_pid: pid1(),
           stats: [size: {2048, :bytes}, pct_done_decimal: 0.5],
           type: :ownership
         ],
         opts
       )}
    end

    test "cluster_key: the cluster name as text, the same on every member" do
      assert Data.cluster_key({:a@h, {1_761, 400_000, 5}}) == "a@h 2025-10-25T13:46:40Z"
      assert Data.cluster_key(:undefined) == ":undefined"
    end

    test "cluster_handoffs: each handoff once, from its sender; who answered" do
      out = handoff([])
      inbound = handoff(direction: :inbound)
      other = handoff(src_partition: 1, target_partition: 1, start_ts: :undefined)

      {active, answered} =
        Data.cluster_handoffs(%{
          a@h: {:ok, [out, other]},
          b@h: {:ok, [inbound, {:junk}]},
          c@h: {:error, "no answer in time"}
        })

      assert answered == MapSet.new([:a@h, :b@h])
      assert map_size(active) == 2

      assert %{reporter: :a@h, direction: :outbound, start_ms: 1_761_400_000_000} =
               active[{:kv_vnode, 0, 0, :a@h, :b@h}]

      assert %{start_ms: nil} = active[{:kv_vnode, 1, 1, :a@h, :b@h}]
    end

    test "handoff_changes: a handoff gone from its answering reporter has ended" do
      {active, answered} = Data.cluster_handoffs(%{a@h: {:ok, [handoff([])]}, b@h: {:ok, []}})
      id = {:kv_vnode, 0, 0, :a@h, :b@h}

      # the first look: nothing has ended
      assert {[], tracked} = Data.handoff_changes(nil, active, answered, 1_000)
      assert %{^id => %{first_seen: 1_000, last_seen: 1_000}} = tracked

      # still running: first_seen is kept
      assert {[], tracked} = Data.handoff_changes(tracked, active, answered, 6_000)
      assert %{^id => %{first_seen: 1_000, last_seen: 6_000}} = tracked

      # gone, a@h answered: ended
      {active, answered} = Data.cluster_handoffs(%{a@h: {:ok, []}})
      assert active == %{}
      now = 1_761_400_090_000
      assert {[ended], tracked} = Data.handoff_changes(tracked, %{}, answered, now)
      assert tracked == %{}

      assert ended == %{
               type: :ownership,
               module: :kv_vnode,
               src_partition: 0,
               target_partition: 0,
               from: :a@h,
               to: :b@h,
               started_ms: 1_761_400_000_000,
               ended_ms: now,
               duration_ms: 90_000,
               last_seen_ms: 6_000,
               size: %{n: 2048, unit: :bytes},
               progress: 0.5
             }
    end

    test "handoff_changes: a reporter that didn't answer keeps its handoffs, for a while" do
      {active, answered} =
        Data.cluster_handoffs(%{a@h: {:ok, [handoff(start_ts: :undefined)]}})

      {[], tracked} = Data.handoff_changes(nil, active, answered, 0)

      assert {[], ^tracked} = Data.handoff_changes(tracked, %{}, MapSet.new([:b@h]), 60_000)
      assert {[], %{}} = Data.handoff_changes(tracked, %{}, MapSet.new(), 600_000)

      # with no start time, the duration is from when it was first seen
      assert {[%{started_ms: 0, duration_ms: 5_000}], _} =
               Data.handoff_changes(tracked, %{}, MapSet.new([:a@h]), 5_000)
    end

    test "split_ended: hinted handoffs are stored as they are, ring ones kept for the ring" do
      hinted = %{type: :hinted, module: :kv_vnode, src_partition: 0, from: :a@h, ended_ms: 10}
      owned = %{hinted | type: :ownership}
      old = %{owned | src_partition: 1, ended_ms: -3_600_000}

      assert {[^hinted], live} =
               Data.split_ended([hinted, owned], %{{:kv_vnode, 1, :a@h} => [old]}, 10)

      assert live == %{{:kv_vnode, 0, :a@h} => [owned]}
    end

    test "transfer_changes: a module the ring lists as done is a completed transfer, however short" do
      entry = fn mods, status -> {0, :a@h, :b@h, mods, status} end

      # the first look: what is already done isn't recorded
      assert {[], tracked, %{}} =
               Data.transfer_changes(
                 nil,
                 [entry.([:kv_vnode], :awaiting), {8, :a@h, :c@h, [], :awaiting}],
                 [:kv_vnode, :x_vnode],
                 %{},
                 0
               )

      assert {[], tracked, %{}} =
               Data.transfer_changes(
                 tracked,
                 [entry.([:kv_vnode], :awaiting), {8, :a@h, :c@h, [], :awaiting}],
                 [:kv_vnode, :x_vnode],
                 %{},
                 5_000
               )

      # x_vnode's transfer of 0 completed, and kv_vnode's of 8
      pending = [
        entry.([:kv_vnode, :x_vnode], :complete),
        {8, :a@h, :c@h, [:kv_vnode], :awaiting}
      ]

      assert {events, tracked, %{}} =
               Data.transfer_changes(tracked, pending, [:kv_vnode, :x_vnode], %{}, 10_000)

      assert Enum.sort(for e <- events, do: {e.src_partition, e.module, e.to}) ==
               [{0, :x_vnode, :b@h}, {8, :kv_vnode, :c@h}]

      assert Enum.all?(
               events,
               &match?(
                 %{
                   type: :ownership,
                   from: :a@h,
                   ended_ms: 10_000,
                   started_ms: 0,
                   handoffs: 0,
                   seen_complete: true
                 },
                 &1
               )
             )

      assert map_size(tracked) == 2

      # seen once: not again
      assert {[], _, _} =
               Data.transfer_changes(tracked, pending, [:kv_vnode, :x_vnode], %{}, 15_000)
    end

    test "transfer_changes: a live handoff caught gives its size and times; resize has no next owner" do
      live = %{
        {:kv_vnode, 0, :a@h} => [
          %{started_ms: 100, ended_ms: 900, size: %{n: 5, unit: :objects}, target_partition: 3},
          %{started_ms: 200, ended_ms: 800, size: %{n: 2, unit: :objects}, target_partition: 3}
        ],
        {:kv_vnode, 9, :a@h} => [%{started_ms: 1, ended_ms: 2, size: nil, target_partition: 9}]
      }

      tracked = %{{0, :a@h, :"$resize"} => %{first_seen: 500, done: []}}

      assert {[done], _, rest} =
               Data.transfer_changes(
                 tracked,
                 [{0, :a@h, :"$resize", [:kv_vnode], :complete}],
                 [:kv_vnode],
                 live,
                 5_000
               )

      assert %{type: :resize, to: nil, src_partition: 0, target_partition: 3} = done
      assert %{started_ms: 100, ended_ms: 900, duration_ms: 800, handoffs: 2} = done
      assert done.size == %{n: 7, unit: :objects}
      assert Map.keys(rest) == [{:kv_vnode, 9, :a@h}]
    end

    test "transfer_changes: an entry cleared before its modules were seen done" do
      tracked = %{{0, :a@h, :b@h} => %{first_seen: 0, done: [:kv_vnode]}}

      assert {[%{module: :x_vnode, seen_complete: false, ended_ms: 7_000}], tracked, _} =
               Data.transfer_changes(tracked, [], [:kv_vnode, :x_vnode], %{}, 7_000)

      assert tracked == %{}
    end

    @membership %{
      cluster: {:a@h, {1, 2, 3}},
      size: 8,
      claimant: :a@h,
      members: [{:a@h, :valid}, {:b@h, :valid}],
      pending: 0,
      counts: [{:a@h, 4}, {:b@h, 4}],
      up: [:a@h, :b@h]
    }

    test "cluster_snapshot: members with their up state; when pending changes began" do
      s0 = Data.cluster_snapshot(@membership, nil, 0)
      assert s0.members == %{a@h: {:valid, true}, b@h: {:valid, true}}
      assert s0.pending_since == nil

      s1 = Data.cluster_snapshot(%{@membership | pending: 3}, s0, 1_000)
      assert s1.pending_since == 1_000
      s2 = Data.cluster_snapshot(%{@membership | pending: 2}, s1, 2_000)
      assert s2.pending_since == 1_000
      assert Data.cluster_snapshot(@membership, s2, 3_000).pending_since == nil
    end

    test "member_changes: status and up/down changes, joins and removals" do
      before = Data.cluster_snapshot(@membership, nil, 0)

      later =
        Data.cluster_snapshot(
          %{@membership | members: [{:a@h, :leaving}, {:c@h, :joining}], up: [:a@h]},
          before,
          1
        )

      assert Data.member_changes(before, before) == []

      assert Data.member_changes(before, later) == [
               %{node: :a@h, from: :valid, to: :leaving, was_up: true, up: true},
               %{node: :b@h, from: :valid, to: nil, was_up: true, up: nil},
               %{node: :c@h, from: nil, to: :joining, was_up: nil, up: false}
             ]
    end

    test "ring_changes: claimant, size, transfers started and done, ownership" do
      s0 = Data.cluster_snapshot(@membership, nil, 0)
      assert Data.ring_changes(s0, s0, 0) == []

      moving = %{@membership | pending: 2, claimant: :b@h}
      s1 = Data.cluster_snapshot(moving, s0, 1_000)

      assert Data.ring_changes(s0, s1, 1_000) == [
               %{event: "claimant", from: :a@h, to: :b@h},
               %{
                 event: "transfers_started",
                 pending: 2,
                 partitions: [%{node: :a@h, partitions: 4}, %{node: :b@h, partitions: 4}]
               }
             ]

      done = %{moving | pending: 0, counts: [{:b@h, 8}]}
      s2 = Data.cluster_snapshot(done, s1, 61_000)

      assert Data.ring_changes(s1, s2, 61_000) == [
               %{
                 event: "transfers_done",
                 duration_ms: 60_000,
                 partitions: [%{node: :b@h, partitions: 8}]
               }
             ]

      removed = Data.cluster_snapshot(%{done | counts: [{:c@h, 8}], size: 16}, s2, 70_000)

      assert [
               %{event: "size", from: 8, to: 16},
               %{event: "ownership", before: [%{node: :b@h}], partitions: [%{node: :c@h}]}
             ] = Data.ring_changes(s2, removed, 70_000)
    end

    test "load_sample: the figures the history keeps of a load view" do
      view =
        Data.load_view(%{
          elapsed: 1_000,
          schedulers: {[{1, 0, 0}, {2, 0, 0}], [{1, 250, 1000}, {2, 750, 1000}]},
          io: {{{:input, 0}, {:output, 0}}, {{:input, 100}, {:output, 50}}},
          memory: [total: 4096, processes: 1],
          top: []
        })

      assert Data.load_sample(view) == %{
               avg_busy: 50.0,
               max_busy: 75.0,
               max_scheduler: 2,
               min_busy: 25.0,
               schedulers: [25.0, 75.0],
               input: 100,
               output: 50,
               memory: 4096
             }
    end

    test "history_rows: stored rows (JSON data) as text" do
      at = 1_761_400_000_000

      assert [
               %{
                 at: "2025-10-25 13:46:40 UTC",
                 partition: "0 → 1",
                 took: "1m 30s",
                 size: "2.0 KB",
                 from: "a@h"
               }
             ] =
               Data.history_rows("handoffs", [
                 %{
                   at: at,
                   data: %{
                     "src_partition" => 0,
                     "target_partition" => 1,
                     "from" => "a@h",
                     "duration_ms" => 90_000,
                     "size" => %{"n" => 2048, "unit" => "bytes"}
                   }
                 }
               ])

      assert [%{change: "valid → leaving, went down"}] =
               Data.history_rows("members", [
                 %{
                   at: at,
                   data: %{
                     "node" => "a@h",
                     "from" => "valid",
                     "to" => "leaving",
                     "was_up" => true,
                     "up" => false
                   }
                 }
               ])

      assert [%{change: "transfers done after 1m 0s; partitions: b@h 8"}] =
               Data.history_rows("ring", [
                 %{
                   at: at,
                   data: %{
                     "event" => "transfers_done",
                     "duration_ms" => 60_000,
                     "partitions" => [%{"node" => "b@h", "partitions" => 8}]
                   }
                 }
               ])

      assert [%{busiest: "2: 75.0%", avg_busy: 50.0, node: "a@h"}] =
               Data.history_rows("load", [
                 %{
                   at: at,
                   node: "a@h",
                   data: %{"avg_busy" => 50.0, "max_busy" => 75.0, "max_scheduler" => 2}
                 }
               ])

      assert [%{result: "done", changes: ["a@h: leave"]}, %{result: "failed: no"}] =
               Data.history_rows("admin", [
                 %{
                   at: at,
                   node: "a@h",
                   data: %{"action" => "x", "ok" => true, "changes" => ["a@h: leave"]}
                 },
                 %{at: at, node: "a@h", data: %{"action" => "y", "ok" => false, "error" => "no"}}
               ])
    end

    test "member_change_text: joins, removals, down and up" do
      assert Data.member_change_text(%{"from" => nil, "to" => "joining", "up" => true}) ==
               "joined (joining)"

      assert Data.member_change_text(%{"from" => "leaving", "to" => nil, "was_up" => true}) ==
               "left the ring (was leaving)"

      assert Data.member_change_text(%{
               "from" => "valid",
               "to" => "valid",
               "was_up" => false,
               "up" => true
             }) == "came back up"
    end
  end
end
