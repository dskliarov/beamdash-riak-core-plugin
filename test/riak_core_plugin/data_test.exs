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
               progress: 0.25
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
end
