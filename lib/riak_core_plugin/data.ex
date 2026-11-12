defmodule RiakCorePlugin.Data do
  @moduledoc """
  Pure functions that shape riak_core rpc results into rows for the pages.
  Nothing here calls the attached node, so all of it is tested with terms
  recorded from riak_core_lite.
  """

  @doc """
  Partitions per node from `riak_core_ring:all_owners/1` (`[{index, node}]`):
  `[{node, count}]`, sorted by node.
  """
  @spec owners_by_node([{integer(), node()}]) :: [{node(), non_neg_integer()}]
  def owners_by_node(owners) do
    owners
    |> Enum.frequencies_by(fn {_idx, node} -> node end)
    |> Enum.sort()
  end

  @doc """
  The owners that are down: owners of `ring` (`RiakCorePlugin.Riak.ring/1`)
  not among its `up` nodes, sorted. None when `up` isn't known.
  """
  @spec down_owners(map()) :: [node()]
  def down_owners(%{owners: owners, up: up}) when is_list(up) do
    for {node, _count} <- owners_by_node(owners), node not in up, do: node
  end

  def down_owners(_ring), do: []

  @doc """
  The partition list: one `%{index, owner, next}` per owner, sorted by index.
  `next` is the next owner when `pending` (`riak_core_ring:pending_changes/1`,
  `[{index, owner, next_owner, modules, status}]`) has a change for it.
  """
  @spec partition_rows([{integer(), node()}], list()) :: [map()]
  def partition_rows(owners, pending) do
    next = for {idx, _owner, next, _mods, _status} <- pending, into: %{}, do: {idx, next}

    owners
    |> Enum.sort()
    |> Enum.map(fn {idx, owner} -> %{index: idx, owner: owner, next: Map.get(next, idx)} end)
  end

  @doc """
  The members table: `statuses` from `riak_core_ring:all_member_status/1`,
  `counts` from `owners_by_node/1` and `up`, the nodes the target sees
  (itself included). Sorted by node.
  """
  @spec member_rows([{node(), atom()}], [{node(), non_neg_integer()}], [node()]) :: [map()]
  def member_rows(statuses, counts, up) do
    counts = Map.new(counts)
    up = MapSet.new(up)

    statuses
    |> Enum.sort()
    |> Enum.map(fn {node, status} ->
      %{
        node: node,
        status: status,
        partitions: Map.get(counts, node, 0),
        up: MapSet.member?(up, node)
      }
    end)
  end

  @doc """
  The Members page's rows, from `RiakCorePlugin.Riak.members/1`: each
  `member_rows/3` row plus

    * `share`: its partitions as a % of the ring (one decimal);
    * `moving_out` / `moving_in`: partitions a committed change is moving
      away from it / to it (`pending_changes/1`);
    * `info`: what the node said about itself (see `Riak.members/1`), `nil`
      for a node that is down, or `{:error, text}`;
    * `ring_agrees`: whether its ring is the target's (`nil` unknown);
    * `claimant`: whether it is the ring's claimant (`m.claimant`).
  """
  @spec member_details(map()) :: [map()]
  def member_details(%{members: members, counts: counts, up: up} = m) do
    total = counts |> Enum.map(&elem(&1, 1)) |> Enum.sum()
    pending = Map.get(m, :pending, [])
    nodes = Map.get(m, :nodes, %{})

    members
    |> member_rows(counts, up)
    |> Enum.map(fn row ->
      info = Map.get(nodes, row.node)

      Map.merge(row, %{
        share: if(total > 0, do: Float.round(row.partitions * 100 / total, 1), else: 0.0),
        moving_out: Enum.count(pending, fn {_, owner, _, _, _} -> owner == row.node end),
        moving_in: Enum.count(pending, fn {_, _, next, _, _} -> next == row.node end),
        info: info,
        ring_agrees: if(is_map(info) and m[:ring], do: info.ring == m.ring),
        claimant: row.node == m[:claimant]
      })
    end)
  end

  @doc "A duration in ms as text: `45s`, `12m 5s`, `3h 20m`, `4d 2h`."
  @spec uptime(non_neg_integer()) :: String.t()
  def uptime(ms) when is_integer(ms) do
    s = div(ms, 1000)

    cond do
      s >= 86_400 -> "#{div(s, 86_400)}d #{div(rem(s, 86_400), 3600)}h"
      s >= 3600 -> "#{div(s, 3600)}h #{div(rem(s, 3600), 60)}m"
      s >= 60 -> "#{div(s, 60)}m #{rem(s, 60)}s"
      true -> "#{s}s"
    end
  end

  @doc """
  The vnodes to fetch process info for, from
  `riak_core_vnode_manager:all_vnodes/0` (`[{module, index, pid}]`):

    * `taken`: the vnodes of `module` (a module name, or `""` for all),
      sorted by module and index, the first `cap` of them;
    * `total`: how many vnodes `module` has (all of them for `""`);
    * `modules`: the modules of all vnodes, sorted;
    * `module`: the filter that was applied. A module that has no vnodes
      any more is dropped (`""`).
  """
  @spec select_vnodes([{module(), integer(), pid()}], String.t(), pos_integer()) :: map()
  def select_vnodes(vnodes, module, cap) do
    modules = vnodes |> Enum.map(fn {mod, _idx, _pid} -> mod end) |> Enum.uniq() |> Enum.sort()
    module = if Enum.any?(modules, &(Atom.to_string(&1) == module)), do: module, else: ""

    selected =
      if module == "",
        do: vnodes,
        else: Enum.filter(vnodes, fn {mod, _, _} -> Atom.to_string(mod) == module end)

    sorted = Enum.sort_by(selected, fn {mod, idx, _pid} -> {mod, idx} end)
    %{taken: Enum.take(sorted, cap), total: length(selected), modules: modules, module: module}
  end

  @vnode_columns [:module, :index, :pid, :type, :mode, :mailbox, :memory, :reductions]

  @doc "The columns the vnodes table can be sorted by."
  def vnode_columns, do: @vnode_columns

  @doc """
  The vnodes table from `RiakCorePlugin.Riak.vnodes/2` rows (or the older
  `{{module, index, pid}, info}` shape). Each row has `module`, `index`,
  `pid`, `type`, `mode`, `target`, `mailbox`, `memory` and `reductions`.
  `mode` is `:active`, `:forward`, `:handoff` or `:not_started`; `target` is
  the forwarding or handoff node. Sorted by `{column, :asc | :desc}`;
  missing values always come last.
  """
  @spec vnode_rows([map() | {{module(), integer(), pid()}, list() | :undefined}], {
          atom(),
          :asc | :desc
        }) :: [map()]
  def vnode_rows(fetched, {column, dir}) when column in @vnode_columns do
    rows = Enum.map(fetched, &vnode_row/1)

    {present, missing} = Enum.split_with(rows, &(Map.fetch!(&1, column) != nil))
    sorted = Enum.sort_by(present, &{Map.fetch!(&1, column), &1.module, &1.index}, dir)
    sorted ++ Enum.sort_by(missing, &{&1.module, &1.index})
  end

  defp vnode_row(%{module: mod, index: idx, pid: pid} = row) do
    info = if is_list(row[:info]), do: row[:info], else: []
    {mode, target} = vnode_state(row)

    %{
      module: mod,
      index: idx,
      pid: pid,
      type: row[:type],
      mode: mode,
      target: target,
      mailbox: info[:message_queue_len],
      memory: info[:memory],
      reductions: info[:reductions]
    }
  end

  defp vnode_row({{mod, idx, pid}, info}) do
    info = if is_list(info), do: info, else: []

    %{
      module: mod,
      index: idx,
      pid: pid,
      type: nil,
      mode: nil,
      target: nil,
      mailbox: info[:message_queue_len],
      memory: info[:memory],
      reductions: info[:reductions]
    }
  end

  defp vnode_state(%{forwarding: node}) when not is_nil(node), do: {:forward, node}
  defp vnode_state(%{should_handoff: node}) when not is_nil(node), do: {:handoff, node}
  defp vnode_state(%{pid: nil}), do: {:not_started, nil}
  defp vnode_state(_row), do: {:active, nil}

  @doc """
  The active handoffs table from `riak_core_handoff_manager:status/0`. Each
  entry is `{status_v2, Proplist}` (or a bare proplist). The keys that are
  present are read: `mod`, `src_partition`, `target_partition` (or an older
  `mod_src_tgt`), `src_node`, `target_node`, `direction`, `status`, `type`,
  `start_ts`, `sender_pid`, and the size and progress from `stats` (or a
  top-level `size`).
  An entry of another shape becomes `%{known: false, text: inspected}`.
  """
  @spec handoff_rows(list()) :: [map()]
  def handoff_rows(statuses), do: Enum.map(statuses, &handoff_row/1)

  defp handoff_row({:status_v2, props}) when is_list(props), do: handoff_row(props)

  defp handoff_row(props) when is_list(props) and props != [] do
    if Enum.all?(props, &match?({key, _} when is_atom(key), &1)),
      do: known_handoff(props),
      else: unknown(props)
  end

  defp handoff_row(other), do: unknown(other)

  defp known_handoff(props) do
    {mod, src, tgt} =
      case props[:mod_src_tgt] do
        {m, s, t} -> {m, s, t}
        _ -> {props[:mod], props[:src_partition], props[:target_partition]}
      end

    stats = if is_list(props[:stats]), do: props[:stats], else: []
    size = size(stats[:size] || props[:size])
    progress = progress(stats[:pct_done_decimal])

    %{
      known: true,
      type: props[:type],
      module: mod,
      src_partition: src,
      target_partition: tgt,
      from: props[:src_node],
      to: props[:target_node],
      direction: props[:direction],
      status: props[:status],
      start: timestamp(props[:start_ts]),
      sender: props[:sender_pid],
      size: size,
      progress: progress,
      objs_total: non_neg_int(stats[:objs_total]),
      objs_per_s: non_neg_int(stats[:objs_per_s]),
      bytes_per_s: non_neg_int(stats[:bytes_per_s]),
      eta_ms: eta_ms(size, progress, stats)
    }
  end

  defp non_neg_int(n) when is_integer(n) and n >= 0, do: n
  defp non_neg_int(_), do: nil

  # Remaining work at the reported rate; nil until the rate and size are known.
  defp eta_ms({total, unit}, progress, stats)
       when is_integer(total) and total > 0 and is_number(progress) and progress < 1 do
    rate =
      case unit do
        :objects -> stats[:objs_per_s]
        :bytes -> stats[:bytes_per_s]
        _ -> nil
      end

    if is_number(rate) and rate > 0,
      do: round(total * (1 - progress) / rate * 1000)
  end

  defp eta_ms(_size, _progress, _stats), do: nil

  defp unknown(term), do: %{known: false, text: inspect(term)}

  defp size({n, unit}) when is_integer(n) and unit in [:bytes, :objects], do: {n, unit}
  defp size(_), do: nil

  defp progress(p) when is_number(p), do: p
  defp progress(_), do: nil

  @doc "An `erlang:timestamp()` as ISO 8601 UTC text, or nil."
  @spec timestamp(term()) :: String.t() | nil
  def timestamp({mega, sec, micro})
      when is_integer(mega) and is_integer(sec) and is_integer(micro) do
    case DateTime.from_unix(mega * 1_000_000 + sec) do
      {:ok, dt} -> DateTime.to_iso8601(dt)
      _ -> nil
    end
  end

  def timestamp(_), do: nil

  @doc """
  The pending changes table from `riak_core_ring:pending_changes/1`. An entry
  of another shape keeps its text in `status`.
  """
  @spec pending_rows(list()) :: [map()]
  def pending_rows(pending) do
    Enum.map(pending, fn
      {idx, owner, next, mods, status} ->
        %{
          index: idx,
          owner: owner,
          next: next,
          modules: if(is_list(mods), do: mods, else: [mods]),
          status: status
        }

      other ->
        %{index: nil, owner: nil, next: nil, modules: [], status: inspect(other)}
    end)
  end

  @doc """
  `riak_core_status:ringready/0`'s result as `{:ok | :error, text}`.
  """
  @spec ring_ready(term()) :: {:ok | :error, String.t()}
  def ring_ready({:ok, nodes}) when is_list(nodes), do: {:ok, "ready (#{plural(nodes)})"}

  def ring_ready({:error, {:nodes_down, down}}) when is_list(down),
    do: {:error, "nodes down: " <> Enum.map_join(down, ", ", &to_string/1)}

  def ring_ready({:error, {:different_owners, n1, n2}}),
    do: {:error, "#{n1} and #{n2} see different owners"}

  def ring_ready({:error, reason}), do: {:error, inspect(reason)}
  def ring_ready(other), do: {:error, inspect(other)}

  defp plural([_]), do: "1 node"
  defp plural(nodes), do: "#{length(nodes)} nodes"

  @doc """
  The nodes that could join: up as seen from the target, and not members.
  Sorted.
  """
  @spec join_candidates([node()], [{node(), atom()}]) :: [node()]
  def join_candidates(up, members) do
    members = MapSet.new(members, fn {node, _status} -> node end)
    up |> Enum.reject(&MapSet.member?(members, &1)) |> Enum.sort()
  end

  @doc """
  Who takes over from `claimant`, as `{:ok | :bad, text}` (`:bad` when the
  claimant is down), or `nil` when no other member could. There is no call
  to pick the claimant: riak_core gives the role to the first valid or
  leaving member by name once the claimant is no longer one
  (`riak_core_claimant:maybe_update_claimant/2`), and only that member can
  take it, so the members ahead of it that are down have to be marked down
  first. `members` is `riak_core_ring:all_member_status/1`, `up` the nodes
  the target sees (`nil` when that isn't known).
  """
  @spec claimant_note(node() | :undefined, [{node(), atom()}], [node()] | nil) ::
          {:ok | :bad, String.t()} | nil
  def claimant_note(claimant, members, up) when is_list(up) do
    eligible = for {node, status} <- Enum.sort(members), status in [:valid, :leaving], do: node

    if claimant in eligible do
      {blocked, rest} = Enum.split_while(eligible -- [claimant], &(&1 not in up))
      claimant_text(claimant, claimant in up, blocked, List.first(rest))
    end
  end

  def claimant_note(_claimant, _members, _up), do: nil

  defp claimant_text(_claimant, true, [], nil), do: nil

  defp claimant_text(claimant, true, [], next),
    do: {:ok, "If #{claimant} leaves or is marked down, #{next} becomes the claimant."}

  defp claimant_text(claimant, true, blocked, next) do
    {:ok,
     "If #{claimant} leaves or is marked down, the role goes to #{Enum.join(blocked, ", ")}, " <>
       "which #{if match?([_], blocked), do: "is", else: "are"} down: " <>
       if(next,
         do: "mark #{them(blocked)} down too for #{next} to become the claimant.",
         else: "no other member is up to take over."
       )}
  end

  defp claimant_text(claimant, false, _blocked, nil) do
    {:bad,
     "#{claimant}, the claimant, is down, so cluster changes can't be planned or committed. " <>
       "No other member is up to take over: start #{claimant} again."}
  end

  defp claimant_text(claimant, false, blocked, next) do
    {:bad,
     "#{claimant}, the claimant, is down, so cluster changes can't be planned or committed. " <>
       "Marking it down" <>
       if(blocked == [], do: "", else: " (and #{Enum.join(blocked, ", ")}, down too)") <>
       " on the Admin page makes #{next} the claimant."}
  end

  defp them([_]), do: "it"
  defp them(_), do: "them"

  @doc """
  A staged change from `riak_core_claimant:plan/0` (`{node, action}`) as
  text, such as `"leave"` or `"replace with b@h"`.
  """
  @spec change_text(term()) :: String.t()
  def change_text(:join), do: "join"
  def change_text(:leave), do: "leave"
  def change_text(:remove), do: "force remove"
  def change_text({:replace, node}), do: "replace with #{node}"
  def change_text({:force_replace, node}), do: "force replace with #{node}"
  def change_text({:resize, size}), do: "resize the ring to #{size} partitions"
  def change_text(:abort_resize), do: "abort the ring resize"
  def change_text(other), do: inspect(other)

  @doc """
  A plan transition, `{before, after, transfers}` with partitions per node
  before and after it: one `%{node, before, after}` per node in either,
  sorted by node.
  """
  @spec transition_rows({list(), list(), non_neg_integer()}) :: [map()]
  def transition_rows({before, after_, _transfers}) do
    before = Map.new(before)
    after_ = Map.new(after_)

    (Map.keys(before) ++ Map.keys(after_))
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.map(&%{node: &1, before: Map.get(before, &1, 0), after: Map.get(after_, &1, 0)})
  end

  @doc """
  The Cluster Health view from `riak_core_status:ring_status/0`,
  `transfers/0` and `RiakCorePlugin.Riak.coverage/1`:
  `%{claimant, ring_ready, down, marked_down, transfer_status,
  pending_transfers, waiting, stopped, coverage, alerts}`.
  `alerts` are `%{level: :bad | :warn | :ok, text}`.
  """
  @spec health(term(), term(), map()) :: map()
  def health(
        {claimant, ring_ready, down, marked_down, transfer_status},
        {_transfer_down, transfers},
        coverage
      ) do
    waiting = for {:waiting_to_handoff, node, n} <- transfers, do: %{node: node, count: n}
    stopped = for {:stopped, node, n} <- transfers, do: %{node: node, count: n}

    pending =
      for {{owner, next}, entries} <- transfer_status,
          {index, waiting_modules, done_modules, status} <- entries do
        %{
          owner: owner,
          next: next,
          index: index,
          waiting_modules: waiting_modules,
          done_modules: done_modules,
          status: status
        }
      end

    %{
      claimant: claimant,
      ring_ready: ring_ready,
      down: down,
      marked_down: marked_down,
      transfer_status: transfer_status,
      transfers: pending,
      pending_transfers: length(pending),
      waiting: waiting,
      stopped: stopped,
      coverage: coverage,
      alerts: health_alerts(ring_ready, down, marked_down, waiting, stopped, coverage)
    }
  end

  defp health_alerts(ring_ready, down, marked_down, waiting, stopped, coverage) do
    [
      ring_ready == :undefined &&
        %{level: :bad, text: "The claimant did not answer; ring state is unknown."},
      ring_ready == false && %{level: :bad, text: "The ring is not ready."},
      down != [] &&
        %{level: :bad, text: "#{length(down)} member(s) are down: #{Enum.join(down, ", ")}."},
      marked_down != [] &&
        %{
          level: :warn,
          text:
            "#{length(marked_down)} member(s) are marked down: #{Enum.join(marked_down, ", ")}."
        },
      coverage.under_count > 0 &&
        %{
          level: :bad,
          text:
            "#{coverage.under_count} partition(s) have fewer than #{coverage.n_val} up replicas."
        },
      waiting != [] &&
        %{
          level: :warn,
          text: "Handoffs waiting: #{Enum.map_join(waiting, ", ", &"#{&1.node} #{&1.count}")}."
        },
      stopped != [] &&
        %{
          level: :bad,
          text: "Handoffs stopped: #{Enum.map_join(stopped, ", ", &"#{&1.node} #{&1.count}")}."
        }
    ]
    |> Enum.filter(& &1)
    |> case do
      [] -> [%{level: :ok, text: "No immediate ring-health warnings."}]
      alerts -> alerts
    end
  end

  @admin_errors %{
    self_join: "a node can't join itself",
    not_reachable: "the node is not reachable from the attached node",
    unable_to_get_join_ring: "the joining node could not get the cluster's ring",
    node_still_starting: "the node is still starting",
    not_single_node: "the node is already in a cluster of its own; it must be a fresh node",
    different_ring_sizes: "the node's ring size differs from the cluster's",
    not_member: "it is not a member of the cluster",
    only_member: "it is the only member of the cluster",
    already_leaving: "it is already leaving",
    is_claimant: "it is the claimant; it can't be force removed or force replaced",
    is_up: "it is up; only a node that is down can be marked down",
    already_replacement: "it is already the replacement of another node",
    invalid_replacement: "the replacement must be a joining node that owns no partitions",
    same_size: "the ring already has that size",
    single_node: "a ring with a single node can't be resized",
    pending_changes: "ownership changes are still pending",
    not_resizing: "the ring isn't being resized",
    nothing_planned: "nothing is planned: plan again before committing",
    ring_not_ready: "the ring isn't ready (a change is still in progress)",
    invalid_resize_claim: "the resize can't be claimed",
    plan_changed: "the plan changed since it was made: review the new plan and commit again"
  }

  @doc """
  The text for a refused admin call: riak_core's `{error, Reason}` (or a
  plain `error` from `commit/0`, or `{badrpc, Reason}` from a join).
  """
  @spec admin_error(term()) :: String.t()
  def admin_error({:error, reason}) when is_map_key(@admin_errors, reason),
    do: Map.fetch!(@admin_errors, reason)

  def admin_error(:error), do: "the claimant refused to commit; plan again and retry"
  def admin_error({:badrpc, :nodedown}), do: Map.fetch!(@admin_errors, :not_reachable)
  def admin_error({:badrpc, :timeout}), do: "the joining node didn't answer in time"

  def admin_error({:badrpc, {:EXIT, {:undef, [{:riak_core, :staged_join, _, _} | _]}}}),
    do: "riak_core is not running on the joining node"

  def admin_error({:error, reason}), do: inspect(reason)
  def admin_error(other), do: inspect(other)

  @doc """
  The text for a failed rpc (the reason from `Beamdash.Plugin.rpc/5`).
  `undef` for a riak_core module, a riak_core server that isn't running, or
  no ring, all mean "riak_core is not running". An erpc timeout or lost
  connection is said in words. Other reasons are shown as they are.
  """
  @spec error_message(term()) :: String.t()
  def error_message(:not_connected), do: "no node is attached"
  def error_message(:no_ring), do: "riak_core is not running"
  def error_message({:error, {:erpc, :timeout}}), do: "timed out"
  def error_message({:error, {:erpc, :noconnection}}), do: "the node is not reachable"

  def error_message({:error, {:exception, :undef, [{mod, _fun, _args, _loc} | _]}} = reason) do
    if riak_module?(mod), do: "riak_core is not running", else: inspect(reason)
  end

  def error_message({:exit, {:exception, {:noproc, {:gen_server, :call, [server | _]}}}} = reason) do
    if riak_module?(server), do: "riak_core is not running", else: inspect(reason)
  end

  # riak_core_claimant's calls go to the claimant node, wherever the
  # attached node is
  def error_message(
        {:exit,
         {:exception, {{:nodedown, node}, {:gen_server, :call, [{:riak_core_claimant, _} | _]}}}}
      ),
      do:
        "the claimant, #{node}, is down. Changes are staged, planned and committed " <>
          "through the claimant: start #{node} again, or mark it down (Members, above) " <>
          "so that another member becomes the claimant"

  def error_message(
        {:exit, {:exception, {{:nodedown, node}, {:gen_server, :call, [server | _]}}}} = reason
      ) do
    if riak_module?(server), do: "#{node} is down", else: inspect(reason)
  end

  def error_message(reason), do: inspect(reason)

  @doc """
  The Load page's view of one sample from `RiakCorePlugin.Riak.load/2`:
  `%{window_ms, schedulers, avg_busy, hottest, coolest, io, memory,
  processes}`. `schedulers` are `[%{id, busy}]` (busy as a percentage of
  wall time); `io` is `%{input, output}` bytes per second; `memory` is
  `[{label, bytes}]`; `processes` are the busiest by reductions.
  """
  @spec load_view(map()) :: map()
  def load_view(%{
        elapsed: ms,
        schedulers: {before, later},
        io: {io1, io2},
        memory: memory,
        top: top
      }) do
    schedulers = scheduler_busy(before, later)
    busy = Enum.map(schedulers, & &1.busy)

    %{
      window_ms: ms,
      schedulers: schedulers,
      avg_busy: Enum.sum(busy) / length(busy),
      hottest: Enum.max_by(schedulers, & &1.busy),
      coolest: Enum.min_by(schedulers, & &1.busy),
      io: io_rates(io1, io2, ms),
      memory: memory_rows(memory),
      processes: Enum.map(top, &process_row(&1, ms))
    }
  end

  @doc """
  Each scheduler's busy percentage over the window, from two reads of
  `erlang:statistics(scheduler_wall_time)`: `[{id, active, total}]`.
  """
  @spec scheduler_busy([{pos_integer(), integer(), integer()}], [
          {pos_integer(), integer(), integer()}
        ]) ::
          [%{id: pos_integer(), busy: float()}]
  def scheduler_busy(before, later) do
    earlier = Map.new(before, fn {id, active, total} -> {id, {active, total}} end)

    later
    |> Enum.map(fn {id, active, total} ->
      {active0, total0} = Map.get(earlier, id, {active, total})
      span = total - total0
      busy = if span > 0, do: (active - active0) / span * 100, else: 0.0
      %{id: id, busy: busy}
    end)
    |> Enum.sort_by(& &1.id)
  end

  @doc "Bytes per second in and out, from two reads of `erlang:statistics(io)` `ms` apart."
  @spec io_rates(term(), term(), non_neg_integer()) :: %{input: integer(), output: integer()}
  def io_rates({{:input, in1}, {:output, out1}}, {{:input, in2}, {:output, out2}}, ms) do
    %{input: per_second(in2 - in1, ms), output: per_second(out2 - out1, ms)}
  end

  @memory_labels [
    total: "Total",
    processes: "Processes",
    system: "System",
    binary: "Binaries",
    ets: "ETS",
    code: "Code"
  ]

  @doc "The `erlang:memory/0` figures the Load page shows, as `{label, bytes}`."
  @spec memory_rows(keyword()) :: [{String.t(), non_neg_integer()}]
  def memory_rows(memory) do
    for {key, label} <- @memory_labels, do: {label, Keyword.get(memory, key, 0)}
  end

  defp process_row({pid, reductions, info}, ms) do
    %{
      pid: pid,
      name: process_name(info),
      mailbox: if(is_list(info), do: info[:message_queue_len]),
      reductions_per_s: per_second(reductions, ms)
    }
  end

  defp process_name(:undefined), do: "exited"

  defp process_name(info) do
    case {info[:registered_name], info[:initial_call]} do
      {name, _} when is_atom(name) -> Atom.to_string(name)
      {_, {m, f, a}} -> Exception.format_mfa(m, f, a)
      _ -> nil
    end
  end

  defp per_second(n, ms), do: if(ms > 0, do: round(n * 1000 / ms), else: 0)

  ## History (RiakCorePlugin.History, RiakCorePlugin.Collect)

  @doc """
  A cluster's key in the history: its name (`riak_core_ring:cluster_name/1`,
  the node and time the cluster's first ring was made) as text. It is the
  same on every member, so a cluster's history is one whichever member
  beamdash is attached to.
  """
  @spec cluster_key(term()) :: String.t()
  def cluster_key({node, {_, _, _} = ts}) when is_atom(node),
    do: "#{node} #{timestamp(ts) || inspect(ts)}"

  def cluster_key(other), do: inspect(other)

  @doc """
  Each handoff of the cluster once, from
  `RiakCorePlugin.Riak.cluster_handoffs/1`'s `nodes`: `{active, answered}`.
  `active` is `%{id => row}`, `id` being `{module, src_partition,
  target_partition, from, to}` and `row` a `handoff_rows/1` row with
  `reporter` (the node that reported it) and `start_ms`. Only the sender's
  entries (outbound, or of no direction) are taken: the receiver reports
  the same handoff as inbound. `answered` are the nodes that answered.
  """
  @spec cluster_handoffs(map()) :: {map(), MapSet.t()}
  def cluster_handoffs(nodes) do
    answered = for {node, {:ok, _}} <- nodes, into: MapSet.new(), do: node

    active =
      for {node, {:ok, statuses}} <- nodes,
          status <- statuses,
          %{known: true} = row <- [handoff_row(status)],
          row.direction != :inbound,
          into: %{} do
        row = Map.merge(row, %{reporter: node, start_ms: start_ms(status)})
        {handoff_id(row), row}
      end

    {active, answered}
  end

  defp handoff_id(row),
    do: {row.module, row.src_partition, row.target_partition, row.from, row.to}

  defp start_ms({:status_v2, props}), do: start_ms(props)

  defp start_ms(props) when is_list(props) do
    case props[:start_ts] do
      {mega, sec, micro} when is_integer(mega) and is_integer(sec) and is_integer(micro) ->
        (mega * 1_000_000 + sec) * 1000 + div(micro, 1000)

      _ ->
        nil
    end
  end

  # A handoff that isn't seen for this long, its reporter not answering
  # meanwhile, is forgotten without being recorded.
  @handoff_stale_ms 600_000

  @doc """
  The handoffs that ended between two looks at the cluster: `{ended,
  tracked}`. `previous` is the `tracked` of the previous look (`nil` at
  first: then nothing has ended), `active` and `answered` are from
  `cluster_handoffs/1`, `now` is unix ms. A handoff gone from `active` has
  ended only when the node that reported it answered this time; otherwise
  it is kept (for up to 10 minutes). `ended` are the events to store (see
  `RiakCorePlugin.History`); `ended_ms` is the time of the look that no
  longer saw it.
  """
  @spec handoff_changes(map() | nil, map(), MapSet.t(), integer()) :: {[map()], map()}
  def handoff_changes(previous, active, answered, now) do
    previous = previous || %{}

    seen =
      Map.new(active, fn {id, row} ->
        first = with %{first_seen: t} <- previous[id], do: t, else: (_ -> now)
        {id, %{row: row, first_seen: first, last_seen: now}}
      end)

    {ended, missing} =
      previous
      |> Map.drop(Map.keys(active))
      |> Enum.split_with(fn {_id, t} -> MapSet.member?(answered, t.row.reporter) end)

    tracked =
      for {id, t} <- missing, now - t.last_seen < @handoff_stale_ms, into: seen, do: {id, t}

    {ended |> Enum.map(fn {_id, t} -> ended_handoff(t, now) end) |> Enum.sort_by(& &1.started_ms),
     tracked}
  end

  defp ended_handoff(%{row: row} = t, now) do
    started = row.start_ms || t.first_seen

    %{
      type: row.type,
      module: row.module,
      src_partition: row.src_partition,
      target_partition: row.target_partition,
      from: row.from,
      to: row.to,
      started_ms: started,
      ended_ms: now,
      duration_ms: now - started,
      last_seen_ms: t.last_seen,
      size: json_size(row.size),
      progress: row.progress
    }
  end

  defp json_size({n, unit}), do: %{n: n, unit: unit}
  defp json_size(nil), do: nil

  # Handoff types the ring's pending changes record (`transfer_changes/5`);
  # the others (hinted, repair) are recorded from the live handoffs alone.
  @ring_types [:ownership, :resize]

  @doc """
  Splits ended live handoffs (`handoff_changes/4`) into those to store as
  they are (hinted, repair) and those the ring records (ownership, resize),
  which are kept in `live` by `{module, partition, from}` to add their size
  and start to the ring's record (`transfer_changes/5`). Kept ones older than
  an hour are dropped.
  """
  @spec split_ended([map()], map() | nil, integer()) :: {[map()], map()}
  def split_ended(ended, live, now) do
    {ring, direct} = Enum.split_with(ended, &(&1.type in @ring_types))

    live =
      for {k, hs} <- live || %{},
          hs = Enum.filter(hs, &(now - &1.ended_ms < 3_600_000)),
          hs != [],
          into: %{},
          do: {k, hs}

    live =
      Enum.reduce(ring, live, fn h, acc ->
        Map.update(acc, {h.module, h.src_partition, h.from}, [h], &[h | &1])
      end)

    {direct, live}
  end

  @doc """
  The ownership, resize and delete transfers that completed between two
  looks at the ring's pending changes (`riak_core_ring:pending_changes/1`,
  `[{index, owner, next_owner, done_modules, status}]`): `{done, tracked,
  live}`. riak_core adds a module to an entry's done modules when its
  transfer completes and keeps the entry until the claimant clears it, so a
  transfer is seen done even when it took far less than the time between
  looks. `previous` is the `tracked` of the previous look (`nil` at first:
  then what is already done isn't recorded); `modules` are the vnode
  modules; `live` the ended live handoffs from `split_ended/3`, whose size
  and times are used (and dropped from `live`) when they match.

  An entry cleared from the ring with modules not seen done (the claimant
  finished it between looks, or the change was cleared or aborted) records
  those modules with `seen_complete: false`.
  """
  @spec transfer_changes(map() | nil, list(), [module()], map(), integer()) ::
          {[map()], map(), map()}
  def transfer_changes(previous, pending, modules, live, now) do
    first? = previous == nil
    previous = previous || %{}

    {done, tracked} =
      Enum.reduce(pending, {[], %{}}, fn
        {idx, owner, next, mods, _status}, {done, tracked} when is_list(mods) ->
          tid = {idx, owner, next}
          t = previous[tid] || %{first_seen: now, done: if(first?, do: mods, else: [])}
          new = for mod <- mods -- t.done, do: {tid, mod, t.first_seen, true}
          {new ++ done, Map.put(tracked, tid, %{t | done: Enum.uniq(t.done ++ mods)})}

        _other, acc ->
          acc
      end)

    cleared =
      for {tid, t} <- previous,
          not Map.has_key?(tracked, tid),
          mod <- modules -- t.done,
          do: {tid, mod, t.first_seen, false}

    {events, live} =
      Enum.map_reduce(Enum.reverse(done) ++ cleared, live, fn {tid, mod, first_seen, seen},
                                                              live ->
        {idx, owner, _next} = tid
        {hs, live} = Map.pop(live, {mod, idx, owner}, [])
        {transfer_done(tid, mod, first_seen, seen, hs, now), live}
      end)

    {Enum.sort_by(events, & &1.ended_ms), tracked, live}
  end

  defp transfer_done({idx, owner, next}, mod, first_seen, seen, handoffs, now) do
    started = Enum.min([first_seen | for(h <- handoffs, do: h.started_ms)])
    ended = if handoffs == [], do: now, else: Enum.max(for h <- handoffs, do: h.ended_ms)

    %{
      type: transfer_type(next),
      module: mod,
      src_partition: idx,
      target_partition: transfer_target(next, idx, handoffs),
      from: owner,
      to: if(transfer_type(next) == :ownership, do: next),
      started_ms: started,
      ended_ms: ended,
      duration_ms: ended - started,
      size: handoffs |> Enum.map(& &1.size) |> sum_sizes(),
      progress: nil,
      handoffs: length(handoffs),
      seen_complete: seen
    }
  end

  # The next owner is a node, or a marker: '$resize', '$delete'.
  defp transfer_type(:"$resize"), do: :resize
  defp transfer_type(:"$delete"), do: :delete

  defp transfer_type(next) when is_atom(next) do
    if String.starts_with?(Atom.to_string(next), "$"), do: next, else: :ownership
  end

  defp transfer_type(_next), do: :ownership

  defp transfer_target(next, idx, handoffs) do
    case {transfer_type(next), Enum.uniq(for h <- handoffs, do: h.target_partition)} do
      {:ownership, _} -> idx
      {_, [target]} -> target
      _ -> nil
    end
  end

  defp sum_sizes([%{unit: unit} | _] = sizes) do
    if Enum.all?(sizes, &match?(%{unit: ^unit}, &1)),
      do: %{n: Enum.sum(for s <- sizes, do: s.n), unit: unit}
  end

  defp sum_sizes(_), do: nil

  @doc """
  What the history keeps of `RiakCorePlugin.Riak.membership/1` at `now`:
  `%{members, size, claimant, pending, counts, pending_since}`, `members`
  being `%{node => {status, up?}}`. `pending_since` is when the pending
  changes were first seen (kept from `previous`), nil while there are none.
  """
  @spec cluster_snapshot(map(), map() | nil, integer()) :: map()
  def cluster_snapshot(m, previous, now) do
    since =
      cond do
        m.pending == 0 -> nil
        previous != nil and previous.pending > 0 -> previous.pending_since
        true -> now
      end

    %{
      members: Map.new(m.members, fn {node, status} -> {node, {status, node in m.up}} end),
      size: m.size,
      claimant: m.claimant,
      pending: m.pending,
      counts: m.counts,
      pending_since: since
    }
  end

  @doc """
  The member changes between two snapshots (`cluster_snapshot/3`):
  `[%{node, from, to, was_up, up}]`, `from`/`to` being the member status
  (nil when not a member) and `was_up`/`up` whether the target saw it up.
  """
  @spec member_changes(map(), map()) :: [map()]
  def member_changes(before, later) do
    (Map.keys(before.members) ++ Map.keys(later.members))
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.flat_map(fn node ->
      case {before.members[node], later.members[node]} do
        {same, same} ->
          []

        {b, l} ->
          {from, was_up} = b || {nil, nil}
          {to, up} = l || {nil, nil}
          [%{node: node, from: from, to: to, was_up: was_up, up: up}]
      end
    end)
  end

  @doc """
  The ring changes between two snapshots at `now`, as events (`event`
  is the kind of change):

    * `claimant`, `size`: `from`, `to`;
    * `transfers_started`: pending changes appeared (`pending`, how many
      partitions; `partitions`, per node, before they move);
    * `transfers_done`: they are all done (`duration_ms` since they were
      first seen; `partitions` per node now);
    * `ownership`: the partitions per node changed with nothing pending
      before or after, as a force remove does (`before`, `partitions`).
  """
  @spec ring_changes(map(), map(), integer()) :: [map()]
  def ring_changes(before, later, now) do
    [
      before.claimant != later.claimant &&
        %{event: "claimant", from: before.claimant, to: later.claimant},
      before.size != later.size && %{event: "size", from: before.size, to: later.size},
      (before.pending == 0 and later.pending > 0) &&
        %{
          event: "transfers_started",
          pending: later.pending,
          partitions: json_counts(later.counts)
        },
      (before.pending > 0 and later.pending == 0) &&
        %{
          event: "transfers_done",
          duration_ms: now - before.pending_since,
          partitions: json_counts(later.counts)
        },
      (before.pending == 0 and later.pending == 0 and before.counts != later.counts) &&
        %{
          event: "ownership",
          before: json_counts(before.counts),
          partitions: json_counts(later.counts)
        }
    ]
    |> Enum.filter(& &1)
  end

  defp json_counts(counts), do: for({node, n} <- counts, do: %{node: node, partitions: n})

  @doc """
  One load sample for the history, from `load_view/1`: `%{avg_busy,
  max_busy, max_scheduler, min_busy, schedulers, input, output, memory}`,
  busy as percentages to one decimal, `schedulers` each scheduler's busy in
  id order, `memory` the node's total bytes.
  """
  @spec load_sample(map()) :: map()
  def load_sample(load) do
    %{
      avg_busy: Float.round(load.avg_busy * 1.0, 1),
      max_busy: Float.round(load.hottest.busy * 1.0, 1),
      max_scheduler: load.hottest.id,
      min_busy: Float.round(load.coolest.busy * 1.0, 1),
      schedulers: Enum.map(load.schedulers, &Float.round(&1.busy * 1.0, 1)),
      input: load.io.input,
      output: load.io.output,
      memory: with({_, bytes} <- List.keyfind(load.memory, "Total", 0), do: bytes)
    }
  end

  @doc "A unix ms time as `2026-11-11 10:00:05 UTC`."
  @spec time_text(integer()) :: String.t()
  def time_text(ms) when is_integer(ms),
    do: ms |> DateTime.from_unix!(:millisecond) |> Calendar.strftime("%Y-%m-%d %H:%M:%S UTC")

  @doc """
  The History page's rows for stored rows of `kind`, as text. The rows come
  from `RiakCorePlugin.History.query/3`: their `data` has string keys, and
  atoms and nodes are strings.
  """
  @spec history_rows(String.t(), [map()]) :: [map()]
  def history_rows(kind, rows), do: Enum.map(rows, &history_row(kind, &1))

  defp history_row("handoffs", %{data: d} = row) do
    %{
      at: time_text(row.at),
      type: d["type"],
      module: d["module"],
      partition:
        if(d["target_partition"] in [nil, d["src_partition"]],
          do: "#{d["src_partition"]}",
          else: "#{d["src_partition"]} → #{d["target_partition"]}"
        ),
      from: d["from"],
      to: d["to"] || d["type"],
      took: took(d),
      size: history_size(d["size"])
    }
  end

  defp history_row("members", %{data: d} = row),
    do: %{at: time_text(row.at), node: d["node"], change: member_change_text(d)}

  defp history_row("ring", %{data: d} = row),
    do: %{at: time_text(row.at), change: ring_change_text(d)}

  defp history_row("load", %{data: d} = row) do
    %{
      at: time_text(row.at),
      at_ms: row.at,
      node: row.node,
      avg_busy: d["avg_busy"],
      max_busy: d["max_busy"],
      busiest: "#{d["max_scheduler"]}: #{d["max_busy"]}%",
      input: d["input"],
      output: d["output"],
      memory: d["memory"]
    }
  end

  defp history_row("admin", %{data: d} = row) do
    %{
      at: time_text(row.at),
      node: row.node,
      action: d["action"],
      result: if(d["ok"], do: "done", else: "failed: #{d["error"]}"),
      changes: d["changes"] || []
    }
  end

  defp history_row(_kind, row), do: %{at: time_text(row.at), text: inspect(row.data)}

  # Under a second when the ring recorded it between two looks without a
  # live handoff seen: the time is only known to the look.
  defp took(%{"duration_ms" => ms} = d) when is_integer(ms) do
    cond do
      d["seen_complete"] == false -> "#{uptime(ms)} (cleared before seen done)"
      ms < 1000 -> "under 1s"
      true -> uptime(ms)
    end
  end

  defp took(_), do: nil

  defp history_size(%{"n" => n, "unit" => "bytes"}), do: Beamdash.Plugin.Format.bytes(n)
  defp history_size(%{"n" => n, "unit" => unit}), do: "#{n} #{unit}"
  defp history_size(_), do: nil

  @doc "A stored member change (`member_changes/2`) in words."
  @spec member_change_text(map()) :: String.t()
  def member_change_text(d) do
    status =
      case {d["from"], d["to"]} do
        {nil, to} -> "joined (#{to})"
        {from, nil} -> "left the ring (was #{from})"
        {same, same} -> nil
        {from, to} -> "#{from} → #{to}"
      end

    up =
      case {d["was_up"], d["up"]} do
        {true, false} -> "went down"
        {false, true} -> "came back up"
        {nil, false} -> "down"
        _ -> nil
      end

    [status, up] |> Enum.reject(&is_nil/1) |> Enum.join(", ")
  end

  @doc "A stored ring change (`ring_changes/3`) in words."
  @spec ring_change_text(map()) :: String.t()
  def ring_change_text(%{"event" => "claimant"} = d), do: "claimant #{d["from"]} → #{d["to"]}"

  def ring_change_text(%{"event" => "size"} = d),
    do: "ring size #{d["from"]} → #{d["to"]} partitions"

  def ring_change_text(%{"event" => "transfers_started"} = d),
    do: "#{d["pending"]} partition transfers pending; partitions: #{counts_text(d["partitions"])}"

  def ring_change_text(%{"event" => "transfers_done"} = d),
    do:
      "transfers done after #{uptime(d["duration_ms"])}; partitions: #{counts_text(d["partitions"])}"

  def ring_change_text(%{"event" => "ownership"} = d),
    do:
      "ownership changed without transfers: #{counts_text(d["before"])} → #{counts_text(d["partitions"])}"

  def ring_change_text(d), do: inspect(d)

  defp counts_text(counts) when is_list(counts),
    do: Enum.map_join(counts, ", ", &"#{&1["node"]} #{&1["partitions"]}")

  defp counts_text(_), do: ""

  defp riak_module?({name, node}) when is_atom(name) and is_atom(node), do: riak_module?(name)

  defp riak_module?(mod) when is_atom(mod),
    do: String.starts_with?(Atom.to_string(mod), "riak_core")

  defp riak_module?(_), do: false
end
