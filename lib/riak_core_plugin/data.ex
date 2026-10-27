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

  @vnode_columns [:module, :index, :pid, :mailbox, :memory, :reductions]

  @doc "The columns the vnodes table can be sorted by."
  def vnode_columns, do: @vnode_columns

  @doc """
  The vnodes table from `[{{module, index, pid}, info}]`, where `info` is
  `erlang:process_info(Pid, [message_queue_len, memory, reductions])` (or
  `undefined` for a vnode that has exited). Sorted by `{column, :asc | :desc}`;
  missing values always come last.
  """
  @spec vnode_rows([{{module(), integer(), pid()}, list() | :undefined}], {atom(), :asc | :desc}) ::
          [map()]
  def vnode_rows(fetched, {column, dir}) when column in @vnode_columns do
    rows =
      Enum.map(fetched, fn {{mod, idx, pid}, info} ->
        info = if is_list(info), do: info, else: []

        %{
          module: mod,
          index: idx,
          pid: pid,
          mailbox: info[:message_queue_len],
          memory: info[:memory],
          reductions: info[:reductions]
        }
      end)

    {present, missing} = Enum.split_with(rows, &(Map.fetch!(&1, column) != nil))
    sorted = Enum.sort_by(present, &{Map.fetch!(&1, column), &1.module, &1.index}, dir)
    sorted ++ Enum.sort_by(missing, &{&1.module, &1.index})
  end

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
      size: size(stats[:size] || props[:size]),
      progress: progress(stats[:pct_done_decimal])
    }
  end

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

  defp riak_module?({name, node}) when is_atom(name) and is_atom(node), do: riak_module?(name)

  defp riak_module?(mod) when is_atom(mod),
    do: String.starts_with?(Atom.to_string(mod), "riak_core")

  defp riak_module?(_), do: false
end
