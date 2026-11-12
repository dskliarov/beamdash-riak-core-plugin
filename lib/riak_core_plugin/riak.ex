defmodule RiakCorePlugin.Riak do
  @moduledoc """
  The riak_core calls, all made on the attached node with
  `Beamdash.Plugin.rpc/5`. Each returns `{:ok, data}` or `{:error, message}`;
  see `RiakCorePlugin.Data.error_message/1` for the messages.

  The ring is an opaque record and can be large. It isn't fetched: the
  `riak_core_ring` accessors run on the target, in one rpc of
  `erl_eval:exprs/2` (stdlib) with Erlang expressions parsed here at compile
  time. Abstract code is plain data, so nothing of the plugin's own has to
  exist on the target, and only the accessors' results cross the wire.

  The functions, arities and return shapes are those of riak_core_lite 0.11
  (shared with riak_core).
  """

  alias RiakCorePlugin.Data

  @timeout 5_000

  # riak_core_status:ringready/0 calls every member (with a 30 s timeout of
  # its own), so it gets longer than that.
  @ready_timeout 35_000

  # The most vnodes whose process info is fetched per refresh.
  @max_vnodes 256

  # Expressions that run `body` with `R` bound to the target's ring, as
  # `{ok, Value}`, or `{no_ring, Other}` when get_my_ring/0 fails. The fun
  # keeps `R` out of the bindings erl_eval returns.
  parse = fn body ->
    src =
      "(fun({ok, R}) -> {ok, #{body}}; (Other) -> {no_ring, Other} end)" <>
        "(riak_core_ring_manager:get_my_ring())."

    {:ok, tokens, _} = :erl_scan.string(String.to_charlist(src))
    {:ok, exprs} = :erl_parse.parse_exprs(tokens)
    exprs
  end

  parse_plain = fn src ->
    {:ok, tokens, _} = :erl_scan.string(String.to_charlist(src))
    {:ok, exprs} = :erl_parse.parse_exprs(tokens)
    exprs
  end

  @ring_exprs parse.("""
              {riak_core_ring:num_partitions(R), riak_core_ring:claimant(R),
               riak_core_ring:all_owners(R), riak_core_ring:pending_changes(R),
               riak_core_ring:all_member_status(R),
               application:get_env(riak_core, target_n_val, 4),
               riak_core_ring:cluster_name(R), riak_core_ring:ring_version(R),
               riak_core_ring_manager:get_ring_id(),
               riak_core_ring_manager:is_stable_ring(),
               riak_core_ring:get_meta(riak_core_ring_tainted, R),
               riak_core_ring:check_lastgasp(R),
               riak_core_ring:is_resizing(R),
               riak_core_ring:future_num_partitions(R),
               riak_core_ring:is_resize_complete(R)}
              """)

  # For each partition whose owner isn't in `Up`: the fallback the owner's
  # requests go to with `N` replicas (riak_core_apl:get_apl_ann/4 for a key
  # that lands in the partition), and the partitions each up node runs a
  # vnode for, to tell a fallback that has started from one that hasn't.
  # `N` and `Up` come in as bindings.
  @fallback_exprs parse.("""
                  (fun(Max) ->
                     Down = [I || {I, O} <- riak_core_ring:all_owners(R),
                                  not lists:member(O, Up)],
                     Fallbacks =
                       [{I, [F || {{I2, F}, fallback} <-
                                    riak_core_apl:get_apl_ann(<<((I - 1 + Max) rem Max):160>>,
                                                              N, R, Up),
                                  I2 =:= I]}
                        || I <- Down],
                     Running =
                       [{Node, [Idx || {_, Idx, _} <- Vnodes]}
                        || {Node, {ok, Vnodes}} <-
                             lists:zip(Up, erpc:multicall(Up, riak_core_vnode_manager,
                                                          all_vnodes, [], 5000))],
                     {Fallbacks, Running}
                   end)(1 bsl 160)
                  """)

  # N replicas per partition among the up nodes: how many partitions have
  # fewer than N of them, a bounded sample of those, and replicas per node.
  @coverage_exprs parse.("""
                  (fun(Up) ->
                     N = application:get_env(riak_core, target_n_val, 4),
                     Owners = riak_core_ring:all_owners(R),
                     Pre = riak_core_ring:all_preflists(R, N),
                     Per = [{Idx, lists:usort([Node || {_, Node} <- PL,
                                                       lists:member(Node, Up)])}
                            || {{Idx, _}, PL} <- lists:zip(Owners, Pre)],
                     Under = [{Idx, Nodes} || {Idx, Nodes} <- Per, length(Nodes) < N],
                     Load = lists:foldl(fun({_, Nodes}, D) ->
                                lists:foldl(fun(Node, D2) ->
                                                    orddict:update_counter(Node, 1, D2)
                                            end, D, Nodes)
                            end, orddict:new(), Per),
                     {length(Owners), N, length(Under), lists:sublist(Under, 50), Load}
                  end)(Up)
                  """)

  @members_exprs parse.("""
                 {riak_core_ring:all_member_status(R),
                  lists:foldl(fun({_, N}, D) -> orddict:update_counter(N, 1, D) end,
                              orddict:new(), riak_core_ring:all_owners(R)),
                  riak_core_ring:pending_changes(R),
                  erlang:phash2({riak_core_ring:all_owners(R),
                                 riak_core_ring:pending_changes(R),
                                 riak_core_ring:all_member_status(R)}),
                  riak_core_ring:claimant(R)}
                 """)

  # Run on each up member (erpc:multicall from the target): what only that
  # node knows. `ring` hashes the same parts of its ring as @members_exprs,
  # so a node whose ring differs from the target's shows up.
  @node_exprs parse.("""
              (fun(Me) ->
                 Owned = [I || {I, N} <- riak_core_ring:all_owners(R), N =:= Me],
                 Vnodes = riak_core_vnode_manager:all_vnodes(),
                 Running = lists:usort([I || {_, I, _} <- Vnodes]),
                 \#{vnodes => length(Vnodes),
                   not_owned => length(Running -- Owned),
                   handoffs => length(riak_core_handoff_manager:status()),
                   ring => erlang:phash2({riak_core_ring:all_owners(R),
                                          riak_core_ring:pending_changes(R),
                                          riak_core_ring:all_member_status(R)}),
                   services => case catch riak_core_node_watcher:services(Me) of
                                 S when is_list(S) -> S;
                                 _ -> undefined
                               end,
                   riak_core => case lists:keyfind(riak_core, 1,
                                                   application:which_applications()) of
                                  {_, _, V} -> list_to_binary(V);
                                  false -> undefined
                                end,
                   otp => list_to_binary(erlang:system_info(otp_release)),
                   uptime_ms => element(1, erlang:statistics(wall_clock)),
                   memory => erlang:memory(total),
                   processes => erlang:system_info(process_count),
                   run_queue => erlang:statistics(run_queue),
                   ports => erlang:system_info(port_count),
                   ets => erlang:system_info(ets_count),
                   atoms => erlang:system_info(atom_count),
                   context_switches => element(1, erlang:statistics(context_switches)),
                   gc_count => element(1, erlang:statistics(garbage_collection)),
                   connected => length(erlang:nodes(connected))}
               end)(node())
              """)

  # How long each member has to answer for the Members page.
  @node_timeout 4_000

  # The Load page's sample: ms between the two reads, and the time the reads
  # of every process take on top of it.
  @load_window 1_000
  @load_slack 3_000

  @pending_exprs parse.("riak_core_ring:pending_changes(R)")

  # One rpc: the reductions of every process are read twice on the target, and
  # only the top ones come back. `Window` (ms) is bound by the caller. The
  # scheduler list also holds the dirty CPU schedulers, which are left out.
  @load_exprs parse_plain.("""
              (fun() ->
                 erlang:system_flag(scheduler_wall_time, true),
                 Normal = fun(L) -> [S || {I, _, _} = S <- L, I =< erlang:system_info(schedulers)] end,
                 Reductions = fun() ->
                                maps:from_list([{P, R} || P <- erlang:processes(),
                                                          {reductions, R} <- [erlang:process_info(P, reductions)]])
                              end,
                 Sched1 = Normal(erlang:statistics(scheduler_wall_time)),
                 Io1 = erlang:statistics(io),
                 Reds1 = Reductions(),
                 T1 = erlang:monotonic_time(millisecond),
                 timer:sleep(Window),
                 Sched2 = Normal(erlang:statistics(scheduler_wall_time)),
                 Io2 = erlang:statistics(io),
                 Reds2 = Reductions(),
                 T2 = erlang:monotonic_time(millisecond),
                 Top = lists:sublist(lists:reverse(lists:keysort(2,
                         [{P, R - maps:get(P, Reds1, 0)} || {P, R} <- maps:to_list(Reds2)])), 20),
                 {T2 - T1, Sched1, Sched2, Io1, Io2, erlang:memory(),
                  [{P, D, erlang:process_info(P, [registered_name, initial_call, message_queue_len])}
                   || {P, D} <- Top]}
               end)().
              """)

  @cluster_exprs parse.("""
                 {riak_core_ring:cluster_name(R),
                  riak_core_ring:num_partitions(R), riak_core_ring:claimant(R),
                  riak_core_ring:all_member_status(R),
                  lists:foldl(fun({_, N}, D) -> orddict:update_counter(N, 1, D) end,
                              orddict:new(), riak_core_ring:all_owners(R))}
                 """)

  @cluster_name_exprs parse.("riak_core_ring:cluster_name(R)")

  # The history's view of the cluster (RiakCorePlugin.Collect.cluster/1).
  @membership_exprs parse.("""
                    {riak_core_ring:cluster_name(R), riak_core_ring:num_partitions(R),
                     riak_core_ring:claimant(R), riak_core_ring:all_member_status(R),
                     length(riak_core_ring:pending_changes(R)),
                     lists:foldl(fun({_, N}, D) -> orddict:update_counter(N, 1, D) end,
                                 orddict:new(), riak_core_ring:all_owners(R))}
                    """)

  # riak_core_handoff_manager:status/0 of each member the target sees, in
  # one erpc:multicall/5 from the target; the ring's pending changes, whose
  # done modules record each transfer that completed; the vnode modules.
  @cluster_handoffs_exprs parse.("""
                          (fun(Members) ->
                             {riak_core_ring:cluster_name(R),
                              lists:zip(Members,
                                        erpc:multicall(Members, riak_core_handoff_manager,
                                                       status, [], 4000)),
                              riak_core_ring:pending_changes(R),
                              case catch riak_core:vnode_modules() of
                                L when is_list(L) -> [M || {_, M} <- L];
                                _ -> []
                              end}
                           end)([N || N <- riak_core_ring:all_members(R),
                                      lists:member(N, erlang:nodes([this, visible]))])
                          """)

  # riak_core_claimant:plan/0, with each ring transition summed up on the
  # target: partitions per node before and after it, and its transfers.
  # Rings are large and opaque, so none of them comes back.
  @plan_exprs parse_plain.("""
              (fun(Count) ->
                 case riak_core_claimant:plan() of
                   {ok, Changes, Transitions} ->
                     {ok, Changes,
                      [{Count(riak_core_ring:all_owners(Before)),
                        Count(riak_core_ring:all_owners(riak_core_ring:future_ring(After))),
                        length(riak_core_ring:pending_changes(After))}
                       || {Before, After} <- Transitions]};
                   Other -> Other
                 end
               end)(fun(Owners) ->
                      lists:foldl(fun({_, N}, D) -> orddict:update_counter(N, 1, D) end,
                                  orddict:new(), Owners)
                    end).
              """)

  # The claimant's calls wait for the claimant (infinity): give them time.
  @admin_timeout 30_000
  # A join is asked of the joining node, by the target, with this timeout.
  @join_timeout 15_000
  # After a join, how long (and how often) its replace is asked again while
  # the claimant hasn't heard of the join yet.
  @join_replace_wait 10_000
  @join_replace_poll 250
  # How long a visible node has to say whether it runs riak_core.
  @candidate_timeout 2_000

  @doc "The most vnodes whose process info is fetched per refresh."
  def max_vnodes, do: @max_vnodes

  @doc """
  The ring: `%{size, claimant, owners, pending, members, n_val, up,
  fallbacks}`, from `riak_core_ring:num_partitions/1`, `claimant/1`,
  `all_owners/1` (`[{index, node}]`), `pending_changes/1`,
  `all_member_status/1` (`[{node, status}]`) and riak_core's
  `target_n_val` setting on the attached node (`n_val`; 4 when unset) in one
  rpc, and `up_nodes/1`.

  `fallbacks` is `%{index => {node | nil, running?}}` for the partitions
  whose owner is down: the node riak_core sends the owner's requests to
  with `n_val` replicas (`riak_core_apl:get_apl_ann/4` with the up nodes;
  `nil` when there is none), and whether a vnode for the partition runs
  there yet (fallback vnodes start on the first request). One more rpc,
  only when an owner is down. With `fallbacks: false` that rpc is never
  made and `fallbacks` is `nil`.
  """
  @spec ring(GenServer.server(), keyword()) :: {:ok, map()} | {:error, String.t()}
  def ring(target, opts \\ []) do
    with {:ok,
          {size, claimant, owners, pending, members, n_val, cluster_name, ring_version, ring_id,
           stable, tainted_meta, lastgasp, resizing, future_size,
           resize_complete}} <-
           eval_ring(target, @ring_exprs),
         {:ok, up} <- up_nodes(target),
         {:ok, fallbacks} <-
           fallbacks(target, owners, up, n_val, Keyword.get(opts, :fallbacks, true)) do
      {:ok,
       %{
         size: size,
         claimant: claimant,
         owners: owners,
         pending: pending,
         members: members,
         n_val: n_val,
         up: up,
         fallbacks: fallbacks,
         cluster_name: cluster_name,
         ring_version: ring_version,
         ring_id: ring_id,
         stable: stable,
         tainted: match?({:ok, true}, tainted_meta),
         lastgasp: lastgasp,
         resizing: resizing,
         future_size: future_size,
         resize_complete: resize_complete
       }}
    end
  end

  defp fallbacks(_target, _owners, _up, _n_val, false), do: {:ok, nil}

  defp fallbacks(target, owners, up, n_val, true) do
    if Enum.all?(owners, fn {_idx, node} -> node in up end) do
      {:ok, %{}}
    else
      bindings = [{:N, n_val}, {:Up, up}]

      with {:ok, {fallbacks, running}} <- eval_ring(target, @fallback_exprs, bindings) do
        running = Map.new(running, fn {node, idxs} -> {node, MapSet.new(idxs)} end)

        {:ok,
         Map.new(fallbacks, fn {idx, nodes} ->
           node = List.first(nodes)

           {idx,
            {node, node != nil and MapSet.member?(Map.get(running, node, MapSet.new()), idx)}}
         end)}
      end
    end
  end

  @doc """
  Replica coverage: `%{partitions, n_val, under_count, under, replica_load,
  up}`. For each partition, `riak_core_ring:all_preflists/2` gives its N
  successors; only those the target sees up are counted, so a partition
  with fewer than N unique up nodes is under-replicated. `under` is a
  bounded sample of `{index, nodes}`, and `replica_load` counts replicas
  per up node.
  """
  @spec coverage(GenServer.server()) :: {:ok, map()} | {:error, String.t()}
  def coverage(target) do
    with {:ok, up} <- up_nodes(target),
         {:ok, {partitions, n_val, under_count, under, replica_load}} <-
           eval_ring(target, @coverage_exprs, [{:Up, up}]) do
      {:ok,
       %{
         partitions: partitions,
         n_val: n_val,
         under_count: under_count,
         under: under,
         replica_load: replica_load,
         up: up
       }}
    end
  end

  @doc """
  `riak_core_status:ring_status/0`: claimant identity, whether the ring is
  ready, down and marked-down members, and the per-transfer module status,
  with a #{@ready_timeout} ms timeout because it calls every member.
  """
  @spec ring_status(GenServer.server()) :: {:ok, term()} | {:error, String.t()}
  def ring_status(target) do
    case Beamdash.Plugin.rpc(:riak_core_status, :ring_status, [], @ready_timeout, target: target) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, Data.error_message(reason)}
    end
  end

  @doc "`riak_core_status:transfers/0`: down nodes and waiting/stopped partitions per node."
  @spec transfers(GenServer.server()) :: {:ok, term()} | {:error, String.t()}
  def transfers(target), do: call(target, :riak_core_status, :transfers, [])

  @doc """
  The Cluster Health view: `riak_core_status:ring_status/0`,
  `transfers/0` and `coverage/1`, shaped by `RiakCorePlugin.Data.health/3`.
  """
  @spec health(GenServer.server()) :: {:ok, map()} | {:error, String.t()}
  def health(target) do
    with {:ok, status} <- ring_status(target),
         {:ok, transfers} <- transfers(target),
         {:ok, coverage} <- coverage(target) do
      {:ok, Data.health(status, transfers, coverage)}
    end
  end

  @doc """
  What the Members page needs: `%{members, counts, up, pending, ring,
  claimant, nodes}`, from `riak_core_ring:all_member_status/1`, partitions
  per node counted on the target from `all_owners/1` (`[{node, count}]`,
  sorted by node), `up_nodes/1`, `pending_changes/1`, a hash of the
  target's ring (`ring`) and `claimant/1`.

  `nodes` is `%{node => info | {:error, text}}` for the members that are
  up, asked by the target in one `erpc:multicall/5` (#{@node_timeout} ms
  each): `%{vnodes, not_owned, handoffs, ring, services, riak_core, otp,
  uptime_ms, memory, processes}`. `not_owned` counts the partitions it runs
  a vnode for but doesn't own (fallbacks, or handoffs still to do);
  `ring` is the hash of its own ring, to compare with the target's.
  """
  @spec members(GenServer.server()) :: {:ok, map()} | {:error, String.t()}
  def members(target) do
    with {:ok, {members, counts, pending, ring, claimant}} <- eval_ring(target, @members_exprs),
         {:ok, up} <- up_nodes(target),
         {:ok, nodes} <- node_info(target, for({n, _} <- members, n in up, do: n)) do
      {:ok,
       %{
         members: members,
         counts: counts,
         up: up,
         pending: pending,
         ring: ring,
         claimant: claimant,
         nodes: nodes
       }}
    end
  end

  defp node_info(_target, []), do: {:ok, %{}}

  defp node_info(target, nodes) do
    args = [nodes, :erl_eval, :exprs, [@node_exprs, []], @node_timeout]

    with {:ok, results} <- call(target, :erpc, :multicall, args, @node_timeout + 2_000) do
      {:ok, nodes |> Enum.zip(results) |> Map.new(fn {node, r} -> {node, node_result(r)} end)}
    end
  end

  # Erlang's undefined (unknown services, no riak_core app) as nil
  defp node_result({:ok, {:value, {:ok, info}, _}}),
    do: Map.new(info, fn {k, v} -> {k, if(v == :undefined, do: nil, else: v)} end)

  defp node_result({:ok, {:value, {:no_ring, other}, _}}), do: {:error, Data.error_message(other)}
  defp node_result({:error, {:erpc, :timeout}}), do: {:error, "no answer in time"}
  defp node_result({kind, reason}), do: {:error, Data.error_message({kind, reason})}

  @doc """
  `riak_core_status:ringready/0`: `{ok, Nodes} | {error, Reason}`, as
  `{:ok, {:ok | :error, text}}` (see `RiakCorePlugin.Data.ring_ready/1`).
  It calls every member, so it has a timeout of its own (35 s by default);
  a timeout reads "timed out (a member may be unreachable)".
  """
  @spec ring_ready(GenServer.server(), timeout()) ::
          {:ok, {:ok | :error, String.t()}} | {:error, String.t()}
  def ring_ready(target, timeout \\ @ready_timeout) do
    case Beamdash.Plugin.rpc(:riak_core_status, :ringready, [], timeout, target: target) do
      {:ok, result} -> {:ok, Data.ring_ready(result)}
      {:error, {:error, {:erpc, :timeout}}} -> {:error, "timed out (a member may be unreachable)"}
      {:error, reason} -> {:error, Data.error_message(reason)}
    end
  end

  @doc "The nodes the target sees as up: `erlang:nodes([this, visible])`."
  @spec up_nodes(GenServer.server()) :: {:ok, [node()]} | {:error, String.t()}
  def up_nodes(target), do: call(target, :erlang, :nodes, [[:this, :visible]])

  @doc """
  The vnodes: `%{fetched: [row], total, modules, module}`. Rows are the
  primary vnodes this node expects (including ones that have not started)
  and any secondary vnode that is running here for a partition it does not
  own, from `riak_core_vnode_manager:all_vnodes_status/0`: `module`,
  `index`, `pid`, `type` (`:primary` | `:secondary`), `forwarding`,
  `should_handoff` and `info` (process info). The first `max_vnodes/0` of
  the selected module, by module and index, get
  `erlang:process_info(Pid, [message_queue_len, memory, reductions])` in
  one rpc: `lists:zipwith/3` with the external fun
  `fun erlang:process_info/2`, which travels by name. A vnode without a
  pid has `info: []`.
  """
  @spec vnodes(GenServer.server(), String.t()) :: {:ok, map()} | {:error, String.t()}
  def vnodes(target, module) do
    with {:ok, statuses} <- call(target, :riak_core_vnode_manager, :all_vnodes_status, []) do
      rows =
        Enum.flat_map(statuses, fn {{mod, idx}, props} ->
          type = props[:type]
          pid = props[:pid]

          if type == :primary or is_pid(pid) do
            [
              %{
                module: mod,
                index: idx,
                pid: pid,
                type: type,
                forwarding: props[:forwarding],
                should_handoff: props[:should_handoff]
              }
            ]
          else
            []
          end
        end)

      modules = rows |> Enum.map(& &1.module) |> Enum.uniq() |> Enum.sort()
      module = if Enum.any?(modules, &(Atom.to_string(&1) == module)), do: module, else: ""

      selected =
        if module == "",
          do: rows,
          else: Enum.filter(rows, &(Atom.to_string(&1.module) == module))

      taken =
        selected
        |> Enum.sort_by(&{&1.module, &1.index})
        |> Enum.take(@max_vnodes)

      pids = for %{pid: pid} <- taken, is_pid(pid), do: pid

      with {:ok, infos} <- process_infos(target, pids) do
        fetched = Enum.map(taken, &Map.put(&1, :info, Map.get(infos, &1.pid, [])))

        {:ok, %{fetched: fetched, total: length(selected), modules: modules, module: module}}
      end
    end
  end

  defp process_infos(_target, []), do: {:ok, %{}}

  defp process_infos(target, pids) do
    items = List.duplicate([:message_queue_len, :memory, :reductions], length(pids))

    with {:ok, infos} <-
           call(target, :lists, :zipwith, [&:erlang.process_info/2, pids, items]) do
      {:ok, Map.new(Enum.zip(pids, infos))}
    end
  end

  @doc """
  The handoffs: `%{active: status_list, pending: pending_changes}`, from
  `riak_core_handoff_manager:status/0` and `riak_core_ring:pending_changes/1`.
  """
  @spec handoffs(GenServer.server()) :: {:ok, map()} | {:error, String.t()}
  def handoffs(target) do
    with {:ok, active} <- call(target, :riak_core_handoff_manager, :status, []),
         {:ok, pending} <- eval_ring(target, @pending_exprs),
         {:ok, settings} <- handoff_settings(target) do
      {:ok, %{active: active, pending: pending, settings: settings}}
    end
  end

  @doc """
  The handoff controls' view: `%{concurrency, inbound, outbound, cluster}`.
  `concurrency` is `riak_core_handoff_manager:get_concurrency/0`, and
  `inbound`/`outbound` are whether handoffs are enabled in that direction
  (`disable_inbound_handoff`/`disable_outbound_handoff` in riak_core's
  environment). `cluster` is the history key for auditing a change.
  """
  @spec handoff_settings(GenServer.server()) :: {:ok, map()} | {:error, String.t()}
  def handoff_settings(target) do
    with {:ok, concurrency} <-
           call(target, :riak_core_handoff_manager, :get_concurrency, []),
         {:ok, disable_in} <-
           call(target, :application, :get_env, [:riak_core, :disable_inbound_handoff, false]),
         {:ok, disable_out} <-
           call(target, :application, :get_env, [:riak_core, :disable_outbound_handoff, false]),
         {:ok, cluster} <- cluster_name(target) do
      {:ok,
       %{
         concurrency: concurrency,
         inbound: not disable_in,
         outbound: not disable_out,
         cluster: Data.cluster_key(cluster)
       }}
    end
  end

  @doc """
  Runs a handoff control on the attached node: `{:concurrency, n}`,
  `{:direction, :enable | :disable, :inbound | :outbound | :both}`,
  `{:kill_direction, direction}`, `:kill_all`, `:force` (force all running
  vnodes to start handoff) or `:refresh_ring`.
  """
  @spec handoff_action(GenServer.server(), term()) :: :ok | {:error, String.t()}
  def handoff_action(target, {:concurrency, n}) when is_integer(n) and n >= 0,
    do: handoff_call(target, :riak_core_handoff_manager, :set_concurrency, [n])

  def handoff_action(target, {:direction, mode, direction})
      when mode in [:enable, :disable] and direction in [:inbound, :outbound, :both],
      do:
        handoff_call(target, :riak_core_handoff_manager, :handoff_change_enabled_setting, [
          mode,
          direction
        ])

  def handoff_action(target, {:kill_direction, direction})
      when direction in [:inbound, :outbound],
      do:
        handoff_call(target, :riak_core_handoff_manager, :kill_handoffs_in_direction, [direction])

  def handoff_action(target, :kill_all),
    do: handoff_call(target, :riak_core_handoff_manager, :kill_handoffs, [])

  def handoff_action(target, :force),
    do: handoff_call(target, :riak_core_vnode_manager, :force_handoffs, [])

  def handoff_action(target, :refresh_ring),
    do: handoff_call(target, :riak_core_ring_manager, :refresh_my_ring, [])

  defp handoff_call(target, mod, fun, args) do
    case call(target, mod, fun, args) do
      {:ok, :ok} -> :ok
      {:ok, other} -> {:error, "unexpected answer: #{inspect(other)}"}
      error -> error
    end
  end

  @doc """
  The attached node's load over `window` ms, shaped by
  `RiakCorePlugin.Data.load_view/1`. The first call switches
  `scheduler_wall_time` on, and it stays on.
  """
  @spec load(GenServer.server(), pos_integer()) :: {:ok, map()} | {:error, String.t()}
  def load(target, window \\ @load_window) do
    bindings = :orddict.from_list([{:Window, window}])

    case call(target, :erl_eval, :exprs, [@load_exprs, bindings], window + @load_slack) do
      {:ok, {:value, {elapsed, sched1, sched2, io1, io2, memory, top}, _}} ->
        {:ok,
         Data.load_view(%{
           elapsed: elapsed,
           schedulers: {sched1, sched2},
           io: {io1, io2},
           memory: memory,
           top: top
         })}

      error ->
        error
    end
  end

  @doc """
  What the Admin page needs: `%{name, size, claimant, members, counts, up,
  candidates}`, from `riak_core_ring:cluster_name/1`, `num_partitions/1`, `claimant/1`,
  `all_member_status/1`, partitions per node (as in `members/1`) and
  `up_nodes/1`. `candidates` are the nodes that could join: up, not
  members, and running riak_core (its ring manager is registered there),
  asked by the target in one `erpc:multicall/5`.
  """
  @spec cluster(GenServer.server()) :: {:ok, map()} | {:error, String.t()}
  def cluster(target) do
    with {:ok, {name, size, claimant, members, counts}} <- eval_ring(target, @cluster_exprs),
         {:ok, up} <- up_nodes(target),
         {:ok, candidates} <- riak_nodes(target, Data.join_candidates(up, members)) do
      {:ok,
       %{
         name: name,
         size: size,
         claimant: claimant,
         members: members,
         counts: counts,
         up: up,
         candidates: candidates
       }}
    end
  end

  @doc """
  The cluster's name, `riak_core_ring:cluster_name/1`: `{node, timestamp}`
  of the ring the cluster started from (see `RiakCorePlugin.Data.cluster_key/1`).
  """
  @spec cluster_name(GenServer.server()) :: {:ok, term()} | {:error, String.t()}
  def cluster_name(target), do: eval_ring(target, @cluster_name_exprs)

  @doc """
  What the history keeps of the cluster: `%{cluster, size, claimant,
  members, pending, counts, up}`, from `riak_core_ring:cluster_name/1`,
  `num_partitions/1`, `claimant/1`, `all_member_status/1`, the number of
  `pending_changes/1`, partitions per node (`[{node, count}]`) and
  `up_nodes/1`.
  """
  @spec membership(GenServer.server()) :: {:ok, map()} | {:error, String.t()}
  def membership(target) do
    with {:ok, {cluster, size, claimant, members, pending, counts}} <-
           eval_ring(target, @membership_exprs),
         {:ok, up} <- up_nodes(target) do
      {:ok,
       %{
         cluster: cluster,
         size: size,
         claimant: claimant,
         members: members,
         pending: pending,
         counts: counts,
         up: up
       }}
    end
  end

  @doc """
  The handoffs of the whole cluster: `%{cluster, nodes, pending,
  modules}`. `nodes` is `%{node => {:ok, status_list} | {:error, text}}`
  for each member the target sees, from `riak_core_handoff_manager:status/0`
  on each, asked by the target in one `erpc:multicall/5` (4 s each).
  `pending` is `riak_core_ring:pending_changes/1`, whose entries list the
  modules whose transfer is done, and `modules` the vnode modules
  (`riak_core:vnode_modules/0`; `[]` when unknown).
  """
  @spec cluster_handoffs(GenServer.server()) :: {:ok, map()} | {:error, String.t()}
  def cluster_handoffs(target) do
    with {:ok, {cluster, results, pending, modules}} <-
           eval_ring(target, @cluster_handoffs_exprs, [], 7_000) do
      nodes =
        Map.new(results, fn
          {node, {:ok, statuses}} when is_list(statuses) -> {node, {:ok, statuses}}
          {node, {:error, {:erpc, :timeout}}} -> {node, {:error, "no answer in time"}}
          {node, other} -> {node, {:error, Data.error_message(other)}}
        end)

      {:ok, %{cluster: cluster, nodes: nodes, pending: pending, modules: modules}}
    end
  end

  defp riak_nodes(_target, []), do: {:ok, []}

  defp riak_nodes(target, nodes) do
    args = [nodes, :erlang, :whereis, [:riak_core_ring_manager], @candidate_timeout]

    with {:ok, results} <- call(target, :erpc, :multicall, args, @candidate_timeout + 2_000) do
      {:ok, for({node, {:ok, pid}} <- Enum.zip(nodes, results), is_pid(pid), do: node)}
    end
  end

  @doc """
  `riak_core_claimant:plan/0`: the staged changes and what committing them
  would do, as `{:ok, %{changes, transitions}}`; `changes` is
  `[{node, action}]` (joining nodes included) and each transition is
  `{before, after, transfers}`, partitions per node (`[{node, count}]`)
  before and after it and the partitions it moves. The claimant keeps the
  plan, and `admin(target, :commit)` commits exactly this plan. A refusal
  (the ring not ready, say) is `{:ok, {:error, reason}}`.
  """
  @spec plan(GenServer.server()) :: {:ok, map() | {:error, term()}} | {:error, String.t()}
  def plan(target) do
    case call(target, :erl_eval, :exprs, [@plan_exprs, []], @admin_timeout) do
      {:ok, {:value, {:ok, changes, transitions}, _}} ->
        {:ok, %{changes: changes, transitions: transitions}}

      {:ok, {:value, {:error, reason}, _}} ->
        {:ok, {:error, reason}}

      {:ok, {:value, other, _}} ->
        {:ok, {:error, other}}

      error ->
        error
    end
  end

  @doc """
  Runs one cluster admin action on the attached node (`target_node`):

    * `{:join, node}`: `riak_core:staged_join(target_node)` on `node`, the
      node that joins, asked by the target (`rpc:call/5`);
    * `{:leave, node}`, `{:remove, node}`, `{:replace, node, new}`,
      `{:force_replace, node, new}`, `{:resize, size}`, `:abort_resize`:
      staged with `riak_core_claimant` (leave_member/1, remove_member/1,
      replace/2, force_replace/2, resize_ring/1, abort_resize/0);
    * `:commit` and `:clear`: `riak_core_claimant:commit/0` and `clear/0`;
    * `{:down, node}`: `riak_core:down/1`, at once (no staging);
    * `{:join_and, {:replace | :force_replace, node, new}}`: the join of
      `new`, then its replace of `node`. The cluster hears of a join by
      gossip, so until the claimant's ring has `new` as joining the replace
      is refused (`invalid_replacement`): it is asked again for up to
      #{div(@join_replace_wait, 1000)} s.

  `:ok`, or `{:error, text}` with riak_core's reason in words (see
  `RiakCorePlugin.Data.admin_error/1`).
  """
  @spec admin(GenServer.server(), node(), term()) :: :ok | {:error, String.t()}
  def admin(target, target_node, {:join_and, {kind, _node, new} = replace})
      when kind in [:replace, :force_replace] do
    with :ok <- admin(target, target_node, {:join, new}) do
      deadline = System.monotonic_time(:millisecond) + @join_replace_wait

      case stage_replace(target, target_node, replace, deadline) do
        :ok -> :ok
        {:error, message} -> {:error, "#{new} joined, but the replace wasn't staged: #{message}"}
      end
    end
  end

  def admin(target, target_node, action) do
    action |> raw_admin(target, target_node) |> admin_result()
  end

  defp stage_replace(target, target_node, replace, deadline) do
    case raw_admin(replace, target, target_node) do
      {:ok, {:error, :invalid_replacement}} = refused ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(@join_replace_poll)
          stage_replace(target, target_node, replace, deadline)
        else
          admin_result(refused)
        end

      result ->
        admin_result(result)
    end
  end

  defp raw_admin(action, target, target_node) do
    {mod, fun, args, timeout} = admin_call(action, target_node)
    call(target, mod, fun, args, timeout)
  end

  defp admin_result({:ok, :ok}), do: :ok
  defp admin_result({:ok, other}), do: {:error, Data.admin_error(other)}
  defp admin_result({:error, message}), do: {:error, message}

  defp admin_call({:join, node}, target_node) when is_atom(node),
    do:
      {:rpc, :call, [node, :riak_core, :staged_join, [target_node], @join_timeout],
       @join_timeout + 5_000}

  defp admin_call({:leave, node}, _), do: claimant(:leave_member, [node])
  defp admin_call({:remove, node}, _), do: claimant(:remove_member, [node])
  defp admin_call({:replace, node, new}, _), do: claimant(:replace, [node, new])
  defp admin_call({:force_replace, node, new}, _), do: claimant(:force_replace, [node, new])
  defp admin_call({:resize, size}, _) when is_integer(size), do: claimant(:resize_ring, [size])
  defp admin_call(:abort_resize, _), do: claimant(:abort_resize, [])
  defp admin_call(:commit, _), do: claimant(:commit, [])
  defp admin_call(:clear, _), do: claimant(:clear, [])
  defp admin_call({:down, node}, _), do: {:riak_core, :down, [node], @admin_timeout}

  defp claimant(fun, args), do: {:riak_core_claimant, fun, args, @admin_timeout}

  # `bindings` are the variables the expressions use besides `R`
  defp eval_ring(target, exprs, bindings \\ [], timeout \\ @timeout) do
    case call(target, :erl_eval, :exprs, [exprs, :orddict.from_list(bindings)], timeout) do
      {:ok, {:value, {:ok, value}, _bindings}} -> {:ok, value}
      {:ok, {:value, {:no_ring, {:error, reason}}, _}} -> {:error, Data.error_message(reason)}
      {:ok, {:value, {:no_ring, other}, _}} -> {:error, Data.error_message(other)}
      error -> error
    end
  end

  defp call(target, mod, fun, args, timeout \\ @timeout) do
    case Beamdash.Plugin.rpc(mod, fun, args, timeout, target: target) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, Data.error_message(reason)}
    end
  end
end
