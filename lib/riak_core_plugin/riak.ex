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
               application:get_env(riak_core, target_n_val, 4)}
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

  @members_exprs parse.("""
                 {riak_core_ring:all_member_status(R),
                  lists:foldl(fun({_, N}, D) -> orddict:update_counter(N, 1, D) end,
                              orddict:new(), riak_core_ring:all_owners(R)),
                  riak_core_ring:pending_changes(R),
                  erlang:phash2({riak_core_ring:all_owners(R),
                                 riak_core_ring:pending_changes(R),
                                 riak_core_ring:all_member_status(R)})}
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
                   processes => erlang:system_info(process_count)}
               end)(node())
              """)

  # How long each member has to answer for the Members page.
  @node_timeout 4_000

  @pending_exprs parse.("riak_core_ring:pending_changes(R)")

  @cluster_exprs parse.("""
                 {riak_core_ring:num_partitions(R), riak_core_ring:claimant(R),
                  riak_core_ring:all_member_status(R),
                  lists:foldl(fun({_, N}, D) -> orddict:update_counter(N, 1, D) end,
                              orddict:new(), riak_core_ring:all_owners(R))}
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
  only when an owner is down.
  """
  @spec ring(GenServer.server()) :: {:ok, map()} | {:error, String.t()}
  def ring(target) do
    with {:ok, {size, claimant, owners, pending, members, n_val}} <-
           eval_ring(target, @ring_exprs),
         {:ok, up} <- up_nodes(target),
         {:ok, fallbacks} <- fallbacks(target, owners, up, n_val) do
      {:ok,
       %{
         size: size,
         claimant: claimant,
         owners: owners,
         pending: pending,
         members: members,
         n_val: n_val,
         up: up,
         fallbacks: fallbacks
       }}
    end
  end

  defp fallbacks(target, owners, up, n_val) do
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
  What the Members page needs: `%{members, counts, up, pending, ring,
  nodes}`, from `riak_core_ring:all_member_status/1`, partitions per node
  counted on the target from `all_owners/1` (`[{node, count}]`, sorted by
  node), `up_nodes/1`, `pending_changes/1` and a hash of the target's ring
  (`ring`).

  `nodes` is `%{node => info | {:error, text}}` for the members that are
  up, asked by the target in one `erpc:multicall/5` (#{@node_timeout} ms
  each): `%{vnodes, not_owned, handoffs, ring, services, riak_core, otp,
  uptime_ms, memory, processes}`. `not_owned` counts the partitions it runs
  a vnode for but doesn't own (fallbacks, or handoffs still to do);
  `ring` is the hash of its own ring, to compare with the target's.
  """
  @spec members(GenServer.server()) :: {:ok, map()} | {:error, String.t()}
  def members(target) do
    with {:ok, {members, counts, pending, ring}} <- eval_ring(target, @members_exprs),
         {:ok, up} <- up_nodes(target),
         {:ok, nodes} <- node_info(target, for({n, _} <- members, n in up, do: n)) do
      {:ok,
       %{members: members, counts: counts, up: up, pending: pending, ring: ring, nodes: nodes}}
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
  The vnodes: `%{fetched: [{{module, index, pid}, info}], total, modules,
  module}` (see `RiakCorePlugin.Data.select_vnodes/3`).
  `riak_core_vnode_manager:all_vnodes/0` gives `[{Mod, Idx, Pid}]`. The
  vnodes of `module` (`""` for all) are taken, and the first `max_vnodes/0`
  of them (by module and index) get
  `erlang:process_info(Pid, [message_queue_len, memory, reductions])`, all
  in one rpc: `lists:zipwith/3` with the external fun
  `fun erlang:process_info/2`, which travels by name.
  """
  @spec vnodes(GenServer.server(), String.t()) :: {:ok, map()} | {:error, String.t()}
  def vnodes(target, module) do
    with {:ok, all} <- call(target, :riak_core_vnode_manager, :all_vnodes, []),
         selected = Data.select_vnodes(all, module, @max_vnodes),
         pids = Enum.map(selected.taken, fn {_mod, _idx, pid} -> pid end),
         items = List.duplicate([:message_queue_len, :memory, :reductions], length(pids)),
         {:ok, infos} <- call(target, :lists, :zipwith, [&:erlang.process_info/2, pids, items]) do
      {:ok,
       %{
         fetched: Enum.zip(selected.taken, infos),
         total: selected.total,
         modules: selected.modules,
         module: selected.module
       }}
    end
  end

  @doc """
  The handoffs: `%{active: status_list, pending: pending_changes}`, from
  `riak_core_handoff_manager:status/0` and `riak_core_ring:pending_changes/1`.
  """
  @spec handoffs(GenServer.server()) :: {:ok, map()} | {:error, String.t()}
  def handoffs(target) do
    with {:ok, active} <- call(target, :riak_core_handoff_manager, :status, []),
         {:ok, pending} <- eval_ring(target, @pending_exprs) do
      {:ok, %{active: active, pending: pending}}
    end
  end

  @doc """
  What the Admin page needs: `%{size, claimant, members, counts, up,
  candidates}`, from `riak_core_ring:num_partitions/1`, `claimant/1`,
  `all_member_status/1`, partitions per node (as in `members/1`) and
  `up_nodes/1`. `candidates` are the nodes that could join: up, not
  members, and running riak_core (its ring manager is registered there),
  asked by the target in one `erpc:multicall/5`.
  """
  @spec cluster(GenServer.server()) :: {:ok, map()} | {:error, String.t()}
  def cluster(target) do
    with {:ok, {size, claimant, members, counts}} <- eval_ring(target, @cluster_exprs),
         {:ok, up} <- up_nodes(target),
         {:ok, candidates} <- riak_nodes(target, Data.join_candidates(up, members)) do
      {:ok,
       %{
         size: size,
         claimant: claimant,
         members: members,
         counts: counts,
         up: up,
         candidates: candidates
       }}
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
    * `{:down, node}`: `riak_core:down/1`, at once (no staging).

  `:ok`, or `{:error, text}` with riak_core's reason in words (see
  `RiakCorePlugin.Data.admin_error/1`).
  """
  @spec admin(GenServer.server(), node(), term()) :: :ok | {:error, String.t()}
  def admin(target, target_node, action) do
    {mod, fun, args, timeout} = admin_call(action, target_node)

    case call(target, mod, fun, args, timeout) do
      {:ok, :ok} -> :ok
      {:ok, other} -> {:error, Data.admin_error(other)}
      {:error, message} -> {:error, message}
    end
  end

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
  defp eval_ring(target, exprs, bindings \\ []) do
    case call(target, :erl_eval, :exprs, [exprs, :orddict.from_list(bindings)]) do
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
