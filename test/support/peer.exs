defmodule RiakCorePlugin.Test.Peer do
  @moduledoc false
  # A :peer node with riak_core_lite's compiled modules on its code path
  # (riak_core_ring, chash, vclock: real rings) and small fakes of the
  # riak_core servers the plugin calls. The fakes read what to return from
  # persistent_term on the peer; set it with put/3. None of the plugin's
  # modules are loaded there.

  @fakes %{
    riak_core_ring_manager: """
    -module(riak_core_ring_manager).
    -export([get_my_ring/0]).
    get_my_ring() ->
        case persistent_term:get(rcp_ring, undefined) of
            undefined -> {error, no_ring};
            Ring -> {ok, Ring}
        end.
    """,
    riak_core_status: """
    -module(riak_core_status).
    -export([ringready/0]).
    ringready() ->
        timer:sleep(persistent_term:get(rcp_ready_sleep, 0)),
        persistent_term:get(rcp_ready, {ok, [node()]}).
    """,
    riak_core_vnode_manager: """
    -module(riak_core_vnode_manager).
    -export([all_vnodes/0]).
    all_vnodes() -> persistent_term:get(rcp_vnodes, []).
    """,
    riak_core_handoff_manager: """
    -module(riak_core_handoff_manager).
    -export([status/0]).
    status() -> persistent_term:get(rcp_handoffs, []).
    """,
    # The admin calls: each is recorded (calls/1) and answers with the next
    # of rcp_replies, then rcp_reply (ok by default); plan/0 answers with
    # rcp_plan.
    riak_core_claimant: """
    -module(riak_core_claimant).
    -export([leave_member/1, remove_member/1, replace/2, force_replace/2,
             resize_ring/1, abort_resize/0, plan/0, commit/0, clear/0]).
    leave_member(N) -> rec({leave_member, [N]}).
    remove_member(N) -> rec({remove_member, [N]}).
    replace(N, New) -> rec({replace, [N, New]}).
    force_replace(N, New) -> rec({force_replace, [N, New]}).
    resize_ring(S) -> rec({resize_ring, [S]}).
    abort_resize() -> rec({abort_resize, []}).
    commit() -> rec({commit, []}).
    clear() -> rec({clear, []}).
    plan() -> persistent_term:get(rcp_plan, {ok, [], []}).
    rec(Call) ->
        persistent_term:put(rcp_calls, persistent_term:get(rcp_calls, []) ++ [{riak_core_claimant, Call}]),
        case persistent_term:get(rcp_replies, []) of
            [Reply | Rest] -> persistent_term:put(rcp_replies, Rest), Reply;
            [] -> persistent_term:get(rcp_reply, ok)
        end.
    """,
    riak_core: """
    -module(riak_core).
    -export([staged_join/1, down/1]).
    staged_join(N) -> rec({staged_join, [N]}).
    down(N) -> rec({down, [N]}).
    rec(Call) ->
        persistent_term:put(rcp_calls, persistent_term:get(rcp_calls, []) ++ [{riak_core, Call}]),
        persistent_term:get(rcp_reply, ok).
    """
  }

  @doc "Starts a peer; with `riak: true`, riak_core's modules and the fakes."
  def start(opts \\ []) do
    ebin = :riak_core_ring |> :code.which() |> Path.dirname() |> String.to_charlist()
    riak? = Keyword.get(opts, :riak, true)
    path_args = if riak?, do: [~c"-pa", ebin], else: []

    {:ok, peer, node} =
      :peer.start_link(%{
        name: :peer.random_name(),
        args: [~c"-setcookie", Atom.to_charlist(Node.get_cookie()) | path_args],
        connection: :standard_io
      })

    if riak?, do: Enum.each(@fakes, fn {mod, src} -> load(node, mod, src) end)
    {peer, node}
  end

  def stop(peer) do
    :peer.stop(peer)
  catch
    _, _ -> :ok
  end

  @doc "Sets persistent_term `key` on `node` (what the fakes return)."
  def put(node, key, value), do: :ok = :erpc.call(node, :persistent_term, :put, [key, value])

  @doc "The admin calls the fakes on `node` recorded, oldest first."
  def calls(node), do: :erpc.call(node, :persistent_term, :get, [:rcp_calls, []])

  @doc "A fresh riak_core ring of `size` owned by `node`, built on `node`."
  def fresh_ring(node, size), do: :erpc.call(node, :riak_core_ring, :fresh, [size, node])

  @doc "A process on `node` that waits forever (a stand-in vnode)."
  def spawn_idle(node), do: Node.spawn(node, :timer, :sleep, [:infinity])

  defp load(node, mod, src) do
    {:ok, tokens, _} = src |> String.to_charlist() |> :erl_scan.string()
    forms = split_forms(tokens, [], [])
    {:ok, ^mod, bin} = :compile.forms(forms, [:binary, :return_errors])
    {:module, ^mod} = :erpc.call(node, :code, :load_binary, [mod, ~c"#{mod}.erl", bin])
  end

  defp split_forms([], [], acc), do: Enum.reverse(acc)

  defp split_forms([{:dot, _} = dot | rest], cur, acc) do
    {:ok, form} = :erl_parse.parse_form(Enum.reverse([dot | cur]))
    split_forms(rest, [], [form | acc])
  end

  defp split_forms([t | rest], cur, acc), do: split_forms(rest, [t | cur], acc)
end
