defmodule RiakCorePlugin.Tools do
  @moduledoc """
  The MCP tools of the riak_core plugin: read-only views of the attached
  node's ring, members, handoffs, load and vnodes, one node's detail, and
  the cluster's history. Cluster changes are not offered. Each
  tool returns `{:ok, data}` or `{:error, message}`, with data made of maps
  and lists that encode as JSON.
  """

  alias Beamdash.Plugin.Format
  alias RiakCorePlugin.{Data, History, Riak}

  @history_minutes 1_440
  @history_limit 50
  @history_max 500

  def ring(_args) do
    with {:ok, ring} <- Riak.ring(Beamdash.Target, fallbacks: false) do
      {:ok,
       %{
         size: ring.size,
         claimant: ring.claimant,
         n_val: ring.n_val,
         members: Enum.map(ring.members, fn {node, status} -> %{node: node, status: status} end),
         partitions:
           Enum.map(Data.owners_by_node(ring.owners), fn {node, n} ->
             %{node: node, partitions: n}
           end),
         up: ring.up,
         down: Data.down_owners(ring),
         pending: length(ring.pending)
       }}
    end
  end

  def members(_args) do
    with {:ok, members} <- Riak.members(Beamdash.Target) do
      {:ok, members |> Data.member_details() |> Enum.map(&json_member/1)}
    end
  end

  def handoffs(_args) do
    with {:ok, handoffs} <- Riak.handoffs(Beamdash.Target) do
      {:ok,
       %{
         active: handoffs.active |> Data.handoff_rows() |> Enum.map(&json_handoff/1),
         pending: Data.pending_rows(handoffs.pending)
       }}
    end
  end

  @doc "The attached node's vnodes, including expected-but-not-started ones."
  def vnodes(args) do
    module = Map.get(args, "module", "")

    if is_binary(module) do
      with {:ok, vnodes} <- Riak.vnodes(Beamdash.Target, module) do
        {:ok,
         %{
           module: vnodes.module,
           modules: Enum.map(vnodes.modules, &Atom.to_string/1),
           total: vnodes.total,
           vnodes: vnodes.fetched |> Data.vnode_rows({:index, :asc}) |> Enum.map(&json_vnode/1)
         }}
      end
    else
      {:error, "module must be a string"}
    end
  end

  @doc "Active handoffs with rates and ETA, plus the queue and pending changes."
  def handoff_stats(_args) do
    with {:ok, handoffs} <- Riak.handoffs(Beamdash.Target),
         {:ok, {down, transfers}} <- Riak.transfers(Beamdash.Target) do
      {:ok,
       %{
         active: handoffs.active |> Data.handoff_rows() |> Enum.map(&json_handoff/1),
         pending: Data.pending_rows(handoffs.pending),
         waiting:
           for({:waiting_to_handoff, node, n} <- transfers, do: %{node: node, partitions: n}),
         stopped: for({:stopped, node, n} <- transfers, do: %{node: node, partitions: n}),
         down: down
       }}
    end
  end

  @doc "One member with its partitions, services and system pressure."
  def node(%{"node" => name}) when is_binary(name) do
    with {:ok, members} <- Riak.members(Beamdash.Target) do
      case Enum.find(Data.member_details(members), &(Atom.to_string(&1.node) == name)) do
        nil -> {:error, "unknown member #{name}"}
        row -> {:ok, json_member(row)}
      end
    end
  end

  def node(_args), do: {:error, "node is required"}

  def load(_args) do
    with {:ok, load} <- Riak.load(Beamdash.Target) do
      {:ok,
       %{
         load
         | memory: Enum.map(load.memory, fn {label, bytes} -> %{label: label, bytes: bytes} end),
           processes: Enum.map(load.processes, &json_process/1)
       }}
    end
  end

  @doc """
  The cluster's history of `kind` (see `RiakCorePlugin.History`): rows of the
  last `minutes` (default #{@history_minutes}), newest first, at most `limit` (default
  #{@history_limit}, at most #{@history_max}).
  """
  def history(%{"kind" => kind} = args) do
    with :ok <- history_kind(kind),
         {:ok, minutes} <- positive(args, "minutes", @history_minutes),
         {:ok, limit} <- positive(args, "limit", @history_limit),
         {:ok, cluster} <- Riak.cluster_name(Beamdash.Target),
         key = Data.cluster_key(cluster),
         from = System.system_time(:millisecond) - minutes * 60_000,
         {:ok, rows} <- History.query(kind, key, from: from, limit: min(limit, @history_max)) do
      {:ok,
       %{
         cluster: key,
         kind: kind,
         rows: for(r <- rows, do: %{at: Data.time_text(r.at), node: r.node, data: r.data})
       }}
    end
  end

  def history(_args), do: {:error, "kind is required"}

  defp history_kind(kind) do
    if History.kind?(kind),
      do: :ok,
      else:
        {:error,
         "kind must be one of #{Enum.map_join(History.kinds(), ", ", &elem(&1, 0))}, got: #{inspect(kind)}"}
  end

  defp positive(args, name, default) do
    case Map.get(args, name, default) do
      n when is_integer(n) and n > 0 -> {:ok, n}
      other -> {:error, "#{name} must be a positive integer, got: #{inspect(other)}"}
    end
  end

  defp json_member(%{info: {:error, text}} = row), do: %{row | info: %{error: text}}
  defp json_member(row), do: row

  defp json_handoff(row) do
    row =
      if is_pid(row[:sender]),
        do: %{row | sender: Format.pid(row.sender, :erlang)},
        else: row

    case row.size do
      {n, unit} -> %{row | size: %{n: n, unit: unit}}
      _ -> row
    end
  end

  defp json_vnode(row) do
    %{
      module: Atom.to_string(row.module),
      index: row.index,
      pid: if(is_pid(row.pid), do: Format.pid(row.pid, :erlang)),
      type: row.type && Atom.to_string(row.type),
      state: vnode_state(row),
      target: row.target && Atom.to_string(row.target),
      mailbox: row.mailbox,
      memory: row.memory,
      reductions: row.reductions
    }
  end

  defp vnode_state(%{mode: :not_started}), do: "not_started"
  defp vnode_state(%{mode: :forward}), do: "forward"
  defp vnode_state(%{mode: :handoff}), do: "handoff"
  defp vnode_state(%{mode: :active}), do: "active"
  defp vnode_state(_row), do: nil

  defp json_process(%{pid: pid} = row), do: %{row | pid: Format.pid(pid, :erlang)}
end
