defmodule RiakCorePlugin.Tools do
  @moduledoc """
  The MCP tools of the riak_core plugin: read-only views of the attached
  node's ring, members, handoffs and load, and of the cluster's history.
  Cluster changes are not offered. Each
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

  defp json_handoff(%{size: {n, unit}} = row), do: %{row | size: %{n: n, unit: unit}}
  defp json_handoff(row), do: row

  defp json_process(%{pid: pid} = row), do: %{row | pid: Format.pid(pid, :erlang)}
end
