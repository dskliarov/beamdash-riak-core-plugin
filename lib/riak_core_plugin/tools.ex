defmodule RiakCorePlugin.Tools do
  @moduledoc """
  The MCP tools of the riak_core plugin: read-only views of the attached
  node's ring, members and handoffs. Cluster changes are not offered. Each
  tool returns `{:ok, data}` or `{:error, message}`, with data made of maps
  and lists that encode as JSON.
  """

  alias RiakCorePlugin.{Data, Riak}

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

  defp json_member(%{info: {:error, text}} = row), do: %{row | info: %{error: text}}
  defp json_member(row), do: row

  defp json_handoff(%{size: {n, unit}} = row), do: %{row | size: %{n: n, unit: unit}}
  defp json_handoff(row), do: row
end
