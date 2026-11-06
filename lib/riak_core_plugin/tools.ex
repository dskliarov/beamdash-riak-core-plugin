defmodule RiakCorePlugin.Tools do
  @moduledoc """
  The MCP tools of the riak_core plugin: read-only views of the attached
  node's ring, members and handoffs. Cluster changes are not offered. Each
  tool returns `{:ok, data}` or `{:error, message}`, with data made of maps
  and lists that encode as JSON.
  """

  alias RiakCorePlugin.{Data, Riak}

  def ring(_args) do
    with {:ok, ring} <- Riak.ring(Beamdash.Target) do
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
         pending: length(ring.pending),
         fallbacks: map_size(ring.fallbacks)
       }}
    end
  end

  def members(_args) do
    with {:ok, members} <- Riak.members(Beamdash.Target) do
      {:ok, Data.member_details(members)}
    end
  end

  def handoffs(_args) do
    with {:ok, handoffs} <- Riak.handoffs(Beamdash.Target) do
      {:ok,
       %{
         active: Data.handoff_rows(handoffs.active),
         pending: Data.pending_rows(handoffs.pending)
       }}
    end
  end
end
