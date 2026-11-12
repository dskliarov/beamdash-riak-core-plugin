defmodule RiakCorePlugin.Collect do
  @moduledoc """
  The plugin's collectors. beamdash calls each on its interval while
  riak_core runs on the attached node, with the state its previous call
  returned (`nil` at first, and after the attached node changes). Each
  stores only what changed since its previous call, so the first call just
  takes note of the cluster as it is. A call that returns `{:error, reason}`
  stores nothing and returns no state, so the caller keeps the state it
  passed in. See `RiakCorePlugin.History` for what each kind holds.
  """

  alias RiakCorePlugin.{Data, History, Riak}

  @doc """
  The handoffs that completed since the previous call, stored as
  `handoffs`. Ownership, resize and delete transfers are taken from the
  ring's pending changes, which record each completed transfer however
  short; the live handoffs of every member
  (`RiakCorePlugin.Riak.cluster_handoffs/1`) add their size and start when
  a look caught them, and are the only record of hinted and repair
  handoffs.
  """
  def handoffs(state) do
    with {:ok, h} <- Riak.cluster_handoffs(Beamdash.Target) do
      key = Data.cluster_key(h.cluster)
      now = System.system_time(:millisecond)
      {active, answered} = Data.cluster_handoffs(h.nodes)

      {ended, tracked} =
        Data.handoff_changes(previous(state, key, :tracked), active, answered, now)

      {direct, live} = Data.split_ended(ended, previous(state, key, :live), now)

      {done, transfers, live} =
        Data.transfer_changes(previous(state, key, :transfers), h.pending, h.modules, live, now)

      rows =
        for e <- Enum.sort_by(direct ++ done, & &1.ended_ms),
            do: %{at: e.ended_ms, node: e.from, data: e}

      History.record("handoffs", key, rows)
      {:ok, %{key: key, tracked: tracked, live: live, transfers: transfers}}
    end
  end

  @doc """
  The ring's members, claimant, size and pending changes
  (`RiakCorePlugin.Riak.membership/1`); what changed since the previous
  call is stored as `members` and `ring`.
  """
  def cluster(state) do
    with {:ok, membership} <- Riak.membership(Beamdash.Target) do
      key = Data.cluster_key(membership.cluster)
      now = System.system_time(:millisecond)
      before = previous(state, key, :snapshot)
      snapshot = Data.cluster_snapshot(membership, before, now)

      if before do
        members = for c <- Data.member_changes(before, snapshot), do: %{node: c.node, data: c}
        History.record("members", key, members)

        History.record(
          "ring",
          key,
          for(c <- Data.ring_changes(before, snapshot, now), do: %{data: c})
        )
      end

      {:ok, %{key: key, snapshot: snapshot}}
    end
  end

  @doc "A 1 s load sample of the attached node (`RiakCorePlugin.Riak.load/2`), stored as `load`."
  def load(_state) do
    with {:ok, cluster} <- Riak.cluster_name(Beamdash.Target),
         {:ok, load} <- Riak.load(Beamdash.Target) do
      History.record("load", Data.cluster_key(cluster), [%{data: Data.load_sample(load)}])
      {:ok, nil}
    end
  end

  # The previous call's `field`, when it saw the same cluster (nil from a
  # state an older version of the plugin left, as a first look).
  defp previous(%{key: key} = state, key, field), do: Map.get(state, field)
  defp previous(_state, _key, _field), do: nil
end
