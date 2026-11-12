defmodule RiakCorePlugin.History do
  @moduledoc """
  The plugin's history, kept in beamdash's event store
  (`Beamdash.Plugin.record/4` and `query/3`). Each row's key is the cluster
  (`RiakCorePlugin.Data.cluster_key/1`), so a cluster's history is one
  whichever member beamdash is attached to. The kinds:

    * `handoffs`: each completed handoff (`RiakCorePlugin.Collect.handoffs/1`,
      every 5 s): type, module, partitions, from, to, start, end, duration
      and size. Ownership, resize and delete transfers come from the ring's
      pending changes, which mark each transfer done however short it was;
      their times are those of the looks (±5 s) unless a look caught the
      handoff running, which also gives its size. Hinted and repair
      handoffs are only known from the live handoffs of every member, so
      one shorter than 5 s may be missed.
    * `members`: a member's status or up/down state changed
      (`RiakCorePlugin.Collect.cluster/1`, every 10 s).
    * `ring`: the claimant or ring size changed, partition transfers started
      or were all done, or ownership changed without transfers.
    * `load`: a 1 s load sample of the attached node every minute
      (`RiakCorePlugin.Collect.load/1`).
    * `admin`: each action on the Admin page, with its result, and for a
      commit the changes it committed.

  The collectors record whatever page is open, while riak_core runs on the
  attached node. beamdash keeps the rows for 30 days by default.
  """

  @plugin :riak_core_plugin

  @kinds [
    {"handoffs", "Handoffs"},
    {"members", "Members"},
    {"ring", "Ring"},
    {"load", "Load"},
    {"admin", "Admin"}
  ]

  @doc "The kinds of history, `[{kind, label}]`."
  def kinds, do: @kinds

  @doc "Whether `kind` is one of `kinds/0`."
  def kind?(kind), do: List.keymember?(@kinds, kind, 0)

  @doc """
  `:ok` when history is kept, else `{:off, why}`: the event store is off, or
  this beamdash build is too old to keep it.
  """
  @spec status() :: :ok | {:off, String.t()}
  def status do
    cond do
      not function_exported?(Beamdash.Plugin, :record, 4) ->
        {:off, "this beamdash build doesn't keep plugin history"}

      not Beamdash.Plugin.history?() ->
        {:off, "beamdash's event store is switched off"}

      true ->
        :ok
    end
  end

  @doc """
  Stores `rows` (maps with `:data`, and optionally `:at` and `:node`) of
  `kind` for cluster `key`. Does nothing when history is off.
  """
  @spec record(String.t(), String.t() | nil, [map()]) :: :ok | {:error, term()}
  def record(_kind, _key, []), do: :ok

  def record(kind, key, rows) do
    case status() do
      :ok -> Beamdash.Plugin.record(@plugin, kind, Enum.map(rows, &Map.put(&1, :key, key)))
      {:off, _} -> :ok
    end
  end

  @doc """
  Cluster `key`'s rows of `kind`, newest first (see `Beamdash.Plugin.query/3`
  for `opts`), or `{:error, why}` when history is off.
  """
  @spec query(String.t(), String.t(), keyword()) :: {:ok, [map()]} | {:error, String.t()}
  def query(kind, key, opts \\ []) do
    with :ok <- status(),
         {:ok, rows} <- Beamdash.Plugin.query(@plugin, kind, Keyword.put(opts, :key, key)) do
      {:ok, rows}
    else
      {:off, why} -> {:error, "history is off: #{why}"}
      {:error, :disabled} -> {:error, "history is off: beamdash's event store is switched off"}
      {:error, message} -> {:error, message}
    end
  end

  @doc """
  Stores an Admin page action: `text` (what was done, in words) on
  `node`, its `result` (`:ok` or `{:error, message}`) and, for a commit, the
  `changes` it committed (`[{node, action}]`).
  """
  @spec record_admin(String.t() | nil, node(), String.t(), term(), list()) ::
          :ok | {:error, term()}
  def record_admin(key, node, text, result, changes \\ []) do
    error = with {:error, message} <- result, do: message, else: (_ -> nil)

    data = %{
      action: text,
      ok: result == :ok,
      error: error,
      changes:
        for(
          {member, action} <- changes,
          do: "#{member}: #{RiakCorePlugin.Data.change_text(action)}"
        )
    }

    record("admin", key, [%{node: node, data: data}])
  end
end
