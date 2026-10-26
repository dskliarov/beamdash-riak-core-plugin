defmodule RiakCorePlugin.MembersLive do
  @moduledoc """
  The Members page: each cluster member's status (valid, joining, leaving,
  exiting, down), the partitions it owns, and whether the target sees it up.
  """
  use Beamdash.Plugin.Page

  import RiakCorePlugin.Components
  alias RiakCorePlugin.{Data, Riak}

  @impl true
  def plugin_mount(_params, _session, socket),
    do: {:ok, setup_loads(socket, members: {5_000, &load_members/1})}

  @impl true
  def connection_changed(event, socket), do: {:noreply, reconnect(socket, event)}

  @impl true
  def plugin_info({:rcp_refresh, key, token}, socket), do: {:noreply, refresh(socket, key, token)}
  def plugin_info(_msg, socket), do: {:noreply, socket}

  @impl true
  def handle_async(key, result, socket), do: {:noreply, loaded(socket, key, result)}

  @impl true
  def handle_event(_event, _params, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    ~H"""
    <div class="rcp-page">
      <.header title="Members" node={@node} />
      <.result :let={rows} node={@node} result={@members}>
        <table class="rcp-table">
          <thead>
            <tr>
              <th>Node</th><th>Status</th><th class="rcp-num">Partitions</th><th>Up</th>
            </tr>
          </thead>
          <tbody>
            <tr :for={row <- rows}>
              <td>
                <.node_link node={row.node} attached={@node} return={page_path(assigns, "members")} />
              </td>
              <td class={"rcp-status-#{row.status}"}>{row.status}</td>
              <td class="rcp-num">{row.partitions}</td>
              <td class={if row.up, do: "rcp-ok", else: "rcp-bad"}>
                {if row.up, do: "up", else: "down"}
              </td>
            </tr>
          </tbody>
        </table>
        <p :if={rows == []} class="rcp-note">No members.</p>
      </.result>
    </div>
    """
  end

  defp load_members(%{beamdash_target: target}) do
    fn ->
      with {:ok, m} <- Riak.members(target),
           do: {:ok, Data.member_rows(m.members, m.counts, m.up)}
    end
  end
end
