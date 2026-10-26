defmodule RiakCorePlugin.MembersLive do
  @moduledoc """
  The Members page: per cluster member, its status (valid, joining,
  leaving, exiting, down), whether the target sees it up, the partitions it
  owns (and their share of the ring) and those a committed change is
  moving; and, asked of each member that is up, its vnodes (and those for
  partitions it doesn't own), its active handoffs, whether its ring is the
  target's, its riak_core services, riak_core and OTP versions, uptime and
  memory (`RiakCorePlugin.Riak.members/1`).
  """
  use Beamdash.Plugin.Page

  import RiakCorePlugin.Components
  alias Beamdash.Plugin.Format
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
        <div class="rcp-scroll">
          <table class="rcp-table rcp-members">
            <thead>
              <tr>
                <th>Node</th><th>Status</th><th>Up</th>
                <th class="rcp-num">Partitions</th><th>Moving</th>
                <th class="rcp-num">Vnodes</th><th class="rcp-num">Not owned</th>
                <th class="rcp-num">Handoffs</th><th>Ring</th><th>Services</th>
                <th>Versions</th><th class="rcp-num">Uptime</th><th class="rcp-num">Memory</th>
              </tr>
            </thead>
            <tbody>
              <tr :for={row <- rows}>
                <td>
                  <.node_link node={row.node} attached={@node} return={page_path(assigns, "members")} />
                </td>
                <td class={"rcp-status-#{row.status}"}>{row.status}</td>
                <td class={if row.up, do: "rcp-ok", else: "rcp-bad"}>
                  {if row.up, do: "up", else: "down"}
                </td>
                <td class="rcp-num">
                  {row.partitions} <span class="rcp-muted">· {row.share}%</span>
                </td>
                <td>
                  <span :if={row.moving_out > 0} class="rcp-pending" title="moving away">
                    ↑{row.moving_out}
                  </span>
                  <span :if={row.moving_in > 0} class="rcp-pending" title="moving here">
                    ↓{row.moving_in}
                  </span>
                </td>
                <.node_cells info={row.info} ring_agrees={row.ring_agrees} />
              </tr>
            </tbody>
          </table>
        </div>
        <p :if={rows == []} class="rcp-note">No members.</p>

        <dl class="rcp-explain">
          <dt>Moving</dt>
          <dd>
            Partitions a committed change is moving away from the node (↑) or to it (↓), until
            their handoffs are done.
          </dd>
          <dt>Not owned</dt>
          <dd>
            Partitions the node runs a vnode for but doesn't own: fallbacks for a node that is
            down, or data it still has to hand off to the new owner. riak_core hands them off once
            the vnode has been idle for a while; a count that stays up points at a handoff that
            can't happen.
          </dd>
          <dt>Ring</dt>
          <dd>
            Whether the node's copy of the ring (owners, pending changes, member statuses) is the
            same as {@node}'s. "differs" for long means gossip hasn't converged.
          </dd>
          <dt>Services</dt>
          <dd>
            The riak_core services the node registered as up
            (<code>riak_core_node_watcher</code>); requests for a service only go to nodes where it is up.
          </dd>
        </dl>
      </.result>
    </div>
    """
  end

  attr :info, :any, required: true
  attr :ring_agrees, :any, default: nil

  # the columns only the member itself knows
  defp node_cells(%{info: nil} = assigns) do
    ~H"""
    <td colspan="8" class="rcp-muted">— (down)</td>
    """
  end

  defp node_cells(%{info: {:error, _}} = assigns) do
    ~H"""
    <td colspan="8" class="rcp-error">{elem(@info, 1)}</td>
    """
  end

  defp node_cells(assigns) do
    ~H"""
    <td class="rcp-num">{@info.vnodes}</td>
    <td class={["rcp-num", @info.not_owned > 0 && "rcp-pending"]}>{@info.not_owned}</td>
    <td class="rcp-num">{@info.handoffs}</td>
    <td class={if @ring_agrees == false, do: "rcp-bad", else: "rcp-ok"}>
      {if @ring_agrees == false, do: "differs", else: "same"}
    </td>
    <td>{services(@info.services)}</td>
    <td class="rcp-nowrap">
      {@info.riak_core && "riak_core #{@info.riak_core} · "}OTP {@info.otp}
    </td>
    <td class="rcp-num">{Data.uptime(@info.uptime_ms)}</td>
    <td class="rcp-num">{Format.bytes(@info.memory)}</td>
    """
  end

  defp services(nil), do: "?"
  defp services([]), do: "none"
  defp services(list), do: list |> Enum.map(&to_string/1) |> Enum.join(", ")

  defp load_members(%{beamdash_target: target}) do
    fn ->
      with {:ok, m} <- Riak.members(target), do: {:ok, Data.member_details(m)}
    end
  end
end
