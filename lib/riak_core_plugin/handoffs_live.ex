defmodule RiakCorePlugin.HandoffsLive do
  @moduledoc """
  The Handoffs page: active handoffs from `riak_core_handoff_manager:status/0`
  (type, module, partitions, from → to, status, size or progress, start,
  sender pid, linked to the process) and
  pending ownership changes from `riak_core_ring:pending_changes/1`.
  """
  use Beamdash.Plugin.Page

  import RiakCorePlugin.Components
  alias Beamdash.Plugin.Format
  alias RiakCorePlugin.{Data, Riak}

  @impl true
  def plugin_mount(_params, _session, socket),
    do: {:ok, setup_loads(socket, handoffs: {5_000, &load_handoffs/1})}

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
      <.header title="Handoffs" node={@node} />
      <.result :let={handoffs} node={@node} result={@handoffs}>
        <p :if={handoffs.active == [] and handoffs.pending == []} class="rcp-note">No handoffs.</p>

        <div :if={handoffs.active != []}>
          <h3>Active handoffs</h3>
          <table class="rcp-table">
            <thead>
              <tr>
                <th>Type</th><th>Module</th><th>Partition</th><th>From → to</th><th>Direction</th>
                <th>Status</th><th>Size / progress</th><th>Started</th><th>Sender</th>
              </tr>
            </thead>
            <tbody>
              <tr :for={row <- Data.handoff_rows(handoffs.active)}>
                <%= if row.known do %>
                  <td>{name(row.type)}</td>
                  <td>{name(row.module)}</td>
                  <td class="rcp-mono">{partitions(row)}</td>
                  <td>
                    <.node_link
                      node={row.from}
                      attached={@node}
                      return={page_path(assigns, "handoffs")}
                    /> →
                    <.node_link
                      node={row.to}
                      attached={@node}
                      return={page_path(assigns, "handoffs")}
                    />
                  </td>
                  <td>{name(row.direction)}</td>
                  <td>{row.status && Format.term(row.status, @syntax, 60)}</td>
                  <td>{size(row)}</td>
                  <td>{row.start}</td>
                  <td class="rcp-mono"><.sender pid={row.sender} syntax={@syntax} /></td>
                <% else %>
                  <td colspan="9" class="rcp-mono">{row.text}</td>
                <% end %>
              </tr>
            </tbody>
          </table>
        </div>

        <div :if={handoffs.pending != []}>
          <h3>Pending changes</h3>
          <table class="rcp-table">
            <thead>
              <tr>
                <th>Index</th><th>Owner</th><th>Next owner</th><th>Modules</th><th>Status</th>
              </tr>
            </thead>
            <tbody>
              <tr :for={row <- Data.pending_rows(handoffs.pending)}>
                <td class="rcp-mono">{row.index}</td>
                <td>
                  <.node_link
                    node={row.owner}
                    attached={@node}
                    return={page_path(assigns, "handoffs")}
                  />
                </td>
                <td>
                  <.node_link
                    node={row.next}
                    attached={@node}
                    return={page_path(assigns, "handoffs")}
                  />
                </td>
                <td>{Enum.map_join(row.modules, ", ", &name/1)}</td>
                <td>{name(row.status)}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </.result>
    </div>
    """
  end

  defp partitions(%{src_partition: same, target_partition: same}), do: to_string(same)

  defp partitions(%{src_partition: src, target_partition: tgt}),
    do: "#{src} → #{tgt}"

  defp sender(%{pid: pid} = assigns) when is_pid(pid) do
    ~H"""
    <.link navigate={Beamdash.Plugin.process_path(@pid)} class="rcp-pid">{Format.pid(@pid, @syntax)}</.link>
    """
  end

  defp sender(%{pid: nil} = assigns), do: ~H""
  defp sender(assigns), do: ~H"{inspect(@pid)}"

  defp size(row) do
    size =
      case row.size do
        {n, :bytes} -> Format.bytes(n)
        {n, :objects} -> "#{n} objects"
        nil -> nil
      end

    progress = row.progress && "#{Float.round(row.progress * 100.0, 1)}%"
    [size, progress] |> Enum.reject(&is_nil/1) |> Enum.join(", ")
  end

  defp load_handoffs(%{beamdash_target: target}), do: fn -> Riak.handoffs(target) end
end
