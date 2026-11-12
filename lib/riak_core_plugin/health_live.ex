defmodule RiakCorePlugin.HealthLive do
  @moduledoc """
  The Cluster Health page: ring readiness, down and marked-down members,
  handoff queue and replica coverage, with the warnings worth acting on.
  It loads every 30 s because `riak_core_status:ring_status/0` asks every
  member.
  """
  use Beamdash.Plugin.Page

  import RiakCorePlugin.Components
  alias RiakCorePlugin.Riak

  @health_ms 30_000

  @impl true
  def plugin_mount(_params, _session, socket),
    do: {:ok, setup_loads(socket, health: {@health_ms, &load_health/1})}

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
      <.header title="Health" node={@node} refresh="refreshes every 30 s" />
      <.result :let={health} node={@node} result={@health}>
        <div class="rcp-cards">
          <.card
            label="Ring ready"
            value={ready_text(health.ring_ready)}
            class={if health.ring_ready == true, do: "rcp-ok", else: "rcp-bad"}
          />
          <.card label="Claimant" value={name(health.claimant)} />
          <.card label="Members down" value={"#{length(health.down)}"} />
          <.card
            label="Under-replicated"
            value={"#{health.coverage.under_count} / #{health.coverage.partitions}"}
            class={health.coverage.under_count > 0 && "rcp-bad"}
          />
          <.card label="Pending transfers" value={"#{health.pending_transfers}"} />
        </div>

        <h3>Warnings</h3>
        <ul class="rcp-alerts">
          <li :for={alert <- health.alerts} class={"rcp-alert rcp-#{alert.level}"}>
            {alert.text}
          </li>
        </ul>

        <h3>Replica coverage</h3>
        <p class="rcp-note">
          N = {health.coverage.n_val}. A partition is under-replicated when fewer than N of its
          preflist nodes are up.
        </p>
        <table class="rcp-table">
          <thead>
            <tr>
              <th>Node</th><th class="rcp-num">Replicas</th>
            </tr>
          </thead>
          <tbody>
            <tr :for={{node, count} <- health.coverage.replica_load}>
              <td>
                <.node_link node={node} attached={@node} return={page_path(assigns, "health")} />
              </td>
              <td class="rcp-num">{count}</td>
            </tr>
          </tbody>
        </table>
        <div :if={health.coverage.under != []}>
          <h4>Under-replicated sample</h4>
          <table class="rcp-table">
            <thead>
              <tr>
                <th class="rcp-num">Partition</th><th class="rcp-num">Up</th><th>Up replicas</th>
              </tr>
            </thead>
            <tbody>
              <tr :for={{index, nodes} <- health.coverage.under}>
                <td class="rcp-mono">{index}</td>
                <td class="rcp-num rcp-bad">{length(nodes)} of {health.coverage.n_val}</td>
                <td>
                  <span :if={nodes == []} class="rcp-bad">none</span>
                  <ul :if={nodes != []} class="rcp-node-list">
                    <li :for={node <- nodes}>
                      <.node_link node={node} attached={@node} return={page_path(assigns, "health")} />
                    </li>
                  </ul>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <h3>Handoff queue</h3>
        <p :if={health.waiting == [] and health.stopped == []} class="rcp-note">
          No waiting or stopped partitions.
        </p>
        <table :if={health.waiting != [] or health.stopped != []} class="rcp-table">
          <thead>
            <tr>
              <th>Node</th><th>State</th><th class="rcp-num">Partitions</th>
            </tr>
          </thead>
          <tbody>
            <tr :for={row <- health.waiting}>
              <td>
                <.node_link node={row.node} attached={@node} return={page_path(assigns, "health")} />
              </td>
              <td>waiting to handoff</td>
              <td class="rcp-num">{row.count}</td>
            </tr>
            <tr :for={row <- health.stopped}>
              <td>
                <.node_link node={row.node} attached={@node} return={page_path(assigns, "health")} />
              </td>
              <td class="rcp-bad">stopped</td>
              <td class="rcp-num">{row.count}</td>
            </tr>
          </tbody>
        </table>

        <h3>Pending transfers</h3>
        <p :if={health.transfer_status == []} class="rcp-note">Nothing is transferring.</p>
        <table :if={health.transfer_status != []} class="rcp-table">
          <thead>
            <tr>
              <th>Owner</th><th>Next owner</th><th class="rcp-num">Partition</th>
              <th>Waiting modules</th><th>Done modules</th><th>Status</th>
            </tr>
          </thead>
          <tbody>
            <tr :for={transfer <- health.transfers}>
              <td>
                <.node_link
                  node={transfer.owner}
                  attached={@node}
                  return={page_path(assigns, "health")}
                />
              </td>
              <td>
                <.node_link
                  node={transfer.next}
                  attached={@node}
                  return={page_path(assigns, "health")}
                />
              </td>
              <td class="rcp-mono">{transfer.index}</td>
              <td><.modules list={transfer.waiting_modules} /></td>
              <td><.modules list={transfer.done_modules} /></td>
              <td>{inspect(transfer.status)}</td>
            </tr>
          </tbody>
        </table>
      </.result>
    </div>
    """
  end

  defp ready_text(true), do: "ready"
  defp ready_text(false), do: "not ready"
  defp ready_text(:undefined), do: "unknown"
  defp ready_text(other), do: inspect(other)

  defp modules(%{list: list} = assigns) when is_list(list),
    do: ~H"<.module_links modules={@list} />"

  defp modules(%{list: :down} = assigns), do: ~H"down"
  defp modules(assigns), do: ~H"{inspect(@list)}"

  defp load_health(%{beamdash_target: target}), do: fn -> Riak.health(target) end
end
