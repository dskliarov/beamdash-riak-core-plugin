defmodule RiakCorePlugin.LoadLive do
  @moduledoc """
  The Load page: how busy each scheduler is, the node's I/O rates and
  memory, and the processes that used the most reductions over a sample. The
  first sample switches `scheduler_wall_time` on at the attached node, and it
  stays on after that. Erlang doesn't say which scheduler a process ran on, so
  the busiest processes are where to look for a busy scheduler, not proof of it.
  """
  use Beamdash.Plugin.Page

  import RiakCorePlugin.Components
  alias Beamdash.Plugin.Format
  alias RiakCorePlugin.Riak

  @impl true
  def plugin_mount(_params, _session, socket) do
    {:ok, setup_loads(socket, load: {5_000, &load_load/1})}
  end

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
      <.header title="Load" node={@node} />
      <.result :let={load} node={@node} result={@load}>
        <p class="rcp-note">
          Busy is the share of wall time each scheduler ran Erlang code, measured over {load.window_ms} ms.
        </p>
        <div class="rcp-cards">
          <.card label="Average busy" value={percent(load.avg_busy)} />
          <.card
            label="Busiest scheduler"
            value={"#{load.hottest.id}: #{percent(load.hottest.busy)}"}
          />
          <.card label="Least busy" value={"#{load.coolest.id}: #{percent(load.coolest.busy)}"} />
          <.card label="Input" value={rate(load.io.input)} />
          <.card label="Output" value={rate(load.io.output)} />
        </div>

        <h3>Schedulers</h3>
        <table class="rcp-table">
          <thead>
            <tr>
              <th>Scheduler</th>
              <th class="rcp-num">Busy</th>
              <th>Share of time</th>
            </tr>
          </thead>
          <tbody>
            <tr :for={s <- load.schedulers}>
              <td class="rcp-num">{s.id}</td>
              <td class="rcp-num">{percent(s.busy)}</td>
              <td>
                <div class="rcp-bar">
                  <div class="rcp-bar-fill" style={"width: #{Float.round(s.busy, 1)}%"}></div>
                </div>
              </td>
            </tr>
          </tbody>
        </table>

        <h3>Memory</h3>
        <table class="rcp-table">
          <tbody>
            <tr :for={{label, bytes} <- load.memory}>
              <td>{label}</td>
              <td class="rcp-num">{Format.bytes(bytes)}</td>
            </tr>
          </tbody>
        </table>

        <h3>Busiest processes</h3>
        <p class="rcp-note">
          The processes that used the most reductions (Erlang's unit of work) in the sample.
        </p>
        <table class="rcp-table">
          <thead>
            <tr>
              <th>Pid</th>
              <th>Name</th>
              <th class="rcp-num">Reductions/s</th>
              <th class="rcp-num">Mailbox</th>
            </tr>
          </thead>
          <tbody>
            <tr :for={p <- load.processes}>
              <td class="rcp-mono">
                <.link navigate={Beamdash.Plugin.process_path(p.pid)} class="rcp-pid">
                  {Format.pid(p.pid, @syntax)}
                </.link>
              </td>
              <td class="rcp-mono">{p.name}</td>
              <td class="rcp-num">{p.reductions_per_s}</td>
              <td class="rcp-num">{p.mailbox}</td>
            </tr>
          </tbody>
        </table>
      </.result>
    </div>
    """
  end

  defp percent(x), do: :erlang.float_to_binary(x * 1.0, decimals: 1) <> "%"

  defp rate(bytes), do: Format.bytes(bytes) <> "/s"

  defp load_load(%{beamdash_target: target}), do: fn -> Riak.load(target) end
end
