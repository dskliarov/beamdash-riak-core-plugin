defmodule RiakCorePlugin.HistoryLive do
  @moduledoc """
  The History page: what the plugin's collectors and the Admin page kept of
  the attached node's cluster (see `RiakCorePlugin.History`), one kind at a
  time, newest first: ended handoffs, member changes, ring changes, load
  samples (with a chart of the busy share) and Admin actions. It shows why
  when no history is kept.
  """
  use Beamdash.Plugin.Page

  import RiakCorePlugin.Components
  alias Beamdash.Plugin.Format
  alias RiakCorePlugin.{Data, History, Riak}

  @rows 200

  @impl true
  def plugin_mount(_params, _session, socket) do
    {:ok,
     socket
     |> assign(kind: "handoffs")
     |> setup_loads(history: {10_000, &load_history/1})}
  end

  @impl true
  def connection_changed(event, socket), do: {:noreply, reconnect(socket, event)}

  @impl true
  def plugin_info({:rcp_refresh, key, token}, socket), do: {:noreply, refresh(socket, key, token)}
  def plugin_info(_msg, socket), do: {:noreply, socket}

  @impl true
  def handle_async(key, result, socket), do: {:noreply, loaded(socket, key, result)}

  @impl true
  def handle_event("kind", %{"kind" => kind}, socket) do
    if History.kind?(kind),
      do: {:noreply, socket |> assign(kind: kind, history: nil) |> load(:history)},
      else: {:noreply, socket}
  end

  def handle_event(_event, _params, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    ~H"""
    <div class="rcp-page">
      <.header title="History" node={@node} />

      <div class="rcp-tabs" role="tablist" aria-label="History">
        <button
          :for={{value, label} <- History.kinds()}
          type="button"
          role="tab"
          class={["rcp-tab", @kind == value && "active"]}
          aria-selected={to_string(@kind == value)}
          phx-click="kind"
          phx-value-kind={value}
        >
          {label}
        </button>
      </div>

      <.result :let={history} node={@node} result={@history}>
        <p class="rcp-note">
          Cluster {history.cluster}. {explain(history.kind)}
        </p>
        <p :if={history.rows == []} class="rcp-note">Nothing yet.</p>
        <.rows :if={history.rows != []} kind={history.kind} rows={history.rows} />
      </.result>
    </div>
    """
  end

  defp explain("handoffs"),
    do:
      "Each completed handoff. Ownership and resize transfers are read from the ring, which marks each one done however short; their times are within 5 s unless a look caught them running. Hinted and repair handoffs are only seen running, so ones shorter than 5 s may be missed."

  defp explain("members"),
    do: "Changes of a member's status, or of whether it is up, looked at every 10 s."

  defp explain("ring"),
    do:
      "Claimant and ring size changes, and partition transfers starting and finishing, looked at every 10 s."

  defp explain("load"), do: "A 1 s sample of the attached node every minute."
  defp explain("admin"), do: "Actions on the Admin page, with their result."

  attr :kind, :string, required: true
  attr :rows, :list, required: true

  defp rows(%{kind: "handoffs"} = assigns) do
    ~H"""
    <table class="rcp-table">
      <thead>
        <tr>
          <th>Ended</th><th>Type</th><th>Module</th><th>Partition</th><th>From → to</th>
          <th>Took</th><th>Size</th>
        </tr>
      </thead>
      <tbody>
        <tr :for={row <- @rows}>
          <td class="rcp-nowrap">{row.at}</td>
          <td>{row.type}</td>
          <td>{row.module}</td>
          <td class="rcp-mono">{row.partition}</td>
          <td>{row.from} → {row.to}</td>
          <td>{row.took}</td>
          <td>{row.size}</td>
        </tr>
      </tbody>
    </table>
    """
  end

  defp rows(%{kind: "members"} = assigns) do
    ~H"""
    <table class="rcp-table">
      <thead>
        <tr>
          <th>Time</th><th>Node</th><th>Change</th>
        </tr>
      </thead>
      <tbody>
        <tr :for={row <- @rows}>
          <td class="rcp-nowrap">{row.at}</td>
          <td>{row.node}</td>
          <td>{row.change}</td>
        </tr>
      </tbody>
    </table>
    """
  end

  defp rows(%{kind: "ring"} = assigns) do
    ~H"""
    <table class="rcp-table">
      <thead>
        <tr>
          <th>Time</th><th>Change</th>
        </tr>
      </thead>
      <tbody>
        <tr :for={row <- @rows}>
          <td class="rcp-nowrap">{row.at}</td>
          <td>{row.change}</td>
        </tr>
      </tbody>
    </table>
    """
  end

  defp rows(%{kind: "load"} = assigns) do
    ~H"""
    <.busy_chart rows={@rows} />
    <table class="rcp-table">
      <thead>
        <tr>
          <th>Time</th><th>Node</th><th class="rcp-num">Average busy</th><th>Busiest scheduler</th>
          <th class="rcp-num">Input</th><th class="rcp-num">Output</th>
          <th class="rcp-num">Memory</th>
        </tr>
      </thead>
      <tbody>
        <tr :for={row <- @rows}>
          <td class="rcp-nowrap">{row.at}</td>
          <td>{row.node}</td>
          <td class="rcp-num">{row.avg_busy}%</td>
          <td>{row.busiest}</td>
          <td class="rcp-num">{rate(row.input)}</td>
          <td class="rcp-num">{rate(row.output)}</td>
          <td class="rcp-num">{row.memory && Format.bytes(row.memory)}</td>
        </tr>
      </tbody>
    </table>
    """
  end

  defp rows(%{kind: "admin"} = assigns) do
    ~H"""
    <table class="rcp-table">
      <thead>
        <tr>
          <th>Time</th><th>Node</th><th>Action</th><th>Result</th>
        </tr>
      </thead>
      <tbody>
        <tr :for={row <- @rows}>
          <td class="rcp-nowrap">{row.at}</td>
          <td>{row.node}</td>
          <td>
            {row.action}
            <ul :if={row.changes != []} class="rcp-history-changes">
              <li :for={change <- row.changes}>{change}</li>
            </ul>
          </td>
          <td class={if row.result == "done", do: "rcp-ok", else: "rcp-bad"}>{row.result}</td>
        </tr>
      </tbody>
    </table>
    """
  end

  @chart_w 600
  @chart_h 120

  attr :rows, :list, required: true

  # The average and busiest scheduler's busy share of the samples shown,
  # oldest on the left, 0–100% from bottom to top.
  defp busy_chart(assigns) do
    points = assigns.rows |> Enum.reverse() |> Enum.filter(&is_number(&1.avg_busy))
    assigns = assign(assigns, points: points, w: @chart_w, h: @chart_h)

    ~H"""
    <figure :if={length(@points) > 1} class="rcp-history-chart">
      <svg
        viewBox={"0 0 #{@w} #{@h}"}
        preserveAspectRatio="none"
        role="img"
        aria-label="Busy share over time"
      >
        <line x1="0" y1={@h} x2={@w} y2={@h} class="rcp-history-axis" />
        <polyline points={polyline(@points, :max_busy, @w, @h)} class="rcp-history-max" />
        <polyline points={polyline(@points, :avg_busy, @w, @h)} class="rcp-history-avg" />
      </svg>
      <figcaption class="rcp-note">
        <span class="rcp-history-key rcp-history-key-avg"></span>
        average busy <span class="rcp-history-key rcp-history-key-max"></span>
        busiest scheduler
        · 0–100%, {List.first(@points).at} to {List.last(@points).at}
      </figcaption>
    </figure>
    """
  end

  defp polyline(points, field, w, h) do
    step = w / max(length(points) - 1, 1)

    points
    |> Enum.with_index()
    |> Enum.map_join(" ", fn {p, i} ->
      busy = p |> Map.fetch!(field) |> Kernel.||(0) |> min(100) |> max(0)
      "#{Float.round(i * step, 1)},#{Float.round(h - busy / 100 * h, 1)}"
    end)
  end

  defp rate(nil), do: nil
  defp rate(bytes), do: "#{Format.bytes(bytes)}/s"

  defp load_history(%{beamdash_target: target, kind: kind}) do
    fn ->
      with {:ok, cluster} <- Riak.cluster_name(target),
           key = Data.cluster_key(cluster),
           {:ok, rows} <- History.query(kind, key, limit: @rows) do
        {:ok, %{kind: kind, cluster: key, rows: Data.history_rows(kind, rows)}}
      end
    end
  end
end
