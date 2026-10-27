defmodule RiakCorePlugin.RingLive do
  @moduledoc """
  The Ring page: ring size, claimant and ring-ready state, then two views.
  Table: partitions per node and the partition list (index, owner, pending
  next owner, and for an owner that is down the fallback serving it with
  N replicas, riak_core's `target_n_val` on the attached node) with a node
  filter. Ring: the ring drawn as a circle of
  partitions numbered from 0, coloured by owner, with a legend
  (`RiakCorePlugin.RingChart`), and zoom buttons; zoomed in, the ring
  scrolls in its box and big rings number more partitions. The zoom is
  saved in beamdash's settings, so it survives restarts.
  """
  use Beamdash.Plugin.Page

  import RiakCorePlugin.Components
  alias RiakCorePlugin.{Data, Riak, RingChart}

  # riak_core_status:ringready/0 calls every member: less often, on its own.
  @ring_ms 5_000
  @ready_ms 30_000

  @impl true
  def plugin_mount(_params, _session, socket) do
    loads = [ring: {@ring_ms, &load_ring/1}, ready: {@ready_ms, &load_ready/1}]
    {:ok, socket |> assign(filter: "", view: "table", zoom: saved_zoom()) |> setup_loads(loads)}
  end

  @impl true
  def connection_changed(event, socket), do: {:noreply, reconnect(socket, event)}

  @impl true
  def plugin_info({:rcp_refresh, key, token}, socket), do: {:noreply, refresh(socket, key, token)}
  def plugin_info(_msg, socket), do: {:noreply, socket}

  @impl true
  def handle_async(key, result, socket),
    do: {:noreply, socket |> loaded(key, result) |> keep_filter()}

  @impl true
  def handle_event("filter", %{"node" => node}, socket) when is_binary(node),
    do: {:noreply, assign(socket, filter: node)}

  def handle_event("view", %{"view" => view}, socket) when view in ~w(table ring),
    do: {:noreply, assign(socket, view: view)}

  def handle_event("zoom", %{"by" => by}, socket) when by in ~w(in out reset) do
    zooms = RingChart.zooms()
    at = Enum.find_index(zooms, &(&1 == socket.assigns.zoom)) || 0

    at =
      case by do
        "in" -> min(at + 1, length(zooms) - 1)
        "out" -> max(at - 1, 0)
        "reset" -> 0
      end

    zoom = Enum.at(zooms, at)
    save_zoom(zoom)
    {:noreply, assign(socket, zoom: zoom)}
  end

  def handle_event(_event, _params, socket), do: {:noreply, socket}

  # As a whole percent, in beamdash's plugin settings. Older beamdash builds
  # have none: the zoom then starts at 100% each time.
  @zoom_key "ring_zoom"

  @doc false
  def saved_zoom do
    pct = if settings?(), do: Beamdash.Plugin.get_setting(:riak_core_plugin, @zoom_key, 100)
    Enum.find(RingChart.zooms(), hd(RingChart.zooms()), &(round(&1 * 100) == pct))
  end

  defp save_zoom(zoom) do
    if settings?(),
      do: Beamdash.Plugin.put_setting(:riak_core_plugin, @zoom_key, round(zoom * 100))
  end

  defp settings?, do: function_exported?(Beamdash.Plugin, :put_setting, 3)

  @impl true
  def render(assigns) do
    ~H"""
    <div class="rcp-page">
      <.header title="Ring" node={@node} />
      <.result :let={ring} node={@node} result={@ring}>
        <div class="rcp-cards">
          <.card label="Ring size" value={"#{ring.size} partitions"} />
          <.card
            label="Claimant"
            value={name(ring.claimant) <> if(down?(ring, ring.claimant), do: " (down)", else: "")}
            class={down?(ring, ring.claimant) && "rcp-bad"}
          />
          <.card
            label="Ring ready"
            value={ready_text(@ready)}
            class={if match?({:ok, {:ok, _}}, @ready), do: "rcp-ok", else: "rcp-bad"}
          />
        </div>
        <.claimant_note note={Data.claimant_note(ring.claimant, ring.members, ring[:up])} />

        <div :if={Data.down_owners(ring) != []} class="rcp-down-note" role="status">
          <p :for={node <- Data.down_owners(ring)}>
            <strong>{node}</strong>
            is down. It still owns its {Enum.count(ring.owners, &(elem(&1, 1) == node))} partitions: riak_core doesn't move
            ownership when a node goes down. Fallback vnodes on the other nodes serve them until it is back.
          </p>
          <p class="rcp-muted">
            To hand its partitions to the others for good, use the Admin page: Leave if it can come
            back, or Mark down and then Force remove if it can't.
          </p>
        </div>

        <div class="rcp-tabs" role="tablist" aria-label="View">
          <button
            :for={{value, label} <- [{"table", "Table"}, {"ring", "Ring"}]}
            type="button"
            role="tab"
            class={["rcp-tab", @view == value && "active"]}
            aria-selected={to_string(@view == value)}
            phx-click="view"
            phx-value-view={value}
          >
            {label}
          </button>
        </div>

        <.ring_chart
          :if={@view == "ring"}
          ring={ring}
          node={@node}
          zoom={@zoom}
          return={page_path(assigns, "ring")}
        />

        <div :if={@view == "table"}>
          <h3>Partitions per node</h3>
          <table class="rcp-table">
            <thead>
              <tr>
                <th>Node</th><th class="rcp-num">Partitions</th>
              </tr>
            </thead>
            <tbody>
              <tr :for={{node, count} <- Data.owners_by_node(ring.owners)}>
                <td>
                  <.node_link
                    node={node}
                    attached={@node}
                    return={page_path(assigns, "ring")}
                    claimant={node == ring.claimant}
                  />
                  <span :if={down?(ring, node)} class="rcp-down">down</span>
                </td>
                <td class="rcp-num">{count}</td>
              </tr>
            </tbody>
          </table>

          <h3>Partitions</h3>
          <form phx-change="filter" class="rcp-filter">
            <label>
              Owner
              <select name="node">
                <option value="" selected={@filter == ""}>All nodes</option>
                <option
                  :for={{node, _} <- Data.owners_by_node(ring.owners)}
                  value={node}
                  selected={@filter == name(node)}
                >
                  {node}
                </option>
              </select>
            </label>
          </form>
          <dl class="rcp-explain">
            <dt>Owner</dt>
            <dd>
              The node the ring assigns the partition to. Its vnode holds the partition's data,
              and it stays the owner while the node is down: ownership only changes with a
              committed cluster change.
            </dd>
            <dt>Next owner</dt>
            <dd>
              Where a committed change (join, leave, remove, replace, resize) is moving the
              partition. It shows while the partition is handed off, from <code>riak_core_ring:pending_changes/1</code>,
              and is empty once the handoff is done or when nothing is moving.
              A change that is staged but not committed is only a plan: see the Admin page.
            </dd>
            <dt>Fallback</dt>
            <dd>
              Only while the owner is down: the up node that takes the owner's requests meanwhile,
              as riak_core picks it for N replicas (<code>riak_core_apl:get_apl_ann/4</code>,
              N = riak_core's <code>target_n_val</code> on {@node}; an application routing with
              its own N may pick another node). It isn't in the ring and doesn't change ownership:
              when the owner is back, the fallback hands the data back to it (hinted handoff).
              The fallback's vnode starts on the first request routed there, so "not started"
              means nothing was sent there yet.
            </dd>
          </dl>
          <table class="rcp-table rcp-partitions">
            <thead>
              <tr>
                <th class="rcp-num">#</th><th>Index</th><th>Owner</th><th>Next owner</th>
                <th>Fallback{if ring[:n_val], do: " (N=#{ring.n_val})"}</th>
              </tr>
            </thead>
            <tbody>
              <tr
                :for={{row, n} <- Enum.with_index(Data.partition_rows(ring.owners, ring.pending))}
                :if={@filter == "" or name(row.owner) == @filter or name(row.next) == @filter}
              >
                <td class="rcp-num">{n}</td>
                <td class="rcp-mono">{row.index}</td>
                <td>
                  <.node_link
                    node={row.owner}
                    attached={@node}
                    return={page_path(assigns, "ring")}
                    claimant={row.owner == ring.claimant}
                  />
                  <span :if={down?(ring, row.owner)} class="rcp-down">down</span>
                </td>
                <td class={row.next && "rcp-pending"}>
                  <.node_link
                    node={row.next}
                    attached={@node}
                    return={page_path(assigns, "ring")}
                    claimant={row.next != nil and row.next == ring.claimant}
                  />
                </td>
                <td>
                  <.fallback
                    value={fallbacks(ring)[row.index]}
                    node={@node}
                    return={page_path(assigns, "ring")}
                  />
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </.result>
    </div>
    """
  end

  attr :ring, :map, required: true
  attr :node, :atom, default: nil
  attr :zoom, :any, default: 1
  attr :return, :string, default: nil

  defp ring_chart(assigns) do
    owners = Data.owners_by_node(assigns.ring.owners)
    nodes = Enum.map(owners, &elem(&1, 0))
    next = for {_idx, _owner, n, _mods, _status} <- assigns.ring.pending, do: n
    colors = RingChart.colors(Enum.uniq(nodes ++ Enum.sort(next)))
    rows = Data.partition_rows(assigns.ring.owners, assigns.ring.pending)

    assigns =
      assign(assigns,
        owners: owners,
        colors: colors,
        segments:
          rows
          |> RingChart.segments(colors, assigns.zoom)
          |> Enum.map(&Map.put(&1, :fallback, fallbacks(assigns.ring)[&1.row.index])),
        font: elem(RingChart.labels(length(rows), assigns.zoom), 1),
        pending?: Enum.any?(rows, & &1.next),
        down: Data.down_owners(assigns.ring),
        incoming: Enum.uniq(next) -- nodes
      )

    ~H"""
    <div class="rcp-zoom" role="group" aria-label="Zoom">
      <button
        type="button"
        class="rcp-zoom-btn"
        phx-click="zoom"
        phx-value-by="out"
        disabled={@zoom == hd(RingChart.zooms())}
        title="Zoom out"
        aria-label="Zoom out"
      >
        −
      </button>
      <span class="rcp-zoom-level">{round(@zoom * 100)}%</span>
      <button
        type="button"
        class="rcp-zoom-btn"
        phx-click="zoom"
        phx-value-by="in"
        disabled={@zoom == List.last(RingChart.zooms())}
        title="Zoom in"
        aria-label="Zoom in"
      >
        +
      </button>
      <button
        :if={@zoom != hd(RingChart.zooms())}
        type="button"
        class="rcp-zoom-btn rcp-zoom-reset"
        phx-click="zoom"
        phx-value-by="reset"
      >
        Reset
      </button>
    </div>
    <div class="rcp-ring">
      <div class="rcp-ring-box">
        <svg
          viewBox={"0 0 #{RingChart.size()} #{RingChart.size()}"}
          class="rcp-ring-svg"
          style={"width: #{RingChart.width(@zoom)}px"}
          role="img"
          aria-label={"Ring of #{length(@segments)} partitions, coloured by owner"}
        >
          <defs :if={@down != []}>
            <pattern
              id="rcp-hatch"
              width="6"
              height="6"
              patternUnits="userSpaceOnUse"
              patternTransform="rotate(45)"
            >
              <rect width="6" height="6" class="rcp-hatch-bg" />
              <line x1="0" y1="0" x2="0" y2="6" class="rcp-hatch-line" />
            </pattern>
          </defs>
          <g :for={s <- @segments}>
            <path d={s.path} fill={s.fill} class="rcp-ring-seg">
              <title>{segment_title(s)}</title>
            </path>
            <path :if={s.row.owner in @down} d={s.path} fill="url(#rcp-hatch)" class="rcp-ring-down">
              <title>{segment_title(s)} (down)</title>
            </path>
            <path :if={s.band} d={elem(s.band, 0)} fill={elem(s.band, 1)} class="rcp-ring-band">
              <title>{segment_title(s)}</title>
            </path>
            <text
              :if={s.label?}
              x={s.x}
              y={s.y}
              fill={s.text}
              font-size={@font}
              transform={s.rotate != 0 && "rotate(#{s.rotate} #{s.x} #{s.y})"}
              text-anchor="middle"
              dominant-baseline="central"
              class="rcp-ring-label"
            >
              {s.n}
            </text>
          </g>
          <text
            x={RingChart.center()}
            y={RingChart.center() - 8}
            text-anchor="middle"
            class="rcp-ring-center"
          >
            {length(@segments)}
          </text>
          <text
            x={RingChart.center()}
            y={RingChart.center() + 12}
            text-anchor="middle"
            class="rcp-ring-sub"
          >
            partitions
          </text>
        </svg>
      </div>

      <ul class="rcp-legend">
        <li :for={{node, count} <- @owners}>
          <span
            class={["rcp-swatch", node in @down && "rcp-swatch-down"]}
            style={"background: #{elem(@colors[node], 0)}"}
          ></span>
          <.node_link node={node} attached={@node} return={@return} claimant={node == @ring.claimant} />
          <span class="rcp-muted">{count} partitions</span>
          <span :if={node in @down} class="rcp-down">down</span>
        </li>
        <li :for={node <- @incoming}>
          <span class="rcp-swatch" style={"background: #{elem(@colors[node], 0)}"}></span>
          <.node_link node={node} attached={@node} return={@return} claimant={node == @ring.claimant} />
          <span class="rcp-muted">no partitions yet</span>
        </li>
        <li :if={@down != []} class="rcp-muted rcp-legend-note">
          Hatched segments belong to a node that is down.
        </li>
        <li :if={@pending?} class="rcp-muted rcp-legend-note">
          The outer band marks a pending change, in the next owner's colour.
        </li>
      </ul>
    </div>
    """
  end

  attr :value, :any, required: true, doc: "nil, or `{node | nil, running?}`"
  attr :node, :atom, default: nil
  attr :return, :string, default: nil

  defp fallback(%{value: nil} = assigns), do: ~H""

  defp fallback(%{value: {nil, _}} = assigns) do
    ~H"""
    <span class="rcp-bad">none up</span>
    """
  end

  defp fallback(assigns) do
    ~H"""
    <.node_link node={elem(@value, 0)} attached={@node} return={@return} />
    <span class={["rcp-fallback", elem(@value, 1) && "running"]}>
      {if elem(@value, 1), do: "running", else: "not started"}
    </span>
    """
  end

  defp fallbacks(ring), do: Map.get(ring, :fallbacks, %{})

  defp segment_title(%{n: n, row: row} = s) do
    "##{n} · index #{row.index}\nowner #{row.owner}" <>
      if(row.next, do: "\nnext owner #{row.next}", else: "") <>
      case s[:fallback] do
        {node, running} when node != nil ->
          "\nfallback #{node} (#{if running, do: "running", else: "not started"})"

        {nil, _} ->
          "\nno fallback up"

        nil ->
          ""
      end
  end

  defp down?(ring, node), do: node in Data.down_owners(ring) or down_claimant?(ring, node)

  # the claimant may own nothing (a fresh member)
  defp down_claimant?(%{claimant: node, up: up}, node) when is_list(up) and node != :undefined,
    do: node not in up

  defp down_claimant?(_ring, _node), do: false

  defp ready_text({:ok, {_status, text}}), do: text
  defp ready_text({:error, message}), do: message
  defp ready_text(nil), do: "…"

  defp load_ring(%{beamdash_target: target}), do: fn -> Riak.ring(target) end

  defp load_ready(%{beamdash_target: target}), do: fn -> Riak.ring_ready(target) end

  # An owner filter for a node that owns no partition any more is reset.
  defp keep_filter(%{assigns: %{filter: filter, ring: {:ok, ring}}} = socket) when filter != "" do
    owners = for {node, _} <- Data.owners_by_node(ring.owners), do: name(node)
    if filter in owners, do: socket, else: assign(socket, filter: "")
  end

  defp keep_filter(socket), do: socket
end
