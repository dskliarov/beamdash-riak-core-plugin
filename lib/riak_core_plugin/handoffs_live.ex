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
  alias RiakCorePlugin.{Data, History, Riak}

  @impl true
  def plugin_mount(_params, _session, socket) do
    {:ok,
     socket
     |> assign(confirm: nil, busy: false, note: nil)
     |> setup_loads(handoffs: {5_000, &load_handoffs/1})}
  end

  @impl true
  def connection_changed(event, socket), do: {:noreply, reconnect(socket, event)}

  @impl true
  def plugin_info({:rcp_refresh, key, token}, socket), do: {:noreply, refresh(socket, key, token)}
  def plugin_info(_msg, socket), do: {:noreply, socket}

  @impl true
  def handle_async(:action, result, socket) do
    note =
      case result do
        {:ok, {text, :ok}} -> {:ok, text}
        {:ok, {text, {:error, message}}} -> {:error, "#{text} failed: #{message}"}
        other -> {:error, "failed: #{inspect(other)}"}
      end

    {:noreply, socket |> assign(busy: false, note: note) |> load(:handoffs)}
  end

  def handle_async(key, result, socket), do: {:noreply, loaded(socket, key, result)}

  @impl true
  def handle_event("concurrency", %{"count" => text}, socket) do
    case Integer.parse(String.trim(text)) do
      {n, ""} when n >= 0 ->
        action = {:concurrency, n}
        label = "Handoff concurrency set to #{n}"

        if n == 0,
          do:
            {:noreply,
             ask(socket, action, "Set concurrency to 0? This stops all handoffs.", label)},
          else: {:noreply, run(socket, action, label)}

      _ ->
        {:noreply, assign(socket, note: {:error, "The concurrency limit must be zero or more"})}
    end
  end

  def handle_event("pause_direction", %{"direction" => direction}, socket) do
    with {:ok, direction} <- direction(direction) do
      {:noreply,
       ask(
         socket,
         {:direction, :disable, direction},
         "Pause #{direction} handoffs? Their active transfers are killed.",
         "Paused #{direction} handoffs"
       )}
    else
      :error -> {:noreply, socket}
    end
  end

  def handle_event("enable_direction", %{"direction" => direction}, socket) do
    with {:ok, direction} <- direction(direction) do
      {:noreply, run(socket, {:direction, :enable, direction}, "Enabled #{direction} handoffs")}
    else
      :error -> {:noreply, socket}
    end
  end

  def handle_event("kill_all", _params, socket),
    do:
      {:noreply,
       ask(socket, :kill_all, "Kill all active handoffs?", "Killed all active handoffs")}

  def handle_event("force", _params, socket),
    do:
      {:noreply,
       ask(
         socket,
         :force,
         "Force every running vnode to start handoff?",
         "Forced every running vnode to start handoff"
       )}

  # What was done (`done`), not the question asked, is shown and recorded.
  def handle_event(
        "confirm",
        _params,
        %{assigns: %{confirm: {action, _question, done}}} = socket
      ),
      do: {:noreply, socket |> assign(confirm: nil) |> run(action, done)}

  def handle_event("cancel", _params, socket), do: {:noreply, assign(socket, confirm: nil)}
  def handle_event(_event, _params, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    ~H"""
    <div class="rcp-page">
      <.header title="Handoffs" node={@node} />
      <.result :let={handoffs} node={@node} result={@handoffs}>
        <p
          :if={@note}
          class={["rcp-note-line", if(elem(@note, 0) == :ok, do: "rcp-ok", else: "rcp-error")]}
          role="status"
        >
          {elem(@note, 1)}
        </p>

        <div class="rcp-cards">
          <.card label="Concurrency" value={"#{handoffs.settings.concurrency}"} />
          <.card
            label="Inbound"
            value={if handoffs.settings.inbound, do: "enabled", else: "paused"}
            class={if handoffs.settings.inbound, do: "rcp-ok", else: "rcp-bad"}
          />
          <.card
            label="Outbound"
            value={if handoffs.settings.outbound, do: "enabled", else: "paused"}
            class={if handoffs.settings.outbound, do: "rcp-ok", else: "rcp-bad"}
          />
        </div>

        <div class="rcp-form">
          <form phx-submit="concurrency">
            <input
              type="number"
              name="count"
              min="0"
              value={handoffs.settings.concurrency}
              aria-label="Handoff concurrency"
            />
            <button type="submit" disabled={@busy}>Set limit</button>
          </form>
          <button
            :if={handoffs.settings.inbound}
            type="button"
            class="rcp-danger"
            phx-click="pause_direction"
            phx-value-direction="inbound"
            disabled={@busy}
          >
            Pause inbound
          </button>
          <button
            :if={!handoffs.settings.inbound}
            type="button"
            class="secondary"
            phx-click="enable_direction"
            phx-value-direction="inbound"
            disabled={@busy}
          >
            Enable inbound
          </button>
          <button
            :if={handoffs.settings.outbound}
            type="button"
            class="rcp-danger"
            phx-click="pause_direction"
            phx-value-direction="outbound"
            disabled={@busy}
          >
            Pause outbound
          </button>
          <button
            :if={!handoffs.settings.outbound}
            type="button"
            class="secondary"
            phx-click="enable_direction"
            phx-value-direction="outbound"
            disabled={@busy}
          >
            Enable outbound
          </button>
          <button type="button" class="rcp-danger" phx-click="kill_all" disabled={@busy}>
            Kill all
          </button>
          <button type="button" class="secondary" phx-click="force" disabled={@busy}>
            Force handoffs
          </button>
        </div>

        <div :if={@confirm} class="rcp-confirm" role="alertdialog" aria-label="Confirm">
          <span>{elem(@confirm, 1)}</span>
          <button type="button" class="rcp-danger-solid" phx-click="confirm" disabled={@busy}>
            Confirm
          </button>
          <button type="button" class="secondary" phx-click="cancel">Cancel</button>
        </div>

        <p :if={handoffs.active == [] and handoffs.pending == []} class="rcp-note">No handoffs.</p>

        <div :if={handoffs.active != []}>
          <h3>Active handoffs</h3>
          <table class="rcp-table">
            <thead>
              <tr>
                <th>Type</th><th>Module</th><th>Partition</th><th>From → to</th><th>Direction</th>
                <th>Status</th><th>Size / progress</th><th>Rate</th><th>ETA</th><th>Started</th><th>
                  Sender
                </th>
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
                  <td>{rate(row)}</td>
                  <td>{eta(row)}</td>
                  <td>{row.start}</td>
                  <td class="rcp-mono"><.sender pid={row.sender} syntax={@syntax} /></td>
                <% else %>
                  <td colspan="11" class="rcp-mono">{row.text}</td>
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

  defp rate(%{size: {_, :bytes}, bytes_per_s: n}) when is_integer(n),
    do: Format.bytes(n) <> "/s"

  defp rate(%{objs_per_s: n}) when is_integer(n), do: "#{n} objects/s"
  defp rate(%{bytes_per_s: n}) when is_integer(n), do: Format.bytes(n) <> "/s"
  defp rate(_row), do: nil

  defp eta(%{eta_ms: ms}) when is_integer(ms), do: Data.uptime(ms)
  defp eta(_row), do: nil

  defp direction("inbound"), do: {:ok, :inbound}
  defp direction("outbound"), do: {:ok, :outbound}
  defp direction(_), do: :error

  defp ask(socket, action, question, done),
    do: assign(socket, confirm: {action, question, done}, note: nil)

  defp run(%{assigns: %{busy: true}} = socket, _action, _text), do: socket

  defp run(socket, action, text) do
    target = socket.assigns.beamdash_target
    node = socket.assigns.node

    key =
      case socket.assigns.handoffs do
        {:ok, %{settings: %{cluster: cluster}}} -> cluster
        _ -> nil
      end

    if live?(socket) do
      socket
      |> assign(busy: true, note: nil, confirm: nil)
      |> start_async(:action, fn ->
        result = Riak.handoff_action(target, action)
        History.record_admin(key, node, text, result)
        {text, result}
      end)
    else
      socket
    end
  end

  defp load_handoffs(%{beamdash_target: target}), do: fn -> Riak.handoffs(target) end
end
