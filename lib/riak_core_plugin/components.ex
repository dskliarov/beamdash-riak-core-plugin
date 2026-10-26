defmodule RiakCorePlugin.Components do
  @moduledoc """
  What the pages share: their loads and refresh timers, the result
  states, and node names that switch beamdash to that node (`node_link/1`).

  A page declares its loads with `setup_loads/2` in `plugin_mount/3`, and
  passes `handle_async/3`, its `{:rcp_refresh, key, token}` messages and
  `connection_changed/2` on to `loaded/3`, `refresh/3` and `reconnect/2`.
  A load runs with `start_async/3`, so the page renders "Loading…" first and
  stays responsive while the node answers. The next load of a key is
  scheduled when the previous one completes, and only while a node is
  attached: there is at most one timer per load.
  """
  use Phoenix.Component
  require Phoenix.LiveView

  @doc """
  Sets up the page's loads, `[{key, {interval_ms, loader}}]`, and starts them
  when a node is attached. `loader` gets the assigns and returns the
  0-arity function to run; its result is assigned to `key` (`nil` until the
  first load completes).
  """
  def setup_loads(socket, loads) do
    socket
    |> assign(rcp_loads: Map.new(loads), rcp_timers: %{})
    |> assign(Enum.map(loads, fn {key, _} -> {key, nil} end))
    |> load_all()
  end

  @doc """
  Starts the load of `key` now when a node is attached, in place of its
  timer and of a load of it that is still running.
  """
  def load(socket, key) do
    # The replaced load's result is dropped anyway; stop its rpcs too.
    socket = socket |> cancel_timer(key) |> Phoenix.LiveView.cancel_async(key)

    if live?(socket) do
      {_ms, loader} = Map.fetch!(socket.assigns.rcp_loads, key)
      Phoenix.LiveView.start_async(socket, key, loader.(socket.assigns))
    else
      socket
    end
  end

  @doc """
  The page's `handle_async/3` for a load: assigns the result to `key` and
  schedules the next load. With no node attached any more it assigns `nil`
  and schedules nothing.
  """
  def loaded(socket, key, result) do
    socket = cancel_timer(socket, key)

    cond do
      socket.assigns.node == nil -> assign(socket, key, nil)
      match?({:ok, _}, result) -> socket |> assign(key, elem(result, 1)) |> schedule(key)
      true -> socket |> assign(key, {:error, "load failed: #{inspect(result)}"}) |> schedule(key)
    end
  end

  @doc """
  The `{:rcp_refresh, key, token}` timer message: loads `key` again. A
  message of a timer that was replaced or cancelled is ignored.
  """
  def refresh(socket, key, token) do
    case socket.assigns.rcp_timers do
      %{^key => {^token, _tref}} -> load(socket, key)
      _ -> socket
    end
  end

  @doc """
  The page's `connection_changed/2`: drops the timers and the results, and
  loads everything again on `{:connected, node}`.
  """
  def reconnect(socket, event) do
    keys = Map.keys(socket.assigns.rcp_loads)

    socket =
      Enum.reduce(keys, socket, fn key, socket ->
        socket |> cancel_timer(key) |> Phoenix.LiveView.cancel_async(key) |> assign(key, nil)
      end)

    case event do
      {:connected, _node} -> load_all(socket)
      _ -> socket
    end
  end

  defp load_all(socket),
    do: socket.assigns.rcp_loads |> Map.keys() |> Enum.reduce(socket, &load(&2, &1))

  defp schedule(socket, key) do
    {ms, _loader} = Map.fetch!(socket.assigns.rcp_loads, key)
    token = make_ref()
    tref = Process.send_after(self(), {:rcp_refresh, key, token}, ms)
    assign(socket, rcp_timers: Map.put(socket.assigns.rcp_timers, key, {token, tref}))
  end

  defp cancel_timer(socket, key) do
    case Map.pop(socket.assigns.rcp_timers, key) do
      {nil, _} ->
        socket

      {{_token, tref}, timers} ->
        Process.cancel_timer(tref)
        assign(socket, rcp_timers: timers)
    end
  end

  @doc """
  Whether a page should call the node now: the LiveView is connected and a
  node is attached.
  """
  def live?(socket), do: Phoenix.LiveView.connected?(socket) and socket.assigns.node != nil

  @doc "A node or other atom as text; text as it is; other terms inspected."
  def name(nil), do: ""
  def name(atom) when is_atom(atom), do: Atom.to_string(atom)
  def name(text) when is_binary(text), do: text
  def name(other), do: inspect(other)

  attr :node, :any, required: true, doc: "the node to show, an atom (or nil)"
  attr :attached, :atom, default: nil, doc: "the attached node, `@node`"
  attr :return, :string, default: nil, doc: "the page to come back to, `page_path/2`"

  @doc """
  A node's name. Another node than the attached one is a link that moves
  beamdash's connection there, as on beamdash's Nodes tab, and then comes
  back to `return`.
  """
  def node_link(assigns) do
    ~H"""
    <.link
      :if={is_atom(@node) and @node not in [nil, :undefined, @attached]}
      navigate={Beamdash.Plugin.switch_path(@node, @return)}
      class="rcp-node"
      title={"Connect beamdash to #{@node}"}
    >{@node}</.link><span :if={not is_atom(@node) or @node in [nil, :undefined, @attached]}>{name(
      @node
    )}</span>
    """
  end

  @doc "The path of this plugin's page `slug`, for `node_link/1`'s `return`."
  def page_path(%{plugin: %{app: app}}, slug) when app != nil, do: "/plugins/#{app}/#{slug}"
  def page_path(_assigns, _slug), do: nil

  attr :title, :string, required: true
  attr :node, :atom, default: nil

  def header(assigns) do
    ~H"""
    <header class="rcp-header">
      <h2>riak_core: {@title}</h2>
      <span :if={@node} class="rcp-muted">{@node} · refreshes every 5 s</span>
    </header>
    """
  end

  attr :node, :atom, default: nil
  attr :result, :any, required: true
  slot :inner_block, required: true

  @doc """
  Renders the slot with the data of `{:ok, data}`, or the state instead: no
  node attached, loading (`nil`), or the error message.
  """
  def result(assigns) do
    ~H"""
    <%= cond do %>
      <% is_nil(@node) -> %>
        <p class="rcp-note">No node is attached.</p>
      <% is_nil(@result) -> %>
        <p class="rcp-note">Loading…</p>
      <% match?({:error, _}, @result) -> %>
        <p class="rcp-error">{elem(@result, 1)}</p>
      <% true -> %>
        {render_slot(@inner_block, elem(@result, 1))}
    <% end %>
    """
  end

  attr :label, :string, required: true
  attr :value, :string, required: true
  attr :class, :string, default: nil

  def card(assigns) do
    ~H"""
    <div class="rcp-card">
      <div class="rcp-card-label">{@label}</div>
      <div class={["rcp-card-value", @class]}>{@value}</div>
    </div>
    """
  end
end
