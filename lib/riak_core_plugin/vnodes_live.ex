defmodule RiakCorePlugin.VnodesLive do
  @moduledoc """
  The Vnodes page: module, partition, pid, mailbox, memory and reductions of
  each vnode, sortable by column, with a module filter. The filter's options
  are the modules of all vnodes. Process info is fetched for the first
  `RiakCorePlugin.Riak.max_vnodes/0` vnodes of the filtered set (by module
  and partition); sorting uses what was fetched. A pid links to the
  process on beamdash's Processes tab.
  """
  use Beamdash.Plugin.Page

  import RiakCorePlugin.Components
  alias Beamdash.Plugin.Format
  alias RiakCorePlugin.{Data, Riak}

  @impl true
  def plugin_mount(_params, _session, socket) do
    socket = assign(socket, sort: {:mailbox, :desc}, module: "")
    {:ok, setup_loads(socket, vnodes: {5_000, &load_vnodes/1})}
  end

  @impl true
  def connection_changed(event, socket), do: {:noreply, reconnect(socket, event)}

  @impl true
  def plugin_info({:rcp_refresh, key, token}, socket), do: {:noreply, refresh(socket, key, token)}
  def plugin_info(_msg, socket), do: {:noreply, socket}

  # Keeps the module filter that was applied: one with no vnodes any more
  # comes back as "".
  @impl true
  def handle_async(key, result, socket) do
    socket = loaded(socket, key, result)

    case socket.assigns.vnodes do
      {:ok, %{module: module}} -> {:noreply, assign(socket, module: module)}
      _ -> {:noreply, socket}
    end
  end

  @impl true
  def handle_event("sort", %{"col" => col}, socket) do
    case Enum.find(Data.vnode_columns(), &(Atom.to_string(&1) == col)) do
      nil ->
        {:noreply, socket}

      column ->
        sort =
          case socket.assigns.sort do
            {^column, :desc} -> {column, :asc}
            {^column, :asc} -> {column, :desc}
            _ -> {column, if(column in [:module, :index, :pid], do: :asc, else: :desc)}
          end

        {:noreply, assign(socket, sort: sort)}
    end
  end

  # The cap applies after the filter: fetch the module's vnodes now.
  def handle_event("filter", %{"module" => module}, socket) when is_binary(module),
    do: {:noreply, socket |> assign(module: module) |> load(:vnodes)}

  def handle_event(_event, _params, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    ~H"""
    <div class="rcp-page">
      <.header title="Vnodes" node={@node} />
      <.result :let={vnodes} node={@node} result={@vnodes}>
        <p :if={vnodes.total > length(vnodes.fetched)} class="rcp-note">
          Showing the first {length(vnodes.fetched)} of {vnodes.total} {if vnodes.module != "",
            do: vnodes.module <> " "}vnodes.
        </p>
        <form phx-change="filter" class="rcp-filter">
          <label>
            Module
            <select name="module">
              <option value="" selected={@module == ""}>All modules</option>
              <option
                :for={mod <- vnodes.modules}
                value={mod}
                selected={@module == name(mod)}
              >
                {mod}
              </option>
            </select>
          </label>
        </form>
        <table class="rcp-table">
          <thead>
            <tr>
              <th
                :for={{col, label} <- columns()}
                class={["rcp-sortable", col in [:index, :mailbox, :memory, :reductions] && "rcp-num"]}
                phx-click="sort"
                phx-value-col={col}
              >
                {label}{arrow(@sort, col)}
              </th>
            </tr>
          </thead>
          <tbody>
            <tr :for={row <- Data.vnode_rows(vnodes.fetched, @sort)}>
              <td>{row.module}</td>
              <td class="rcp-num rcp-mono">{row.index}</td>
              <td class="rcp-mono">
                <.link navigate={Beamdash.Plugin.process_path(row.pid)} class="rcp-pid">
                  {Format.pid(row.pid, @syntax)}
                </.link>
              </td>
              <td class="rcp-num">{row.mailbox || "exited"}</td>
              <td class="rcp-num">{row.memory && Format.bytes(row.memory)}</td>
              <td class="rcp-num">{row.reductions}</td>
            </tr>
          </tbody>
        </table>
        <p :if={vnodes.fetched == []} class="rcp-note">No vnodes.</p>
      </.result>
    </div>
    """
  end

  defp columns do
    [
      module: "Module",
      index: "Partition",
      pid: "Pid",
      mailbox: "Mailbox",
      memory: "Memory",
      reductions: "Reductions"
    ]
  end

  defp arrow({col, :asc}, col), do: " ▲"
  defp arrow({col, :desc}, col), do: " ▼"
  defp arrow(_sort, _col), do: ""

  defp load_vnodes(%{beamdash_target: target, module: module}),
    do: fn -> Riak.vnodes(target, module) end
end
