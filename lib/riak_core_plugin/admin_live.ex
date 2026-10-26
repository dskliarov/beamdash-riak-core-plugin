defmodule RiakCorePlugin.AdminLive do
  @moduledoc """
  The Admin page: cluster changes with riak_core's staged workflow (as
  `riak-admin cluster` does). Joins, leaves, force removes, replacements and
  ring resizes are staged with the claimant; the page shows the plan the
  claimant makes of them (`riak_core_claimant:plan/0`) and commits or
  clears it. Marking a down node as down (`riak_core:down/1`) takes effect
  at once.

  Committing, clearing, force removing, force replacing and marking down
  ask for a confirmation first. Node names from the browser are only taken
  when they are cluster members (or, for a join, nodes the target sees or a
  well-formed `name@host`).
  """
  use Beamdash.Plugin.Page

  import RiakCorePlugin.Components
  alias RiakCorePlugin.{Data, Riak}

  @cluster_ms 5_000
  @plan_ms 10_000
  @node_name ~r/\A[A-Za-z0-9_.+-]+@[A-Za-z0-9_.-]+\z/

  @impl true
  def plugin_mount(_params, _session, socket) do
    loads = [cluster: {@cluster_ms, &load_cluster/1}, plan: {@plan_ms, &load_plan/1}]

    {:ok,
     socket
     |> assign(confirm: nil, busy: false, note: nil, join_node: "")
     |> setup_loads(loads)}
  end

  @impl true
  def connection_changed(event, socket) do
    {:noreply, socket |> assign(confirm: nil, busy: false, note: nil) |> reconnect(event)}
  end

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

    {:noreply, socket |> assign(busy: false, note: note) |> load(:plan) |> load(:cluster)}
  end

  def handle_async(key, result, socket), do: {:noreply, loaded(socket, key, result)}

  @impl true
  def handle_event("join", %{"node" => text}, socket) do
    text = String.trim(text)
    socket = assign(socket, join_node: text)

    case join_node(text, socket.assigns.cluster) do
      {:ok, node} -> {:noreply, run(socket, {:join, node}, "Join of #{node} staged")}
      :error -> {:noreply, assign(socket, note: {:error, "#{inspect(text)} is not a node name"})}
    end
  end

  def handle_event("leave", %{"node" => node}, socket),
    do: member_action(socket, node, &{:leave, &1}, &"Leave of #{&1} staged")

  def handle_event("remove", %{"node" => node}, socket),
    do: member_action(socket, node, &{:remove, &1}, &"Force remove of #{&1} staged")

  def handle_event("down", %{"node" => node}, socket),
    do: member_action(socket, node, &{:down, &1}, &"#{&1} marked down")

  def handle_event("replace", %{"node" => node, "new" => new, "mode" => mode}, socket)
      when mode in ["replace", "force_replace"] do
    with {:ok, node} <- member(socket, node),
         {:ok, new} <- member(socket, new, [:joining]),
         true <- node != new do
      if mode == "replace",
        do: {:noreply, run(socket, {:replace, node, new}, "Replace of #{node} by #{new} staged")},
        else:
          {:noreply,
           ask(socket, {:force_replace, node, new}, "Force replace of #{node} by #{new} staged")}
    else
      _ -> {:noreply, assign(socket, note: {:error, "Pick a member and a joining node"})}
    end
  end

  def handle_event("resize", %{"size" => size}, socket) do
    case Integer.parse(String.trim(size)) do
      {n, ""} when n > 0 ->
        {:noreply, run(socket, {:resize, n}, "Resize to #{n} partitions staged")}

      _ ->
        {:noreply, assign(socket, note: {:error, "The ring size must be a positive number"})}
    end
  end

  def handle_event("abort_resize", _params, socket),
    do: {:noreply, run(socket, :abort_resize, "Abort of the ring resize staged")}

  def handle_event("commit", _params, socket),
    do: {:noreply, ask(socket, :commit, "Plan committed")}

  def handle_event("clear", _params, socket),
    do: {:noreply, ask(socket, :clear, "Staged changes cleared")}

  def handle_event("refresh_plan", _params, socket), do: {:noreply, load(socket, :plan)}

  def handle_event("confirm", _params, %{assigns: %{confirm: {action, text}}} = socket),
    do: {:noreply, socket |> assign(confirm: nil) |> run(action, text)}

  def handle_event("cancel", _params, socket), do: {:noreply, assign(socket, confirm: nil)}

  def handle_event(_event, _params, socket), do: {:noreply, socket}

  # The actions that can lose data or change the cluster at once ask first.
  defp member_action(socket, text, action, done) do
    case member(socket, text) do
      {:ok, node} ->
        case action.(node) do
          {:leave, _} = a -> {:noreply, run(socket, a, done.(node))}
          a -> {:noreply, ask(socket, a, done.(node))}
        end

      :error ->
        {:noreply, assign(socket, note: {:error, "#{inspect(text)} is not a member"})}
    end
  end

  defp ask(socket, action, text), do: assign(socket, confirm: {action, text}, note: nil)

  defp run(%{assigns: %{busy: true}} = socket, _action, _text), do: socket

  defp run(socket, action, text) do
    target = socket.assigns.beamdash_target
    node = socket.assigns.node

    if live?(socket) do
      socket
      |> assign(busy: true, note: nil, confirm: nil)
      |> start_async(:action, fn -> {text, Riak.admin(target, node, action)} end)
    else
      socket
    end
  end

  # A member's node from its name in the browser (with a status in
  # `statuses`, when given).
  defp member(socket, text, statuses \\ nil) do
    with {:ok, cluster} <- socket.assigns.cluster || :error,
         {node, status} <- Enum.find(cluster.members, fn {n, _} -> name(n) == text end),
         true <- statuses == nil or status in statuses do
      {:ok, node}
    else
      _ -> :error
    end
  end

  # A node the target sees, or a new name@host (an atom made from the
  # browser's text, as beamdash's own connect form does).
  defp join_node(text, cluster) do
    candidates =
      case cluster do
        {:ok, c} -> c.candidates
        _ -> []
      end

    case Enum.find(candidates, &(name(&1) == text)) do
      nil ->
        if byte_size(text) <= 255 and Regex.match?(@node_name, text),
          do: {:ok, String.to_atom(text)},
          else: :error

      node ->
        {:ok, node}
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div class="rcp-page">
      <.header title="Admin" node={@node} />
      <p
        :if={@note}
        class={["rcp-note-line", if(elem(@note, 0) == :ok, do: "rcp-ok", else: "rcp-error")]}
        role="status"
      >
        {elem(@note, 1)}
      </p>

      <.result :let={cluster} node={@node} result={@cluster}>
        <div class="rcp-cards">
          <.card label="Ring size" value={"#{cluster.size} partitions"} />
          <.card label="Claimant" value={name(cluster.claimant)} />
          <.card label="Members" value={"#{length(cluster.members)}"} />
        </div>

        <h3>Members</h3>
        <.confirm_box :if={@confirm && not plan_action?(@confirm)} confirm={@confirm} busy={@busy} />
        <table class="rcp-table">
          <thead>
            <tr>
              <th>Node</th><th>Status</th><th class="rcp-num">Partitions</th><th>Up</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            <tr :for={row <- Data.member_rows(cluster.members, cluster.counts, cluster.up)}>
              <td>
                <.node_link node={row.node} attached={@node} return={page_path(assigns, "admin")} />
              </td>
              <td class={"rcp-status-#{row.status}"}>{row.status}</td>
              <td class="rcp-num">{row.partitions}</td>
              <td class={if row.up, do: "rcp-ok", else: "rcp-bad"}>
                {if row.up, do: "up", else: "down"}
              </td>
              <td class="rcp-actions">
                <button
                  :if={row.status in [:valid, :joining]}
                  type="button"
                  class="secondary"
                  phx-click="leave"
                  phx-value-node={row.node}
                  disabled={@busy}
                >
                  Leave
                </button>
                <button
                  :if={row.status in [:valid, :joining, :leaving, :down]}
                  type="button"
                  class="rcp-danger"
                  phx-click="remove"
                  phx-value-node={row.node}
                  disabled={@busy}
                >
                  Force remove
                </button>
                <button
                  :if={not row.up and row.status != :down}
                  type="button"
                  class="rcp-danger"
                  phx-click="down"
                  phx-value-node={row.node}
                  disabled={@busy}
                >
                  Mark down
                </button>
              </td>
            </tr>
          </tbody>
        </table>

        <h3>Join a node</h3>
        <form phx-submit="join" class="rcp-form">
          <input
            type="text"
            name="node"
            value={@join_node}
            list="rcp-join-candidates"
            placeholder="name@host"
            aria-label="Node to join"
            autocomplete="off"
          />
          <datalist id="rcp-join-candidates">
            <option :for={node <- cluster.candidates} value={node} />
          </datalist>
          <button type="submit" disabled={@busy}>Stage join</button>
        </form>
        <p class="rcp-muted">
          The node is asked to join {@node} with <code>riak_core:staged_join/1</code>. It must run
          riak_core with the same ring size, not be in a cluster of its own, and be reachable from {@node}. It shows as joining until the plan is committed.
        </p>

        <h3>Replace a node</h3>
        <form :if={joining(cluster) != []} phx-submit="replace" class="rcp-form">
          <select name="node" aria-label="Node to replace">
            <option :for={{n, status} <- cluster.members} :if={status != :joining} value={n}>
              {n}
            </option>
          </select>
          <span>with</span>
          <select name="new" aria-label="Replacement node">
            <option :for={n <- joining(cluster)} value={n}>{n}</option>
          </select>
          <select name="mode" aria-label="Replace mode">
            <option value="replace">replace (hand off its data)</option>
            <option value="force_replace">force replace (its data is lost)</option>
          </select>
          <button type="submit" disabled={@busy}>Stage</button>
        </form>
        <p :if={joining(cluster) == []} class="rcp-muted">
          Join the replacement node first: a node is replaced by a joining node.
        </p>

        <h3>Resize the ring</h3>
        <form phx-submit="resize" class="rcp-form">
          <input
            type="number"
            name="size"
            min="1"
            placeholder={cluster.size}
            aria-label="New ring size"
          />
          <button type="submit" disabled={@busy}>Stage resize</button>
          <button type="button" class="secondary" phx-click="abort_resize" disabled={@busy}>
            Stage abort
          </button>
        </form>
      </.result>

      <h3>Staged changes</h3>
      <.result :let={plan} node={@node} result={@plan}>
        <%= case plan do %>
          <% {:error, reason} -> %>
            <p class="rcp-error">Can't plan: {Data.admin_error({:error, reason})}</p>
          <% %{changes: []} -> %>
            <p class="rcp-note">Nothing is staged.</p>
          <% %{changes: changes, transitions: transitions} -> %>
            <table class="rcp-table">
              <thead>
                <tr>
                  <th>Node</th><th>Change</th>
                </tr>
              </thead>
              <tbody>
                <tr :for={{node, action} <- changes}>
                  <td>{node}</td>
                  <td>{Data.change_text(action)}</td>
                </tr>
              </tbody>
            </table>
            <div :for={{t, i} <- Enum.with_index(transitions, 1)} class="rcp-transition">
              <h4>
                Transition {i} of {length(transitions)}: {elem(t, 2)} partition transfers
              </h4>
              <table class="rcp-table">
                <thead>
                  <tr>
                    <th>Node</th><th class="rcp-num">Partitions now</th>
                    <th class="rcp-num">After</th>
                  </tr>
                </thead>
                <tbody>
                  <tr :for={row <- Data.transition_rows(t)}>
                    <td>
                      <.node_link
                        node={row.node}
                        attached={@node}
                        return={page_path(assigns, "admin")}
                      />
                    </td>
                    <td class="rcp-num">{row.before}</td>
                    <td class={["rcp-num", row.before != row.after && "rcp-pending"]}>
                      {row.after}
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
        <% end %>
        <.confirm_box :if={@confirm && plan_action?(@confirm)} confirm={@confirm} busy={@busy} />
        <div class="rcp-form">
          <button
            type="button"
            phx-click="commit"
            disabled={@busy or not match?(%{changes: [_ | _]}, plan)}
          >
            Commit
          </button>
          <button type="button" class="secondary" phx-click="clear" disabled={@busy}>
            Clear
          </button>
          <button type="button" class="secondary" phx-click="refresh_plan">Plan again</button>
        </div>
      </.result>
    </div>
    """
  end

  attr :confirm, :any, required: true
  attr :busy, :boolean, required: true

  # Shown next to the buttons that asked for it.
  defp confirm_box(assigns) do
    ~H"""
    <div class="rcp-confirm" role="alertdialog" aria-label="Confirm">
      <span>{confirm_text(elem(@confirm, 0))}</span>
      <button type="button" class="rcp-danger-solid" phx-click="confirm" disabled={@busy}>
        Confirm
      </button>
      <button type="button" class="secondary" phx-click="cancel">Cancel</button>
    </div>
    """
  end

  defp plan_action?({action, _text}), do: action in [:commit, :clear]

  defp joining(cluster), do: for({n, :joining} <- cluster.members, do: n)

  defp confirm_text(:commit),
    do: "Commit the plan above? The cluster starts moving partitions."

  defp confirm_text(:clear), do: "Clear all staged changes?"

  defp confirm_text({:remove, node}),
    do: "Stage a force remove of #{node}? Its partitions are reassigned without handoff."

  defp confirm_text({:force_replace, node, new}),
    do: "Stage a force replace of #{node} by #{new}? Its data is not handed off."

  defp confirm_text({:down, node}),
    do: "Mark #{node} as down now? The ring converges without it until it comes back."

  defp load_cluster(%{beamdash_target: target}), do: fn -> Riak.cluster(target) end
  defp load_plan(%{beamdash_target: target}), do: fn -> Riak.plan(target) end
end
