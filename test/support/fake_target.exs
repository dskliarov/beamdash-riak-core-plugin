defmodule RiakCorePlugin.Test.FakeTarget do
  @moduledoc false
  # Stands in for a Beamdash.Target server: answers :status with the node
  # set here (nil: none attached) and accepts subscriptions.
  use GenServer

  def start_link(node \\ nil), do: GenServer.start_link(__MODULE__, node)

  def set_node(target, node), do: GenServer.call(target, {:set_node, node})

  @impl true
  def init(node), do: {:ok, node}

  @impl true
  def handle_call(:status, _from, nil), do: {:reply, %{status: :disconnected, node: nil}, nil}
  def handle_call(:status, _from, node), do: {:reply, %{status: :connected, node: node}, node}
  def handle_call({:subscribe, _pid}, _from, node), do: {:reply, :ok, node}
  def handle_call({:set_node, node}, _from, _old), do: {:reply, :ok, node}
end
