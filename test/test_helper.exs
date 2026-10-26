# The Riak and page tests call a :peer node over distribution: make sure
# epmd runs, then make this VM a node.
System.cmd("epmd", ["-daemon"])

unless Node.alive?() do
  {:ok, _} = Node.start(:"rcp_test_#{System.unique_integer([:positive])}", :shortnames)
end

Code.require_file("support/fake_target.exs", __DIR__)
Code.require_file("support/peer.exs", __DIR__)

ExUnit.start()
