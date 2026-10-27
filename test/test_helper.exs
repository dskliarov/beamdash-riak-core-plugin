# The Riak and page tests call a :peer node over distribution: make sure
# epmd runs, then make this VM a node.
System.cmd("epmd", ["-daemon"])

unless Node.alive?() do
  {:ok, _} = Node.start(:"rcp_test_#{System.unique_integer([:positive])}", :shortnames)
end

# beamdash's settings (the ring zoom is saved there) go to a scratch file
settings_dir = Path.join(System.tmp_dir!(), "rcp_settings_#{System.unique_integer([:positive])}")
File.mkdir_p!(settings_dir)
Application.put_env(:beamdash, :settings_file, Path.join(settings_dir, "settings.json"))

Code.require_file("support/fake_target.exs", __DIR__)
Code.require_file("support/peer.exs", __DIR__)

ExUnit.start()
