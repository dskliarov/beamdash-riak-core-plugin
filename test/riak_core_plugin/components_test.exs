defmodule RiakCorePlugin.ComponentsTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest, only: [render_component: 2]
  alias RiakCorePlugin.Components

  test "another node's name links to switching beamdash to it, then back" do
    html =
      render_component(&Components.node_link/1,
        node: :"kv2@127.0.0.1",
        attached: :"kv1@127.0.0.1",
        return: "/plugins/riak_core_plugin/ring"
      )

    assert html =~
             ~s(href="/nodes?switch=kv2%40127.0.0.1&amp;return=%2Fplugins%2Friak_core_plugin%2Fring")

    assert html =~ ~s(class="rcp-node")
    assert html =~ ">kv2@127.0.0.1</a>"
  end

  test "the attached node, nil and other terms are plain text" do
    for {node, text} <- [{:"kv1@127.0.0.1", "kv1@127.0.0.1"}, {nil, ""}, {"x", "x"}] do
      html = render_component(&Components.node_link/1, node: node, attached: :"kv1@127.0.0.1")
      refute html =~ "<a"
      assert html =~ "<span>#{text}</span>"
    end
  end

  test "page_path is the plugin page's path" do
    assert Components.page_path(%{plugin: %{app: :riak_core_plugin}}, "ring") ==
             "/plugins/riak_core_plugin/ring"
  end

  test "the claimant note: muted, in the error colour when the claimant is down, or nothing" do
    html = render_component(&Components.claimant_note/1, note: {:ok, "b@h is next"})
    assert html =~ ~s(class="rcp-claimant-note rcp-muted")
    assert html =~ "b@h is next"

    html = render_component(&Components.claimant_note/1, note: {:bad, "a@h is down"})
    assert html =~ ~s(class="rcp-claimant-note rcp-bad")

    refute render_component(&Components.claimant_note/1, note: nil) =~ "<p"
  end

  test "the claimant's name is set apart, as a link or as plain text" do
    html =
      render_component(&Components.node_link/1,
        node: :"kv2@127.0.0.1",
        attached: :"kv1@127.0.0.1",
        claimant: true
      )

    assert html =~ ~s(class="rcp-node rcp-claimant")
    assert html =~ ~s|title="Connect beamdash to kv2@127.0.0.1 (the claimant)"|

    html =
      render_component(&Components.node_link/1,
        node: :"kv1@127.0.0.1",
        attached: :"kv1@127.0.0.1",
        claimant: true
      )

    assert html =~ ~s(<span class="rcp-claimant" title="the claimant">kv1@127.0.0.1</span>)
  end
end
