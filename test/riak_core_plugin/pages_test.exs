defmodule RiakCorePlugin.PagesTest do
  # The pages rendered from recorded data, by calling render/1 directly.
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest, only: [rendered_to_string: 1]

  alias RiakCorePlugin.{AdminLive, HandoffsLive, MembersLive, RingLive, VnodesLive}

  defp pid, do: :c.pid(0, 101, 0)

  defp render(module, assigns) do
    %{__changed__: nil, node: :a@h, syntax: :erlang}
    |> Map.merge(assigns)
    |> module.render()
    |> rendered_to_string()
  end

  @ring %{
    size: 4,
    claimant: :a@h,
    owners: [{0, :a@h}, {1, :b@h}, {2, :a@h}, {3, :b@h}],
    pending: [{1, :b@h, :c@h, [:my_vnode], :awaiting}],
    members: [{:a@h, :valid}, {:b@h, :leaving}]
  }

  test "the entry module declares the five pages, the Ring page the main one" do
    assert %{name: "riak_core", pages: pages, main: "ring"} = RiakCorePlugin.__beamdash_plugin__()
    assert Enum.map(pages, & &1.slug) == ["ring", "members", "vnodes", "handoffs", "admin"]
  end

  test "states: no node, loading, error" do
    assert render(RingLive, %{
             node: nil,
             ring: nil,
             ready: nil,
             filter: "",
             view: "table"
           }) =~
             "No node is attached"

    assert render(RingLive, %{ring: nil, ready: nil, filter: "", view: "table"}) =~
             "Loading"

    assert render(MembersLive, %{members: {:error, "riak_core is not running"}}) =~
             "riak_core is not running"
  end

  test "ring" do
    html =
      render(RingLive, %{
        ring: {:ok, @ring},
        ready: {:ok, {:ok, "ready (2 nodes)"}},
        filter: "",
        view: "table"
      })

    assert html =~ "4 partitions"
    assert html =~ "ready (2 nodes)"
    assert html =~ "c@h"

    filtered =
      render(RingLive, %{
        ring: {:ok, @ring},
        ready: {:error, "boom"},
        filter: "a@h",
        view: "table"
      })

    # what the columns mean, also when nothing is moving or down
    assert html =~ ~s(<dl class="rcp-explain">)
    assert html =~ "<dt>Next owner</dt>"
    assert html =~ "riak_core_ring:pending_changes/1"
    assert html =~ "<dt>Fallback</dt>"
    assert html =~ "hinted handoff"

    assert filtered =~ "boom"
    # partition 1 belongs to b@h (moving to c@h): filtered out of the list
    refute filtered =~ "rcp-pending"
  end

  test "admin: Mark down only on the members that don't respond and aren't marked down yet" do
    cluster = %{
      size: 4,
      claimant: :a@h,
      members: [{:a@h, :valid}, {:b@h, :valid}, {:c@h, :down}],
      counts: [{:a@h, 2}, {:b@h, 2}],
      up: [:a@h],
      candidates: []
    }

    html =
      render(AdminLive, %{
        cluster: {:ok, cluster},
        plan: nil,
        confirm: nil,
        busy: false,
        note: nil,
        join_node: ""
      })

    buttons = Regex.scan(~r/<button[^>]*phx-click="down"[^>]*>/, html) |> List.flatten()
    assert [b] = buttons
    assert b =~ ~s(phx-value-node="b@h") and not (b =~ "disabled")
    assert b =~ ~s(title="Mark b@h as down now")
  end

  test "ring: the claimant is set apart wherever it is listed" do
    html = render(RingLive, %{ring: {:ok, @ring}, ready: nil, filter: "", view: "table"})
    # a@h is the attached node (plain text), b@h a link
    assert html =~ ~s(<span class="rcp-claimant" title="the claimant">a@h</span>)
    refute html =~ ~r/class="rcp-node rcp-claimant"[^>]*>b@h/
    assert html =~ ~s(<table class="rcp-table rcp-partitions">)

    html = render(RingLive, %{ring: {:ok, @ring}, ready: nil, filter: "", view: "ring", zoom: 1})
    assert html =~ ~s(<span class="rcp-claimant" title="the claimant">a@h</span>)
  end

  test "ring: who takes over from the claimant, when the up nodes are known" do
    assigns = %{ready: nil, filter: "", view: "table"}

    html = render(RingLive, Map.put(assigns, :ring, {:ok, Map.put(@ring, :up, [:a@h, :b@h])}))
    assert html =~ "If a@h leaves or is marked down, b@h becomes the claimant."

    html = render(RingLive, Map.put(assigns, :ring, {:ok, Map.put(@ring, :up, [:b@h])}))
    assert html =~ ~s(class="rcp-claimant-note rcp-bad")
    assert html =~ "makes b@h the claimant"

    refute render(RingLive, Map.put(assigns, :ring, {:ok, @ring})) =~ "rcp-claimant-note"
  end

  test "ring view: one numbered segment per partition, coloured by owner" do
    html = render(RingLive, %{ring: {:ok, @ring}, ready: nil, filter: "", view: "ring", zoom: 1})
    assert html =~ ~s(role="tab" class="rcp-tab active" aria-selected="true")
    refute html =~ "Partitions per node"

    # a@h first in the palette, b@h second; c@h only as the next owner
    segments = Regex.scan(~r/<path d="[^"]+" fill="(#[0-9a-f]+)" class="rcp-ring-seg">/, html)
    assert Enum.map(segments, &List.last/1) == ~w(#4e79a7 #f28e2b #4e79a7 #f28e2b)

    for n <- 0..3, do: assert(html =~ ~r/class="rcp-ring-label">\s*#{n}\s*</)
    assert [[_, "#e15759"]] = Regex.scan(~r/fill="(#[0-9a-f]+)" class="rcp-ring-band"/, html)
    assert html =~ "#1 · index 1\nowner b@h\nnext owner c@h"
    assert html =~ "no partitions yet"
    assert html =~ "The outer band marks a pending change"

    # zoom: 100% to start, out disabled, no Reset
    assert html =~ ~s(style="width: 460px")
    assert html =~ ~r/<button[^>]*phx-value-by="out"[^>]*disabled/
    assert html =~ "100%"
    refute html =~ "Reset"
  end

  test "ring view zoomed in is wider, with Reset; in stops at the largest" do
    html = render(RingLive, %{ring: {:ok, @ring}, ready: nil, filter: "", view: "ring", zoom: 4})
    assert html =~ ~s(style="width: 1840px")
    assert html =~ "400%"
    assert html =~ "Reset"
    assert html =~ ~r/<button[^>]*phx-value-by="in"[^>]*disabled/
    refute html =~ ~r/<button[^>]*phx-value-by="out"[^>]*disabled/
  end

  test "zoom steps through the levels and resets" do
    socket = %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}, zoom: 1}}

    step = fn socket, by ->
      {:noreply, socket} = RingLive.handle_event("zoom", %{"by" => by}, socket)
      socket
    end

    socket = step.(socket, "out")
    assert socket.assigns.zoom == 1
    assert step.(socket, "in").assigns.zoom == 1.05
    socket = Enum.reduce(1..61, socket, fn _, s -> step.(s, "in") end)
    assert socket.assigns.zoom == 4
    assert step.(socket, "out").assigns.zoom == 3.95
    assert step.(socket, "reset").assigns.zoom == 1
  end

  test "ring: an owner that is down is said so, in the table and the ring" do
    ring = Map.put(@ring, :up, [:a@h])
    html = render(RingLive, %{ring: {:ok, ring}, ready: nil, filter: "", view: "table"})
    assert html =~ ~r/<strong>b@h<\/strong>\s*is down. It still owns its\s+2 partitions/
    assert html =~ "Mark down and then Force remove"
    assert length(Regex.scan(~r/class="rcp-down">down</, html)) == 3
    refute html =~ "a@h is down"
    # the claimant a@h is up
    refute html =~ "(down)"

    html = render(RingLive, %{ring: {:ok, ring}, ready: nil, filter: "", view: "ring", zoom: 1})
    assert html =~ ~s(<pattern id="rcp-hatch")
    assert length(Regex.scan(~r/fill="url\(#rcp-hatch\)"/, html)) == 2
    assert html =~ "Hatched segments belong to a node that is down."

    html =
      render(RingLive, %{
        ring: {:ok, %{ring | up: [:b@h]}},
        ready: nil,
        filter: "",
        view: "table"
      })

    assert html =~ "a@h (down)"
  end

  test "ring: the Fallback column for partitions whose owner is down" do
    ring =
      @ring
      |> Map.put(:up, [:a@h])
      |> Map.put(:fallbacks, %{1 => {:a@h, true}, 3 => {nil, false}})

    html = render(RingLive, %{ring: {:ok, ring}, ready: nil, filter: "", view: "table"})
    assert html =~ "<th>Fallback</th>"
    assert html =~ ~r/<span>a@h<\/span>\s*<span class="rcp-fallback running">\s*running/
    assert html =~ ~s(<span class="rcp-bad">none up</span>)
    assert html =~ "riak_core_apl:get_apl_ann/4"

    # N is the attached node's riak_core target_n_val, in the header; no picking
    html =
      render(RingLive, %{
        ring: {:ok, Map.put(ring, :n_val, 4)},
        ready: nil,
        filter: "",
        view: "table"
      })

    assert html =~ "<th>Fallback (N=4)</th>"
    refute html =~ ~s(name="n_val")

    not_started = %{ring | fallbacks: %{1 => {:c@h, false}}}

    html =
      render(RingLive, %{
        ring: {:ok, not_started},
        ready: nil,
        filter: "",
        view: "table"
      })

    assert html =~ ~r/switch=c%40h[^>]*>c@h<\/a>\s*<span class="rcp-fallback\s*">\s*not started/

    html =
      render(RingLive, %{ring: {:ok, not_started}, ready: nil, filter: "", view: "ring", zoom: 1})

    assert html =~ "fallback c@h (not started)"
  end

  defp member(node, extra) do
    Map.merge(
      %{
        node: node,
        status: :valid,
        partitions: 2,
        share: 50.0,
        up: true,
        moving_out: 0,
        moving_in: 0,
        info: nil,
        ring_agrees: nil,
        claimant: false
      },
      extra
    )
  end

  test "members: what each member says about itself, and when it can't" do
    info = %{
      vnodes: 3,
      not_owned: 1,
      handoffs: 2,
      ring: 7,
      services: [:kv, :kv_store],
      riak_core: "0.11.0",
      otp: "28",
      uptime_ms: 3_725_000,
      memory: 2_097_152,
      processes: 99
    }

    rows = [
      member(:a@h, %{info: info, ring_agrees: true, moving_out: 1}),
      member(:b@h, %{
        info: %{info | not_owned: 0, services: nil},
        ring_agrees: false,
        moving_in: 1,
        claimant: true
      }),
      member(:c@h, %{up: false, status: :down}),
      member(:d@h, %{info: {:error, "no answer in time"}})
    ]

    html = render(MembersLive, %{members: {:ok, rows}})
    assert html =~ ~r/class="rcp-node rcp-claimant"[^>]*>b@h</
    refute html =~ ~r/class="rcp-node rcp-claimant"[^>]*>[cd]@h</
    assert html =~ ~r/2 <span class="rcp-muted">· 50.0%/
    assert html =~ ~r/title="moving away">\s*↑1/
    assert html =~ ~r/title="moving here">\s*↓1/
    assert html =~ ~s(<td class="rcp-num rcp-pending">1</td>)
    assert html =~ ~r/class="rcp-ok">\s*same/
    assert html =~ ~r/class="rcp-bad">\s*differs/
    assert html =~ "kv, kv_store"
    assert html =~ "<td>?</td>"
    assert html =~ "riak_core 0.11.0 · OTP 28"
    assert html =~ "1h 2m"
    assert html =~ "2.0 MB"
    assert html =~ "— (down)"
    assert html =~ ~s(<td colspan="8" class="rcp-error">no answer in time</td>)
    assert html =~ "<dt>Not owned</dt>"
  end

  test "vnodes" do
    fetched = [
      {{:my_vnode, 0, pid()}, [message_queue_len: 7, memory: 2048, reductions: 99]},
      {{:my_vnode, 1, pid()}, :undefined}
    ]

    html =
      render(VnodesLive, %{
        vnodes:
          {:ok, %{fetched: fetched, total: 300, modules: [:my_vnode, :other_vnode], module: ""}},
        sort: {:mailbox, :desc},
        module: ""
      })

    assert html =~ "Showing the first 2 of 300 vnodes"
    assert html =~ "other_vnode"

    filtered =
      render(VnodesLive, %{
        vnodes: {:ok, %{fetched: fetched, total: 280, modules: [:my_vnode], module: "my_vnode"}},
        sort: {:mailbox, :desc},
        module: "my_vnode"
      })

    assert filtered =~ "Showing the first 2 of 280 my_vnode vnodes"
    assert html =~ "&lt;0.101.0&gt;"
    assert html =~ "2.0 KB"
    assert html =~ "exited"
    assert html =~ "▼"
  end

  test "handoffs" do
    assert render(HandoffsLive, %{handoffs: {:ok, %{active: [], pending: []}}}) =~
             "No handoffs"

    active = [
      {:status_v2,
       [
         mod: :my_vnode,
         src_partition: 0,
         target_partition: 0,
         src_node: :a@h,
         target_node: :b@h,
         direction: :outbound,
         status: :running,
         start_ts: {1_761, 400_000, 0},
         sender_pid: pid(),
         stats: [size: {1000, :objects}, pct_done_decimal: 0.25],
         type: :hinted
       ]},
      :odd
    ]

    html = render(HandoffsLive, %{handoffs: {:ok, %{active: active, pending: @ring.pending}}})
    assert html =~ "hinted"
    # the attached node (a@h) is plain text, other nodes switch beamdash
    assert html =~ ~r{<span>a@h</span> →\s*<a href="/nodes\?switch=b%40h"[^>]*>b@h</a>}
    assert html =~ ~r{<a href="/nodes\?switch=c%40h"[^>]*>c@h</a>}
    assert html =~ "1000 objects, 25.0%"
    assert html =~ "2025-10-25T13:46:40Z"
    assert html =~ ":odd"
    assert html =~ "Pending changes"
    assert html =~ "awaiting"
    assert html =~ "Sender"
    assert html =~ "&lt;0.101.0&gt;"

    # an odd pending entry's text is shown once, not inspected again
    html = render(HandoffsLive, %{handoffs: {:ok, %{active: [], pending: [:odd]}}})
    assert html =~ ":odd"
    refute html =~ "&quot;:odd&quot;"
  end
end
