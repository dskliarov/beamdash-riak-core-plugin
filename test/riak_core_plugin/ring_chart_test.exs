defmodule RiakCorePlugin.RingChartTest do
  use ExUnit.Case, async: true

  alias RiakCorePlugin.RingChart

  defp rows(owners), do: Enum.map(owners, fn {i, o} -> %{index: i, owner: o, next: nil} end)

  test "colours by place among the nodes, repeating after ten" do
    nodes = for i <- 1..11, do: :"n#{i}@h"
    colors = RingChart.colors(nodes)
    assert colors[:n1@h] != colors[:n2@h]
    assert colors[:n11@h] == colors[:n1@h]
  end

  test "segments go clockwise from the top, labelled at their middle" do
    colors = RingChart.colors([:a@h, :b@h])

    [first, second, third, fourth] =
      RingChart.segments(rows([{0, :a@h}, {1, :b@h}, {2, :a@h}, {3, :b@h}]), colors)

    c = RingChart.center()
    # the first quarter is top-right, the second bottom-right
    assert first.x > c and first.y < c
    assert second.x > c and second.y > c
    assert third.x < c and third.y > c
    assert fourth.x < c and fourth.y < c
    assert String.starts_with?(first.path, "M#{c * 1.0} 20.0 A150 150 0 0 1 ")
    assert first.fill == elem(colors[:a@h], 0)
    assert second.fill == elem(colors[:b@h], 0)
    assert first.band == nil
  end

  test "a pending change gets a band in the next owner's colour" do
    colors = RingChart.colors([:a@h, :b@h])
    [s] = RingChart.segments([%{index: 0, owner: :a@h, next: :b@h}], colors)
    assert {path, fill} = s.band
    assert fill == elem(colors[:b@h], 0)
    # a single partition is (almost) the whole circle
    assert path =~ " 0 1 1 "
  end

  test "labels: small rings number every segment upright, at full size" do
    assert RingChart.labels(16, 1) == {1, 13.0}
    refute RingChart.radial?(16)
    owners = for i <- 0..15, do: {i, :a@h}

    assert Enum.all?(
             RingChart.segments(rows(owners), RingChart.colors([:a@h])),
             &(&1.rotate == 0)
           )
  end

  test "labels: big rings number every k-th segment along the radius, more when zoomed" do
    {step, font} = RingChart.labels(256, 1)
    assert step == 3
    # on screen at least 9 px tall
    assert font * RingChart.width(1) / RingChart.size() >= 9

    {zoomed_step, zoomed_font} = RingChart.labels(256, 4)
    assert zoomed_step == 1
    assert zoomed_font * RingChart.width(4) / RingChart.size() >= 9
    assert zoomed_font < font

    owners = for i <- 0..255, do: {i, :a@h}
    segments = RingChart.segments(rows(owners), RingChart.colors([:a@h]))
    labelled = for s <- segments, s.label?, do: s.n
    assert Enum.take(labelled, 3) == [0, 3, 6]
    assert RingChart.radial?(256)

    # along the radius, never upside down
    rotations = Enum.map(segments, & &1.rotate)
    assert Enum.all?(rotations, &(&1 >= -90 and &1 <= 90))
    assert hd(segments).rotate != 0

    zoomed = RingChart.segments(rows(owners), RingChart.colors([:a@h]), 4)
    assert Enum.all?(zoomed, & &1.label?)
  end
end
