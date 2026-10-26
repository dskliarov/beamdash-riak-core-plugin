defmodule RiakCorePlugin.RingChart do
  @moduledoc """
  The Ring page's picture of the ring: one segment per partition, clockwise
  from the top in index order, coloured by its owner. A partition with a
  pending change gets an outer band in its next owner's colour. Pure
  geometry for an SVG `viewBox="0 0 #{2 * 170} #{2 * 170}"`.
  """

  @center 170
  @outer 150
  @inner 95
  @band_inner 155
  @band_outer 165

  # Tableau 10, readable on light and dark backgrounds; {fill, label colour}
  @palette [
    {"#4e79a7", "#fff"},
    {"#f28e2b", "#1f1f1f"},
    {"#e15759", "#fff"},
    {"#76b7b2", "#1f1f1f"},
    {"#59a14f", "#fff"},
    {"#edc948", "#1f1f1f"},
    {"#b07aa1", "#fff"},
    {"#ff9da7", "#1f1f1f"},
    {"#9c755f", "#fff"},
    {"#bab0ac", "#1f1f1f"}
  ]

  @doc "The SVG's width and height."
  def size, do: 2 * @center

  @doc "The centre's coordinate (x and y)."
  def center, do: @center

  @doc """
  Each node's `{fill, label colour}`, by its place in `nodes` (sorted
  owners); after ten nodes the colours repeat.
  """
  @spec colors([node()]) :: %{node() => {String.t(), String.t()}}
  def colors(nodes) do
    nodes
    |> Enum.with_index()
    |> Map.new(fn {node, i} -> {node, Enum.at(@palette, rem(i, length(@palette)))} end)
  end

  @zooms [1, 1.5, 2, 3, 4]

  # the SVG is drawn 460 px wide at zoom 1; labels stay at least this many
  # px tall on screen, and are numbered on every k-th segment to fit
  @base_px 460
  @min_label_px 9
  @max_font 13.0

  @doc "The zoom levels, smallest first."
  def zooms, do: @zooms

  @doc "The SVG's width in px at `zoom`."
  def width(zoom), do: round(@base_px * zoom)

  @doc """
  How partition numbers are labelled for `count` partitions at `zoom`:
  `{step, font}`, every `step`-th segment in `font` (SVG units). The font
  fits the arc it may use (its segment and the unlabelled ones after it)
  and is at least #{@min_label_px} px on screen; zooming in labels more
  segments.
  """
  def labels(count, zoom) do
    arc = 2 * :math.pi() * label_r() / max(count, 1)
    px_per_unit = @base_px * zoom / size()
    step = max(1, ceil(@min_label_px / (arc * 0.8 * px_per_unit)))
    {step, Float.round(min(@max_font, step * arc * 0.8), 1)}
  end

  @doc """
  Whether labels run along the radius (dense rings, where a segment is
  narrower than a number) rather than upright.
  """
  def radial?(count), do: count > 32

  @doc """
  The segments for `rows` (`RiakCorePlugin.Data.partition_rows/2`, in index
  order): `%{n, row, path, fill, text, label?, x, y, rotate, band}` where
  `band` is `nil` or `{path, fill}` for a pending change and `rotate` the
  label's rotation in degrees (0 upright). Which segments are labelled
  follows `labels/2`.
  """
  @spec segments([map()], %{node() => {String.t(), String.t()}}, number()) :: [map()]
  def segments(rows, colors, zoom \\ 1) do
    count = length(rows)
    {step, _font} = labels(count, zoom)

    rows
    |> Enum.with_index()
    |> Enum.map(fn {row, n} ->
      {from, to} = angles(n, count)
      {fill, text} = Map.get(colors, row.owner, {"#888", "#fff"})
      mid = (from + to) / 2

      %{
        n: n,
        row: row,
        path: arc(from, to, @inner, @outer),
        fill: fill,
        text: text,
        label?: rem(n, step) == 0,
        x: round1(@center + label_r() * :math.cos(mid)),
        y: round1(@center + label_r() * :math.sin(mid)),
        rotate: if(radial?(count), do: rotation(mid), else: 0),
        band: band(row, from, to, colors)
      }
    end)
  end

  defp label_r, do: (@outer + @inner) / 2

  # along the radius, turned on the left half so it never reads upside down
  defp rotation(angle) do
    deg = angle * 180 / :math.pi()
    deg = if deg > 90 and deg < 270, do: deg - 180, else: deg
    Float.round(deg, 2)
  end

  defp band(%{next: nil}, _from, _to, _colors), do: nil

  defp band(%{next: next}, from, to, colors) do
    {fill, _} = Map.get(colors, next, {"#888", "#fff"})
    {arc(from, to, @band_inner, @band_outer), fill}
  end

  # clockwise from the top; a single partition stops just short of a full
  # turn, which an SVG arc can't draw
  defp angles(n, count) do
    turn = 2 * :math.pi()
    from = turn * n / count - :math.pi() / 2
    to = turn * (n + 1) / count - :math.pi() / 2
    if count == 1, do: {from, to - 0.0001}, else: {from, to}
  end

  defp arc(from, to, r_in, r_out) do
    large = if to - from > :math.pi(), do: 1, else: 0
    {x0, y0} = point(r_out, from)
    {x1, y1} = point(r_out, to)
    {x2, y2} = point(r_in, to)
    {x3, y3} = point(r_in, from)

    "M#{x0} #{y0} A#{r_out} #{r_out} 0 #{large} 1 #{x1} #{y1} " <>
      "L#{x2} #{y2} A#{r_in} #{r_in} 0 #{large} 0 #{x3} #{y3} Z"
  end

  defp point(r, angle),
    do: {round1(@center + r * :math.cos(angle)), round1(@center + r * :math.sin(angle))}

  defp round1(x), do: Float.round(x * 1.0, 2)
end
