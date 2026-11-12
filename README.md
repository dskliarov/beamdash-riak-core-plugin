# riak_core plugin for beamdash

A [beamdash](https://github.com/dskliarov/beamdash) plugin that adds eight
pages for riak_core and riak_core_lite, and keeps the cluster's history. It shows them in beamdash's Plugins
menu while `riak_core ~> 0.10` runs on the attached node. The application
name of riak_core_lite is `riak_core`.

## Install

In beamdash, open Settings → Plugins and enter one of these dependencies:

```elixir
# a local checkout
{:riak_core_plugin, path: "~/projects/erl/beamdash-riak-core-plugin"}

# from GitHub, at a tag
{:riak_core_plugin, git: "https://github.com/dskliarov/beamdash-riak-core-plugin", tag: "0.0.1"}

# from Hex, once it is published
{:riak_core_plugin, "0.0.1"}
```

beamdash fetches and compiles only this plugin's own source. Its deps are
read as data and are not fetched.

The plugin needs a beamdash build with plugin collectors and history
(`Beamdash.Plugin.record/4`). An older build refuses to compile it, with
"unknown option :collectors".

## Pages

Each page shows "Loading…" first and loads in the background
(`start_async`), so a slow node doesn't hold up the page. It loads again 5
seconds after the previous load completed, and only while a node is
attached; attaching a node loads it at once. It gets all its data with
`Beamdash.Plugin.rpc/5` on the attached node. When riak_core isn't running
there, the page shows "riak_core is not running". A timeout reads "timed
out"; other rpc errors are shown as they are.

Node names (owners, members, handoff ends) other than the attached node
are links: clicking one moves beamdash's connection to that node, as on
beamdash's Nodes tab, and comes back to the same page
(`Beamdash.Plugin.switch_path/2`). The node must be connected to the
attached one; otherwise the Nodes tab says why it couldn't switch.

Module names (vnode modules, handoff and transfer modules, a process's
initial call on the Load page) are links too: clicking one opens it on
beamdash's Modules tab, when it is loaded on the attached node.

- **Health**: ring readiness, claimant, down and marked-down members,
  under-replicated partitions (N replicas among the up nodes), the handoff
  queue and pending transfers, with the warnings worth acting on. It loads
  every 30 s because `riak_core_status:ring_status/0` asks every member.
- **Ring**: ring size, claimant and ring-ready state
  (`riak_core_status:ringready/0`), partitions per node, and the partition
  list (index, owner, and the next owner when a change is pending), with an
  owner filter. `ringready/0` calls every member, so it is loaded on its own
  every 30 seconds, with a 35 s timeout ("timed out (a member may be
  unreachable)").
  A **Ring** view next to the **Table** draws the ring as a circle of
  partitions numbered from 0, clockwise from the top, each coloured by its
  owner, with a legend of nodes and their partition counts. A pending
  change shows as an outer band in the next owner's colour; hovering a
  segment shows its number, index, owner and next owner. **−** / **+**
  zoom from 100% to 400% (**Reset** goes back); zoomed in, the ring
  scrolls in its box. Numbers stay at least 9 px tall on screen: a dense
  ring numbers every k-th segment, along the radius, and more of them as
  you zoom in (a 256-partition ring: every 3rd at 100%, all from 300%).
  An owner that is down (not among the attached node's visible nodes)
  keeps its partitions, as in riak_core, which moves ownership only on a
  committed change: the page marks it down (a badge in the tables and the
  legend, hatched segments in the ring, "(down)" on the claimant) and says
  what to do on the Admin page to move its partitions.
  For those partitions the Table's **Fallback** column shows the node
  riak_core sends the owner's requests to, worked out on the attached node
  with `riak_core_apl:get_apl_ann/4` for the up nodes and N replicas, N
  being riak_core's `target_n_val` on the attached node (4 when unset),
  shown in the column header. An application that routes with its own N
  may pick another node. The column also says whether that fallback
  vnode is **running** or **not started** (it starts on the first request
  routed to it); "none up" when no node can take it. The Ring view's tooltips say the
  same. This is one more rpc, made only while an owner is down.
  Above the partition list, the page explains what Owner, Next owner and
  Fallback mean.
- **Members**: each member's node, status (valid, joining, leaving,
  exiting, down), whether the target sees it up (`erlang:nodes([this,
  visible])` on the target), the partitions it owns and their share of the
  ring, and the partitions a committed change is moving away from it (↑)
  or to it (↓). Each member that is up is also asked, in one
  `erpc:multicall/5` from the target (4 s each), for what only it knows:
  its vnodes, the partitions it runs a vnode for but doesn't own
  (fallbacks, or handoffs still to do), its active handoffs, whether its
  ring (owners, pending changes, member statuses) is the same as the
  target's, its services (`riak_core_node_watcher`), its riak_core and OTP
  versions, uptime and memory. A member that doesn't answer shows why. The
  page explains Moving, Not owned, Ring and Services under the table.
- **Vnodes**: module, partition, pid, mailbox, memory and reductions of each
  vnode (`riak_core_vnode_manager:all_vnodes/0`), sortable by column, with a
  module filter that lists the modules of all vnodes. Process info is
  fetched in one rpc for the first 256 vnodes of the selected module (or of
  all), by module and partition; changing the filter fetches again. With
  more, the page says "Showing the first 256 of N", N counting the selected
  module's vnodes, and sorting uses the vnodes that were fetched. A pid
  opens that vnode's process on beamdash's Processes tab.
- **Load**: how busy each scheduler is, over a 1 s sample on the attached
  node: the share of wall time each normal scheduler ran Erlang code
  (`erlang:statistics(scheduler_wall_time)`), the average, the busiest and
  the least busy one. Also input and output per second
  (`erlang:statistics(io)`), memory by kind (`erlang:memory/0`), and the 20
  processes that used the most reductions in the sample, with their name and
  mailbox length. The first sample switches `scheduler_wall_time` on at the
  attached node, and it stays on after that. Erlang doesn't say which
  scheduler a process ran on: the busiest processes are where to look for a
  busy scheduler, not proof of it.
- **Handoffs**: active handoffs (`riak_core_handoff_manager:status/0`):
  type, module, partitions, from → to, direction, status, size or progress,
  start time and sender pid (a link to the process). Also the pending ownership changes
  (`riak_core_ring:pending_changes/1`). It shows "No handoffs" when both are
  empty. It shows the handoff concurrency limit and whether inbound/outbound
  handoffs are enabled, and can set the limit, pause/resume a direction
  (pausing also kills that direction's running handoffs), kill all handoffs
  or force handoffs. Pausing, killing, forcing and
  setting the limit to zero ask for confirmation; each action is kept in
  the history as what was done. There is no "refresh ring":
  `riak_core_ring_manager:refresh_my_ring/0` is the last step of a node
  leaving, and gives the node a fresh ring of its own, so it drops out of
  its cluster.
- **History**: what the plugin has kept of the attached node's cluster
  (see [History](#history)), one kind at a time, newest first, with a tab
  for each: **Handoffs** (each completed handoff: type, module,
  partition, from → to, how long it took, its size when known, with a
  completions-over-time chart), **Members**
  (status changes, a member going down or coming back up, joins and
  removals), **Ring** (claimant and ring size changes, partition transfers
  starting and finishing, ownership changing without transfers), **Load**
  (a sample every minute, with a chart of the average and busiest
  scheduler's busy share) and **Admin** (the Admin page's actions and their
  results). It loads every 10 seconds. When no history is kept it says why.
- **Admin**: changes the cluster with riak_core's staged workflow, as
  `riak-admin cluster` does. Per member: **Leave** (hands its data off,
  `riak_core_claimant:leave_member/1`), **Force remove** (its partitions
  are reassigned without handoff, `remove_member/1`) and, for a member the
  target can't reach, **Mark down** (`riak_core:down/1`, at once). **Join a
  node** asks that node to run `riak_core:staged_join/1` towards the
  attached node (the attached node makes the call, so the new node must be
  reachable from it); the list suggests the nodes it sees that run riak_core
  and aren't members. **Replace a node** with a joining one (`replace/2`,
  or `force_replace/2` without handoff), and **Resize the ring**
  (`resize_ring/1`, `abort_resize/0`). The **Staged changes** section shows
  `riak_core_claimant:plan/0`: each change, and per transition the
  partition transfers and the partitions per node now and after, summed up
  on the target (the rings themselves never cross the wire). **Commit**
  commits exactly that plan; **Clear** drops the staged changes. The plan
  reloads after every action and every 10 seconds. Commit, Clear, Force
  remove, force replace and Mark down ask for a confirmation first. Node
  names from the browser are only taken when they are members, or, for a
  join, a suggested node or a well-formed `name@host`.
  Every staged change goes through the claimant: while it is down, the
  page says so (instead of the raw nodedown) and that **Mark down** on it
  lets another member become the claimant.
  Each action is kept in the history with its result; a commit also keeps
  the changes of the plan it committed.

The ring record itself never crosses the wire. The `riak_core_ring`
accessors run on the target in one rpc of `erl_eval:exprs/2` (stdlib), with
Erlang expressions that call `riak_core_ring_manager:get_my_ring/0` and pass
the ring to them. The expressions are parsed when the plugin compiles; none
of the plugin's modules has to exist on the target. The Ring page reads
size, claimant, owners, pending changes and member statuses that way; the
Members page only member statuses and partitions per node.

## History

The plugin keeps the cluster's history in beamdash's event store, the
SQLite file beamdash keeps its own snapshots in. Its collectors run in
beamdash while riak_core runs on the attached node, whichever page is
open, and each stores only what changed since its previous run:

| Collector | Every | Stores | Cost per run |
| --- | --- | --- | --- |
| `handoffs` | 5 s | `handoffs`: each completed handoff | one rpc; the target reads the ring's pending changes and asks every member it sees for `riak_core_handoff_manager:status/0` (one `erpc:multicall/5`, 4 s each) |
| `cluster` | 10 s | `members` and `ring` changes | two rpcs (the ring accessors, `erlang:nodes/1`) |
| `load` | 60 s | `load`: a 1 s sample of the attached node | two rpcs, one of which reads every process twice, as the Load page does |

The Admin page stores `admin` rows itself.

Every row is keyed by the cluster's name (`riak_core_ring:cluster_name/1`),
so the history of one cluster is the same whichever member beamdash is
attached to. Ownership, resize and delete transfers are read from the
ring's pending changes: riak_core marks each module's transfer done there
and keeps the entry until the claimant clears it, so a transfer is recorded
even when it took a few milliseconds (an in-memory store's usually do).
Their start and end are those of the runs, within 5 s, unless a run caught
the handoff running on its sender, which also gives its size. An entry the
claimant clears before a run saw it done (or a change that was cleared or
aborted) is recorded as "cleared before seen done". Hinted and repair
handoffs aren't in the ring: they are only seen running, so one shorter
than 5 s may be missed. A member that doesn't answer keeps its running
handoffs open (for up to 10 minutes) instead of ending them. The first run after attaching only takes
note of the cluster: changes from before it are not known.

beamdash keeps the rows for 30 days, at most 100,000 for this plugin
(`config :beamdash, :retention, plugin_events: %{max_age_days: ..., max_rows: ...}`).
With beamdash's event store off (`config :beamdash, :store_file, false`)
nothing is kept or collected, and the History page says so.

## MCP tools

While riak_core runs on the attached node, beamdash's MCP server offers
five read-only tools from this plugin: `riak_core.ring`,
`riak_core.members`, `riak_core.handoffs`, `riak_core.load` and
`riak_core.history`. The first four return
the same data as the Ring, Members, Handoffs and Load pages, as JSON, except
that `riak_core.ring` leaves out the fallback nodes (the Ring page's Fallback
column): finding them takes one more rpc. `riak_core.load` takes a 1-second
sample when it is called, as the Load page does, so its rates are for that
second. `riak_core.health` returns a readiness/coverage summary within 4 s;
`riak_core.vnodes` returns expected/running vnode type and state
(optionally for one `module`); `riak_core.handoff_stats` adds handoff
throughput, ETA and the waiting/stopped queue; `riak_core.node` returns one
member's services, versions and system pressure. `riak_core.history` returns the cluster's history of one `kind`
(`handoffs`, `members`, `ring`, `load` or `admin`) over the last `minutes`
(1440 by default), newest first, at most `limit` rows (50 by default, at
most 500). Cluster changes (join, leave, commit) are not offered.
Each tool has a 5-second limit set by beamdash, so on a large cluster or a
slow node `members`, `handoffs` and `load` can run out of time.

## Develop

The plugin is an ordinary Mix project next to a beamdash checkout:

```
projects/erl/beamdash
projects/erl/beamdash-riak-core-plugin
```

`mix.exs` depends on `{:beamdash, path: "../beamdash", only: [:dev, :test],
runtime: false}` for the plugin API (`Beamdash.Plugin`,
`Beamdash.Plugin.Page`, `Beamdash.Plugin.Format`). The supported application
is declared as `{:riak_core, "~> 0.10", hex: :riak_core_lite, runtime:
false}`.

```sh
mix deps.get
mix test
```

The tests start `:peer` nodes (epmd is started, and the test VM becomes a
node) with riak_core_lite's compiled ring modules on their code path and
small fakes of the riak_core servers (`test/support/peer.exs`). No
riak_core cluster is needed.

- `lib/riak_core_plugin.ex`: the entry module (`use Beamdash.Plugin`).
- `lib/riak_core_plugin/riak.ex`: the rpc calls.
- `lib/riak_core_plugin/data.ex`: pure functions that turn rpc results
  into rows.
- `lib/riak_core_plugin/*_live.ex`: the pages (`use Beamdash.Plugin.Page`).
- `priv/static/plugin.css`: styles, with `rcp-` classes and beamdash's CSS
  variables.

To try it, install it in beamdash by `path:`. Use Rebuild in Settings →
Plugins after a change, then attach a node that runs riak_core.

Rules from beamdash: every module is `RiakCorePlugin` or nested under it.
Code on the attached node is only called with `Beamdash.Plugin.rpc/5`. Only
libraries beamdash ships are used.

beamdash's own integration test installs this plugin from
`../beamdash-riak-core-plugin`. Run it with `mix test --only
riak_core_plugin` in beamdash.

## License

GPL-3.0-or-later; see `LICENSE`.
