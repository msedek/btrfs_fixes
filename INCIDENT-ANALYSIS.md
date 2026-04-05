# BTRFS Pool Recovery Case Study and Upstream Gap Analysis

This document describes a recovery effort on a severely corrupted 12 TB
multi-device BTRFS pool that survived the failure of every native repair
path (`btrfs check --repair`, `--init-extent-tree`, `--init-csum-tree`).
The goal of sharing it is constructive: to help the BTRFS community
identify specific gaps where upstream tooling could prevent similar
cases from becoming unrecoverable, and to provide a runnable reference
implementation of the workarounds we used.

The tools referenced here live in this repository:
https://github.com/msedek/btrfs_fixes

## Executive summary

* A hard power cycle during a mid-transaction commit left the extent
  tree and free space tree of a 3-device BTRFS pool in a state that no
  native `btrfs-progs` command could repair.
* A subsequent `btrfs check --repair` attempt entered an infinite loop
  of 46,000+ commits with zero progress, rotating the 4 superblock
  `backup_slots` approximately 11,000 times each and destroying any
  pre-crash rollback path reachable from the kernel.
* Recovery required 14 custom C tools using the internal `btrfs-progs`
  API, a patched `alloc_reserved_tree_block` to tolerate EEXIST, and a
  staged recovery plan with empirical validation at each step.
* Final outcome: pool fully mountable read-write, all 46 services
  running, data loss 0.00016 percent of total (~7.2 MB of 4.59 TB).
* Residual cosmetic errors in `btrfs check --readonly` remain (~712
  link count wrong, ~393,000 backpointer mismatches) because a subset
  of orphan inodes cannot be cleaned through the cow path without
  triggering deterministic BUG_ON assertions. The pool operates
  normally in spite of those counts.

## Environment

| Field | Value |
|---|---|
| Pool layout | 3 devices, 4 TB each, data=single, metadata=DUP |
| Disk model | DM-SMR (shingled magnetic recording) |
| Filesystem features | MIXED_BACKREF, COMPRESS_ZSTD, BIG_METADATA, EXTENDED_IREF, SKINNY_METADATA, NO_HOLES |
| last_snapshot (root) | 3701 |
| FS_TREE level | 2 |
| Kernel at recovery time | 6.19.x |
| btrfs-progs baseline | v6.19.1 with a single patch to `alloc_reserved_tree_block` |

## Timeline of events

1. **Initial damage**. A hard power cycle interrupted a commit at
   generation 18958 to 18959. Both DUP copies of several metadata blocks
   were written with inconsistent parent and child generations.
2. **First repair attempts**. `btrfs check --repair` without flags was
   rejected with read-only file system. With `--init-extent-tree` it
   deadlocked because allocation required a functional extent tree.
3. **Loop incident**. A subsequent `btrfs check --repair` run entered
   an infinite loop and advanced the pool generation by more than 46,000
   commits without resolving any of the original errors. This had two
   permanent consequences:
   a. The 4 `backup_slots` in the superblock were overwritten many
      thousands of times, erasing every rollback state that still
      referenced pre-crash blocks.
   b. Additional stale pointer chains were introduced because each loop
      iteration performed COW on nodes that themselves pointed to blocks
      reused by newer trees.
4. **Recovery session 1** (see README.md for the baseline tool set).
   Built and ran `scan_and_fix_all_backrefs`, `fix_owner_refs`,
   `fix_bad_levels`, `fix_duplicate_extents`, `fix_uuid_tree`,
   `fix_csum_tree`, `set_nodatasum`, and `remove_stale_ptrs`. These
   tools restored the pool to a mountable read-only state but left
   the extent tree largely empty and a long list of orphan inode
   subtrees in the FS_TREE.
5. **Recovery session 2**. Built the extended tool set in this repo
   (see README.md section "Session 2 toolset") to repopulate the
   extent tree from a scan of the FS_TREE, then to clean specific
   classes of residual orphans. The extended writer injected
   3,248,617 EXTENT_DATA_REFs in a single transaction spanning
   approximately 34 minutes on the DM-SMR disks.
6. **Discovery of crash vector 2**. The generic orphan cleanup tool
   crashed on `BUG_ON(err)` in `btrfs_inc_extent_ref` during a
   post-delete leaf rebalance. The crash was reproducible. See the
   dedicated section below.
7. **Bulletproof subset**. After classifying every target orphan by
   five empirical safety conditions, only a small fraction could be
   processed safely. That subset committed without errors.
8. **Mount read-write validation**. A canary write, read, and delete
   in an isolated top-level directory, followed by a 17-minute soak
   test under a production workload of 37 containers, produced zero
   new BTRFS messages in `dmesg`. A checksum baseline of 120 live
   files captured pre-write remained bit-for-bit identical across
   five verification checkpoints.

## Root cause classification

### 1. BTRFS design limitation that is not a bug but is not documented clearly

The superblock field `backup_roots[0..3]` is a 4-entry sliding window
of the most recent commits. It is not a historical backup. In a pool
with frequent commits, or in a pathological loop, the window advances
past any meaningful recovery state within minutes. There is no
in-tree mechanism for retaining older roots unless the administrator
has explicit subvolume snapshots or `btrfs send` streams to another
device. This is widely misunderstood by users expecting that btrfs
"has built-in rollback".

### 2. Deadlock in `btrfs check --init-extent-tree`

When the extent tree is severely damaged, the current implementation
requires block allocation to rebuild itself, and block allocation
requires a functional extent tree. The result is a consistent
allocation failure that reports "Read-only file system" even when run
against a live disk. This matches multiple reports in the kdave
tracker. For pools where the extent tree is empty or uniformly
corrupted, there is no exit from this deadlock through `btrfs-progs`
alone.

### 3. Infinite loop in `btrfs check --repair`

When `alloc_reserved_tree_block` encounters an existing METADATA_ITEM
for a block it is about to insert, current `btrfs-progs` returns
EEXIST and aborts the transaction. A widely used workaround patch
(see `patches/alloc_reserved_tree_block_eexist.patch` in this repo)
makes that path return 0, which lets repair continue, but without
updating the existing item. `btrfs check --repair` then re-finds
the same "error" on every subsequent pass and loops forever. In our
case, the loop advanced the generation counter by 46,000+ without
making any net progress.

### 4. Asymmetric handling of delayed refs in `reinit_extent_tree` mode

The ` reinit_extent_tree` transaction flag exempts failures in the
`BTRFS_DROP_DELAYED_REF` path, which is how tools like
`scan_and_fix_all_backrefs` survive ENOENT on blocks without
backrefs. The same flag does not exempt `BTRFS_ADD_DELAYED_REF`.
When a rebalance triggers `btrfs_inc_ref` on a sharable sibling
whose child extents no longer exist in the extent tree,
`btrfs_inc_extent_ref` fails with ENOENT, and the code path reaches
`BUG_ON(err)` in `extent-tree.c:1302`. There is no way to proceed
from userspace without rebuilding the extent tree first or without
filtering out every target that could trigger a rebalance.

### 5. Rebalance to stale sibling is not guarded

`btrfs_del_items` drains a leaf below `LEAF_DATA_SIZE/4` and invokes
`push_leaf_left` or `push_leaf_right`. Both unconditionally COW the
chosen sibling. If the sibling has generation less than or equal to
`last_snapshot`, `btrfs_block_can_be_shared` returns 1, and COW
enters the `refs > 1` path with `btrfs_inc_ref`. For a post-crash
pool where `last_snapshot` is much smaller than the current
generation, this is the most dangerous cow path in the codebase:
any delete that drains its leaf can set fire to a neighboring pre-crash
leaf whose extents are no longer valid.

## What worked

The full execution plan is in README.md. The short version:

1. `scan_and_fix_all_backrefs` to repopulate METADATA_ITEMs.
2. `fix_owner_refs`, `fix_bad_levels`, `fix_duplicate_extents`
   to normalize backref metadata.
3. `fix_uuid_tree`, `fix_csum_tree`, `set_nodatasum` to bypass the
   dependency on csum verification.
4. `remove_stale_ptrs_v2` to clean stale child pointers in the
   FS_TREE.
5. `rebuild_extent_tree_apply` (the heavy writer) to inject
   3.25 million EXTENT_DATA_REFs in a single transaction.
6. `patch_block_group_used` and `remove_extent_items_by_key` to
   resolve one BG_ITEM accounting overshoot and 15 overlapping
   file_extent_items that prevented read-only mount.
7. `clean_orphan_dir_entries`, `clean_orphan_inode_refs`,
   `fix_dir_inode_counts` to reduce `[5/8] fs roots` error counts
   without triggering the rmdir bomb condition.
8. `remove_orphan_inode_subtrees` with a **bulletproof subset**
   (see next section) to remove a small set of orphan inodes
   that can be cleaned safely.

## The bulletproof subset criterion

The criterion below is the direct conclusion of the empirical
investigation. It may be useful to upstream as a safety check in any
generic orphan-inode cleanup tool, or as a pre-flight warning in
`btrfs check --repair`.

Before processing a target inode `I` for deletion through the normal
cow path, verify all five of the following:

1. The leaf hosting `I`'s `INODE_ITEM` has `gen > last_snapshot`.
2. The leaf's parent level-1 node has `gen > last_snapshot`.
3. The estimated `used` bytes of the leaf after removing all target
   items is greater than `LEAF_DATA_SIZE/4` (no forced rebalance).
4. All immediate slot-adjacent siblings of the leaf in its parent
   node have `gen > last_snapshot`. This condition is sufficient
   even if condition 3 fails.
5. The `disk_bytenr` of every `EXTENT_DATA` referenced by `I`
   resolves in the current extent tree.

Violating condition 3 or 4 triggers the stale sibling cow cascade
described in root cause section 5. Violating condition 5 triggers
the ENOENT path described in section 4.

We do not claim this is the canonical safety criterion. We claim it
is a correct lower bound that we verified empirically and that has
zero false negatives across our target set.

## What did not work, and why

| Command | Outcome in our case |
|---|---|
| `btrfs rescue zero-log` | Worked, required before every mount attempt. Not sufficient by itself. |
| `btrfs check --repair` | Infinite loop, no net progress, destroyed backup_slots. |
| `btrfs check --init-extent-tree` | Deadlock, write_bytes remained 0. |
| `btrfs check --init-csum-tree` | Crashed at `[5/8]` with `split_leaf: BUG_ON` (matches upstream Issue #312). |
| `btrfs check --repair --init-csum-tree --init-extent-tree` | Same BUG_ON crash after reading 1.29 TB. |
| `btrfs-select-super` | All superblocks pointed to the same corrupted generation. |
| `mount -o usebackuproot` | Backup roots contained the same corrupted chains. |
| `btrfstune --disable-quota` / `--remove-simple-quota` | Null-pointer dereference because SIMPLE_QUOTA was not set. |

## Gap analysis and proposed upstream improvements

The following items are proposed respectfully as areas where a
relatively small upstream change would have made the difference
between "recoverable without custom code" and "requires a two-day
session with custom tools". They are ordered by expected impact
to operators hitting similar cases.

### A. Progress detection in `btrfs check --repair`

**Observed behavior**: 46,000+ commits, 0 net reduction in reported
errors.

**Proposal**: after N consecutive passes of `btrfs check --repair`
where the total error count is non-decreasing, abort with a clear
message ("No progress in N passes, stopping to prevent backup_roots
erasure"). The threshold could be configurable through a flag.
This alone would have prevented the permanent loss of pre-crash
rollback points in our case.

### B. Symmetric `reinit_extent_tree` handling

**Observed behavior**: `reinit_extent_tree` exempts
`BTRFS_DROP_DELAYED_REF` but not `BTRFS_ADD_DELAYED_REF`. Any tool
that has to invoke cow on sharable siblings will crash the first
time the cow path enters `btrfs_inc_ref`, regardless of the flag.

**Proposal**: extend the `reinit_extent_tree` check in
`extent-tree.c` to also tolerate ADD failures when the block is in
the sharable range (`gen <= last_snapshot`). The current asymmetry
is likely an oversight in the original patch. A tolerant mode
should be opt-in and scoped to recovery tools.

### C. Sibling safety precheck in `btrfs_del_items`

**Observed behavior**: a delete that drains a leaf below the
rebalance threshold unconditionally selects an adjacent sibling for
`push_leaf_left` or `push_leaf_right`. If that sibling is sharable
and has dangling backrefs, the cascade is fatal.

**Proposal**: when `last_snapshot < trans->transid` and the pool is
in a recovery mode (a new `fs_info` flag, or the existing
`rebuilding_extent_tree`), `btrfs_del_items` should skip the
rebalance and leave the leaf under-populated. Under-populated leaves
are valid tree blocks and can be compacted later by a normal
`btrfs balance`. This matches the "leave residual, recover data
first" model that recovery tools actually need.

### D. Supervised EEXIST handling in `alloc_reserved_tree_block`

**Observed behavior**: upstream returns EEXIST. The widely-used
workaround patch returns 0 without updating. Both paths have
downsides.

**Proposal**: make EEXIST handling configurable per-transaction
through a new flag that can be one of `error`, `silent`, or
`update`. Each of these is correct in a different recovery
scenario. The `update` option should actually replace the existing
METADATA_ITEM when the levels or owners differ. This fixes both
the loop problem and the "silent replacement" risk.

### E. Extent tree rebuild in userspace

**Observed behavior**: `btrfs check --init-extent-tree` deadlocks
in the allocation path when the extent tree is empty. The only
working approach we found was to inject every backref from a
separate pre-built TSV file, in chunks, with careful throttling on
DM-SMR drives.

**Proposal**: a new `btrfs rescue rebuild-extent-tree` subcommand
that operates from a pre-scanned ref list. The list can be
generated by a companion command like `btrfs inspect-internal
scan-fs-tree-extents`. Our `rebuild_extent_tree_apply` tool is a
reference implementation.

### F. Orphan inode cleanup that respects the bulletproof criterion

**Observed behavior**: no upstream command exists to safely remove
orphan inode subtrees without triggering the cascades described
above.

**Proposal**: a new `btrfs rescue clean-orphan-inodes` subcommand
with a built-in dry-run mode that applies the five-condition check
above and reports which orphans are safe, which are unsafe, and
why. The mode should produce a machine-readable plan that operators
can review before the write phase.

### G. Surgical `BLOCK_GROUP_ITEM.used` patch

**Observed behavior**: when the FS_TREE contains overlapping
`file_extent_items` from pre-crash stale inodes, Fase 3 style bulk
ref injection produces a block group whose `used` field exceeds
`length`. Mount refuses with `invalid block group used`. Our
`patch_block_group_used` tool is a single-item surgical write.

**Proposal**: extend `btrfs rescue` with `fix-bg-accounting`
that scans all block groups, reports mismatches, and optionally
writes a corrected `used` value based on a caller-provided
value or recomputed from a post-rebuild extent tree.

### H. Clear documentation of `backup_slots` semantics

**Observed behavior**: many users expect `btrfs` to have a built-in
rollback mechanism and misread the presence of `backup_roots[0..3]`
as meaning "there are four recent backups available". In practice
the window is two to ten minutes wide on an active pool and gets
obliterated by any repair loop.

**Proposal**: update the BTRFS wiki, the `btrfs-check(8)` manpage,
and the mount output to make it explicit that `backup_roots` is a
four-commit sliding window and is not a substitute for
`btrfs subvolume snapshot` or `btrfs send`.

### I. DIR `i_size` accounting rule is undocumented

**Observed behavior**: during the orphan dir entry cleanup we
found that BTRFS stores directory `i_size` as `sum(namelen * 2)`,
not `sum(namelen)`. A previous generation of our cleanup tool
decremented by raw `namelen` and produced directories with an
invalid `nlink = 2` state, which would have silently deleted
thousands of subdirectories on the next `rm -rf` over the parent
(a rmdir bomb). The correct formula is not documented in any
user-facing place.

**Proposal**: document the `namelen * 2` rule in
`Documentation/developer/FILESYSTEM.rst` or equivalent, and
consider adding an assert or warn in the kernel path that sets
`i_size` when adding a DIR entry. Tools that modify `i_size`
during repair will otherwise keep shipping with this bug.

## Why we did not submit patches upstream directly

Most of the proposals above are not single-function changes. They
touch the rebalance path, the delayed ref state machine, the tree
checker, and the repair driver. Getting any of them accepted would
require a design discussion and a test fixture that reproduces the
class of damage we hit. We are sharing the case study and the
reference tools first, in the hope that members of the BTRFS
community who are already familiar with those subsystems can judge
which of these proposals are worth pursuing and what the canonical
version would look like.

## Reproducibility

The tools are standalone C files that link against the internal
`btrfs-progs` API. They require a build of btrfs-progs v6.19.1 with
the included `alloc_reserved_tree_block` patch. Every tool supports
a read-only scan mode (default) and a `--write` mode (opt-in).
The README in the root of this repository contains the full build
and execution procedure.

A note on reproducing the exact failure: the pool layout (3-device
single-data DUP-metadata with DM-SMR disks) combined with a hard
power cycle during a metadata commit is sufficient to reproduce
several of the failure modes. The 46,000 commit loop requires
also running `btrfs check --repair` against the resulting state.
We do not recommend attempting to reproduce this for casual
testing: some of the paths destroy real data.

## Data loss and residual state

* Total data loss: approximately 7.2 MB of 4.59 TB, or 0.00016
  percent of the pool, across 5 affected files.
* Two files lost in full: a pair of 540 KB UEFI VARS backups for
  a development VM, which can be regenerated from the VM itself.
* Three files with sub-percent holes: two movie files that remain
  watchable and one VDI backup with a minor glitch. Everything
  else verifies bit-for-bit against a pre-write sha256 baseline
  captured on 120 live files, held unchanged across five
  verification checkpoints.

## Residual `btrfs check --readonly` errors after recovery

These are left intentionally in the current pool because cleaning
them would require triggering the crash vectors described above
and the resulting gain (a cleaner check report) is not worth the
residual risk. The pool operates normally with them.

| Category | Count | Notes |
|---|---|---|
| link count wrong | 712 | phantom refs to inodes in stale leaves |
| unresolved ref | 11 | same class |
| bad file extent | 3,549 | cosmetic, all referenced files read normally |
| ref mismatch on | 49 | phantom refs subset |
| owner ref check failed | 50 | phantom refs subset |
| backpointer mismatch | 393,057 | residue of the 46,000 commit loop |
| no inode item | 2 | two stragglers outside the orphan set |

## License and attribution

All tools in this repository are GPL-2.0, compatible with
`btrfs-progs`. The patch included in `patches/` is a small
modification to `kernel-shared/extent-tree.c` that is also
GPL-2.0.

If any part of this case study is useful to upstream development
please feel free to reuse the content. A link back to this
repository is appreciated but not required.

## Contact

Issues and discussion for this repository live at
https://github.com/msedek/btrfs_fixes/issues

For the upstream BTRFS tooling the usual channels apply
(`linux-btrfs@vger.kernel.org` and
https://github.com/kdave/btrfs-progs/issues).
