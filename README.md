# btrfs_fixes — Custom BTRFS Repair Tools

Custom tools written during recovery of a 12TB multi-device BTRFS pool with
severe extent tree corruption that the native commands (`btrfs check --repair`,
`--init-extent-tree`, etc.) could not repair.

## When to use these tools

Use these tools **ONLY** if `btrfs check --repair` segfaults, enters an infinite
loop, or leaves the filesystem in worse shape than before.

Documented cases where they help:
- `btrfs check --repair` segfaults at `[3/8] checking extents` (Issue #525)
- `btrfs check --init-extent-tree` deadlocks
- `btrfs check --repair` enters an infinite loop repeating the same repairs
- Extent tree with thousands of METADATA_ITEMs carrying wrong owner/level/generation
- FS_TREE with stale child pointers that reference blocks reused by other trees
- Pool only mounts with `rescue=all,ro`, fails to mount RW

**These tools are NOT for light corruption.** For normal damage, try
`btrfs check --repair` first.

## Warnings

- **BACK UP metadata BEFORE running any tool with `--write`**:
  ```
  for DEV in sda1 sdb1 sdc1; do
    sudo dd if=/dev/$DEV of=sb_${DEV}.bin bs=4096 count=1 skip=16
  done
  ```
- These tools make irreversible changes to the filesystem
- All default to scan-only mode (`--write` is opt-in)
- The filesystem **must be unmounted** when running these tools
- Requires btrfs-progs v6.19.1 or similar, with the EEXIST patch applied

## Build

The tools use the internal btrfs-progs API and must be built inside the
btrfs-progs source tree:

```bash
# 1. Clone btrfs-progs
git clone --depth 1 --branch v6.19.1 https://github.com/kdave/btrfs-progs.git
cd btrfs-progs

# 2. Apply the EEXIST patch (required for batch backref injection)
patch -p1 < path/to/btrfs_fixes/patches/alloc_reserved_tree_block_eexist.patch

# 3. Configure and build base btrfs-progs
./autogen.sh
./configure
make -j$(nproc)

# 4. Copy the .c files from this repo into the btrfs-progs directory
cp path/to/btrfs_fixes/programs/*.c .

# 5. For each program, add to the Makefile:
echo '
PROGNAME: PROGNAME.o $(objects) $(libs_shared)
	@echo "  [LD]    $@"
	$(Q)$(CC) -o $@ PROGNAME.o $(objects) $(libs_shared) $(LDFLAGS) $(LIBS)
' >> Makefile

# 6. Build
make PROGNAME
```

## Tools

Recommended execution order:

### 1. `scan_and_fix_all_backrefs.c` (most important)
**The most important tool.** Recursively walks every tree in the filesystem
(ROOT, CHUNK, EXTENT, FS, DEV, CSUM, UUID, FREE_SPACE) and detects metadata
blocks that are missing a METADATA_ITEM backref in the extent tree. Injects
all missing backrefs in **a single transaction** to avoid the "root tree
moves between commits" problem.

Usage:
```bash
sudo ./scan_and_fix_all_backrefs /dev/sdX          # scan only
sudo ./scan_and_fix_all_backrefs /dev/sdX --write  # scan + inject
```

### 2. `fix_owner_refs.c`
Fixes the `owner` in the inline TREE_BLOCK_REF when it doesn't match the
block's actual `btrfs_header_owner()`. Mismatches occur when blocks are
reassigned between trees during failed repairs.

```bash
sudo ./fix_owner_refs /dev/sdX          # scan
sudo ./fix_owner_refs /dev/sdX --write  # fix
```

### 3. `fix_bad_levels.c`
Fixes METADATA_ITEM and EXTENT_ITEM entries with an incorrect level. Corrupt
levels (e.g. 50, 55, 237) are garbage left behind by `btrfs check --repair`
entering a loop. Verified against the block's real `btrfs_header_level()`.

```bash
sudo ./fix_bad_levels /dev/sdX          # scan
sudo ./fix_bad_levels /dev/sdX --write  # fix
```

### 4. `fix_duplicate_extents.c`
Deletes duplicate METADATA_ITEMs (same bytenr, different levels in the key).
Keeps the one whose level matches `btrfs_header_level` and deletes the other.

```bash
sudo ./fix_duplicate_extents /dev/sdX          # scan
sudo ./fix_duplicate_extents /dev/sdX --write  # delete duplicates
```

### 5. `remove_stale_ptrs.c`
Scans every level-1 node of the FS_TREE. Detects stale child pointers using
three checks: owner mismatch, first_key mismatch, or a first key whose type
is invalid for the FS_TREE (e.g. BLOCK_GROUP_ITEM). Removes them with
`btrfs_del_ptr`.

```bash
sudo ./remove_stale_ptrs /dev/sdX          # scan
sudo ./remove_stale_ptrs /dev/sdX --write  # remove
```

### 6. `fix_uuid_tree.c` / `fix_csum_tree.c`
Creates an empty leaf for the UUID tree / CSUM tree respectively. Useful when
the ROOT_ITEM points to a block that was reassigned to another tree. The
kernel regenerates the UUID tree automatically on RW mount. With an empty
CSUM tree, files flagged NODATASUM do not fail verification.

```bash
sudo ./fix_uuid_tree /dev/sdX
sudo ./fix_csum_tree /dev/sdX
```

### 7. `set_nodatasum.c`
Sets the `BTRFS_INODE_NODATASUM` flag on regular file inodes. Use this if
the csum tree is empty but files still have expected checksums, which causes
read errors. With NODATASUM, the kernel skips csum lookups.

```bash
sudo ./set_nodatasum /dev/sdX          # scan
sudo ./set_nodatasum /dev/sdX --write  # apply
```

### 8. `fix_fstree_node.c`
Version with a hardcoded list of stale blocks. **Prefer `remove_stale_ptrs`**,
which detects them automatically. Only use this if you need manual control
over which specific blocks to remove.

### 9. `add_backrefs.c`
Initial version with a hardcoded list of missing backrefs. **Prefer
`scan_and_fix_all_backrefs`**, which detects them automatically.

## Session 2 toolset (2026-04-04/05) — Extended recovery for massive corruption

When the baseline tools above were insufficient (pool with 200K+ errors spread
across multiple trees), these additional tools were built:

### `scan_fstree_extents.c` + `scan_extent_tree.c`
Pass 1 and Pass 2 scanners that walk the FS_TREE and extent tree respectively,
producing TSV files with every ref/extent mapping. Used to build input for
`rebuild_extent_tree_apply` when the extent tree needs to be rebuilt from scratch.

### `rebuild_extent_tree_apply.c` (heavy writer)
The main Phase 3 writer. Takes a pre-folded list of refs (from `scan_fstree_extents`
+ `scan_extent_tree` diff) and injects 3M+ EXTENT_DATA_REFs into the extent tree
in chunks of 5000 per transaction. Throttles every 50K items to avoid DM-SMR
re-shingle stalls. Verified successful at 3,248,617 inserts in ~34 min on
3× WD40EFAX SMR disks.

```bash
sudo ./rebuild_extent_tree_apply /dev/sdX1 refs_folded.txt to_insert.txt watermark.txt --dryrun
sudo ./rebuild_extent_tree_apply /dev/sdX1 refs_folded.txt to_insert.txt watermark.txt --write
```

### `patch_block_group_used.c`
Surgical single-field patcher for `BLOCK_GROUP_ITEM.used` when Fase 3 writer
leaves a specific bg with overshoot due to pre-existing overlapping file_extent_items.
Uses `btrfs_set_block_group_used` direct setter to avoid `btrfs_update_block_group`
space_info accounting (which we DON'T want here). Pre-validates `flags & BTRFS_BLOCK_GROUP_DATA`.

```bash
sudo ./patch_block_group_used /dev/sdX1 <bg_bytenr> <bg_length> <new_used> --write
```

### `remove_extent_items_by_key.c`
Deletes a hardcoded list of (bytenr, num_bytes, expected_inode) EXTENT_ITEMs from
the extent tree. Used to clean up overlapping stale extents in a single leaf that
prevent RO mount. Per-item sanity checks before delete (7 invariants including
inode allowlist). Runs with `rebuilding_extent_tree=1` + `reinit_extent_tree=true`
to skip space accounting (caller patches `used` manually first via
`patch_block_group_used`).

### `clean_orphan_dir_entries.c`
Cleans orphan DIR_ITEM + DIR_INDEX entries from the FS_TREE. Chunks of 100
entries per transaction. Updates parent INODE_ITEM `i_size` (decrement by
`namelen × 2` — **critical bug fixed**: v1 decremented by `namelen` only,
leaving dirs in invalid state). Hardcoded exclusion list for critical top-level
directory names (e.g. `pelis`, `series`, `music`, `backups`, `homestorage`).
**NEVER decrement i_size by raw namelen** — BTRFS stores `namelen × 2` accounting.

### `clean_orphan_inode_refs.c`
Walks FS_TREE for `INODE_REF` items whose `key.offset` (parent inode) is in an
orphan parent list. **Skips `INODE_EXTREF`** to avoid false positives (EXTREF's
`key.offset` is a hash, not a parent ID). Chunks of 32 per transaction.

### `fix_dir_inode_counts.c`
Recomputes `i_size = sum(name_len × 2)` and `nlink = 1` for DIR inodes whose
counts were corrupted by previous orphan cleanup bugs. **CRITICAL for safety**:
if any DIR has `nlink = 2`, a single `rm -rf` on its path will silently delete
thousands of subdirectories (rmdir bomb). Walks DIR_INDEX entries, cross-checks
DIR_ITEM for hash collision detection (0 collisions verified empirically).

### `remove_orphan_inode_subtrees.c`
Removes orphan inode subtrees (DIR families + standalone REGs) from the FS_TREE.
For each target: walks and deletes EXTENT_DATA, INODE_REF, INODE_EXTREF, XATTR,
and finally INODE_ITEM. Transaction per DIR family (atomic per subtree),
chunks of 50 for standalone REGs. Hardcoded paranoid exclusion list.

**⚠️ MAJOR SAFETY CAVEAT — see "Bulletproof subset criterion" below.**

### `remove_stale_ptrs_v2.c`
Improved version of `remove_stale_ptrs`: detects empty leaves with `parent expected_key`
(v1 skipped this case), recursive 2-level scan (root→level1 + level1→leaves),
dynamic buffer (no 512 limit), tolerates `read_tree_block` failures.

### `insert_one_extent_poc.c`
PoC for single extent insertion with validation. Used to validate the API path
before running `rebuild_extent_tree_apply`.

## Bulletproof subset criterion (CRITICAL)

During the 2026-04-05 session, `remove_orphan_inode_subtrees` crashed twice on
the same `BUG_ON` assertion for two **different reasons**:

**Crash vector 1**: Direct `btrfs_cow_block(leaf)` over MIXED leaf (gen 3601,
contains both orphan and live inodes) → `update_ref_for_cow` walks children →
`__btrfs_mod_ref(inc=1)` over stale sibling children → `btrfs_free_extent(phantom)`
returns `-ENOENT` → `BUG_ON` → SIGABRT.

**Crash vector 2** (discovered later, evaded by filtering):
`btrfs_del_items` post-purge drains a leaf below `LEAF_DATA_SIZE/4 = 4096` bytes →
invokes `push_leaf_left(sibling)` or `push_leaf_right(sibling)` → if sibling has
`gen ≤ last_snapshot = 3701`, `btrfs_block_can_be_shared` returns 1 →
`update_ref_for_cow` enters `refs > 1` path → `btrfs_inc_ref(cow_sibling, 0)` →
`__btrfs_mod_ref(cow, level=0, inc=1)` → iterates all EXTENT_DATAs of stale sibling →
`btrfs_inc_extent_ref(phantom_bytenr)` → `BUG_ON(err)` in `extent-tree.c:1302` → SIGABRT.

**The flags `fs_info->rebuilding_extent_tree = 1` and `trans->reinit_extent_tree = true`
do NOT save the INC path** — they only exempt `BTRFS_DROP_DELAYED_REF` (verified in
`extent-tree.c:3885`). `BTRFS_ADD_DELAYED_REF` (from `btrfs_inc_ref`) is fatal.

**Bulletproof criterion** for any target inode that will be deleted:
1. The leaf hosting the inode's `INODE_ITEM` has `gen > 3701` (post-crash)
2. The leaf's parent level-1 has `gen > 3701`
3. **Post-purge estimated `used` bytes > 4096** (no rebalance trigger)
4. **ALL immediate siblings in the parent node have `gen > 3701`** (even if
   condition 3 fails, rebalance to post-crash siblings is safe)
5. Any backref targets (EXTENT_DATA `disk_bytenr`) resolve in the current
   extent tree (no `-ENOENT` on backref lookup)

**Violating any of conditions 3+4 triggers crash vector 2**. Condition 5 is
exonerated by `reinit_extent_tree` for DROP but NOT for INC (which is what
`push_leaf_left` invokes).

### Empirical validation pattern

For any candidate orphan inode set, walk the FS_TREE dump and classify each
target leaf by the 5 bulletproof conditions. Example pattern (anonymized):

| Leaf | Gen | Orphan items / total | Post-purge used (est) | Rebalance? | Immediate siblings | Verdict |
|---|---|---|---|---|---|---|
| `$LEAF_A` | post-crash | mostly orphan, heavy purge | below threshold | YES | all post-crash | ✓ safe |
| `$LEAF_B` | post-crash | mostly live, light purge | above threshold | NO | clean parent | ✓ safe |
| `$LEAF_C` | post-crash | nearly 100% orphan | far below 4096 | YES forced | pre-crash stale | ❌ **CRASH** |

Leaves where **≥90% of items are orphan** are the danger zone: they will drain
below the rebalance threshold (LEAF_DATA_SIZE/4 = 4096 bytes) with certainty,
forcing `push_leaf_left/right`. If any immediate sibling in the parent node has
`gen ≤ last_snapshot`, the push triggers CoW on that sibling, which enters the
`btrfs_block_can_be_shared → refs > 1 → btrfs_inc_ref → __btrfs_mod_ref(inc=1)`
path and crashes with `BUG_ON(err)` in `btrfs_inc_extent_ref`.

**Mitigation**: exclude the offending inodes from the input file. The tool
processes whatever passes the pre-flight validation; leaves with mixed
safe/unsafe targets can be partially processed by only listing the safe
subset. Per-family transaction semantics mean each safe family commits
atomically even if other families are excluded.

**Empirical result from one session**: starting from N candidate orphans,
after applying all 5 conditions the final safe subset was ~14% of the input,
but that subset committed without a single `BUG_ON`, with a 0-byte diff on
a baseline sha256 of live files captured pre-write.

## The patch: EEXIST in alloc_reserved_tree_block

`patches/alloc_reserved_tree_block_eexist.patch` modifies btrfs-progs so that
when `alloc_reserved_tree_block` finds the METADATA_ITEM already exists it
returns 0 instead of propagating EEXIST. This is required for batch backref
injection to work: when injecting many backrefs, the delayed refs system also
tries to create METADATA_ITEMs for blocks newly allocated via COW and collides
with the ones we already inserted.

## Full workflow for severe recovery

```bash
# 1. Backup
mkdir -p backup
for DEV in /dev/sdX1 /dev/sdY1; do
  sudo dd if=$DEV of=backup/$(basename $DEV).sb bs=4096 count=1 skip=16
done

# 2. Make sure the filesystem is unmounted
sudo umount /mnt/pool 2>/dev/null

# 3. Zero the log tree (if applicable)
sudo btrfs rescue zero-log /dev/sdX1

# 4. Scan + fix everything (in order)
sudo ./scan_and_fix_all_backrefs /dev/sdX1 --write
sudo ./fix_bad_levels /dev/sdX1 --write
sudo ./fix_owner_refs /dev/sdX1 --write
sudo ./fix_duplicate_extents /dev/sdX1 --write
sudo ./remove_stale_ptrs /dev/sdX1 --write

# 5. Re-scan to verify convergence
sudo ./scan_and_fix_all_backrefs /dev/sdX1
sudo ./remove_stale_ptrs /dev/sdX1

# 6. If the csum tree is broken:
sudo ./fix_csum_tree /dev/sdX1
sudo ./set_nodatasum /dev/sdX1 --write

# 7. Try mounting RW
sudo mount -o rw /dev/sdX1 /mnt/pool

# 8. If it mounts, verify with btrfs check readonly
sudo btrfs check --force /dev/sdX1
```

## Known limitations

1. **Each repair can create new problems through COW**: when a tool modifies
   the extent tree, btrfs COWs the affected nodes. The new nodes copy
   pointers from the old ones, which can propagate stale pointers. Multiple
   passes may be required.

2. **Data extent ref mismatches are not fixed**: these tools only touch
   metadata backrefs. Incorrect ref counts on data extents (common after
   failed `btrfs check --repair` runs) are not cleaned up.

3. **Orphan inodes are not cleaned**: orphan directory entries in the
   FS_TREE (references to inodes that no longer exist) are not removed.

4. **Does not replace `btrfs check --repair`**: these tools target specific
   scenarios. For light or moderate damage, `btrfs check --repair` is better.

## Lessons learned

1. **NEVER hard power-cycle** a multi-device BTRFS filesystem — combined
   free space tree + extent tree corruption is extremely hard to repair.

2. **NEVER run `btrfs check --repair` multiple times in a row** if the first
   run did not resolve everything — it can enter an infinite loop and make
   the filesystem dramatically worse.

3. **Always back up the superblocks** before every write operation.

4. **`trans->reinit_extent_tree = true`** is key to ignoring DROP failures
   in delayed refs for blocks without backrefs.

5. **`fs_info->rebuilding_extent_tree = 1`** disables space checks during
   repairs.

6. **One large commit** with many inserts is better than many small commits,
   because intermediate commits move the root tree.

7. **`backup_slots` in the SB are NOT historical backups** — they are a sliding
   window of the most recent 4 commits only. A `btrfs check --repair` loop of
   46,000+ commits will rotate every slot ~11,000 times in minutes, obliterating
   any pre-crash state recoverable from the kernel. For real retention you need
   explicit `btrfs subvolume snapshot` or `btrfs send` streams to another device.

8. **`reinit_extent_tree` is ASYMMETRIC**: only exempts `BTRFS_DROP_DELAYED_REF`,
   NOT `BTRFS_ADD_DELAYED_REF`. Any code path that calls `btrfs_inc_ref` on a
   stale leaf (including `push_leaf_left/right` during rebalance) will still
   crash via `btrfs_inc_extent_ref` → `BUG_ON(err)`.

9. **The safety criterion for processing inodes in a damaged FS_TREE must
   include siblings**, not just the target leaf itself. See "Bulletproof subset
   criterion" section.

10. **Baseline sha256 of LIVE files is the only empirical proof of invariants**.
    Capture it before any write operation, diff after. Any mismatch = rollback.

11. **DIR `i_size` is stored as `sum(name_len × 2)`, NOT `sum(name_len)`**.
    Any orphan cleanup tool that decrements i_size on entry removal must
    decrement by `namelen × 2`. Getting this wrong leaves DIRs in an invalid
    state that can manifest as `nlink = 2` later — which triggers a `rmdir`
    bomb if the pool is mounted RW (a single `rm -rf` on a parent can delete
    thousands of subdirs silently).

12. **Expert reviewer agents with empirical evidence are critical**. The
    2026-04-05 session used two parallel Opus reviewers (btrfs internals +
    ops) that analyzed the proposed plan against dump-tree output. They
    caught a deterministic crash vector (push_leaf_left → stale sibling)
    that would have repeated the previous failures. A textual plan review
    without empirical dump-tree analysis would have missed this.

## Disclaimer

These tools were written for a specific recovery case where the native tools
were failing. **They are not tested for general use cases.** Only use them
if you understand the code and accept the risk of data loss.

Always **copy your data before attempting any repair** if at all possible.

## License

GPL-2.0 (compatible with btrfs-progs, whose internal API these tools use).
