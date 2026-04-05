/*
 * patch_block_group_used.c — Surgical patch of BLOCK_GROUP_ITEM.used
 *
 * After Fase 3 rebuild, the first DATA block group at bytenr 13631488 (8 MB
 * single) ended with `used=15626240` because the FS_TREE has stale
 * file_extent_items from pre-crash inode 274083 (gen 1907) overlapping valid
 * extents from inode 306071 (gen 65341) + inode 306077 (gen 65341). The writer
 * faithfully summed every num_bytes from the filter as each insert called
 * btrfs_update_block_group(+num_bytes). Result: used > bg_length, which the
 * kernel tree_checker rejects with
 *   "invalid block group used, have 15626240 expect [0, 8388608)"
 * blocking mount.
 *
 * Minimal surgical fix: set `used` to a value strictly < bg_length for the one
 * affected block group. Target value comes from summing the *valid* extents
 * (items 0 and 9 of leaf 4973823410176: 4595712 + 3719168 = 8314880). Stale
 * overlapping extents will be removed later (extended Fase 2). For now, this
 * lets the pool mount and data be read.
 *
 * Usage:
 *   patch_block_group_used <device> <bg_bytenr> <bg_length> <new_used> [--write]
 *
 * Without --write: dry-run (reads current value, reports delta, no mutation).
 * With --write:    commits the patch.
 *
 * Safety:
 *   - Refuses if new_used >= bg_length (kernel check is strict)
 *   - Re-verifies current value inside the transaction before mutating
 *   - Single leaf modification, single commit
 */

#include "kerncompat.h"
#include "kernel-shared/ctree.h"
#include "kernel-shared/disk-io.h"
#include "kernel-shared/transaction.h"
#include "kernel-shared/accessors.h"
#include "kernel-shared/tree-checker.h"
#include "kernel-shared/extent-tree.h"
#include "kernel-shared/uapi/btrfs_tree.h"
#include "common/messages.h"
#include "common/open-utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <errno.h>

int main(int argc, char *argv[])
{
	if (argc < 5) {
		fprintf(stderr,
			"Usage: %s <device> <bg_bytenr> <bg_length> <new_used> [--write]\n",
			argv[0]);
		return 1;
	}

	int do_write = 0;
	if (argc >= 6 && strcmp(argv[5], "--write") == 0) do_write = 1;

	char *endp;
	errno = 0;
	u64 bg_bytenr = strtoull(argv[2], &endp, 10);
	if (errno || *endp) { fprintf(stderr, "ERROR: bad bg_bytenr: %s\n", argv[2]); return 1; }
	u64 bg_length = strtoull(argv[3], &endp, 10);
	if (errno || *endp) { fprintf(stderr, "ERROR: bad bg_length: %s\n", argv[3]); return 1; }
	u64 new_used  = strtoull(argv[4], &endp, 10);
	if (errno || *endp) { fprintf(stderr, "ERROR: bad new_used: %s\n",  argv[4]); return 1; }

	/* Kernel requires used strictly less than bg_length */
	if (new_used >= bg_length) {
		fprintf(stderr, "ERROR: new_used (%llu) must be STRICTLY LESS THAN bg_length (%llu)\n",
			(unsigned long long)new_used, (unsigned long long)bg_length);
		return 1;
	}

	printf("=== patch_block_group_used %s ===\n", do_write ? "WRITE" : "DRY-RUN");
	printf("Device:    %s\n", argv[1]);
	printf("Target BG: bytenr=%llu length=%llu\n",
	       (unsigned long long)bg_bytenr, (unsigned long long)bg_length);
	printf("New used:  %llu\n", (unsigned long long)new_used);
	fflush(stdout);

	struct open_ctree_args oca = {
		.filename = argv[1],
		.flags = OPEN_CTREE_PARTIAL | OPEN_CTREE_ALLOW_TRANSID_MISMATCH |
			 OPEN_CTREE_SKIP_CSUM_CHECK | OPEN_CTREE_SKIP_LEAF_ITEM_CHECKS |
			 (do_write ? OPEN_CTREE_WRITES : 0),
	};

	struct btrfs_fs_info *fs_info = open_ctree_fs_info(&oca);
	if (!fs_info) {
		fprintf(stderr, "ERROR: open_ctree_fs_info failed\n");
		return 1;
	}

	struct btrfs_root *extent_root = btrfs_extent_root(fs_info, 0);
	if (!extent_root || IS_ERR(extent_root)) {
		fprintf(stderr, "ERROR: cannot get extent_root\n");
		close_ctree(fs_info->tree_root);
		return 1;
	}

	/* Phase 1: read-only search to verify current state */
	struct btrfs_path *path = btrfs_alloc_path();
	if (!path) { fprintf(stderr, "ERROR: alloc path\n"); close_ctree(fs_info->tree_root); return 1; }

	struct btrfs_key key = { bg_bytenr, BTRFS_BLOCK_GROUP_ITEM_KEY, bg_length };
	int ret = btrfs_search_slot(NULL, extent_root, &key, path, 0, 0);
	if (ret > 0) {
		fprintf(stderr, "ERROR: BLOCK_GROUP_ITEM not found at (%llu, 192, %llu)\n",
			(unsigned long long)bg_bytenr, (unsigned long long)bg_length);
		btrfs_free_path(path);
		close_ctree(fs_info->tree_root);
		return 2;
	} else if (ret < 0) {
		fprintf(stderr, "ERROR: search_slot: %d\n", ret);
		btrfs_free_path(path);
		close_ctree(fs_info->tree_root);
		return 1;
	}

	struct extent_buffer *leaf = path->nodes[0];
	int slot = path->slots[0];
	struct btrfs_block_group_item *bgi =
		btrfs_item_ptr(leaf, slot, struct btrfs_block_group_item);
	u64 current_used = btrfs_block_group_used(leaf, bgi);
	u64 chunk_obj    = btrfs_block_group_chunk_objectid(leaf, bgi);
	u64 flags        = btrfs_block_group_flags(leaf, bgi);
	u64 leaf_bytenr  = btrfs_header_bytenr(leaf);
	u64 leaf_gen     = btrfs_header_generation(leaf);

	printf("FOUND BG_ITEM:\n");
	printf("  leaf_bytenr:      %llu\n", (unsigned long long)leaf_bytenr);
	printf("  leaf_gen:         %llu\n", (unsigned long long)leaf_gen);
	printf("  slot:             %d\n", slot);
	printf("  current used:     %llu\n", (unsigned long long)current_used);
	printf("  target new_used:  %llu\n", (unsigned long long)new_used);
	printf("  delta:            %+lld\n", (long long)new_used - (long long)current_used);
	printf("  chunk_objectid:   %llu\n", (unsigned long long)chunk_obj);
	printf("  flags:            0x%llx\n", (unsigned long long)flags);
	fflush(stdout);

	/* Defensive: only patch DATA block groups. Refuse anything else to avoid
	 * accidental damage to METADATA or SYSTEM bgs which have different semantics. */
	if (!(flags & BTRFS_BLOCK_GROUP_DATA)) {
		fprintf(stderr, "ERROR: target BG is not DATA (flags=0x%llx) — refusing\n",
			(unsigned long long)flags);
		btrfs_free_path(path);
		close_ctree(fs_info->tree_root);
		return 1;
	}

	btrfs_release_path(path);

	if (current_used == new_used) {
		printf("NO-OP: current value already matches target\n");
		btrfs_free_path(path);
		close_ctree(fs_info->tree_root);
		return 0;
	}

	if (!do_write) {
		printf("DRY-RUN — no mutation performed\n");
		btrfs_free_path(path);
		close_ctree(fs_info->tree_root);
		return 0;
	}

	/* Phase 2: transactional update — standard path, no rebuild flags so that
	 * space_info accounting and SB bytes_used propagate normally on commit. */
	struct btrfs_trans_handle *trans = btrfs_start_transaction(extent_root, 1);
	if (IS_ERR(trans)) {
		fprintf(stderr, "ERROR: start_transaction: %ld\n", PTR_ERR(trans));
		btrfs_free_path(path);
		close_ctree(fs_info->tree_root);
		return 1;
	}

	ret = btrfs_search_slot(trans, extent_root, &key, path, 0, 1);
	if (ret != 0) {
		fprintf(stderr, "ERROR: re-search inside trans: %d\n", ret);
		btrfs_abort_transaction(trans, ret < 0 ? ret : -ENOENT);
		btrfs_free_path(path);
		close_ctree(fs_info->tree_root);
		return 1;
	}

	leaf = path->nodes[0];
	slot = path->slots[0];
	bgi = btrfs_item_ptr(leaf, slot, struct btrfs_block_group_item);

	u64 verify_used = btrfs_block_group_used(leaf, bgi);
	if (verify_used != current_used) {
		fprintf(stderr,
			"ERROR: used changed between RO pass and trans pass: was %llu now %llu — aborting\n",
			(unsigned long long)current_used, (unsigned long long)verify_used);
		btrfs_abort_transaction(trans, -EINVAL);
		btrfs_free_path(path);
		close_ctree(fs_info->tree_root);
		return 1;
	}

	btrfs_set_block_group_used(leaf, bgi, new_used);
	btrfs_mark_buffer_dirty(leaf);
	btrfs_free_path(path);

	printf("Committing transaction...\n");
	fflush(stdout);
	ret = btrfs_commit_transaction(trans, extent_root);
	if (ret) {
		fprintf(stderr, "ERROR: commit: %d\n", ret);
		close_ctree(fs_info->tree_root);
		return 1;
	}

	printf("=== SUCCESS: BLOCK_GROUP_ITEM.used patched %llu -> %llu ===\n",
	       (unsigned long long)current_used, (unsigned long long)new_used);

	close_ctree(fs_info->tree_root);
	return 0;
}
