/*
 * remove_extent_items_by_key.c — Remove specific EXTENT_ITEMs from extent tree
 *
 * Used post-Fase3 (after patch_block_group_used fixed BG_ITEM.used to 8314880
 * for the first block group) to remove 15 overlapping EXTENT_ITEMs from the
 * first leaf of the extent tree. These items correspond to stale
 * file_extent_items in pre-crash inodes {274083, 274072, 749, 777, 780} whose
 * physical ranges overlap the valid extents of inodes 306071 and 306077 within
 * the first DATA block group [13631488, 22020096).
 *
 * The keys are hardcoded — this is a one-shot operation for a known list. Each
 * delete is defended with sanity checks before mutation:
 *   1. refs between 1 and 5
 *   2. inline ref is BTRFS_EXTENT_DATA_REF_KEY
 *   3. data_ref root is BTRFS_FS_TREE_OBJECTID (5)
 *   4. data_ref objectid (inode) is in the STALE_INODES allowlist
 * If ANY sanity check fails, the transaction is aborted — no partial commit.
 *
 * This tool does NOT call btrfs_update_block_group. The used field of the
 * affected block group was ALREADY patched to the correct post-delete value
 * (8314880 = items 0+9) by patch_block_group_used. Calling
 * btrfs_update_block_group here would double-decrement used, producing an
 * inconsistent state.
 *
 * Usage:
 *   remove_extent_items_by_key <device> [--dryrun|--write]
 *
 * --dryrun (default if absent): find each key, print refs/inode, sanity-check,
 *          report the plan. No mutation.
 * --write:  apply deletes + single commit.
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

/* The 15 stale EXTENT_ITEMs to remove from the first leaf. Extracted
 * verbatim from the pre-patch leaf dump
 * (metadata-backup/pre_patch_bg_13631488/leaf_4973823410176_pre_patch.txt).
 * Slots 2-8 and 10-17 of that leaf. Items 0 and 9 (the valid ones) are kept. */
struct target_key {
	u64 bytenr;
	u64 num_bytes;
	u64 expected_inode;
};

static const struct target_key STALE_KEYS[] = {
	/* slot 2  */ { 14581760, 1032192, 274083 },
	/* slot 3  */ { 15613952,  147456, 274083 },
	/* slot 4  */ { 15761408,  229376, 274083 },
	/* slot 5  */ { 15990784,  278528, 274083 },
	/* slot 6  */ { 16269312,  327680, 274083 },
	/* slot 7  */ { 16596992,  212992, 274083 },
	/* slot 8  */ { 16809984, 2310144, 274083 },
	/* slot 10 */ { 19120128,  212992, 274083 },
	/* slot 11 */ { 19333120,  180224, 274083 },
	/* slot 12 */ { 19513344,  135168, 274072 },
	/* slot 13 */ { 19648512,  163840, 274083 },
	/* slot 14 */ { 19812352,  212992, 274083 },
	/* slot 15 */ { 20037632,  786432,    749 },
	/* slot 16 */ { 20824064,  540672,    777 },
	/* slot 17 */ { 21364736,  540672,    780 },
};
#define NUM_TARGETS (sizeof(STALE_KEYS)/sizeof(STALE_KEYS[0]))

/* Allowlist of inodes that may have file_extent_items removed. Any match
 * outside this set causes an abort. */
static const u64 ALLOWED_INODES[] = { 274083, 274072, 749, 777, 780 };
#define NUM_ALLOWED (sizeof(ALLOWED_INODES)/sizeof(ALLOWED_INODES[0]))

static int inode_allowed(u64 inode)
{
	size_t i;
	for (i = 0; i < NUM_ALLOWED; i++)
		if (ALLOWED_INODES[i] == inode) return 1;
	return 0;
}

static int sanity_check(struct extent_buffer *leaf, int slot,
			const struct target_key *tk, u64 *out_inode)
{
	struct btrfs_extent_item *ei =
		btrfs_item_ptr(leaf, slot, struct btrfs_extent_item);
	u64 refs  = btrfs_extent_refs(leaf, ei);
	u64 flags = btrfs_extent_flags(leaf, ei);

	/* Item size sanity: must be large enough for ei + inline_ref header + data_ref */
	u32 item_size = btrfs_item_size(leaf, slot);
	u32 min_size = sizeof(struct btrfs_extent_item) +
		       sizeof(struct btrfs_extent_inline_ref) +
		       sizeof(struct btrfs_extent_data_ref) - sizeof(__le64);
	if (item_size < min_size) {
		fprintf(stderr, "SANITY FAIL (%llu,%llu): item_size=%u < min_size=%u\n",
		        (unsigned long long)tk->bytenr, (unsigned long long)tk->num_bytes,
		        item_size, min_size);
		return -1;
	}

	if (refs < 1 || refs > 5) {
		fprintf(stderr, "SANITY FAIL (%llu,%llu): refs=%llu out of range\n",
		        (unsigned long long)tk->bytenr, (unsigned long long)tk->num_bytes,
		        (unsigned long long)refs);
		return -1;
	}
	if (!(flags & BTRFS_EXTENT_FLAG_DATA)) {
		fprintf(stderr, "SANITY FAIL (%llu,%llu): flags=0x%llx not DATA\n",
		        (unsigned long long)tk->bytenr, (unsigned long long)tk->num_bytes,
		        (unsigned long long)flags);
		return -1;
	}

	struct btrfs_extent_inline_ref *iref =
		(struct btrfs_extent_inline_ref *)(ei + 1);
	u8 type = btrfs_extent_inline_ref_type(leaf, iref);
	if (type != BTRFS_EXTENT_DATA_REF_KEY) {
		fprintf(stderr, "SANITY FAIL (%llu,%llu): inline ref type=%u not EXTENT_DATA_REF_KEY\n",
		        (unsigned long long)tk->bytenr, (unsigned long long)tk->num_bytes, type);
		return -1;
	}

	struct btrfs_extent_data_ref *dref =
		(struct btrfs_extent_data_ref *)(&iref->offset);
	u64 root_id  = btrfs_extent_data_ref_root(leaf, dref);
	u64 inode    = btrfs_extent_data_ref_objectid(leaf, dref);
	u64 ref_off  = btrfs_extent_data_ref_offset(leaf, dref);
	u32 ref_cnt  = btrfs_extent_data_ref_count(leaf, dref);

	if (root_id != BTRFS_FS_TREE_OBJECTID) {
		fprintf(stderr, "SANITY FAIL (%llu,%llu): data_ref root=%llu not FS_TREE\n",
		        (unsigned long long)tk->bytenr, (unsigned long long)tk->num_bytes,
		        (unsigned long long)root_id);
		return -1;
	}
	if (!inode_allowed(inode)) {
		fprintf(stderr, "SANITY FAIL (%llu,%llu): inode=%llu NOT in allowlist\n",
		        (unsigned long long)tk->bytenr, (unsigned long long)tk->num_bytes,
		        (unsigned long long)inode);
		return -1;
	}
	if (inode != tk->expected_inode) {
		fprintf(stderr, "SANITY FAIL (%llu,%llu): inode=%llu differs from expected %llu\n",
		        (unsigned long long)tk->bytenr, (unsigned long long)tk->num_bytes,
		        (unsigned long long)inode, (unsigned long long)tk->expected_inode);
		return -1;
	}

	printf("  OK (%llu,%llu) refs=%llu inode=%llu ref_off=%llu cnt=%u\n",
	       (unsigned long long)tk->bytenr, (unsigned long long)tk->num_bytes,
	       (unsigned long long)refs, (unsigned long long)inode,
	       (unsigned long long)ref_off, ref_cnt);

	*out_inode = inode;
	return 0;
}

int main(int argc, char *argv[])
{
	if (argc < 2 || argc > 3) {
		fprintf(stderr, "Usage: %s <device> [--dryrun|--write]\n", argv[0]);
		return 1;
	}

	int do_write = 0;
	if (argc == 3) {
		if (strcmp(argv[2], "--write") == 0) do_write = 1;
		else if (strcmp(argv[2], "--dryrun") == 0) do_write = 0;
		else { fprintf(stderr, "ERROR: unknown option %s\n", argv[2]); return 1; }
	}

	printf("=== remove_extent_items_by_key %s ===\n", do_write ? "WRITE" : "DRY-RUN");
	printf("Device: %s\n", argv[1]);
	printf("Targets: %zu hardcoded EXTENT_ITEM keys\n", NUM_TARGETS);
	fflush(stdout);

	struct open_ctree_args oca = {
		.filename = argv[1],
		.flags = OPEN_CTREE_PARTIAL | OPEN_CTREE_ALLOW_TRANSID_MISMATCH |
			 OPEN_CTREE_SKIP_CSUM_CHECK | OPEN_CTREE_SKIP_LEAF_ITEM_CHECKS |
			 (do_write ? OPEN_CTREE_WRITES : 0),
	};
	struct btrfs_fs_info *fs_info = open_ctree_fs_info(&oca);
	if (!fs_info) { fprintf(stderr, "ERROR: open_ctree\n"); return 1; }

	struct btrfs_root *extent_root = btrfs_extent_root(fs_info, 0);
	if (!extent_root || IS_ERR(extent_root)) {
		fprintf(stderr, "ERROR: extent_root\n");
		close_ctree(fs_info->tree_root);
		return 1;
	}

	/* Phase 1: read-only validation of ALL 15 targets before touching anything */
	printf("\n--- Phase 1: validate all 15 targets (read-only) ---\n");
	int validated = 0;
	size_t i;
	for (i = 0; i < NUM_TARGETS; i++) {
		const struct target_key *tk = &STALE_KEYS[i];
		struct btrfs_path *path = btrfs_alloc_path();
		struct btrfs_key key = { tk->bytenr, BTRFS_EXTENT_ITEM_KEY, tk->num_bytes };
		int ret = btrfs_search_slot(NULL, extent_root, &key, path, 0, 0);
		if (ret > 0) {
			fprintf(stderr, "VALIDATE FAIL: key (%llu,EXTENT_ITEM,%llu) NOT FOUND\n",
			        (unsigned long long)tk->bytenr, (unsigned long long)tk->num_bytes);
			btrfs_free_path(path);
			close_ctree(fs_info->tree_root);
			return 2;
		} else if (ret < 0) {
			fprintf(stderr, "VALIDATE FAIL: search_slot ret=%d\n", ret);
			btrfs_free_path(path);
			close_ctree(fs_info->tree_root);
			return 1;
		}
		u64 inode;
		if (sanity_check(path->nodes[0], path->slots[0], tk, &inode) < 0) {
			fprintf(stderr, "ABORT: sanity check failed on target %zu\n", i);
			btrfs_free_path(path);
			close_ctree(fs_info->tree_root);
			return 3;
		}
		validated++;
		btrfs_free_path(path);
	}
	printf("all %d targets validated\n", validated);

	if (!do_write) {
		printf("\nDRY-RUN complete. No mutation.\n");
		close_ctree(fs_info->tree_root);
		return 0;
	}

	/* Phase 2: delete all in one transaction, with bulk-mode flags (skips
	 * normal block group accounting — used field was already patched
	 * externally by patch_block_group_used). */
	printf("\n--- Phase 2: transactional delete ---\n");
	fs_info->rebuilding_extent_tree = 1;
	struct btrfs_trans_handle *trans = btrfs_start_transaction(extent_root, 32);
	if (IS_ERR(trans)) {
		fprintf(stderr, "ERROR: start_transaction: %ld\n", PTR_ERR(trans));
		close_ctree(fs_info->tree_root);
		return 1;
	}
	trans->reinit_extent_tree = true;

	for (i = 0; i < NUM_TARGETS; i++) {
		const struct target_key *tk = &STALE_KEYS[i];
		struct btrfs_path *path = btrfs_alloc_path();
		struct btrfs_key key = { tk->bytenr, BTRFS_EXTENT_ITEM_KEY, tk->num_bytes };

		int ret = btrfs_search_slot(trans, extent_root, &key, path, -1, 1);
		if (ret != 0) {
			fprintf(stderr, "ERROR: in-trans search_slot (%llu,%llu) ret=%d — aborting\n",
			        (unsigned long long)tk->bytenr, (unsigned long long)tk->num_bytes, ret);
			btrfs_abort_transaction(trans, ret < 0 ? ret : -ENOENT);
			btrfs_free_path(path);
			close_ctree(fs_info->tree_root);
			return 1;
		}

		/* Re-verify sanity inside the transaction (paranoia: COW could
		 * reveal a stale view otherwise) */
		u64 inode;
		if (sanity_check(path->nodes[0], path->slots[0], tk, &inode) < 0) {
			fprintf(stderr, "ERROR: sanity check failed inside trans on target %zu — aborting\n", i);
			btrfs_abort_transaction(trans, -EINVAL);
			btrfs_free_path(path);
			close_ctree(fs_info->tree_root);
			return 1;
		}

		ret = btrfs_del_item(trans, extent_root, path);
		if (ret) {
			fprintf(stderr, "ERROR: btrfs_del_item on (%llu,%llu): %d — aborting\n",
			        (unsigned long long)tk->bytenr, (unsigned long long)tk->num_bytes, ret);
			btrfs_abort_transaction(trans, ret);
			btrfs_free_path(path);
			close_ctree(fs_info->tree_root);
			return 1;
		}

		printf("  deleted %zu/%zu: (%llu,%llu) inode=%llu\n",
		       i + 1, NUM_TARGETS,
		       (unsigned long long)tk->bytenr,
		       (unsigned long long)tk->num_bytes,
		       (unsigned long long)inode);
		btrfs_free_path(path);
	}

	printf("\ncommitting transaction...\n");
	fflush(stdout);
	int ret = btrfs_commit_transaction(trans, extent_root);
	if (ret) {
		fprintf(stderr, "ERROR: commit: %d\n", ret);
		close_ctree(fs_info->tree_root);
		return 1;
	}

	printf("\n=== SUCCESS: removed %zu EXTENT_ITEMs ===\n", NUM_TARGETS);
	close_ctree(fs_info->tree_root);
	return 0;
}
