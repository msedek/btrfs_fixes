/*
 * insert_one_extent_poc.c — Proof of concept for Fase 3
 *
 * Insert exactly ONE EXTENT_ITEM + one inline EXTENT_DATA_REF into the extent
 * tree. Uses btrfs_update_block_group to update bytes_used in SB.
 *
 * Reads ONE line from a file (argv[2]) with format:
 *   disk_bytenr|disk_num_bytes|root|inode|ref_offset|count|gen|compression
 *
 * Then:
 *   1. Opens ctree WRITES
 *   2. Checks if EXTENT_ITEM already exists at (bytenr, EXTENT_ITEM, num_bytes)
 *   3. If exists: logs and exits 2
 *   4. If not: builds the EXTENT_ITEM and inline EXTENT_DATA_REF, inserts
 *   5. Calls btrfs_update_block_group(trans, bytenr, num_bytes, 1, 0)
 *   6. Commits
 *
 * Usage: insert_one_extent_poc <device> <refs_file> [--write]
 *   Without --write: dry-run, prints what would happen
 *   With --write: actually inserts
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

int main(int argc, char *argv[])
{
	if (argc < 3) {
		fprintf(stderr, "Usage: %s <device> <refs_file> [--write]\n", argv[0]);
		return 1;
	}

	int do_write = 0;
	if (argc >= 4 && strcmp(argv[3], "--write") == 0) do_write = 1;

	/* Parse the first line of refs_file */
	FILE *fp = fopen(argv[2], "r");
	if (!fp) { fprintf(stderr, "ERROR: cannot open %s\n", argv[2]); return 1; }

	char line[512];
	if (!fgets(line, sizeof(line), fp)) {
		fprintf(stderr, "ERROR: empty refs file\n");
		fclose(fp);
		return 1;
	}
	fclose(fp);

	u64 bytenr, num_bytes, root_id, inode, ref_offset, gen;
	int count, compression;
	if (sscanf(line, "%llu|%llu|%llu|%llu|%llu|%d|%llu|%d",
		   (unsigned long long*)&bytenr,
		   (unsigned long long*)&num_bytes,
		   (unsigned long long*)&root_id,
		   (unsigned long long*)&inode,
		   (unsigned long long*)&ref_offset,
		   &count,
		   (unsigned long long*)&gen,
		   &compression) != 8) {
		fprintf(stderr, "ERROR: parse failed: %s", line);
		return 1;
	}

	printf("=== insert_one_extent_poc %s ===\n", do_write ? "WRITE" : "DRY-RUN");
	printf("Target extent: bytenr=%llu num_bytes=%llu\n",
	       (unsigned long long)bytenr, (unsigned long long)num_bytes);
	printf("Ref: root=%llu inode=%llu ref_offset=%llu count=%d gen=%llu\n",
	       (unsigned long long)root_id, (unsigned long long)inode,
	       (unsigned long long)ref_offset, count, (unsigned long long)gen);

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

	/* 1. Check if EXTENT_ITEM already exists */
	struct btrfs_path *path = btrfs_alloc_path();
	struct btrfs_key key = { bytenr, BTRFS_EXTENT_ITEM_KEY, num_bytes };
	int ret = btrfs_search_slot(NULL, extent_root, &key, path, 0, 0);
	if (ret == 0) {
		printf("ALREADY EXISTS at slot %d in leaf %llu\n",
		       path->slots[0],
		       (unsigned long long)btrfs_header_bytenr(path->nodes[0]));
		btrfs_free_path(path);
		close_ctree(fs_info->tree_root);
		return 2;
	} else if (ret < 0) {
		fprintf(stderr, "ERROR: search_slot: %d\n", ret);
		btrfs_free_path(path);
		close_ctree(fs_info->tree_root);
		return 1;
	}
	printf("NOT PRESENT — would be at slot %d of leaf %llu\n",
	       path->slots[0],
	       path->nodes[0] ? (unsigned long long)btrfs_header_bytenr(path->nodes[0]) : 0ULL);
	btrfs_free_path(path);

	/* Dry-run exits here */
	if (!do_write) {
		printf("DRY-RUN — would insert EXTENT_ITEM %llu,%llu with refs=%d\n",
		       (unsigned long long)bytenr, (unsigned long long)num_bytes, count);
		close_ctree(fs_info->tree_root);
		return 0;
	}

	/* 2. Start transaction */
	fs_info->rebuilding_extent_tree = 1;
	struct btrfs_trans_handle *trans = btrfs_start_transaction(extent_root, 1);
	if (IS_ERR(trans)) {
		fprintf(stderr, "ERROR: start_transaction: %ld\n", PTR_ERR(trans));
		close_ctree(fs_info->tree_root);
		return 1;
	}
	trans->reinit_extent_tree = true;

	/* 3. Allocate path and build the item */
	path = btrfs_alloc_path();
	u32 item_size = sizeof(struct btrfs_extent_item) +
			btrfs_extent_inline_ref_size(BTRFS_EXTENT_DATA_REF_KEY);
	printf("Inserting EXTENT_ITEM size=%u bytes\n", item_size);

	ret = btrfs_insert_empty_item(trans, extent_root, path, &key, item_size);
	if (ret) {
		fprintf(stderr, "ERROR: insert_empty_item: %d\n", ret);
		btrfs_free_path(path);
		btrfs_abort_transaction(trans, ret);
		close_ctree(fs_info->tree_root);
		return 1;
	}

	struct extent_buffer *leaf = path->nodes[0];
	struct btrfs_extent_item *ei = btrfs_item_ptr(leaf, path->slots[0],
						       struct btrfs_extent_item);

	btrfs_set_extent_refs(leaf, ei, count);
	btrfs_set_extent_generation(leaf, ei, trans->transid);
	btrfs_set_extent_flags(leaf, ei, BTRFS_EXTENT_FLAG_DATA);

	struct btrfs_extent_inline_ref *iref =
		(struct btrfs_extent_inline_ref *)(ei + 1);
	btrfs_set_extent_inline_ref_type(leaf, iref, BTRFS_EXTENT_DATA_REF_KEY);

	/* The inline_ref's offset field is overlaid with the btrfs_extent_data_ref
	 * so we set it via btrfs_set_extent_data_ref_* which writes at iref+offsetof(offset). */
	struct btrfs_extent_data_ref *dref =
		(struct btrfs_extent_data_ref *)(&iref->offset);
	btrfs_set_extent_data_ref_root(leaf, dref, root_id);
	btrfs_set_extent_data_ref_objectid(leaf, dref, inode);
	btrfs_set_extent_data_ref_offset(leaf, dref, ref_offset);
	btrfs_set_extent_data_ref_count(leaf, dref, count);

	btrfs_mark_buffer_dirty(leaf);
	btrfs_free_path(path);

	/* 4. Update block group (updates bytes_used in SB) */
	ret = btrfs_update_block_group(trans, bytenr, num_bytes, 1, 0);
	if (ret) {
		fprintf(stderr, "WARN: update_block_group: %d (continuing)\n", ret);
	}

	/* 5. Commit */
	printf("Committing transaction...\n");
	ret = btrfs_commit_transaction(trans, extent_root);
	if (ret) {
		fprintf(stderr, "ERROR: commit: %d\n", ret);
		close_ctree(fs_info->tree_root);
		return 1;
	}

	printf("=== SUCCESS: inserted 1 EXTENT_ITEM ===\n");
	close_ctree(fs_info->tree_root);
	return 0;
}
