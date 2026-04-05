/*
 * add_backrefs.c — Inject missing METADATA_ITEM backrefs into the extent tree
 *
 * Build inside btrfs-progs source tree. Add to Makefile:
 *   add_backrefs: add_backrefs.o $(objects) $(libs_shared)
 *       $(CC) -o $@ $^ $(LDFLAGS) $(LIBS)
 *
 * Then: make add_backrefs
 *
 * Run:
 *   sudo ./add_backrefs /dev/sdX          # scan only (read-only)
 *   sudo ./add_backrefs /dev/sdX --write  # scan + inject backrefs
 */

#include "kerncompat.h"
#include "kernel-shared/ctree.h"
#include "kernel-shared/disk-io.h"
#include "kernel-shared/transaction.h"
#include "kernel-shared/volumes.h"
#include "kernel-shared/accessors.h"
#include "kernel-shared/tree-checker.h"
#include "kernel-shared/uapi/btrfs_tree.h"
#include "common/messages.h"
#include "common/open-utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Missing backrefs from lowmem check log */
struct missing_ref {
	u64 bytenr;
	u8 level;
	u64 owner;  /* root objectid that owns this block */
};

/*
 * EDIT THIS LIST with YOUR filesystem's missing backrefs.
 *
 * How to find them:
 *   sudo btrfs check --mode=lowmem /dev/sdX 2>&1 | grep 'backref lost\|lost referencer'
 *
 * Format: { bytenr, level, owner_root_objectid }
 *
 * Owner root objectids:
 *   1 = ROOT_TREE, 2 = EXTENT_TREE, 3 = CHUNK_TREE, 4 = DEV_TREE,
 *   5 = FS_TREE, 7 = CSUM_TREE, 9 = UUID_TREE, 10 = FREE_SPACE_TREE
 *
 * NOTE: For most cases, use scan_and_fix_all_backrefs instead — it finds
 * missing backrefs automatically by walking all trees.
 */
static struct missing_ref missing[] = {
	/* Example format — REPLACE with your own: */
	/* { BYTENR_HERE, LEVEL_HERE, OWNER_ROOT_HERE }, */
};

#define NUM_MISSING (sizeof(missing) / sizeof(missing[0]))

int main(int argc, char *argv[])
{
	struct btrfs_fs_info *fs_info;
	struct btrfs_root *extent_root;
	struct btrfs_trans_handle *trans = NULL;
	struct btrfs_path *path = NULL;
	struct btrfs_key key;
	struct extent_buffer *leaf;
	struct btrfs_extent_item *ei;
	struct btrfs_extent_inline_ref *iref;
	int ret, i;
	int do_write = 0;
	int found = 0, injected = 0, skipped = 0;

	if (argc < 2) {
		fprintf(stderr, "Usage: %s <device> [--write]\n", argv[0]);
		return 1;
	}

	if (argc >= 3 && strcmp(argv[2], "--write") == 0)
		do_write = 1;

	printf("=== add_backrefs %s ===\n", do_write ? "WRITE MODE" : "SCAN ONLY");
	printf("Target: %s\n", argv[1]);
	printf("Missing backrefs to process: %zu\n\n", NUM_MISSING);

	/* Open filesystem — v6.19.1 API uses struct */
	struct open_ctree_args oca = {
		.filename = argv[1],
		.sb_bytenr = 0,
		.root_tree_bytenr = 0,
		.chunk_tree_bytenr = 0,
		.flags = OPEN_CTREE_PARTIAL |
			 OPEN_CTREE_ALLOW_TRANSID_MISMATCH |
			 OPEN_CTREE_SKIP_CSUM_CHECK |
			 OPEN_CTREE_SKIP_LEAF_ITEM_CHECKS |
			 (do_write ? OPEN_CTREE_WRITES : 0),
	};

	fs_info = open_ctree_fs_info(&oca);
	if (!fs_info) {
		fprintf(stderr, "ERROR: cannot open filesystem on %s\n", argv[1]);
		return 1;
	}

	extent_root = btrfs_extent_root(fs_info, 0);
	if (!extent_root) {
		fprintf(stderr, "ERROR: cannot get extent root\n");
		close_ctree(fs_info->tree_root);
		return 1;
	}

	printf("Filesystem opened. Generation: %llu\n",
	       (unsigned long long)btrfs_super_generation(fs_info->super_copy));
	printf("Extent tree root: bytenr=%llu level=%d\n\n",
	       (unsigned long long)btrfs_root_bytenr(&extent_root->root_item),
	       btrfs_root_level(&extent_root->root_item));

	path = btrfs_alloc_path();
	if (!path) {
		fprintf(stderr, "ERROR: cannot allocate path\n");
		close_ctree(fs_info->tree_root);
		return 1;
	}

	/* Phase 1: Scan — check which backrefs are actually missing */
	printf("=== Phase 1: SCAN ===\n");
	for (i = 0; i < (int)NUM_MISSING; i++) {
		key.objectid = missing[i].bytenr;
		key.type = BTRFS_METADATA_ITEM_KEY;
		key.offset = missing[i].level;

		ret = btrfs_search_slot(NULL, extent_root, &key, path, 0, 0);
		btrfs_release_path(path);

		if (ret == 0) {
			printf("  [%d] bytenr=%llu level=%u owner=%llu — EXISTS (skip)\n",
			       i, (unsigned long long)missing[i].bytenr,
			       missing[i].level, (unsigned long long)missing[i].owner);
			skipped++;
		} else {
			printf("  [%d] bytenr=%llu level=%u owner=%llu — MISSING\n",
			       i, (unsigned long long)missing[i].bytenr,
			       missing[i].level, (unsigned long long)missing[i].owner);
			found++;
		}
	}

	printf("\nScan result: %d missing, %d already exist\n\n", found, skipped);

	if (!do_write) {
		printf("=== SCAN ONLY — no changes made ===\n");
		printf("Run with --write to inject missing backrefs\n");
		btrfs_free_path(path);
		close_ctree(fs_info->tree_root);
		return 0;
	}

	if (found == 0) {
		printf("Nothing to inject.\n");
		btrfs_free_path(path);
		close_ctree(fs_info->tree_root);
		return 0;
	}

	/* Phase 2: Inject — all in ONE transaction */
	printf("=== Phase 2: INJECT (%d backrefs) ===\n", found);

	trans = btrfs_start_transaction(extent_root, 1);
	if (IS_ERR(trans)) {
		fprintf(stderr, "ERROR: cannot start transaction: %ld\n", PTR_ERR(trans));
		btrfs_free_path(path);
		close_ctree(fs_info->tree_root);
		return 1;
	}

	for (i = 0; i < (int)NUM_MISSING; i++) {
		/* Check if already exists */
		key.objectid = missing[i].bytenr;
		key.type = BTRFS_METADATA_ITEM_KEY;
		key.offset = missing[i].level;

		ret = btrfs_search_slot(trans, extent_root, &key, path, 0, 0);
		btrfs_release_path(path);
		if (ret == 0)
			continue;  /* already exists */

		/* Read the block to get its real generation */
		struct btrfs_tree_parent_check check = {
			.owner_root = missing[i].owner,
			.transid = 0,       /* 0 = skip transid check */
			.level = missing[i].level,
			.has_first_key = false,
		};
		u64 gen;
		struct extent_buffer *eb;

		eb = read_tree_block(fs_info, missing[i].bytenr, &check);
		if (IS_ERR(eb) || !extent_buffer_uptodate(eb)) {
			fprintf(stderr, "  [%d] WARNING: cannot read block %llu, using gen=%llu\n",
				i, (unsigned long long)missing[i].bytenr,
				(unsigned long long)btrfs_super_generation(fs_info->super_copy));
			gen = btrfs_super_generation(fs_info->super_copy);
			if (!IS_ERR_OR_NULL(eb))
				free_extent_buffer(eb);
		} else {
			gen = btrfs_header_generation(eb);
			free_extent_buffer(eb);
		}

		/* Insert METADATA_ITEM with inline TREE_BLOCK_REF */
		u32 item_size = sizeof(struct btrfs_extent_item) +
				sizeof(struct btrfs_extent_inline_ref);

		ret = btrfs_insert_empty_item(trans, extent_root, path, &key, item_size);
		if (ret) {
			fprintf(stderr, "  [%d] ERROR inserting bytenr=%llu: %d\n",
				i, (unsigned long long)missing[i].bytenr, ret);
			btrfs_release_path(path);
			if (ret == -ENOSPC) {
				fprintf(stderr, "FATAL: no space, aborting\n");
				break;
			}
			continue;
		}

		leaf = path->nodes[0];

		/* Fill btrfs_extent_item */
		ei = btrfs_item_ptr(leaf, path->slots[0], struct btrfs_extent_item);
		btrfs_set_extent_refs(leaf, ei, 1);
		btrfs_set_extent_generation(leaf, ei, gen);
		btrfs_set_extent_flags(leaf, ei, BTRFS_EXTENT_FLAG_TREE_BLOCK);

		/* Fill inline TREE_BLOCK_REF (right after extent_item) */
		iref = (struct btrfs_extent_inline_ref *)(ei + 1);
		btrfs_set_extent_inline_ref_type(leaf, iref, BTRFS_TREE_BLOCK_REF_KEY);
		btrfs_set_extent_inline_ref_offset(leaf, iref, missing[i].owner);

		btrfs_mark_buffer_dirty(leaf);
		btrfs_release_path(path);

		printf("  [%d] INJECTED bytenr=%llu level=%u owner=%llu gen=%llu\n",
		       i, (unsigned long long)missing[i].bytenr,
		       missing[i].level, (unsigned long long)missing[i].owner,
		       (unsigned long long)gen);
		injected++;
	}

	printf("\nInjected %d backrefs. Committing...\n", injected);

	ret = btrfs_commit_transaction(trans, extent_root);
	if (ret) {
		fprintf(stderr, "ERROR: commit failed: %d\n", ret);
		btrfs_free_path(path);
		close_ctree(fs_info->tree_root);
		return 1;
	}

	printf("Transaction committed.\n");
	printf("=== RESULT: %d backrefs injected ===\n", injected);

	btrfs_free_path(path);
	close_ctree(fs_info->tree_root);
	return 0;
}
