/*
 * scan_and_fix_all_backrefs.c
 *
 * Recursively walks ALL trees of the BTRFS filesystem.
 * For each metadata block, verifies whether a METADATA_ITEM exists in
 * the extent tree. If not, records it as missing. Then injects ALL
 * missing backrefs in ONE single transaction.
 *
 * KEY: Uses trans->reinit_extent_tree = 1 so DROP operations for old
 * blocks without backrefs are ignored (same mechanism used by
 * --init-extent-tree).
 *
 * Build: make scan_and_fix_all_backrefs
 * Run:
 *   sudo ./scan_and_fix_all_backrefs /dev/sdX          # scan only
 *   sudo ./scan_and_fix_all_backrefs /dev/sdX --write  # scan + fix
 */

#include "kerncompat.h"
#include "kernel-shared/ctree.h"
#include "kernel-shared/disk-io.h"
#include "kernel-shared/transaction.h"
#include "kernel-shared/accessors.h"
#include "kernel-shared/tree-checker.h"
#include "kernel-shared/uapi/btrfs_tree.h"
#include "common/messages.h"
#include "common/open-utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Max missing backrefs */
#define MAX_MISSING 65536

/* Max visited blocks — 1M entries = 8MB RAM */
#define MAX_VISITED 1048576

struct missing_backref {
	u64 bytenr;
	u8  level;
	u64 owner;
	u64 gen;
};

static struct missing_backref missing[MAX_MISSING];
static int num_missing = 0;

static u64 visited[MAX_VISITED];
static int num_visited = 0;
static int visit_overflow = 0;

static int was_visited(u64 bytenr)
{
	for (int i = 0; i < num_visited; i++) {
		if (visited[i] == bytenr)
			return 1;
	}
	return 0;
}

static void mark_visited(u64 bytenr)
{
	if (num_visited < MAX_VISITED) {
		visited[num_visited++] = bytenr;
	} else if (!visit_overflow) {
		fprintf(stderr, "WARNING: MAX_VISITED reached (%d). Some blocks may be re-visited.\n",
			MAX_VISITED);
		visit_overflow = 1;
	}
}

static void add_missing(u64 bytenr, u8 level, u64 owner, u64 gen)
{
	if (num_missing >= MAX_MISSING) {
		fprintf(stderr, "WARNING: MAX_MISSING reached\n");
		return;
	}
	for (int i = 0; i < num_missing; i++) {
		if (missing[i].bytenr == bytenr)
			return;
	}
	missing[num_missing].bytenr = bytenr;
	missing[num_missing].level = level;
	missing[num_missing].owner = owner;
	missing[num_missing].gen = gen;
	num_missing++;
}

/*
 * Check if a metadata block has a backref in the extent tree.
 * Returns 1 if found OR if there was an error reading (conservative).
 * Returns 0 only if definitively not found.
 */
static int has_backref(struct btrfs_fs_info *fs_info, u64 bytenr, u8 level)
{
	struct btrfs_root *extent_root;
	struct btrfs_path *path;
	struct btrfs_key key;
	int ret;

	extent_root = btrfs_extent_root(fs_info, bytenr);
	if (!extent_root)
		return 1;  /* can't check, assume exists */

	path = btrfs_alloc_path();
	if (!path)
		return 1;

	key.objectid = bytenr;
	key.type = BTRFS_METADATA_ITEM_KEY;
	key.offset = level;

	ret = btrfs_search_slot(NULL, extent_root, &key, path, 0, 0);
	btrfs_release_path(path);

	if (ret < 0) {
		/* I/O error reading extent tree — assume backref exists (conservative) */
		btrfs_free_path(path);
		return 1;
	}
	if (ret == 0) {
		btrfs_free_path(path);
		return 1;  /* found */
	}

	/* Also check EXTENT_ITEM_KEY as fallback */
	key.type = BTRFS_EXTENT_ITEM_KEY;
	key.offset = fs_info->nodesize;
	ret = btrfs_search_slot(NULL, extent_root, &key, path, 0, 0);
	btrfs_free_path(path);

	if (ret <= 0)
		return 1;  /* found or error (conservative) */

	return 0;  /* definitively not found */
}

/*
 * Recursively walk a tree. Read each block once.
 * Check if it has a backref. If not, register as missing.
 */
static int blocks_failed = 0;

static void walk_tree(struct btrfs_fs_info *fs_info, u64 bytenr, u8 level,
		      u64 owner_root)
{
	struct extent_buffer *eb;
	struct btrfs_tree_parent_check check = {
		.owner_root = owner_root,
		.transid = 0,
		.level = level,
		.has_first_key = false,
	};

	if (was_visited(bytenr))
		return;
	mark_visited(bytenr);

	/* Read block once */
	eb = read_tree_block(fs_info, bytenr, &check);
	if (IS_ERR(eb) || !extent_buffer_uptodate(eb)) {
		blocks_failed++;
		if (!has_backref(fs_info, bytenr, level))
			if (bytenr != 0) add_missing(bytenr, level, owner_root, 0);
		if (!IS_ERR_OR_NULL(eb))
			free_extent_buffer(eb);
		return;
	}

	/* Check backref */
	if (!has_backref(fs_info, bytenr, level)) {
		if (bytenr != 0) add_missing(bytenr, level,
			    btrfs_header_owner(eb),
			    btrfs_header_generation(eb));
	}

	/* Descend into children if internal node */
	if (level > 0) {
		int nritems = btrfs_header_nritems(eb);
		for (int i = 0; i < nritems; i++) {
			u64 child = btrfs_node_blockptr(eb, i);
			walk_tree(fs_info, child, level - 1, owner_root);
		}
	}

	free_extent_buffer(eb);
}

static const char *tree_name(u64 oid)
{
	switch (oid) {
	case 1: return "ROOT";
	case 2: return "EXTENT";
	case 3: return "CHUNK";
	case 4: return "DEV";
	case 5: return "FS";
	case 7: return "CSUM";
	case 9: return "UUID";
	case 10: return "FREE_SPACE";
	default: return "OTHER";
	}
}

int main(int argc, char *argv[])
{
	struct btrfs_fs_info *fs_info;
	struct btrfs_root *tree_root, *extent_root;
	struct btrfs_trans_handle *trans;
	struct btrfs_path *path;
	struct btrfs_key key, found_key;
	struct btrfs_root_item ri;
	int ret, slot, i;
	int do_write = 0;
	int injected = 0;

	if (argc < 2) {
		fprintf(stderr, "Usage: %s <device> [--write]\n", argv[0]);
		return 1;
	}
	if (argc >= 3 && strcmp(argv[2], "--write") == 0)
		do_write = 1;

	printf("=== scan_and_fix_all_backrefs %s ===\n",
	       do_write ? "WRITE" : "SCAN ONLY");

	struct open_ctree_args oca = {
		.filename = argv[1],
		.flags = OPEN_CTREE_PARTIAL |
			 OPEN_CTREE_ALLOW_TRANSID_MISMATCH |
			 OPEN_CTREE_SKIP_CSUM_CHECK |
			 OPEN_CTREE_SKIP_LEAF_ITEM_CHECKS |
			 (do_write ? OPEN_CTREE_WRITES : 0),
	};

	fs_info = open_ctree_fs_info(&oca);
	if (!fs_info) {
		fprintf(stderr, "ERROR: cannot open filesystem\n");
		return 1;
	}

	tree_root = fs_info->tree_root;
	printf("Gen: %llu nodesize: %u\n",
	       (unsigned long long)btrfs_super_generation(fs_info->super_copy),
	       fs_info->nodesize);

	/* PHASE 1: SCAN ALL TREES */
	printf("\n=== PHASE 1: SCAN ===\n");

	/* Fixed trees from superblock */
	printf("  ROOT_TREE...\n");
	walk_tree(fs_info, btrfs_root_bytenr(&tree_root->root_item),
		  btrfs_root_level(&tree_root->root_item), 1);
	printf("    visited=%d missing=%d\n", num_visited, num_missing);

	printf("  CHUNK_TREE...\n");
	walk_tree(fs_info, btrfs_super_chunk_root(fs_info->super_copy),
		  btrfs_super_chunk_root_level(fs_info->super_copy), 3);
	printf("    visited=%d missing=%d\n", num_visited, num_missing);

	/* All trees from ROOT_ITEMs */
	path = btrfs_alloc_path();
	key.objectid = 0;
	key.type = BTRFS_ROOT_ITEM_KEY;
	key.offset = 0;

	ret = btrfs_search_slot(NULL, tree_root, &key, path, 0, 0);
	while (ret >= 0) {
		struct extent_buffer *leaf = path->nodes[0];
		slot = path->slots[0];

		if (slot >= btrfs_header_nritems(leaf)) {
			ret = btrfs_next_leaf(tree_root, path);
			if (ret != 0) break;
			continue;
		}

		btrfs_item_key_to_cpu(leaf, &found_key, slot);

		if (found_key.type == BTRFS_ROOT_ITEM_KEY) {
			read_extent_buffer(leaf, &ri,
				btrfs_item_ptr_offset(leaf, slot),
				sizeof(ri));

			u64 root_bytenr = btrfs_root_bytenr(&ri);
			u8 root_level = btrfs_root_level(&ri);

			if (root_bytenr != 0 && !was_visited(root_bytenr)) {
				printf("  %s (oid=%llu)...\n",
				       tree_name(found_key.objectid),
				       (unsigned long long)found_key.objectid);
				walk_tree(fs_info, root_bytenr, root_level,
					  found_key.objectid);
				printf("    visited=%d missing=%d\n",
				       num_visited, num_missing);
			}
		}
		path->slots[0]++;
	}
	btrfs_free_path(path);

	/* Results */
	printf("\n=== SCAN RESULTS ===\n");
	printf("Blocks visited: %d\n", num_visited);
	printf("Blocks unreadable: %d\n", blocks_failed);
	printf("Missing backrefs: %d\n\n", num_missing);

	for (i = 0; i < num_missing; i++) {
		printf("  [%d] bytenr=%llu level=%u owner=%llu(%s) gen=%llu\n",
		       i, (unsigned long long)missing[i].bytenr,
		       missing[i].level,
		       (unsigned long long)missing[i].owner,
		       tree_name(missing[i].owner),
		       (unsigned long long)missing[i].gen);
	}

	if (!do_write) {
		printf("\n=== SCAN ONLY ===\n");
		if (num_missing > 0)
			printf("Run with --write to inject %d backrefs\n", num_missing);
		close_ctree(tree_root);
		return 0;
	}

	if (num_missing == 0) {
		printf("Nothing to do.\n");
		close_ctree(tree_root);
		return 0;
	}

	/* PHASE 2: INJECT */
	printf("\n=== PHASE 2: INJECT %d BACKREFS ===\n", num_missing);

	extent_root = btrfs_extent_root(fs_info, 0);
	if (!extent_root) {
		fprintf(stderr, "ERROR: cannot get extent root\n");
		close_ctree(tree_root);
		return 1;
	}

	/*
	 * CRITICAL: Set reinit_extent_tree flag.
	 * This makes btrfs_run_delayed_refs IGNORE failures when trying
	 * to DROP backrefs for old blocks that don't have backrefs.
	 * Without this, the commit fails with "unable to find ref byte nr".
	 */
	fs_info->rebuilding_extent_tree = 1;

	trans = btrfs_start_transaction(extent_root, num_missing > 4096 ? 4096 : num_missing);
	if (IS_ERR(trans)) {
		fprintf(stderr, "ERROR: cannot start transaction: %ld\n",
			PTR_ERR(trans));
		close_ctree(tree_root);
		return 1;
	}

	/* Set reinit flag on the transaction */
	trans->reinit_extent_tree = true;

	for (i = 0; i < num_missing; i++) {
		struct btrfs_extent_item *ei;
		struct btrfs_extent_inline_ref *iref;
		struct extent_buffer *leaf;
		u32 item_size;

		key.objectid = missing[i].bytenr;
		key.type = BTRFS_METADATA_ITEM_KEY;
		key.offset = missing[i].level;

		/* Insert METADATA_ITEM with inline TREE_BLOCK_REF */
		item_size = sizeof(struct btrfs_extent_item) +
			    sizeof(struct btrfs_extent_inline_ref);

		path = btrfs_alloc_path();
		ret = btrfs_insert_empty_item(trans, extent_root, path,
					      &key, item_size);
		if (ret) {
			if (ret == -EEXIST) {
				/* Already exists — skip */
				btrfs_free_path(path);
				continue;
			}
			fprintf(stderr, "  [%d] ERROR inserting bytenr=%llu: %d\n",
				i, (unsigned long long)missing[i].bytenr, ret);
			btrfs_free_path(path);
			if (ret == -ENOSPC) {
				fprintf(stderr, "FATAL: no space\n");
				break;
			}
			continue;
		}

		leaf = path->nodes[0];
		ei = btrfs_item_ptr(leaf, path->slots[0],
				    struct btrfs_extent_item);
		btrfs_set_extent_refs(leaf, ei, 1);
		btrfs_set_extent_generation(leaf, ei,
			missing[i].gen ? missing[i].gen :
			btrfs_super_generation(fs_info->super_copy));
		btrfs_set_extent_flags(leaf, ei, BTRFS_EXTENT_FLAG_TREE_BLOCK);

		iref = (struct btrfs_extent_inline_ref *)(ei + 1);
		btrfs_set_extent_inline_ref_type(leaf, iref,
						 BTRFS_TREE_BLOCK_REF_KEY);
		btrfs_set_extent_inline_ref_offset(leaf, iref,
						   missing[i].owner);

		btrfs_mark_buffer_dirty(leaf);
		btrfs_free_path(path);

		injected++;
		if (injected % 50 == 0)
			printf("  %d/%d...\n", injected, num_missing);
	}

	printf("\nInjected %d/%d. Committing...\n", injected, num_missing);

	ret = btrfs_commit_transaction(trans, extent_root);
	if (ret) {
		fprintf(stderr, "ERROR: commit failed: %d\n", ret);
		fprintf(stderr, "Re-run to find remaining missing backrefs.\n");
		close_ctree(tree_root);
		return 1;
	}

	printf("=== SUCCESS: %d backrefs injected ===\n", injected);
	close_ctree(tree_root);
	return 0;
}
