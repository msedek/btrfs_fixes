/*
 * fix_fstree_node.c — Remove stale child pointers from a corrupt FS_TREE node
 *
 * A level-1 node has children pointing to blocks that were reused by other
 * trees. This tool removes those stale pointers using btrfs_del_ptr().
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

/*
 * Blocks that are stale (owner != FS_TREE).
 *
 * EDIT THIS LIST with YOUR filesystem's stale blocks.
 * Find them with remove_stale_ptrs (without --write) which scans automatically.
 *
 * NOTE: Use remove_stale_ptrs instead — it auto-detects stale pointers.
 */
static u64 stale_blocks[] = {
	/* Example format — REPLACE with your own: */
	/* BYTENR_HERE, */
};
#define NUM_STALE (sizeof(stale_blocks) / sizeof(stale_blocks[0]))

static int is_stale(u64 bytenr)
{
	for (int i = 0; i < (int)NUM_STALE; i++)
		if (stale_blocks[i] == bytenr)
			return 1;
	return 0;
}

int main(int argc, char *argv[])
{
	struct btrfs_fs_info *fs_info;
	struct btrfs_root *fs_root;
	struct btrfs_trans_handle *trans;
	struct btrfs_path *path;
	struct btrfs_key key;
	int ret, i;
	int do_write = 0;
	int found = 0, removed = 0;

	if (argc < 2) { fprintf(stderr, "Usage: %s <dev> [--write]\n", argv[0]); return 1; }
	if (argc >= 3 && strcmp(argv[2], "--write") == 0) do_write = 1;

	printf("=== fix_fstree_node %s ===\n", do_write ? "WRITE" : "SCAN");

	struct open_ctree_args oca = {
		.filename = argv[1],
		.flags = OPEN_CTREE_PARTIAL | OPEN_CTREE_ALLOW_TRANSID_MISMATCH |
			 OPEN_CTREE_SKIP_CSUM_CHECK | OPEN_CTREE_SKIP_LEAF_ITEM_CHECKS |
			 (do_write ? OPEN_CTREE_WRITES : 0),
	};

	fs_info = open_ctree_fs_info(&oca);
	if (!fs_info) { fprintf(stderr, "ERROR: open\n"); return 1; }

	/* Get FS_TREE */
	key.objectid = BTRFS_FS_TREE_OBJECTID;
	key.type = BTRFS_ROOT_ITEM_KEY;
	key.offset = (u64)-1;
	fs_root = btrfs_read_fs_root(fs_info, &key);
	if (IS_ERR(fs_root)) { fprintf(stderr, "ERROR: fs root\n"); close_ctree(fs_info->tree_root); return 1; }

	printf("FS_TREE root: bytenr=%llu level=%d\n",
	       (unsigned long long)btrfs_root_bytenr(&fs_root->root_item),
	       btrfs_root_level(&fs_root->root_item));

	/* Scan: walk the root node and find stale children */
	printf("\n=== Scanning root node children ===\n");

	struct extent_buffer *root_node = fs_root->node;
	int nritems = btrfs_header_nritems(root_node);
	int level = btrfs_header_level(root_node);
	printf("Root node: level=%d nritems=%d\n", level, nritems);

	if (level < 2) {
		fprintf(stderr, "FS_TREE level < 2, cannot have level-1 nodes to fix\n");
		close_ctree(fs_info->tree_root);
		return 0;
	}

	/* Find the level-1 node that contains stale pointers */
	/* It's at slot 1 of the root: block 5753485017088 */
	for (i = 0; i < nritems; i++) {
		u64 child_bytenr = btrfs_node_blockptr(root_node, i);
		if (1) {
			printf("Found target node at root slot %d: block %llu\n",
			       i, (unsigned long long)child_bytenr);

			/* Read the level-1 node */
			struct btrfs_tree_parent_check check = {
				.owner_root = BTRFS_FS_TREE_OBJECTID,
				.transid = 0, .level = 1, .has_first_key = false,
			};
			struct extent_buffer *node = read_tree_block(fs_info,
				child_bytenr, &check);
			if (IS_ERR(node) || !extent_buffer_uptodate(node)) {
				fprintf(stderr, "ERROR: cannot read node\n");
				break;
			}

			int nn = btrfs_header_nritems(node);
			printf("Node has %d children\n\n", nn);

			/* Find stale children */
			for (int j = 0; j < nn; j++) {
				u64 blk = btrfs_node_blockptr(node, j);
				if (is_stale(blk)) {
					struct btrfs_key child_key;
					btrfs_node_key_to_cpu(node, &child_key, j);
					printf("  STALE slot %d: block=%llu key=(%llu %u %llu)\n",
					       j, (unsigned long long)blk,
					       (unsigned long long)child_key.objectid,
					       child_key.type,
					       (unsigned long long)child_key.offset);
					found++;
				}
			}
			free_extent_buffer(node);
			break;
		}
	}

	printf("\nStale children found: %d\n", found);

	if (!do_write || found == 0) {
		if (found > 0)
			printf("Run with --write to remove %d stale pointers\n", found);
		close_ctree(fs_info->tree_root);
		return 0;
	}

	/* Write: use btrfs path to navigate and delete stale pointers */
	printf("\n=== Removing stale pointers ===\n");

	fs_info->rebuilding_extent_tree = 1;
	trans = btrfs_start_transaction(fs_root, 1);
	if (IS_ERR(trans)) { fprintf(stderr, "ERROR: transaction\n"); close_ctree(fs_info->tree_root); return 1; }
	trans->reinit_extent_tree = true;

	/*
	 * Strategy: for each stale block, search for a key that would land
	 * in that block's range, then delete the pointer using btrfs_del_ptr.
	 * We go from last to first to avoid shifting issues.
	 */
	/* First, collect the keys of stale children */
	struct btrfs_tree_parent_check check2 = {
		.owner_root = BTRFS_FS_TREE_OBJECTID,
		.transid = 0, .level = 1, .has_first_key = false,
	};
	struct extent_buffer *node = read_tree_block(fs_info, 5753485017088ULL, &check2);
	if (IS_ERR(node) || !extent_buffer_uptodate(node)) {
		fprintf(stderr, "ERROR: cannot read node for write\n");
		close_ctree(fs_info->tree_root);
		return 1;
	}

	int nn = btrfs_header_nritems(node);
	struct { int slot; struct btrfs_key key; u64 block; } to_remove[10];
	int nr_remove = 0;

	for (int j = 0; j < nn && nr_remove < 10; j++) {
		u64 blk = btrfs_node_blockptr(node, j);
		if (is_stale(blk)) {
			to_remove[nr_remove].slot = j;
			btrfs_node_key_to_cpu(node, &to_remove[nr_remove].key, j);
			to_remove[nr_remove].block = blk;
			nr_remove++;
		}
	}
	free_extent_buffer(node);

	/* Remove from last to first (to avoid slot shift issues) */
	for (int k = nr_remove - 1; k >= 0; k--) {
		/* Search for the key to position the path at the right node */
		path = btrfs_alloc_path();
		ret = btrfs_search_slot(trans, fs_root, &to_remove[k].key, path, 0, 1);

		/* The path should be positioned with path->nodes[1] = the target node */
		if (path->nodes[1]) {
			int slot_in_parent = path->slots[1];
			u64 blk_at_slot = btrfs_node_blockptr(path->nodes[1], slot_in_parent);

			/* Verify we're deleting the right thing */
			if (blk_at_slot == to_remove[k].block ||
			    is_stale(blk_at_slot)) {
				btrfs_del_ptr(trans, fs_root, path, 1, slot_in_parent);
				printf("  REMOVED slot block=%llu\n",
				       (unsigned long long)to_remove[k].block);
				removed++;
			} else {
				printf("  SKIP: slot points to %llu not %llu\n",
				       (unsigned long long)blk_at_slot,
				       (unsigned long long)to_remove[k].block);
			}
		} else {
			printf("  SKIP: path->nodes[1] is NULL for block=%llu\n",
			       (unsigned long long)to_remove[k].block);
		}
		btrfs_free_path(path);
	}

	printf("\nRemoved %d/%d. Committing...\n", removed, nr_remove);

	ret = btrfs_commit_transaction(trans, fs_root);
	if (ret) {
		fprintf(stderr, "ERROR: commit: %d\n", ret);
		close_ctree(fs_info->tree_root);
		return 1;
	}

	printf("=== SUCCESS: %d stale pointers removed ===\n", removed);
	close_ctree(fs_info->tree_root);
	return 0;
}
