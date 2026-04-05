/*
 * remove_stale_ptrs.c — Remove stale child pointers from FS_TREE
 *
 * Detected as stale if:
 *   1. btrfs_header_owner != FS_TREE
 *   2. The block's first key doesn't match the parent's expected key
 *   3. The block's first key belongs to another tree
 *      (BLOCK_GROUP_ITEM, CHUNK_ITEM, DEV_ITEM, METADATA_ITEM, EXTENT_ITEM)
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

int main(int argc, char *argv[])
{
	struct btrfs_fs_info *fs_info;
	struct btrfs_root *fs_root;
	struct btrfs_trans_handle *trans = NULL;
	struct btrfs_path *path;
	struct btrfs_key key;
	int ret, removed = 0, do_write = 0;

	if (argc < 2) { fprintf(stderr, "Usage: %s <dev> [--write]\n", argv[0]); return 1; }
	if (argc >= 3 && strcmp(argv[2], "--write") == 0) do_write = 1;

	printf("=== remove_stale_ptrs %s ===\n", do_write ? "WRITE" : "SCAN");

	struct open_ctree_args oca = {
		.filename = argv[1],
		.flags = OPEN_CTREE_PARTIAL | OPEN_CTREE_ALLOW_TRANSID_MISMATCH |
			 OPEN_CTREE_SKIP_CSUM_CHECK | OPEN_CTREE_SKIP_LEAF_ITEM_CHECKS |
			 (do_write ? OPEN_CTREE_WRITES : 0),
	};

	fs_info = open_ctree_fs_info(&oca);
	if (!fs_info) { fprintf(stderr, "ERROR: open\n"); return 1; }

	key.objectid = BTRFS_FS_TREE_OBJECTID;
	key.type = BTRFS_ROOT_ITEM_KEY;
	key.offset = (u64)-1;
	fs_root = btrfs_read_fs_root(fs_info, &key);
	if (IS_ERR(fs_root)) { fprintf(stderr, "ERROR: fs root\n"); close_ctree(fs_info->tree_root); return 1; }

	int root_level = btrfs_header_level(fs_root->node);
	printf("FS_TREE root level=%d\n", root_level);

	if (root_level < 2) {
		close_ctree(fs_info->tree_root);
		return 0;
	}

	if (do_write) {
		fs_info->rebuilding_extent_tree = 1;
		trans = btrfs_start_transaction(fs_root, 1);
		if (IS_ERR(trans)) { fprintf(stderr, "ERROR: trans\n"); close_ctree(fs_info->tree_root); return 1; }
		trans->reinit_extent_tree = true;
	}

	u64 stale_list[512];
	struct btrfs_key stale_keys[512];
	int num_stale = 0;

	struct extent_buffer *root_node = fs_root->node;
	int root_nritems = btrfs_header_nritems(root_node);

	printf("Scanning %d level-1 nodes...\n", root_nritems);

	for (int i = 0; i < root_nritems && num_stale < 512; i++) {
		u64 node_bytenr = btrfs_node_blockptr(root_node, i);
		struct btrfs_tree_parent_check check = {
			.owner_root = BTRFS_FS_TREE_OBJECTID,
			.transid = 0, .level = 1, .has_first_key = false,
		};
		struct extent_buffer *node = read_tree_block(fs_info, node_bytenr, &check);
		if (IS_ERR(node) || !extent_buffer_uptodate(node)) {
			if (!IS_ERR_OR_NULL(node)) free_extent_buffer(node);
			continue;
		}

		int nn = btrfs_header_nritems(node);
		for (int j = 0; j < nn && num_stale < 512; j++) {
			u64 blk = btrfs_node_blockptr(node, j);
			struct btrfs_key expected_key;
			btrfs_node_key_to_cpu(node, &expected_key, j);

			struct btrfs_tree_parent_check check2 = {
				.transid = 0, .level = 0, .has_first_key = false,
			};
			struct extent_buffer *child = read_tree_block(fs_info, blk, &check2);
			if (IS_ERR(child) || !extent_buffer_uptodate(child)) {
				if (!IS_ERR_OR_NULL(child)) free_extent_buffer(child);
				continue;
			}

			u64 child_owner = btrfs_header_owner(child);
			int child_nritems = btrfs_header_nritems(child);
			int is_stale = 0;
			const char *reason = "";

			/* Check 1: owner mismatch */
			if (child_owner != BTRFS_FS_TREE_OBJECTID) {
				is_stale = 1;
				reason = "owner";
			}

			/* Check 2: first key mismatch */
			if (!is_stale && child_nritems > 0) {
				struct btrfs_key first_key;
				btrfs_item_key_to_cpu(child, &first_key, 0);

				if (first_key.objectid != expected_key.objectid ||
				    first_key.type != expected_key.type ||
				    first_key.offset != expected_key.offset) {
					is_stale = 1;
					reason = "first_key";
				}

				/* Check 3: first key is type BLOCK_GROUP_ITEM (extent tree) or CHUNK_ITEM */
				if (!is_stale &&
				    (first_key.type == BTRFS_BLOCK_GROUP_ITEM_KEY ||
				     first_key.type == BTRFS_CHUNK_ITEM_KEY ||
				     first_key.type == BTRFS_DEV_ITEM_KEY ||
				     first_key.type == BTRFS_METADATA_ITEM_KEY ||
				     first_key.type == BTRFS_EXTENT_ITEM_KEY)) {
					is_stale = 1;
					reason = "wrong_tree_key_type";
				}
			}

			if (is_stale) {
				stale_list[num_stale] = blk;
				stale_keys[num_stale] = expected_key;
				printf("  STALE[%s]: block=%llu owner=%llu expected_key=(%llu,%u,%llu) parent=%llu\n",
				       reason,
				       (unsigned long long)blk,
				       (unsigned long long)child_owner,
				       (unsigned long long)expected_key.objectid,
				       expected_key.type,
				       (unsigned long long)expected_key.offset,
				       (unsigned long long)node_bytenr);
				num_stale++;
			}
			free_extent_buffer(child);
		}
		free_extent_buffer(node);
	}

	printf("\nTotal stale: %d\n", num_stale);

	if (!do_write || num_stale == 0) {
		if (num_stale > 0) printf("Run with --write to remove\n");
		close_ctree(fs_info->tree_root);
		return 0;
	}

	printf("\nRemoving...\n");

	for (int k = 0; k < num_stale; k++) {
		path = btrfs_alloc_path();
		ret = btrfs_search_slot(trans, fs_root, &stale_keys[k], path, 0, 1);

		if (path->nodes[1]) {
			struct extent_buffer *parent = path->nodes[1];
			int pn = btrfs_header_nritems(parent);
			int found = 0;

			for (int s = 0; s < pn; s++) {
				if (btrfs_node_blockptr(parent, s) == stale_list[k]) {
					btrfs_del_ptr(trans, fs_root, path, 1, s);
					printf("  REMOVED block=%llu slot=%d\n",
					       (unsigned long long)stale_list[k], s);
					removed++;
					found = 1;
					break;
				}
			}
			if (!found)
				printf("  SKIP block=%llu (not found after COW)\n",
				       (unsigned long long)stale_list[k]);
		}
		btrfs_free_path(path);
	}

	printf("\nRemoved %d/%d. Committing...\n", removed, num_stale);
	ret = btrfs_commit_transaction(trans, fs_root);
	if (ret) { fprintf(stderr, "ERROR: commit: %d\n", ret); close_ctree(fs_info->tree_root); return 1; }

	printf("=== SUCCESS: %d removed ===\n", removed);
	close_ctree(fs_info->tree_root);
	return 0;
}
