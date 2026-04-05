/*
 * fix_csum_tree.c — Create an empty CSUM tree and update its ROOT_ITEM
 *
 * The CSUM_TREE ROOT_ITEM points to a block that now belongs to the EXTENT_TREE.
 * This program creates a new empty leaf for the CSUM tree and updates the ROOT_ITEM.
 * With an empty CSUM tree, files marked NODATASUM will skip checksum verification.
 *
 * Build: make fix_csum_tree (add target to Makefile first)
 * Run: sudo ./fix_csum_tree /dev/sdX
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

int main(int argc, char *argv[])
{
	struct btrfs_fs_info *fs_info;
	struct btrfs_root *tree_root;
	struct btrfs_root *csum_root;
	struct btrfs_trans_handle *trans;
	struct btrfs_key key;
	struct btrfs_path *path;
	struct btrfs_root_item root_item;
	struct extent_buffer *leaf;
	int ret;

	if (argc < 2) {
		fprintf(stderr, "Usage: %s <device>\n", argv[0]);
		return 1;
	}

	printf("=== fix_csum_tree ===\n");
	printf("Target: %s\n\n", argv[1]);

	struct open_ctree_args oca = {
		.filename = argv[1],
		.sb_bytenr = 0,
		.root_tree_bytenr = 0,
		.chunk_tree_bytenr = 0,
		.flags = OPEN_CTREE_WRITES |
			 OPEN_CTREE_PARTIAL |
			 OPEN_CTREE_ALLOW_TRANSID_MISMATCH |
			 OPEN_CTREE_SKIP_CSUM_CHECK |
			 OPEN_CTREE_SKIP_LEAF_ITEM_CHECKS,
	};

	fs_info = open_ctree_fs_info(&oca);
	if (!fs_info) {
		fprintf(stderr, "ERROR: cannot open filesystem\n");
		return 1;
	}

	tree_root = fs_info->tree_root;
	printf("Filesystem opened. Gen: %llu\n",
	       (unsigned long long)btrfs_super_generation(fs_info->super_copy));

	trans = btrfs_start_transaction(tree_root, 1);
	if (IS_ERR(trans)) {
		fprintf(stderr, "ERROR: cannot start transaction\n");
		close_ctree(tree_root);
		return 1;
	}

	/* Allocate a new empty leaf for UUID tree */
	printf("Allocating new UUID tree leaf...\n");
	leaf = btrfs_alloc_tree_block(trans, tree_root, 0, BTRFS_CSUM_TREE_OBJECTID, NULL, 0, 0, 0, 0);
	if (IS_ERR(leaf)) {
		fprintf(stderr, "ERROR: cannot allocate tree block: %ld\n", PTR_ERR(leaf));
		btrfs_abort_transaction(trans, PTR_ERR(leaf));
		close_ctree(tree_root);
		return 1;
	}

	/* Set up the leaf as an empty UUID tree root */
	btrfs_set_header_bytenr(leaf, leaf->start);
	btrfs_set_header_generation(leaf, trans->transid);
	btrfs_set_header_backref_rev(leaf, BTRFS_MIXED_BACKREF_REV);
	btrfs_set_header_owner(leaf, BTRFS_CSUM_TREE_OBJECTID);
	btrfs_set_header_nritems(leaf, 0);
	btrfs_set_header_level(leaf, 0);
	btrfs_set_header_flags(leaf, BTRFS_HEADER_FLAG_WRITTEN);
	write_extent_buffer(leaf, fs_info->fs_devices->metadata_uuid,
			    btrfs_header_fsid(), BTRFS_FSID_SIZE);
	write_extent_buffer(leaf, fs_info->chunk_tree_uuid,
			    btrfs_header_chunk_tree_uuid(leaf), BTRFS_UUID_SIZE);
	btrfs_mark_buffer_dirty(leaf);

	printf("New UUID tree leaf at bytenr=%llu gen=%llu\n",
	       (unsigned long long)leaf->start,
	       (unsigned long long)trans->transid);

	/* Update CSUM_TREE ROOT_ITEM in the root tree */
	printf("Updating CSUM_TREE ROOT_ITEM...\n");

	key.objectid = BTRFS_CSUM_TREE_OBJECTID;
	key.type = BTRFS_ROOT_ITEM_KEY;
	key.offset = 0;

	path = btrfs_alloc_path();
	ret = btrfs_search_slot(trans, tree_root, &key, path, 0, 1);
	if (ret != 0) {
		fprintf(stderr, "ERROR: CSUM_TREE ROOT_ITEM not found (ret=%d)\n", ret);
		btrfs_free_path(path);
		btrfs_abort_transaction(trans, ret);
		close_ctree(tree_root);
		return 1;
	}

	/* Read current root_item, update bytenr/gen/level */
	read_extent_buffer(path->nodes[0], &root_item,
			   btrfs_item_ptr_offset(path->nodes[0], path->slots[0]),
			   sizeof(root_item));

	btrfs_set_root_bytenr(&root_item, leaf->start);
	btrfs_set_root_generation(&root_item, trans->transid);
	btrfs_set_root_generation_v2(&root_item, trans->transid);
	btrfs_set_root_level(&root_item, 0);
	btrfs_set_root_refs(&root_item, 1);
	btrfs_set_root_used(&root_item, fs_info->nodesize);

	write_extent_buffer(path->nodes[0], &root_item,
			    btrfs_item_ptr_offset(path->nodes[0], path->slots[0]),
			    sizeof(root_item));
	btrfs_mark_buffer_dirty(path->nodes[0]);
	btrfs_free_path(path);

	free_extent_buffer(leaf);

	/* Commit */
	printf("Committing...\n");
	ret = btrfs_commit_transaction(trans, tree_root);
	if (ret) {
		fprintf(stderr, "ERROR: commit failed: %d\n", ret);
		close_ctree(tree_root);
		return 1;
	}

	printf("=== CSUM_TREE FIXED ===\n");
	close_ctree(tree_root);
	return 0;
}
