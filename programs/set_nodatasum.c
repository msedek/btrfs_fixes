/*
 * set_nodatasum.c — Mark all inodes with the NODATASUM flag
 *
 * When the csum tree is empty but files still expect checksums, reads
 * fail with EIO. This program sets BTRFS_INODE_NODATASUM on every regular
 * file in the FS_TREE so the kernel skips checksum lookups on read.
 *
 * Build: make set_nodatasum
 * Run:
 *   sudo ./set_nodatasum /dev/sdX          # scan (read-only)
 *   sudo ./set_nodatasum /dev/sdX --write  # apply changes
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

#ifndef BTRFS_INODE_NODATASUM
#define BTRFS_INODE_NODATASUM (1U << 0)
#endif

int main(int argc, char *argv[])
{
	struct btrfs_fs_info *fs_info;
	struct btrfs_root *fs_root;
	struct btrfs_trans_handle *trans = NULL;
	struct btrfs_path *path;
	struct btrfs_key key, found_key;
	struct btrfs_inode_item *ii;
	struct extent_buffer *leaf;
	int ret, slot;
	int do_write = 0;
	int total = 0, modified = 0, already = 0, skipped = 0;
	u64 flags;

	if (argc < 2) {
		fprintf(stderr, "Usage: %s <device> [--write]\n", argv[0]);
		return 1;
	}
	if (argc >= 3 && strcmp(argv[2], "--write") == 0)
		do_write = 1;

	printf("=== set_nodatasum %s ===\n", do_write ? "WRITE" : "SCAN");

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

	/* Get the default FS_TREE (subvolid 5) */
	key.objectid = BTRFS_FS_TREE_OBJECTID;
	key.type = BTRFS_ROOT_ITEM_KEY;
	key.offset = (u64)-1;
	fs_root = btrfs_read_fs_root(fs_info, &key);
	if (IS_ERR(fs_root)) {
		fprintf(stderr, "ERROR: cannot read FS_TREE: %ld\n", PTR_ERR(fs_root));
		close_ctree(fs_info->tree_root);
		return 1;
	}

	printf("FS_TREE opened. Gen: %llu\n\n",
	       (unsigned long long)btrfs_super_generation(fs_info->super_copy));

	path = btrfs_alloc_path();

	if (do_write) {
		fs_info->rebuilding_extent_tree = 1;
		fs_info->rebuilding_extent_tree = 1;
	trans = btrfs_start_transaction(fs_root, 1);
		if (!IS_ERR(trans)) trans->reinit_extent_tree = true;
		if (IS_ERR(trans)) {
			fprintf(stderr, "ERROR: cannot start transaction\n");
			btrfs_free_path(path);
			close_ctree(fs_info->tree_root);
			return 1;
		}
	}

	/* Walk ALL INODE_ITEMs in the FS_TREE */
	key.objectid = 0;
	key.type = BTRFS_INODE_ITEM_KEY;
	key.offset = 0;

	ret = btrfs_search_slot(trans, fs_root, &key, path, 0, do_write ? 1 : 0);
	if (ret < 0) {
		fprintf(stderr, "ERROR: search failed: %d\n", ret);
		goto out;
	}

	while (1) {
		leaf = path->nodes[0];
		slot = path->slots[0];

		if (slot >= btrfs_header_nritems(leaf)) {
			ret = btrfs_next_leaf(fs_root, path);
			if (ret > 0)
				break;  /* end of tree */
			if (ret < 0) {
				fprintf(stderr, "ERROR: next_leaf: %d\n", ret);
				break;
			}
			continue;
		}

		btrfs_item_key_to_cpu(leaf, &found_key, slot);

		/* Only process INODE_ITEM entries */
		if (found_key.type != BTRFS_INODE_ITEM_KEY) {
			path->slots[0]++;
			continue;
		}

		ii = btrfs_item_ptr(leaf, slot, struct btrfs_inode_item);
		flags = btrfs_inode_flags(leaf, ii);
		total++;

		/* Check if it's a regular file (mode & S_IFREG) */
		u32 mode = btrfs_inode_mode(leaf, ii);
		if (!S_ISREG(mode)) {
			skipped++;  /* directory, symlink, etc */
			path->slots[0]++;
			continue;
		}

		if (flags & BTRFS_INODE_NODATASUM) {
			already++;
			path->slots[0]++;
			continue;
		}

		if (do_write) {
			btrfs_set_inode_flags(leaf, ii, flags | BTRFS_INODE_NODATASUM);
			btrfs_mark_buffer_dirty(leaf);
			modified++;
		} else {
			modified++;  /* count what WOULD be modified */
		}

		if (modified % 1000 == 0 && modified > 0)
			printf("  Progress: %d inodes processed...\n", modified);

		path->slots[0]++;
	}

out:
	printf("\n=== Result ===\n");
	printf("Total inodes: %d\n", total);
	printf("Regular files without NODATASUM: %d\n", modified);
	printf("Already had NODATASUM: %d\n", already);
	printf("Non-files (dirs, symlinks): %d\n", skipped);

	if (do_write && trans) {
		printf("\nCommitting %d changes...\n", modified);
		ret = btrfs_commit_transaction(trans, fs_root);
		if (ret) {
			fprintf(stderr, "ERROR: commit failed: %d\n", ret);
			btrfs_free_path(path);
			close_ctree(fs_info->tree_root);
			return 1;
		}
		printf("=== NODATASUM applied to %d files ===\n", modified);
	} else {
		printf("\n=== SCAN — %d files need NODATASUM ===\n", modified);
		if (modified > 0)
			printf("Run with --write to apply\n");
	}

	btrfs_free_path(path);
	close_ctree(fs_info->tree_root);
	return 0;
}
