/*
 * fix_duplicate_extents.c — Find and remove duplicate METADATA_ITEMs
 *
 * When two METADATA_ITEMs exist for the same bytenr with different levels,
 * read the actual block to determine the correct level and delete the wrong one.
 *
 * Build: make fix_duplicate_extents
 * Run:
 *   sudo ./fix_duplicate_extents /dev/sdX          # scan
 *   sudo ./fix_duplicate_extents /dev/sdX --write  # fix
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

#define MAX_DUPS 4096

struct dup_entry {
	u64 bytenr;
	u64 wrong_level;  /* the level to DELETE */
	u64 right_level;  /* the level to KEEP */
};

static struct dup_entry dups[MAX_DUPS];
static int num_dups = 0;

int main(int argc, char *argv[])
{
	struct btrfs_fs_info *fs_info;
	struct btrfs_root *extent_root;
	struct btrfs_trans_handle *trans;
	struct btrfs_path *path;
	struct btrfs_key key, prev_key;
	int ret, slot;
	int do_write = 0;
	int deleted = 0;
	int has_prev = 0;

	if (argc < 2) {
		fprintf(stderr, "Usage: %s <device> [--write]\n", argv[0]);
		return 1;
	}
	if (argc >= 3 && strcmp(argv[2], "--write") == 0)
		do_write = 1;

	printf("=== fix_duplicate_extents %s ===\n",
	       do_write ? "WRITE" : "SCAN");

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

	extent_root = btrfs_extent_root(fs_info, 0);
	path = btrfs_alloc_path();

	/* Scan extent tree for duplicate METADATA_ITEMs */
	printf("Scanning extent tree for duplicates...\n");

	key.objectid = 0;
	key.type = BTRFS_METADATA_ITEM_KEY;
	key.offset = 0;

	ret = btrfs_search_slot(NULL, extent_root, &key, path, 0, 0);

	memset(&prev_key, 0, sizeof(prev_key));

	while (ret >= 0) {
		struct extent_buffer *leaf = path->nodes[0];
		slot = path->slots[0];

		if (slot >= btrfs_header_nritems(leaf)) {
			ret = btrfs_next_leaf(extent_root, path);
			if (ret != 0) break;
			continue;
		}

		btrfs_item_key_to_cpu(leaf, &key, slot);

		if (key.type == BTRFS_METADATA_ITEM_KEY) {
			/* Check if same bytenr as previous METADATA_ITEM */
			if (has_prev && prev_key.type == BTRFS_METADATA_ITEM_KEY &&
			    prev_key.objectid == key.objectid &&
			    prev_key.offset != key.offset) {
				/* DUPLICATE: same bytenr, different levels */
				/* Read the actual block to find correct level */
				struct btrfs_tree_parent_check check = {
					.transid = 0,
					.level = -1,
					.has_first_key = false,
				};
				struct extent_buffer *eb;
				u8 real_level = 255;

				eb = read_tree_block(fs_info, key.objectid, &check);
				if (!IS_ERR(eb) && extent_buffer_uptodate(eb)) {
					real_level = btrfs_header_level(eb);
					free_extent_buffer(eb);
				} else {
					if (!IS_ERR_OR_NULL(eb))
						free_extent_buffer(eb);
				}

				u64 wrong = (prev_key.offset == real_level) ? key.offset : prev_key.offset;
				u64 right = (prev_key.offset == real_level) ? prev_key.offset : key.offset;

				if (num_dups < MAX_DUPS) {
					dups[num_dups].bytenr = key.objectid;
					dups[num_dups].wrong_level = wrong;
					dups[num_dups].right_level = right;
					num_dups++;
				}

				printf("  DUP: bytenr=%llu levels=%llu,%llu real=%u -> delete level=%llu\n",
				       (unsigned long long)key.objectid,
				       (unsigned long long)prev_key.offset,
				       (unsigned long long)key.offset,
				       real_level,
				       (unsigned long long)wrong);
			}
			prev_key = key;
			has_prev = 1;
		} else {
			has_prev = 0;
		}

		path->slots[0]++;
	}

	btrfs_free_path(path);

	printf("\nFound %d duplicates\n", num_dups);

	if (!do_write || num_dups == 0) {
		if (num_dups > 0)
			printf("Run with --write to delete wrong entries\n");
		close_ctree(fs_info->tree_root);
		return 0;
	}

	/* Delete wrong entries */
	printf("\nDeleting %d wrong entries...\n", num_dups);

	fs_info->rebuilding_extent_tree = 1;
	trans = btrfs_start_transaction(extent_root, num_dups);
	if (IS_ERR(trans)) {
		fprintf(stderr, "ERROR: cannot start transaction\n");
		close_ctree(fs_info->tree_root);
		return 1;
	}
	trans->reinit_extent_tree = true;

	for (int i = 0; i < num_dups; i++) {
		path = btrfs_alloc_path();
		key.objectid = dups[i].bytenr;
		key.type = BTRFS_METADATA_ITEM_KEY;
		key.offset = dups[i].wrong_level;

		ret = btrfs_search_slot(trans, extent_root, &key, path, -1, 1);
		if (ret == 0) {
			ret = btrfs_del_item(trans, extent_root, path);
			if (ret == 0) {
				deleted++;
				printf("  [%d] DELETED bytenr=%llu wrong_level=%llu\n",
				       i, (unsigned long long)dups[i].bytenr,
				       (unsigned long long)dups[i].wrong_level);
			} else {
				fprintf(stderr, "  [%d] ERROR deleting: %d\n", i, ret);
			}
		} else {
			printf("  [%d] NOT FOUND (already gone?)\n", i);
		}
		btrfs_free_path(path);
	}

	printf("\nDeleted %d. Committing...\n", deleted);
	ret = btrfs_commit_transaction(trans, extent_root);
	if (ret) {
		fprintf(stderr, "ERROR: commit failed: %d\n", ret);
		close_ctree(fs_info->tree_root);
		return 1;
	}

	printf("=== SUCCESS: %d duplicates removed ===\n", deleted);
	close_ctree(fs_info->tree_root);
	return 0;
}
