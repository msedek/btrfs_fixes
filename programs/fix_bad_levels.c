/*
 * fix_bad_levels.c — Fix METADATA_ITEM/EXTENT_ITEM entries with wrong level
 *
 * For METADATA_ITEM: level is stored in key.offset
 * For EXTENT_ITEM with TREE_BLOCK: level is stored in btrfs_tree_block_info.level
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
	struct btrfs_root *extent_root;
	struct btrfs_trans_handle *trans = NULL;
	struct btrfs_path *path;
	struct btrfs_key key;
	int ret;
	int do_write = 0;
	int total = 0, bad = 0, fixed = 0, unreadable = 0;

	if (argc < 2) { fprintf(stderr, "Usage: %s <dev> [--write]\n", argv[0]); return 1; }
	if (argc >= 3 && strcmp(argv[2], "--write") == 0) do_write = 1;

	printf("=== fix_bad_levels %s ===\n", do_write ? "WRITE" : "SCAN");

	struct open_ctree_args oca = {
		.filename = argv[1],
		.flags = OPEN_CTREE_PARTIAL | OPEN_CTREE_ALLOW_TRANSID_MISMATCH |
			 OPEN_CTREE_SKIP_CSUM_CHECK | OPEN_CTREE_SKIP_LEAF_ITEM_CHECKS |
			 (do_write ? OPEN_CTREE_WRITES : 0),
	};

	fs_info = open_ctree_fs_info(&oca);
	if (!fs_info) { fprintf(stderr, "ERROR: cannot open\n"); return 1; }
	extent_root = btrfs_extent_root(fs_info, 0);

	if (do_write) {
		fs_info->rebuilding_extent_tree = 1;
		trans = btrfs_start_transaction(extent_root, 1);
		if (IS_ERR(trans)) { fprintf(stderr, "ERROR: transaction\n"); close_ctree(fs_info->tree_root); return 1; }
		trans->reinit_extent_tree = true;
	}

	path = btrfs_alloc_path();
	key.objectid = 0; key.type = 0; key.offset = 0;
	ret = btrfs_search_slot(NULL, extent_root, &key, path, 0, 0);

	while (ret >= 0) {
		struct extent_buffer *leaf = path->nodes[0];
		struct btrfs_key fk;
		int slot = path->slots[0];

		if (slot >= btrfs_header_nritems(leaf)) {
			ret = btrfs_next_leaf(extent_root, path);
			if (ret != 0) break;
			continue;
		}

		btrfs_item_key_to_cpu(leaf, &fk, slot);

		if (fk.type != BTRFS_METADATA_ITEM_KEY && fk.type != BTRFS_EXTENT_ITEM_KEY) {
			path->slots[0]++; continue;
		}

		/* For EXTENT_ITEM, only process tree blocks */
		u64 stored_level = 0;
		if (fk.type == BTRFS_EXTENT_ITEM_KEY) {
			struct btrfs_extent_item *ei = btrfs_item_ptr(leaf, slot, struct btrfs_extent_item);
			u64 fl = btrfs_extent_flags(leaf, ei);
			if (!(fl & BTRFS_EXTENT_FLAG_TREE_BLOCK)) { path->slots[0]++; continue; }
			u32 isz = btrfs_item_size(leaf, slot);
			if (isz < sizeof(struct btrfs_extent_item) + sizeof(struct btrfs_tree_block_info)) {
				path->slots[0]++; continue;
			}
			struct btrfs_tree_block_info *tbi = (struct btrfs_tree_block_info *)(ei + 1);
			stored_level = btrfs_tree_block_level(leaf, tbi);
		} else {
			stored_level = fk.offset;
		}

		total++;

		if (stored_level > 7) {
			u64 bytenr = fk.objectid;
			struct btrfs_tree_parent_check check = { .transid = 0, .level = -1, .has_first_key = false };
			struct extent_buffer *eb = read_tree_block(fs_info, bytenr, &check);
			u8 real_level = 0; u64 real_gen = 0; u64 real_owner = 0;

			if (!IS_ERR(eb) && extent_buffer_uptodate(eb)) {
				real_level = btrfs_header_level(eb);
				real_gen = btrfs_header_generation(eb);
				real_owner = btrfs_header_owner(eb);
				free_extent_buffer(eb);
			} else {
				unreadable++;
				if (!IS_ERR_OR_NULL(eb)) free_extent_buffer(eb);
				path->slots[0]++; continue;
			}

			bad++;

			if (do_write) {
				if (fk.type == BTRFS_EXTENT_ITEM_KEY) {
					/* Fix level in tree_block_info in-place */
					btrfs_release_path(path);
					ret = btrfs_search_slot(trans, extent_root, &fk, path, 0, 1);
					if (ret == 0) {
						leaf = path->nodes[0];
						struct btrfs_extent_item *ei2 = btrfs_item_ptr(leaf, path->slots[0], struct btrfs_extent_item);
						struct btrfs_tree_block_info *tbi2 = (struct btrfs_tree_block_info *)(ei2 + 1);
						btrfs_set_tree_block_level(leaf, tbi2, real_level);
						btrfs_set_extent_generation(leaf, ei2, real_gen);
						btrfs_mark_buffer_dirty(leaf);
						fixed++;
					}
					btrfs_release_path(path);
					fk.objectid = bytenr + fs_info->nodesize;
					fk.type = 0; fk.offset = 0;
					ret = btrfs_search_slot(NULL, extent_root, &fk, path, 0, 0);
					continue;
				} else {
					/* METADATA_ITEM: delete and reinsert with correct level */
					btrfs_release_path(path);
					ret = btrfs_search_slot(trans, extent_root, &fk, path, -1, 1);
					if (ret == 0) btrfs_del_item(trans, extent_root, path);
					btrfs_release_path(path);

					struct btrfs_key nk = { .objectid = bytenr, .type = BTRFS_METADATA_ITEM_KEY, .offset = real_level };
					u32 isz = sizeof(struct btrfs_extent_item) + sizeof(struct btrfs_extent_inline_ref);
					ret = btrfs_insert_empty_item(trans, extent_root, path, &nk, isz);
					if (ret == 0) {
						leaf = path->nodes[0];
						struct btrfs_extent_item *nei = btrfs_item_ptr(leaf, path->slots[0], struct btrfs_extent_item);
						btrfs_set_extent_refs(leaf, nei, 1);
						btrfs_set_extent_generation(leaf, nei, real_gen);
						btrfs_set_extent_flags(leaf, nei, BTRFS_EXTENT_FLAG_TREE_BLOCK);
						struct btrfs_extent_inline_ref *niref = (struct btrfs_extent_inline_ref *)(nei + 1);
						btrfs_set_extent_inline_ref_type(leaf, niref, BTRFS_TREE_BLOCK_REF_KEY);
						btrfs_set_extent_inline_ref_offset(leaf, niref, real_owner);
						btrfs_mark_buffer_dirty(leaf);
						fixed++;
					} else if (ret == -EEXIST) { fixed++; }
					btrfs_release_path(path);
					fk.objectid = bytenr + fs_info->nodesize;
					fk.type = 0; fk.offset = 0;
					ret = btrfs_search_slot(NULL, extent_root, &fk, path, 0, 0);
					continue;
				}
			}

			if (bad <= 20 || bad % 1000 == 0)
				printf("  BAD bytenr=%llu stored_level=%llu real=%u type=%s\n",
				       (unsigned long long)bytenr, (unsigned long long)stored_level,
				       real_level, fk.type == 169 ? "META" : "EXTENT");
		}

		if (total % 5000 == 0) printf("  progress: %d items, %d bad\n", total, bad);
		path->slots[0]++;
	}

	btrfs_free_path(path);
	printf("\n=== Result ===\nTotal items: %d\nBad levels: %d\nUnreadable: %d\n", total, bad, unreadable);

	if (do_write && trans) {
		printf("Fixed: %d\nCommitting...\n", fixed);
		ret = btrfs_commit_transaction(trans, extent_root);
		if (ret) { fprintf(stderr, "ERROR: commit: %d\n", ret); close_ctree(fs_info->tree_root); return 1; }
		printf("=== SUCCESS: %d fixed ===\n", fixed);
	} else if (bad > 0) {
		printf("Run with --write to fix %d\n", bad);
	} else {
		printf("=== ALL LEVELS OK ===\n");
	}

	close_ctree(fs_info->tree_root);
	return 0;
}
