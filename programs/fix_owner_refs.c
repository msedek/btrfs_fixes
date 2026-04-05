/*
 * fix_owner_refs.c — Fix METADATA_ITEMs with wrong owner
 *
 * For each METADATA_ITEM in the extent tree:
 *   1. Read the actual block from disk
 *   2. Compare btrfs_header_owner() with the inline backref root
 *   3. If they don't match, update the inline backref with the correct owner
 *
 * Does NOT delete or create items — only updates existing data.
 * Uses cow=1 in search_slot to re-fetch the leaf after COW before writing.
 *
 * Build: make fix_owner_refs
 * Run:
 *   sudo ./fix_owner_refs /dev/sdX          # scan
 *   sudo ./fix_owner_refs /dev/sdX --write  # fix
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
	int ret, slot;
	int do_write = 0;
	int total = 0, mismatched = 0, fixed = 0, unreadable = 0;

	if (argc < 2) {
		fprintf(stderr, "Usage: %s <device> [--write]\n", argv[0]);
		return 1;
	}
	if (argc >= 3 && strcmp(argv[2], "--write") == 0)
		do_write = 1;

	printf("=== fix_owner_refs %s ===\n", do_write ? "WRITE" : "SCAN");

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
	if (!extent_root) {
		fprintf(stderr, "ERROR: no extent root\n");
		close_ctree(fs_info->tree_root);
		return 1;
	}

	printf("Gen: %llu\n\n",
	       (unsigned long long)btrfs_super_generation(fs_info->super_copy));

	if (do_write) {
		fs_info->rebuilding_extent_tree = 1;
		trans = btrfs_start_transaction(extent_root, 4096);
		if (IS_ERR(trans)) {
			fprintf(stderr, "ERROR: cannot start transaction\n");
			close_ctree(fs_info->tree_root);
			return 1;
		}
		trans->reinit_extent_tree = true;
	}

	path = btrfs_alloc_path();

	/* Walk ALL METADATA_ITEMs in the extent tree */
	key.objectid = 0;
	key.type = BTRFS_METADATA_ITEM_KEY;
	key.offset = 0;

	ret = btrfs_search_slot(trans, extent_root, &key, path, 0,
				do_write ? 1 : 0);

	while (ret >= 0) {
		struct extent_buffer *leaf = path->nodes[0];
		struct btrfs_key found_key;
		slot = path->slots[0];

		if (slot >= btrfs_header_nritems(leaf)) {
			ret = btrfs_next_leaf(extent_root, path);
			if (ret != 0)
				break;
			continue;
		}

		btrfs_item_key_to_cpu(leaf, &found_key, slot);

		/* Process both METADATA_ITEM and EXTENT_ITEM for tree blocks */
		if (found_key.type != BTRFS_METADATA_ITEM_KEY &&
		    found_key.type != BTRFS_EXTENT_ITEM_KEY) {
			path->slots[0]++;
			continue;
		}

		/* For EXTENT_ITEM, check if it's a tree block */
		if (found_key.type == BTRFS_EXTENT_ITEM_KEY) {
			struct btrfs_extent_item *ei;
			u64 flags;

			ei = btrfs_item_ptr(leaf, slot,
					    struct btrfs_extent_item);
			flags = btrfs_extent_flags(leaf, ei);
			if (!(flags & BTRFS_EXTENT_FLAG_TREE_BLOCK)) {
				path->slots[0]++;
				continue;  /* data extent, skip */
			}
		}

		total++;

		/* Read the actual block to get its real owner */
		u64 bytenr = found_key.objectid;
		struct btrfs_tree_parent_check check = {
			.transid = 0,
			.level = -1,
			.has_first_key = false,
		};

		struct extent_buffer *eb = read_tree_block(fs_info, bytenr,
							   &check);
		if (IS_ERR(eb) || !extent_buffer_uptodate(eb)) {
			unreadable++;
			if (!IS_ERR_OR_NULL(eb))
				free_extent_buffer(eb);
			path->slots[0]++;
			continue;
		}

		u64 real_owner = btrfs_header_owner(eb);
		u64 real_gen = btrfs_header_generation(eb);
		free_extent_buffer(eb);

		/* Find the inline backref and check its root */
		struct btrfs_extent_item *ei;
		u32 item_size;
		unsigned long ptr, end;

		ei = btrfs_item_ptr(leaf, slot, struct btrfs_extent_item);
		item_size = btrfs_item_size(leaf, slot);
		ptr = (unsigned long)(ei + 1);

		/* Skip tree_block_info if present (non-skinny) */
		if (found_key.type == BTRFS_EXTENT_ITEM_KEY) {
			ptr += sizeof(struct btrfs_tree_block_info);
		}

		end = (unsigned long)ei + item_size;

		/* Walk inline refs */
		while (ptr < end) {
			struct btrfs_extent_inline_ref *iref;
			u8 ref_type;
			u64 ref_offset;

			iref = (struct btrfs_extent_inline_ref *)ptr;
			ref_type = btrfs_extent_inline_ref_type(leaf, iref);
			ref_offset = btrfs_extent_inline_ref_offset(leaf, iref);

			if (ref_type == BTRFS_TREE_BLOCK_REF_KEY) {
				/* ref_offset = root objectid */
				if (ref_offset != real_owner) {
					mismatched++;
					if (do_write) {
						/* Re-search with COW to ensure this leaf is properly COW'd */
						btrfs_release_path(path);
						ret = btrfs_search_slot(trans, extent_root,
								&found_key, path, 0, 1);
						if (ret != 0) {
							fprintf(stderr, "  ERROR: re-search failed for %llu\n",
								(unsigned long long)bytenr);
							path->slots[0]++;
							goto next;
						}
						/* Re-get pointers after COW */
						leaf = path->nodes[0];
						slot = path->slots[0];
						ei = btrfs_item_ptr(leaf, slot,
							struct btrfs_extent_item);
						item_size = btrfs_item_size(leaf, slot);
						ptr = (unsigned long)(ei + 1);
						if (found_key.type == BTRFS_EXTENT_ITEM_KEY)
							ptr += sizeof(struct btrfs_tree_block_info);
						iref = (struct btrfs_extent_inline_ref *)ptr;

						btrfs_set_extent_inline_ref_offset(
							leaf, iref, real_owner);
						btrfs_set_extent_generation(
							leaf, ei, real_gen);
						btrfs_mark_buffer_dirty(leaf);
						fixed++;
					}

					if (mismatched <= 20 || mismatched % 1000 == 0) {
						printf("  %s bytenr=%llu: backref root %llu -> real owner %llu\n",
						       do_write ? "FIXED" : "MISMATCH",
						       (unsigned long long)bytenr,
						       (unsigned long long)ref_offset,
						       (unsigned long long)real_owner);
					}
				}
				break;  /* found the tree block ref */
			}

			/* Advance to next inline ref */
			if (ref_type == BTRFS_TREE_BLOCK_REF_KEY ||
			    ref_type == BTRFS_SHARED_BLOCK_REF_KEY)
				ptr += sizeof(struct btrfs_extent_inline_ref);
			else
				ptr += sizeof(struct btrfs_extent_inline_ref) +
				       sizeof(struct btrfs_extent_data_ref);
		}

		if (total % 5000 == 0)
			printf("  progress: %d items, %d mismatched\n",
			       total, mismatched);

next:
		path->slots[0]++;
	}

	btrfs_free_path(path);

	printf("\n=== Result ===\n");
	printf("Total METADATA/EXTENT items: %d\n", total);
	printf("Owner mismatches: %d\n", mismatched);
	printf("Unreadable blocks: %d\n", unreadable);

	if (do_write && trans) {
		printf("Fixed: %d\n", fixed);
		printf("\nCommitting...\n");
		ret = btrfs_commit_transaction(trans, extent_root);
		if (ret) {
			fprintf(stderr, "ERROR: commit failed: %d\n", ret);
			close_ctree(fs_info->tree_root);
			return 1;
		}
		printf("=== SUCCESS: %d owner refs fixed ===\n", fixed);
	} else {
		if (mismatched > 0)
			printf("\nRun with --write to fix %d mismatches\n",
			       mismatched);
		else
			printf("\n=== ALL OWNERS CORRECT ===\n");
	}

	close_ctree(fs_info->tree_root);
	return 0;
}
