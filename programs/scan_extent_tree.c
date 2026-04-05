/*
 * scan_extent_tree.c — Pass 2 of rebuild_extent_tree
 *
 * READ-ONLY. Walks the extent_tree (root 2) and emits one TSV line per
 * EXTENT_ITEM and METADATA_ITEM, plus one line per inline/separate backref.
 *
 * Output format (items):
 *   E\tbytenr\tnum_bytes\trefs\tflags\tgeneration
 * Inline refs under an item:
 *   IR\tbytenr\ttype\toffset\tdata_root\tdata_obj\tdata_off\tdata_count
 * Separate EXTENT_DATA_REF items:
 *   SR\tbytenr\tkey_offset_hash\tdata_root\tdata_obj\tdata_off\tdata_count
 * TREE_BLOCK_REF (for metadata items):
 *   TB\tbytenr\troot
 * SHARED_DATA_REF:
 *   SD\tbytenr\tparent_bytenr\tcount
 * SHARED_BLOCK_REF:
 *   SB\tbytenr\tparent_bytenr
 *
 * Strictly read-only.
 */

#include "kerncompat.h"
#include "kernel-shared/ctree.h"
#include "kernel-shared/disk-io.h"
#include "kernel-shared/accessors.h"
#include "kernel-shared/tree-checker.h"
#include "kernel-shared/uapi/btrfs_tree.h"
#include "common/messages.h"
#include "common/open-utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

static u64 stats_extent_items = 0;
static u64 stats_metadata_items = 0;
static u64 stats_inline_refs = 0;
static u64 stats_extent_data_refs = 0;
static u64 stats_tree_block_refs = 0;
static u64 stats_shared_data_refs = 0;
static u64 stats_shared_block_refs = 0;

int main(int argc, char *argv[])
{
	if (argc < 2) {
		fprintf(stderr, "Usage: %s <device>\n", argv[0]);
		return 1;
	}

	struct open_ctree_args oca = {
		.filename = argv[1],
		.flags = OPEN_CTREE_PARTIAL | OPEN_CTREE_ALLOW_TRANSID_MISMATCH |
			 OPEN_CTREE_SKIP_CSUM_CHECK | OPEN_CTREE_SKIP_LEAF_ITEM_CHECKS,
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

	struct btrfs_path *path = btrfs_alloc_path();
	struct btrfs_key key = { 0, 0, 0 };
	int ret = btrfs_search_slot(NULL, extent_root, &key, path, 0, 0);
	if (ret < 0) {
		fprintf(stderr, "ERROR: search_slot: %d\n", ret);
		btrfs_free_path(path);
		close_ctree(fs_info->tree_root);
		return 1;
	}

	while (1) {
		struct extent_buffer *leaf = path->nodes[0];
		if (path->slots[0] >= btrfs_header_nritems(leaf)) {
			ret = btrfs_next_leaf(extent_root, path);
			if (ret != 0) break;
			continue;
		}

		btrfs_item_key_to_cpu(leaf, &key, path->slots[0]);

		if (key.type == BTRFS_EXTENT_ITEM_KEY || key.type == BTRFS_METADATA_ITEM_KEY) {
			int item_type_is_metadata = (key.type == BTRFS_METADATA_ITEM_KEY);
			struct btrfs_extent_item *ei = btrfs_item_ptr(leaf, path->slots[0],
								       struct btrfs_extent_item);
			u64 refs = btrfs_extent_refs(leaf, ei);
			u64 gen = btrfs_extent_generation(leaf, ei);
			u64 flags = btrfs_extent_flags(leaf, ei);

			if (item_type_is_metadata) {
				printf("M\t%llu\t%d\t%llu\t%llu\t%llu\n",
				       (unsigned long long)key.objectid,
				       (int)key.offset,  /* level for METADATA_ITEM */
				       (unsigned long long)refs,
				       (unsigned long long)flags,
				       (unsigned long long)gen);
				stats_metadata_items++;
			} else {
				printf("E\t%llu\t%llu\t%llu\t%llu\t%llu\n",
				       (unsigned long long)key.objectid,
				       (unsigned long long)key.offset,  /* disk_num_bytes */
				       (unsigned long long)refs,
				       (unsigned long long)flags,
				       (unsigned long long)gen);
				stats_extent_items++;
			}

			/* Walk inline refs */
			unsigned long ptr, end;
			int item_size = btrfs_item_size(leaf, path->slots[0]);
			ptr = (unsigned long)(ei + 1);
			end = (unsigned long)ei + item_size;

			/* Skip tree_block_info if EXTENT_ITEM with tree_block flag and non-skinny */
			if (!item_type_is_metadata && (flags & BTRFS_EXTENT_FLAG_TREE_BLOCK))
				ptr += sizeof(struct btrfs_tree_block_info);

			while (ptr < end) {
				struct btrfs_extent_inline_ref *iref =
					(struct btrfs_extent_inline_ref *)ptr;
				int ref_type = btrfs_extent_inline_ref_type(leaf, iref);
				u64 ref_offset = btrfs_extent_inline_ref_offset(leaf, iref);
				stats_inline_refs++;

				if (ref_type == BTRFS_EXTENT_DATA_REF_KEY) {
					struct btrfs_extent_data_ref *dref =
						(struct btrfs_extent_data_ref *)(iref + 1);
					printf("IR\t%llu\t%d\t%llu\t%llu\t%llu\t%llu\t%u\n",
					       (unsigned long long)key.objectid,
					       ref_type,
					       (unsigned long long)ref_offset,
					       (unsigned long long)btrfs_extent_data_ref_root(leaf, dref),
					       (unsigned long long)btrfs_extent_data_ref_objectid(leaf, dref),
					       (unsigned long long)btrfs_extent_data_ref_offset(leaf, dref),
					       btrfs_extent_data_ref_count(leaf, dref));
					ptr += sizeof(*iref) + sizeof(*dref) - 8;
					stats_extent_data_refs++;
				} else if (ref_type == BTRFS_SHARED_DATA_REF_KEY) {
					struct btrfs_shared_data_ref *sref =
						(struct btrfs_shared_data_ref *)(iref + 1);
					printf("SD\t%llu\t%llu\t%u\n",
					       (unsigned long long)key.objectid,
					       (unsigned long long)ref_offset,
					       btrfs_shared_data_ref_count(leaf, sref));
					ptr += sizeof(*iref) + sizeof(*sref) - 8;
					stats_shared_data_refs++;
				} else if (ref_type == BTRFS_TREE_BLOCK_REF_KEY) {
					printf("TB\t%llu\t%llu\n",
					       (unsigned long long)key.objectid,
					       (unsigned long long)ref_offset);
					ptr += sizeof(*iref) - 8 + sizeof(__le64);
					stats_tree_block_refs++;
				} else if (ref_type == BTRFS_SHARED_BLOCK_REF_KEY) {
					printf("SB\t%llu\t%llu\n",
					       (unsigned long long)key.objectid,
					       (unsigned long long)ref_offset);
					ptr += sizeof(*iref) - 8 + sizeof(__le64);
					stats_shared_block_refs++;
				} else {
					fprintf(stderr, "WARN: unknown inline ref type=%d at bytenr=%llu\n",
						ref_type, (unsigned long long)key.objectid);
					break;
				}
			}
		} else if (key.type == BTRFS_EXTENT_DATA_REF_KEY) {
			struct btrfs_extent_data_ref *dref = btrfs_item_ptr(leaf,
				path->slots[0], struct btrfs_extent_data_ref);
			printf("SR\t%llu\t%llu\t%llu\t%llu\t%llu\t%u\n",
			       (unsigned long long)key.objectid,
			       (unsigned long long)key.offset,
			       (unsigned long long)btrfs_extent_data_ref_root(leaf, dref),
			       (unsigned long long)btrfs_extent_data_ref_objectid(leaf, dref),
			       (unsigned long long)btrfs_extent_data_ref_offset(leaf, dref),
			       btrfs_extent_data_ref_count(leaf, dref));
			stats_extent_data_refs++;
		} else if (key.type == BTRFS_TREE_BLOCK_REF_KEY) {
			printf("TB\t%llu\t%llu\n",
			       (unsigned long long)key.objectid,
			       (unsigned long long)key.offset);
			stats_tree_block_refs++;
		}

		path->slots[0]++;
	}

	btrfs_free_path(path);

	fprintf(stderr, "\n=== STATS ===\n");
	fprintf(stderr, "EXTENT_ITEM:      %llu\n", (unsigned long long)stats_extent_items);
	fprintf(stderr, "METADATA_ITEM:    %llu\n", (unsigned long long)stats_metadata_items);
	fprintf(stderr, "inline refs:      %llu\n", (unsigned long long)stats_inline_refs);
	fprintf(stderr, "EXTENT_DATA_REF:  %llu (total inline+separate)\n", (unsigned long long)stats_extent_data_refs);
	fprintf(stderr, "TREE_BLOCK_REF:   %llu\n", (unsigned long long)stats_tree_block_refs);
	fprintf(stderr, "SHARED_DATA_REF:  %llu\n", (unsigned long long)stats_shared_data_refs);
	fprintf(stderr, "SHARED_BLOCK_REF: %llu\n", (unsigned long long)stats_shared_block_refs);

	close_ctree(fs_info->tree_root);
	return 0;
}
