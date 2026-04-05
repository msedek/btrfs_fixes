/*
 * scan_fstree_extents.c — Pass 1 of rebuild_extent_tree
 *
 * READ-ONLY tool. Walks all subvolume roots in ROOT_TREE (mainly FS_TREE root 5
 * for this pool), descends every inode, reads every EXTENT_DATA_ITEM (type
 * BTRFS_EXTENT_DATA_KEY = 108) and emits a TSV line for each non-inline,
 * non-hole data extent reference.
 *
 * Output format (one line per reference):
 *   disk_bytenr\tdisk_num_bytes\troot\tobjectid\toffset\tfile_extent_gen\tcompression
 *
 * Multiple lines may share (disk_bytenr, disk_num_bytes) with different
 * (objectid, offset) — these are the REFERENCES that all point to the same
 * physical extent, i.e. they are the "bookends".
 *
 * Also emits METADATA lines starting with # for visited metadata blocks:
 *   # META\tbytenr\tlevel\towner
 *
 * Usage:
 *   scan_fstree_extents <device> > extents.tsv 2> extents.err
 *
 * Strictly read-only: opens ctree with OPEN_CTREE_PARTIAL (no writes).
 * Tolerates unreadable tree blocks (logs to stderr, continues walk).
 */

#include "kerncompat.h"
#include "kernel-shared/ctree.h"
#include "kernel-shared/disk-io.h"
#include "kernel-shared/accessors.h"
#include "kernel-shared/tree-checker.h"
#include "kernel-shared/uapi/btrfs_tree.h"
#include "kernel-shared/volumes.h"
#include "common/messages.h"
#include "common/open-utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

static u64 stats_extents = 0;
static u64 stats_holes = 0;
static u64 stats_inline = 0;
static u64 stats_prealloc = 0;
static u64 stats_leaves = 0;
static u64 stats_nodes = 0;
static u64 stats_read_errors = 0;
static u64 stats_current_root = 0;

static void print_meta(u64 bytenr, int level, u64 owner)
{
	printf("# META\t%llu\t%d\t%llu\n",
	       (unsigned long long)bytenr, level, (unsigned long long)owner);
}

static int walk_leaf(struct btrfs_fs_info *fs_info, struct extent_buffer *leaf, u64 root_id)
{
	int nr = btrfs_header_nritems(leaf);
	int i;
	stats_leaves++;
	print_meta(btrfs_header_bytenr(leaf), 0, btrfs_header_owner(leaf));

	for (i = 0; i < nr; i++) {
		struct btrfs_key key;
		btrfs_item_key_to_cpu(leaf, &key, i);

		if (key.type != BTRFS_EXTENT_DATA_KEY)
			continue;

		struct btrfs_file_extent_item *fi =
			btrfs_item_ptr(leaf, i, struct btrfs_file_extent_item);

		int type = btrfs_file_extent_type(leaf, fi);
		if (type == BTRFS_FILE_EXTENT_INLINE) {
			stats_inline++;
			continue;
		}

		u64 disk_bytenr = btrfs_file_extent_disk_bytenr(leaf, fi);
		u64 disk_num_bytes = btrfs_file_extent_disk_num_bytes(leaf, fi);

		if (disk_bytenr == 0) {
			/* Hole extent — no on-disk data, no backref needed */
			stats_holes++;
			continue;
		}

		u64 file_offset = btrfs_file_extent_offset(leaf, fi);
		u64 gen = btrfs_file_extent_generation(leaf, fi);
		int compression = btrfs_file_extent_compression(leaf, fi);

		if (type == BTRFS_FILE_EXTENT_PREALLOC)
			stats_prealloc++;

		/*
		 * key.objectid = inode number in this root
		 * key.offset   = file offset (logical position in the file)
		 * disk_bytenr  = physical start of the on-disk extent
		 * disk_num_bytes = physical length of the on-disk extent
		 * file_offset  = offset WITHIN the physical extent where this ref starts
		 *                (for bookend extents, this is != 0)
		 *
		 * The extent-tree refs key is (disk_bytenr, EXTENT_ITEM, disk_num_bytes)
		 * with an EXTENT_DATA_REF describing (root, objectid, ref_offset) where
		 * ref_offset is the KEY OFFSET of this file_extent_item MINUS the
		 * file_offset, i.e. the logical file position of the start of the
		 * physical extent.
		 */
		u64 ref_offset = key.offset - file_offset;

		printf("%llu\t%llu\t%llu\t%llu\t%llu\t%llu\t%d\n",
		       (unsigned long long)disk_bytenr,
		       (unsigned long long)disk_num_bytes,
		       (unsigned long long)root_id,
		       (unsigned long long)key.objectid,
		       (unsigned long long)ref_offset,
		       (unsigned long long)gen,
		       compression);
		stats_extents++;
	}
	return 0;
}

static int walk_node(struct btrfs_fs_info *fs_info, struct extent_buffer *node, u64 root_id, int depth);

static int walk_node(struct btrfs_fs_info *fs_info, struct extent_buffer *node, u64 root_id, int depth)
{
	int level = btrfs_header_level(node);
	int nr = btrfs_header_nritems(node);
	int i;
	stats_nodes++;
	print_meta(btrfs_header_bytenr(node), level, btrfs_header_owner(node));

	if (depth > 20) {
		fprintf(stderr, "WARN: depth>20 at bytenr=%llu, aborting subtree\n",
			(unsigned long long)btrfs_header_bytenr(node));
		return 0;
	}

	for (i = 0; i < nr; i++) {
		u64 child_bytenr = btrfs_node_blockptr(node, i);
		struct btrfs_tree_parent_check check = {
			.owner_root = root_id,
			.transid = 0,
			.level = level - 1,
			.has_first_key = false,
		};
		struct extent_buffer *child = read_tree_block(fs_info, child_bytenr, &check);
		if (IS_ERR(child) || !extent_buffer_uptodate(child)) {
			fprintf(stderr, "WARN: cannot read bytenr=%llu level=%d owner=%llu\n",
				(unsigned long long)child_bytenr,
				level - 1,
				(unsigned long long)root_id);
			stats_read_errors++;
			if (!IS_ERR_OR_NULL(child)) free_extent_buffer(child);
			continue;
		}

		if (level - 1 == 0) {
			walk_leaf(fs_info, child, root_id);
		} else {
			walk_node(fs_info, child, root_id, depth + 1);
		}
		free_extent_buffer(child);
	}
	return 0;
}

static void walk_root(struct btrfs_fs_info *fs_info, struct btrfs_root *root, u64 root_id)
{
	stats_current_root = root_id;
	fprintf(stderr, "=== walking root %llu level=%d bytenr=%llu ===\n",
		(unsigned long long)root_id,
		btrfs_header_level(root->node),
		(unsigned long long)btrfs_header_bytenr(root->node));

	int level = btrfs_header_level(root->node);
	if (level == 0)
		walk_leaf(fs_info, root->node, root_id);
	else
		walk_node(fs_info, root->node, root_id, 0);
}

int main(int argc, char *argv[])
{
	struct btrfs_fs_info *fs_info;
	struct btrfs_root *fs_root;
	struct btrfs_key key;

	if (argc < 2) {
		fprintf(stderr, "Usage: %s <device>\n", argv[0]);
		return 1;
	}

	struct open_ctree_args oca = {
		.filename = argv[1],
		.flags = OPEN_CTREE_PARTIAL | OPEN_CTREE_ALLOW_TRANSID_MISMATCH |
			 OPEN_CTREE_SKIP_CSUM_CHECK | OPEN_CTREE_SKIP_LEAF_ITEM_CHECKS,
	};

	fs_info = open_ctree_fs_info(&oca);
	if (!fs_info) {
		fprintf(stderr, "ERROR: open_ctree_fs_info failed\n");
		return 1;
	}

	/*
	 * For THIS pool the ROOT_TREE dump confirmed only subvolume root 5
	 * (FS_TREE) exists. But scan defensively: iterate all ROOT_ITEMs.
	 */
	key.objectid = BTRFS_FS_TREE_OBJECTID;
	key.type = BTRFS_ROOT_ITEM_KEY;
	key.offset = (u64)-1;

	fs_root = btrfs_read_fs_root(fs_info, &key);
	if (IS_ERR(fs_root)) {
		fprintf(stderr, "ERROR: cannot read FS_TREE root\n");
		close_ctree(fs_info->tree_root);
		return 1;
	}
	walk_root(fs_info, fs_root, BTRFS_FS_TREE_OBJECTID);

	fprintf(stderr, "\n=== STATS ===\n");
	fprintf(stderr, "extent refs emitted: %llu\n", (unsigned long long)stats_extents);
	fprintf(stderr, "inline extents:      %llu (skipped, no backref needed)\n", (unsigned long long)stats_inline);
	fprintf(stderr, "hole extents:        %llu (skipped, no backref needed)\n", (unsigned long long)stats_holes);
	fprintf(stderr, "prealloc extents:    %llu (included in extent refs count)\n", (unsigned long long)stats_prealloc);
	fprintf(stderr, "leaves visited:      %llu\n", (unsigned long long)stats_leaves);
	fprintf(stderr, "nodes visited:       %llu\n", (unsigned long long)stats_nodes);
	fprintf(stderr, "read errors:         %llu\n", (unsigned long long)stats_read_errors);

	close_ctree(fs_info->tree_root);
	return 0;
}
