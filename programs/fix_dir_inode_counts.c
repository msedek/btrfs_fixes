/*
 * fix_dir_inode_counts.c — Recompute directory INODE_ITEM size + nlink
 *
 * Fixes the bug introduced by clean_orphan_dir_entries.c which decremented
 * parent INODE_ITEM.i_size by raw namelen instead of (namelen * 2), leaving
 * 3 directories with half-decremented size after a buggy orphan cleanup pass
 * and nlink clamped to 2 (invalid for a btrfs regular directory, which should
 * be 1).
 *
 * Formula (verified against a healthy top-level dir):
 *   size  = sum(name_len) * 2  for all DIR_INDEX entries of this inode
 *   nlink = 1                  for regular directories (btrfs convention)
 *
 * Target inodes are HARDCODED to avoid touching unintended dirs.
 * Dry-run shows current vs new values + num_entries counted.
 *
 * Usage:
 *   fix_dir_inode_counts <device> [--write]
 */

#include "kerncompat.h"
#include "kernel-shared/ctree.h"
#include "kernel-shared/disk-io.h"
#include "kernel-shared/transaction.h"
#include "kernel-shared/accessors.h"
#include "kernel-shared/uapi/btrfs_tree.h"
#include "common/messages.h"
#include "common/open-utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <errno.h>
#include <sys/stat.h>

/* Target inodes: the 3 dirs with "dir isize wrong, invalid nlink" per
 * btrfs check post-Fase2C-b (check_post_2Cb.log line 202200 errors). */
static const u64 TARGETS[] = {
	269775,  /* top-level media dir */
	269887,  /* subdir example */
	269969,  /* subdir example */
	305534,  /* subdir example */
};
#define NUM_TARGETS (sizeof(TARGETS)/sizeof(TARGETS[0]))

struct fix_info {
	u64 inode;
	u64 old_size;
	u32 old_nlink;
	u64 old_generation;
	u64 old_transid;
	u64 new_size;
	u32 new_nlink;
	u64 entries;
	u64 dir_entries;
	u64 sum_namelen;
	u64 dir_item_keys;
	int is_dir;
	int found;
};

/* Walk DIR_ITEM items (not names inside) to count unique item keys. Used as a
 * cross-check against DIR_INDEX entries to detect hash collisions. Returns
 * the number of DIR_ITEM items for the given parent. */
static int walk_dir_item_keys(struct btrfs_root *root, u64 parent_ino, u64 *out_items)
{
	struct btrfs_path *path = btrfs_alloc_path();
	if (!path) return -ENOMEM;

	struct btrfs_key key = { parent_ino, BTRFS_DIR_ITEM_KEY, 0 };
	int ret = btrfs_search_slot(NULL, root, &key, path, 0, 0);
	if (ret < 0) { btrfs_free_path(path); return ret; }

	u64 items = 0;
	while (1) {
		if (path->slots[0] >= btrfs_header_nritems(path->nodes[0])) {
			ret = btrfs_next_leaf(root, path);
			if (ret < 0) { btrfs_free_path(path); return ret; }
			if (ret > 0) break;
		}
		struct btrfs_key found;
		btrfs_item_key_to_cpu(path->nodes[0], &found, path->slots[0]);
		if (found.objectid != parent_ino || found.type != BTRFS_DIR_ITEM_KEY) break;
		items++;
		path->slots[0]++;
	}
	btrfs_free_path(path);
	*out_items = items;
	return 0;
}

/* Walk DIR_INDEX entries for a given parent inode, return sum of namelen and
 * counts. Uses a read-only path traversal. Must be called BEFORE the write
 * transaction (we do a separate read pass, then a write pass). */
static int walk_dir_index(struct btrfs_root *root, u64 parent_ino,
			  u64 *out_sum_namelen, u64 *out_entries,
			  u64 *out_dir_entries)
{
	struct btrfs_path *path = btrfs_alloc_path();
	if (!path) return -ENOMEM;

	struct btrfs_key key = { parent_ino, BTRFS_DIR_INDEX_KEY, 0 };
	int ret = btrfs_search_slot(NULL, root, &key, path, 0, 0);
	if (ret < 0) { btrfs_free_path(path); return ret; }

	u64 sum_namelen = 0;
	u64 entries = 0;
	u64 dirs = 0;

	while (1) {
		if (path->slots[0] >= btrfs_header_nritems(path->nodes[0])) {
			ret = btrfs_next_leaf(root, path);
			if (ret < 0) { btrfs_free_path(path); return ret; }
			if (ret > 0) break;
		}
		struct btrfs_key found;
		btrfs_item_key_to_cpu(path->nodes[0], &found, path->slots[0]);
		if (found.objectid != parent_ino || found.type != BTRFS_DIR_INDEX_KEY) {
			break;
		}
		struct btrfs_dir_item *di = btrfs_item_ptr(path->nodes[0],
			path->slots[0], struct btrfs_dir_item);
		u16 name_len = btrfs_dir_name_len(path->nodes[0], di);
		u8 filetype = btrfs_dir_ftype(path->nodes[0], di);
		sum_namelen += name_len;
		entries++;
		if (filetype == BTRFS_FT_DIR) dirs++;
		path->slots[0]++;
	}
	btrfs_free_path(path);

	*out_sum_namelen = sum_namelen;
	*out_entries = entries;
	*out_dir_entries = dirs;
	return 0;
}

/* Read current INODE_ITEM values (read-only) */
static int read_inode_item(struct btrfs_root *root, u64 inode,
			    u64 *out_size, u32 *out_nlink, u32 *out_mode,
			    u64 *out_gen, u64 *out_transid,
			    int *out_found)
{
	struct btrfs_path *path = btrfs_alloc_path();
	if (!path) return -ENOMEM;

	struct btrfs_key key = { inode, BTRFS_INODE_ITEM_KEY, 0 };
	int ret = btrfs_search_slot(NULL, root, &key, path, 0, 0);
	if (ret < 0) { btrfs_free_path(path); return ret; }
	if (ret > 0) {
		btrfs_free_path(path);
		*out_found = 0;
		return 0;
	}

	struct btrfs_inode_item *ii = btrfs_item_ptr(path->nodes[0],
		path->slots[0], struct btrfs_inode_item);
	*out_size = btrfs_inode_size(path->nodes[0], ii);
	*out_nlink = btrfs_inode_nlink(path->nodes[0], ii);
	*out_mode = btrfs_inode_mode(path->nodes[0], ii);
	*out_gen = btrfs_inode_generation(path->nodes[0], ii);
	*out_transid = btrfs_inode_transid(path->nodes[0], ii);
	*out_found = 1;
	btrfs_free_path(path);
	return 0;
}

/* Write new INODE_ITEM values inside a transaction */
static int apply_fix(struct btrfs_trans_handle *trans, struct btrfs_root *root,
		      u64 inode, u64 new_size, u32 new_nlink)
{
	struct btrfs_path *path = btrfs_alloc_path();
	if (!path) return -ENOMEM;

	struct btrfs_key key = { inode, BTRFS_INODE_ITEM_KEY, 0 };
	int ret = btrfs_search_slot(trans, root, &key, path, 0, 1);
	if (ret != 0) {
		btrfs_free_path(path);
		return (ret > 0) ? -ENOENT : ret;
	}

	struct btrfs_inode_item *ii = btrfs_item_ptr(path->nodes[0],
		path->slots[0], struct btrfs_inode_item);
	btrfs_set_inode_size(path->nodes[0], ii, new_size);
	btrfs_set_inode_nlink(path->nodes[0], ii, new_nlink);
	btrfs_mark_buffer_dirty(path->nodes[0]);
	btrfs_free_path(path);
	return 0;
}

int main(int argc, char *argv[])
{
	if (argc < 2 || argc > 3) {
		fprintf(stderr, "Usage: %s <device> [--write]\n", argv[0]);
		return 1;
	}
	int do_write = (argc == 3 && strcmp(argv[2], "--write") == 0);

	printf("=== fix_dir_inode_counts %s ===\n", do_write ? "WRITE" : "DRY-RUN");
	printf("Target inodes: ");
	size_t i;
	for (i = 0; i < NUM_TARGETS; i++) printf("%llu ", (unsigned long long)TARGETS[i]);
	printf("\n");

	struct open_ctree_args oca = {
		.filename = argv[1],
		.flags = OPEN_CTREE_PARTIAL | OPEN_CTREE_ALLOW_TRANSID_MISMATCH |
			 OPEN_CTREE_SKIP_CSUM_CHECK | OPEN_CTREE_SKIP_LEAF_ITEM_CHECKS |
			 (do_write ? OPEN_CTREE_WRITES : 0),
	};
	struct btrfs_fs_info *fs_info = open_ctree_fs_info(&oca);
	if (!fs_info) { fprintf(stderr, "ERROR: open_ctree\n"); return 1; }
	struct btrfs_root *root = fs_info->fs_root;

	/* Phase 1: collect current + new values for all targets (READ ONLY) */
	struct fix_info infos[NUM_TARGETS];
	memset(infos, 0, sizeof(infos));

	for (i = 0; i < NUM_TARGETS; i++) {
		struct fix_info *f = &infos[i];
		f->inode = TARGETS[i];
		u32 mode = 0;
		int ret = read_inode_item(root, f->inode, &f->old_size,
					   &f->old_nlink, &mode,
					   &f->old_generation, &f->old_transid,
					   &f->found);
		if (ret < 0) {
			fprintf(stderr, "ERROR: read inode %llu: %d\n",
				(unsigned long long)f->inode, ret);
			close_ctree(fs_info->tree_root);
			return 1;
		}
		if (!f->found) {
			fprintf(stderr, "SKIP: inode %llu has no INODE_ITEM\n",
				(unsigned long long)f->inode);
			continue;
		}
		f->is_dir = S_ISDIR(mode);
		if (!f->is_dir) {
			fprintf(stderr, "SKIP: inode %llu is not a directory (mode=0%o)\n",
				(unsigned long long)f->inode, mode);
			continue;
		}

		/* Guard: refuse to touch subvolume roots */
		if (f->inode == BTRFS_FIRST_FREE_OBJECTID) {
			fprintf(stderr, "SKIP: inode %llu is subvolume root — refusing\n",
				(unsigned long long)f->inode);
			f->is_dir = 0;
			continue;
		}

		u64 sum_namelen = 0;
		ret = walk_dir_index(root, f->inode, &sum_namelen,
				      &f->entries, &f->dir_entries);
		if (ret < 0) {
			fprintf(stderr, "ERROR: walk DIR_INDEX inode %llu: %d\n",
				(unsigned long long)f->inode, ret);
			close_ctree(fs_info->tree_root);
			return 1;
		}

		/* Cross-check against DIR_ITEM count (hash collision detection) */
		u64 dir_item_count = 0;
		ret = walk_dir_item_keys(root, f->inode, &dir_item_count);
		if (ret < 0) {
			fprintf(stderr, "ERROR: walk DIR_ITEM inode %llu: %d\n",
				(unsigned long long)f->inode, ret);
			close_ctree(fs_info->tree_root);
			return 1;
		}

		f->sum_namelen = sum_namelen;
		f->dir_item_keys = dir_item_count;
		f->new_size = sum_namelen * 2;
		f->new_nlink = 1;  /* btrfs convention for regular dirs */

		/* Hash collision sanity: dir_item keys ≤ DIR_INDEX entries. If they
		 * diverge by more than 1%, abort (unlikely but defensive). */
		if (f->entries > 0 && dir_item_count < f->entries) {
			u64 gap = f->entries - dir_item_count;
			u64 threshold = (f->entries / 100) + 1; /* 1% */
			if (gap > threshold) {
				fprintf(stderr, "WARN: inode %llu has DIR_ITEM keys=%llu vs DIR_INDEX=%llu (gap=%llu > 1%%)\n",
					(unsigned long long)f->inode,
					(unsigned long long)dir_item_count,
					(unsigned long long)f->entries,
					(unsigned long long)gap);
				fprintf(stderr, "  Hash collisions detected. Aborting to be safe.\n");
				close_ctree(fs_info->tree_root);
				return 4;
			}
		}
	}

	/* Phase 2: print plan */
	printf("\n=== FIX PLAN ===\n");
	for (i = 0; i < NUM_TARGETS; i++) {
		struct fix_info *f = &infos[i];
		if (!f->found || !f->is_dir) continue;
		printf("inode %llu (gen %llu, transid %llu):\n",
			(unsigned long long)f->inode,
			(unsigned long long)f->old_generation,
			(unsigned long long)f->old_transid);
		printf("  DIR_INDEX entries=%llu (dirs=%llu) sum_namelen=%llu\n",
			(unsigned long long)f->entries,
			(unsigned long long)f->dir_entries,
			(unsigned long long)f->sum_namelen);
		printf("  DIR_ITEM keys=%llu (cross-check: should equal entries)\n",
			(unsigned long long)f->dir_item_keys);
		printf("  size:  %llu -> %llu (delta %lld)\n",
			(unsigned long long)f->old_size,
			(unsigned long long)f->new_size,
			(long long)f->new_size - (long long)f->old_size);
		printf("  nlink: %u -> %u (delta %d)\n",
			f->old_nlink, f->new_nlink,
			(int)f->new_nlink - (int)f->old_nlink);
	}

	if (!do_write) {
		printf("\nDRY-RUN complete. No mutation.\n");
		close_ctree(fs_info->tree_root);
		return 0;
	}

	/* Phase 3: apply in one transaction */
	printf("\n=== APPLYING ===\n");
	struct btrfs_trans_handle *trans = btrfs_start_transaction(root, NUM_TARGETS * 8);
	if (IS_ERR(trans)) {
		fprintf(stderr, "ERROR: start_transaction: %ld\n", PTR_ERR(trans));
		close_ctree(fs_info->tree_root);
		return 1;
	}

	int applied = 0;
	for (i = 0; i < NUM_TARGETS; i++) {
		struct fix_info *f = &infos[i];
		if (!f->found || !f->is_dir) continue;
		int ret = apply_fix(trans, root, f->inode, f->new_size, f->new_nlink);
		if (ret < 0) {
			fprintf(stderr, "ERROR: apply inode %llu: %d\n",
				(unsigned long long)f->inode, ret);
			btrfs_abort_transaction(trans, ret);
			close_ctree(fs_info->tree_root);
			return 1;
		}
		printf("  applied inode %llu: size=%llu nlink=%u\n",
			(unsigned long long)f->inode,
			(unsigned long long)f->new_size,
			f->new_nlink);
		applied++;
	}

	int ret = btrfs_commit_transaction(trans, root);
	if (ret) {
		fprintf(stderr, "ERROR: commit: %d\n", ret);
		close_ctree(fs_info->tree_root);
		return 1;
	}

	printf("\n=== POST-WRITE VERIFY ===\n");
	int verify_fail = 0;
	for (i = 0; i < NUM_TARGETS; i++) {
		struct fix_info *f = &infos[i];
		if (!f->found || !f->is_dir) continue;
		u64 new_size = 0, new_gen = 0, new_transid = 0;
		u32 new_nlink = 0, new_mode = 0;
		int found = 0;
		int ret = read_inode_item(root, f->inode, &new_size, &new_nlink,
					   &new_mode, &new_gen, &new_transid, &found);
		if (ret < 0 || !found) {
			fprintf(stderr, "VERIFY FAIL: inode %llu re-read error\n",
				(unsigned long long)f->inode);
			verify_fail++;
			continue;
		}
		printf("inode %llu: size=%llu nlink=%u gen=%llu transid=%llu\n",
			(unsigned long long)f->inode,
			(unsigned long long)new_size, new_nlink,
			(unsigned long long)new_gen,
			(unsigned long long)new_transid);
		if (new_size != f->new_size || new_nlink != f->new_nlink) {
			fprintf(stderr, "VERIFY FAIL: inode %llu expected size=%llu nlink=%u got size=%llu nlink=%u\n",
				(unsigned long long)f->inode,
				(unsigned long long)f->new_size, f->new_nlink,
				(unsigned long long)new_size, new_nlink);
			verify_fail++;
		}
	}
	if (verify_fail) {
		fprintf(stderr, "\n=== VERIFY FAILED: %d discrepancies ===\n", verify_fail);
		close_ctree(fs_info->tree_root);
		return 5;
	}

	printf("\n=== SUCCESS: applied %d fixes, verify clean ===\n", applied);
	close_ctree(fs_info->tree_root);
	return 0;
}
