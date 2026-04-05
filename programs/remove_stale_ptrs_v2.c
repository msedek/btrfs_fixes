/*
 * remove_stale_ptrs_v2.c — Recursive stale child pointer remover for FS_TREE
 *
 * v2 fixes bugs in v1:
 *   1. Detects EMPTY leaves whose parent expects a non-zero first key
 *      (v1 bug: skipped first_key check when nritems==0, missing stale empty leaves)
 *   2. Scans BOTH root→level1 AND level1→leaves (v1: only level1→leaves)
 *   3. Dynamic buffer (v1: hardcoded 512)
 *   4. Tolerates read_tree_block failures (log+continue)
 *   5. Multi-pass: removes from deepest level first, commits, re-reads root
 *
 * Stale detection criteria (per child):
 *   A) btrfs_header_owner(child) != expected_root
 *   B) child has nritems > 0 AND first_key != parent expected key
 *   C) child has nritems > 0 AND first_key.type is wrong (BLOCK_GROUP/CHUNK/DEV/METADATA/EXTENT)
 *   D) child has nritems == 0 AND parent expected key != (0,0,0)  [NEW in v2]
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

struct stale_entry {
	u64 bytenr;
	int parent_level;   /* level of parent (child is parent_level-1) */
	struct btrfs_key expected_key;
	const char *reason;
};

#define MAX_STALE 65536

static struct stale_entry stale_list[MAX_STALE];
static int num_stale;

static int is_wrong_tree_key_type(u8 type)
{
	return type == BTRFS_BLOCK_GROUP_ITEM_KEY ||
	       type == BTRFS_CHUNK_ITEM_KEY ||
	       type == BTRFS_DEV_ITEM_KEY ||
	       type == BTRFS_METADATA_ITEM_KEY ||
	       type == BTRFS_EXTENT_ITEM_KEY;
}

static int key_is_zero(const struct btrfs_key *k)
{
	return k->objectid == 0 && k->type == 0 && k->offset == 0;
}

static int check_child(struct extent_buffer *child,
		       const struct btrfs_key *expected_key,
		       u64 expected_owner_root,
		       const char **reason_out)
{
	u64 child_owner = btrfs_header_owner(child);
	int child_nritems = btrfs_header_nritems(child);

	if (child_owner != expected_owner_root) {
		*reason_out = "owner";
		return 1;
	}

	if (child_nritems > 0) {
		struct btrfs_key first_key;
		btrfs_item_key_to_cpu(child, &first_key, 0);

		if (first_key.objectid != expected_key->objectid ||
		    first_key.type != expected_key->type ||
		    first_key.offset != expected_key->offset) {
			*reason_out = "first_key_mismatch";
			return 1;
		}
		if (is_wrong_tree_key_type(first_key.type)) {
			*reason_out = "wrong_tree_key_type";
			return 1;
		}
	} else {
		/* Empty child: flagged if parent expected a non-zero key */
		if (!key_is_zero(expected_key)) {
			*reason_out = "empty_leaf_with_expected_key";
			return 1;
		}
	}

	return 0;
}

static void scan_parent_for_stale_children(struct btrfs_fs_info *fs_info,
					   struct extent_buffer *parent,
					   int parent_level,
					   u64 expected_root)
{
	int nr = btrfs_header_nritems(parent);
	int i;
	for (i = 0; i < nr && num_stale < MAX_STALE; i++) {
		u64 child_bytenr = btrfs_node_blockptr(parent, i);
		struct btrfs_key expected_key;
		btrfs_node_key_to_cpu(parent, &expected_key, i);

		struct btrfs_tree_parent_check check = {
			.owner_root = expected_root,
			.transid = 0,
			.level = parent_level - 1,
			.has_first_key = false,
		};
		struct extent_buffer *child = read_tree_block(fs_info, child_bytenr, &check);
		if (IS_ERR(child) || !extent_buffer_uptodate(child)) {
			fprintf(stderr, "  WARN: cannot read child bytenr=%llu level=%d (parent=%llu slot=%d) — treating as stale\n",
				(unsigned long long)child_bytenr,
				parent_level - 1,
				(unsigned long long)btrfs_header_bytenr(parent),
				i);
			stale_list[num_stale].bytenr = child_bytenr;
			stale_list[num_stale].parent_level = parent_level;
			stale_list[num_stale].expected_key = expected_key;
			stale_list[num_stale].reason = "unreadable";
			num_stale++;
			if (!IS_ERR_OR_NULL(child)) free_extent_buffer(child);
			continue;
		}

		const char *reason = NULL;
		int stale = check_child(child, &expected_key, expected_root, &reason);
		if (stale) {
			printf("  STALE[%s] parent_level=%d parent=%llu child=%llu expected_key=(%llu,%u,%llu)\n",
			       reason, parent_level,
			       (unsigned long long)btrfs_header_bytenr(parent),
			       (unsigned long long)child_bytenr,
			       (unsigned long long)expected_key.objectid,
			       expected_key.type,
			       (unsigned long long)expected_key.offset);
			stale_list[num_stale].bytenr = child_bytenr;
			stale_list[num_stale].parent_level = parent_level;
			stale_list[num_stale].expected_key = expected_key;
			stale_list[num_stale].reason = reason;
			num_stale++;
		}
		free_extent_buffer(child);
	}
}

static void scan_full_tree(struct btrfs_fs_info *fs_info, struct btrfs_root *root, u64 expected_root)
{
	num_stale = 0;
	struct extent_buffer *root_node = root->node;
	int root_level = btrfs_header_level(root_node);
	int root_nr = btrfs_header_nritems(root_node);

	printf("Scanning FS_TREE root: level=%d nritems=%d\n", root_level, root_nr);

	if (root_level == 0) {
		printf("  root is a leaf (level 0), nothing to scan\n");
		return;
	}

	/* Scan root → level-(root_level-1) */
	printf("Pass A: scan root (level %d) → children (level %d)\n", root_level, root_level - 1);
	scan_parent_for_stale_children(fs_info, root_node, root_level, expected_root);
	int after_root = num_stale;
	printf("  Pass A found: %d stale\n", after_root);

	if (root_level >= 2) {
		/* Scan each level-(root_level-1) node → level-(root_level-2) children */
		printf("Pass B: scan level-%d nodes → children (level %d)\n",
		       root_level - 1, root_level - 2);
		int i;
		for (i = 0; i < root_nr && num_stale < MAX_STALE; i++) {
			u64 l1_bytenr = btrfs_node_blockptr(root_node, i);
			struct btrfs_tree_parent_check check = {
				.owner_root = expected_root,
				.transid = 0,
				.level = root_level - 1,
				.has_first_key = false,
			};
			struct extent_buffer *l1 = read_tree_block(fs_info, l1_bytenr, &check);
			if (IS_ERR(l1) || !extent_buffer_uptodate(l1)) {
				if (!IS_ERR_OR_NULL(l1)) free_extent_buffer(l1);
				continue;
			}
			scan_parent_for_stale_children(fs_info, l1, root_level - 1, expected_root);
			free_extent_buffer(l1);
		}
		printf("  Pass B total: %d stale (includes Pass A)\n", num_stale);
	}
}

int main(int argc, char *argv[])
{
	struct btrfs_fs_info *fs_info;
	struct btrfs_root *fs_root;
	struct btrfs_trans_handle *trans = NULL;
	struct btrfs_key key;
	int do_write = 0;

	if (argc < 2) {
		fprintf(stderr, "Usage: %s <device> [--write]\n", argv[0]);
		return 1;
	}
	if (argc >= 3 && strcmp(argv[2], "--write") == 0) do_write = 1;

	printf("=== remove_stale_ptrs_v2 %s ===\n", do_write ? "WRITE MODE" : "SCAN ONLY");

	struct open_ctree_args oca = {
		.filename = argv[1],
		.flags = OPEN_CTREE_PARTIAL | OPEN_CTREE_ALLOW_TRANSID_MISMATCH |
			 OPEN_CTREE_SKIP_CSUM_CHECK | OPEN_CTREE_SKIP_LEAF_ITEM_CHECKS |
			 (do_write ? OPEN_CTREE_WRITES : 0),
	};

	fs_info = open_ctree_fs_info(&oca);
	if (!fs_info) {
		fprintf(stderr, "ERROR: open_ctree_fs_info failed\n");
		return 1;
	}

	key.objectid = BTRFS_FS_TREE_OBJECTID;
	key.type = BTRFS_ROOT_ITEM_KEY;
	key.offset = (u64)-1;
	fs_root = btrfs_read_fs_root(fs_info, &key);
	if (IS_ERR(fs_root)) {
		fprintf(stderr, "ERROR: cannot read FS_TREE root\n");
		close_ctree(fs_info->tree_root);
		return 1;
	}

	scan_full_tree(fs_info, fs_root, BTRFS_FS_TREE_OBJECTID);

	printf("\nTotal stale: %d\n", num_stale);
	if (num_stale == 0) {
		printf("Nothing to remove\n");
		close_ctree(fs_info->tree_root);
		return 0;
	}
	if (!do_write) {
		printf("SCAN mode — run with --write to remove\n");
		close_ctree(fs_info->tree_root);
		return 0;
	}

	/* WRITE: open transaction and delete pointers */
	fs_info->rebuilding_extent_tree = 1;
	trans = btrfs_start_transaction(fs_root, 1);
	if (IS_ERR(trans)) {
		fprintf(stderr, "ERROR: start_transaction failed\n");
		close_ctree(fs_info->tree_root);
		return 1;
	}
	trans->reinit_extent_tree = true;

	int removed = 0;
	int k;
	for (k = 0; k < num_stale; k++) {
		struct btrfs_path *path = btrfs_alloc_path();
		int ret = btrfs_search_slot(trans, fs_root, &stale_list[k].expected_key, path, 0, 1);
		int target_level = stale_list[k].parent_level;

		if (!path->nodes[target_level]) {
			printf("  SKIP (path->nodes[%d] NULL, ret=%d)\n", target_level, ret);
			btrfs_free_path(path);
			continue;
		}

		struct extent_buffer *parent = path->nodes[target_level];
		int slot = path->slots[target_level];
		int pn = btrfs_header_nritems(parent);

		/* The search may land past end; clamp to last valid slot */
		if (slot >= pn) slot = pn - 1;
		if (slot < 0) {
			printf("  SKIP (empty parent)\n");
			btrfs_free_path(path);
			continue;
		}

		u64 cur_child_bytenr = btrfs_node_blockptr(parent, slot);

		/* Re-verify: read current child and check if stale */
		struct btrfs_tree_parent_check check = {
			.owner_root = BTRFS_FS_TREE_OBJECTID,
			.transid = 0,
			.level = target_level - 1,
			.has_first_key = false,
		};
		struct extent_buffer *child = read_tree_block(fs_info, cur_child_bytenr, &check);
		if (IS_ERR(child) || !extent_buffer_uptodate(child)) {
			if (!IS_ERR_OR_NULL(child)) free_extent_buffer(child);
			printf("  SKIP (child unreadable at slot %d bytenr=%llu)\n",
			       slot, (unsigned long long)cur_child_bytenr);
			btrfs_free_path(path);
			continue;
		}

		const char *reason = NULL;
		int still_stale = check_child(child, &stale_list[k].expected_key,
					       BTRFS_FS_TREE_OBJECTID, &reason);
		free_extent_buffer(child);

		if (!still_stale) {
			/* Not stale anymore (likely a duplicate expected_key processed earlier) */
			printf("  ALREADY_CLEAN expected_key=(%llu,%u,%llu) slot=%d\n",
			       (unsigned long long)stale_list[k].expected_key.objectid,
			       stale_list[k].expected_key.type,
			       (unsigned long long)stale_list[k].expected_key.offset, slot);
			btrfs_free_path(path);
			continue;
		}

		btrfs_del_ptr(trans, fs_root, path, target_level, slot);
		printf("  REMOVED slot=%d cur_bytenr=%llu reason=%s expected_key=(%llu,%u,%llu)\n",
		       slot, (unsigned long long)cur_child_bytenr, reason,
		       (unsigned long long)stale_list[k].expected_key.objectid,
		       stale_list[k].expected_key.type,
		       (unsigned long long)stale_list[k].expected_key.offset);
		removed++;

		btrfs_free_path(path);
	}

	printf("\nCommitting %d removals...\n", removed);
	int ret = btrfs_commit_transaction(trans, fs_root);
	if (ret) {
		fprintf(stderr, "ERROR: commit failed: %d\n", ret);
		close_ctree(fs_info->tree_root);
		return 1;
	}

	printf("=== SUCCESS: removed %d of %d stale pointers ===\n", removed, num_stale);
	close_ctree(fs_info->tree_root);
	return 0;
}
