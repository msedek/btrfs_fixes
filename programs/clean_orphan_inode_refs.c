/*
 * clean_orphan_inode_refs.c — Fase 2C-b cleanup
 *
 * Walks FS_TREE and removes INODE_REF + INODE_EXTREF items whose key.offset
 * (parent_inode) is in a given allowlist of orphan parents. These backrefs
 * point to parent dirs that no longer have INODE_ITEMs (and no DIR_ITEM/
 * DIR_INDEX for the child either), so the backrefs are dangling metadata
 * residuals that btrfs check reports as "errors 3, no dir item, no dir index".
 *
 * Input: one parent_inode per line (from orphan_parents_inode_refs.tsv).
 *
 * Usage:
 *   clean_orphan_inode_refs <device> <parents_tsv> [--write]
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

static int cmp_u64(const void *a, const void *b)
{
	u64 av = *(const u64 *)a, bv = *(const u64 *)b;
	return (av > bv) - (av < bv);
}

static int load_parents(const char *path, u64 **out, int *out_count)
{
	FILE *f = fopen(path, "r");
	if (!f) return -1;
	int cap = 1024, count = 0;
	u64 *arr = malloc(cap * sizeof(u64));
	if (!arr) { fclose(f); return -1; }
	char line[128];
	while (fgets(line, sizeof(line), f)) {
		if (count >= cap) { cap *= 2; arr = realloc(arr, cap * sizeof(u64)); }
		arr[count++] = strtoull(line, NULL, 10);
	}
	fclose(f);
	qsort(arr, count, sizeof(u64), cmp_u64);
	*out = arr;
	*out_count = count;
	return 0;
}

static int parent_is_orphan(u64 ino, u64 *arr, int count)
{
	return bsearch(&ino, arr, count, sizeof(u64), cmp_u64) != NULL;
}

int main(int argc, char *argv[])
{
	if (argc < 3 || argc > 4) {
		fprintf(stderr, "Usage: %s <device> <parents_tsv> [--write]\n", argv[0]);
		return 1;
	}
	int do_write = (argc == 4 && strcmp(argv[3], "--write") == 0);

	printf("=== clean_orphan_inode_refs %s ===\n", do_write ? "WRITE" : "DRY-RUN");
	u64 *parents = NULL;
	int n_parents = 0;
	if (load_parents(argv[2], &parents, &n_parents) < 0) {
		fprintf(stderr, "ERROR: load parents\n");
		return 1;
	}
	printf("Loaded %d orphan parent inodes\n", n_parents);

	struct open_ctree_args oca = {
		.filename = argv[1],
		.flags = OPEN_CTREE_PARTIAL | OPEN_CTREE_ALLOW_TRANSID_MISMATCH |
			 OPEN_CTREE_SKIP_CSUM_CHECK | OPEN_CTREE_SKIP_LEAF_ITEM_CHECKS |
			 (do_write ? OPEN_CTREE_WRITES : 0),
	};
	struct btrfs_fs_info *fs_info = open_ctree_fs_info(&oca);
	if (!fs_info) { fprintf(stderr, "ERROR: open_ctree\n"); free(parents); return 1; }

	struct btrfs_root *root = fs_info->fs_root;

	/* Walk FS_TREE: scan all items, find INODE_REF/INODE_EXTREF with matching offset */
	int dry_candidates = 0;
	int deleted = 0;
	struct btrfs_trans_handle *trans = NULL;
	const int CHUNK_COMMITS = 100;  /* commit every N deletes */

	if (do_write) {
		trans = btrfs_start_transaction(root, CHUNK_COMMITS * 8);
		if (IS_ERR(trans)) {
			fprintf(stderr, "ERROR: start_transaction: %ld\n", PTR_ERR(trans));
			close_ctree(fs_info->tree_root);
			free(parents);
			return 1;
		}
	}

restart_walk:;
	struct btrfs_path *path = btrfs_alloc_path();
	struct btrfs_key key = { 0, 0, 0 };
	int ret = btrfs_search_slot(trans, root, &key, path, 0, 0);  /* cow=0: btrfs_del_item COWs lazily */
	if (ret < 0) {
		fprintf(stderr, "ERROR: search_slot: %d\n", ret);
		btrfs_free_path(path);
		if (trans) btrfs_abort_transaction(trans, ret);
		close_ctree(fs_info->tree_root);
		free(parents);
		return 1;
	}

	int since_commit = 0;
	while (1) {
		if (path->slots[0] >= btrfs_header_nritems(path->nodes[0])) {
			ret = btrfs_next_leaf(root, path);
			if (ret < 0) {
				fprintf(stderr, "ERROR: next_leaf: %d\n", ret);
				btrfs_free_path(path);
				if (trans) btrfs_abort_transaction(trans, ret);
				close_ctree(fs_info->tree_root);
				free(parents);
				return 1;
			}
			if (ret > 0) break; /* end of tree */
		}
		struct btrfs_key found_key;
		btrfs_item_key_to_cpu(path->nodes[0], &found_key, path->slots[0]);

		if (found_key.type == BTRFS_INODE_REF_KEY) {
			/* INODE_REF_KEY: key.offset IS the parent_inode. Safe to use directly.
			 * INODE_EXTREF_KEY would need reading content for parent_objectid
			 * (key.offset is a hash there). We SKIP extref to avoid false
			 * positives from hash collisions — residual extref orphans (if any)
			 * are left for Fase 4 diagnostics. */
			u64 parent_ino = found_key.offset;
			if (parent_is_orphan(parent_ino, parents, n_parents)) {
				if (!do_write) {
					dry_candidates++;
					if (dry_candidates <= 20) {
						printf("  WOULD_DELETE key(%llu, %u, %llu) slot=%d\n",
						       (unsigned long long)found_key.objectid,
						       found_key.type,
						       (unsigned long long)found_key.offset,
						       path->slots[0]);
					}
					path->slots[0]++;
					continue;
				}

				/* Delete the item */
				ret = btrfs_del_item(trans, root, path);
				if (ret) {
					fprintf(stderr, "ERROR: del_item: %d\n", ret);
					btrfs_abort_transaction(trans, ret);
					btrfs_free_path(path);
					close_ctree(fs_info->tree_root);
					free(parents);
					return 1;
				}
				deleted++;
				since_commit++;

				if (since_commit >= CHUNK_COMMITS) {
					btrfs_free_path(path);
					ret = btrfs_commit_transaction(trans, root);
					if (ret) {
						fprintf(stderr, "ERROR: commit: %d\n", ret);
						close_ctree(fs_info->tree_root);
						free(parents);
						return 1;
					}
					printf("  committed %d deletes (total=%d)\n", since_commit, deleted);
					fflush(stdout);
					since_commit = 0;
					trans = btrfs_start_transaction(root, CHUNK_COMMITS * 8);
					if (IS_ERR(trans)) {
						fprintf(stderr, "ERROR: re-start_transaction: %ld\n", PTR_ERR(trans));
						close_ctree(fs_info->tree_root);
						free(parents);
						return 1;
					}
					goto restart_walk;
				}
				/* After delete, don't advance — re-check current slot */
				continue;
			}
		}
		path->slots[0]++;
	}

	btrfs_free_path(path);

	if (do_write) {
		if (since_commit > 0) {
			ret = btrfs_commit_transaction(trans, root);
			if (ret) {
				fprintf(stderr, "ERROR: final commit: %d\n", ret);
				close_ctree(fs_info->tree_root);
				free(parents);
				return 1;
			}
		}
		printf("=== SUCCESS: deleted %d INODE_REF/INODE_EXTREF items ===\n", deleted);
	} else {
		printf("=== DRY-RUN: would delete %d items ===\n", dry_candidates);
	}

	close_ctree(fs_info->tree_root);
	free(parents);
	return 0;
}
