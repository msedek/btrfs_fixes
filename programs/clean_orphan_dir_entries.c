/*
 * clean_orphan_dir_entries.c — Fase 2C cleanup
 *
 * Removes the 959 orphan DIR_ITEM + DIR_INDEX entries reported by
 * btrfs check [5/8] "unresolved ref" errors. Updates parent INODE_ITEM
 * i_size and i_nlink accordingly when the parent is still reachable.
 *
 * Safeguards (per reviewer round):
 *   1. Hardcoded PROTECTED_NAMES list — refuses if any orphan entry matches
      * a protected top-level dir name
 *   2. Dry-run (default) writes full plan to fase2c_to_delete.txt
 *   3. Parent INODE_ITEM i_size/i_nlink updated per chunk
 *   4. Deletes DIR_ITEM + DIR_INDEX trio (INODE_REF skipped — child has no
 *      INODE_ITEM so INODE_REF also absent)
 *   5. Chunks of 200 entries per transaction
 *   6. Post-run verify externally via btrfs check
 *
 * Usage:
 *   clean_orphan_dir_entries <device> <orphan_tsv> [--write]
 *
 * Input TSV format (SOH 0x01 delimited):
 *   parent_inode\x01dir_index\x01namelen\x01filetype\x01name
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

static const char *PROTECTED_NAMES[] = {
	"homenetwork", "homestorage", "msechat",
	"pelis", "series", "music", "Roms", "PSP_ISOs",
	"backups", "isos", "globaldisk", "downloads",
	"bkcache", "nomad",
	NULL
};

static int name_is_protected(const char *name)
{
	int i;
	for (i = 0; PROTECTED_NAMES[i]; i++) {
		if (strcmp(name, PROTECTED_NAMES[i]) == 0) return 1;
	}
	return 0;
}

struct orphan_entry {
	u64 parent_ino;
	u64 dir_index;
	u16 namelen;
	u8 filetype;
	char name[256];
};

struct parent_delta {
	u64 ino;
	u64 size_delta;
	int nlink_delta_dirs;
};

static int parse_orphan_tsv(const char *path, struct orphan_entry **out, int *out_count)
{
	FILE *f = fopen(path, "r");
	if (!f) {
		fprintf(stderr, "ERROR: cannot open %s: %s\n", path, strerror(errno));
		return -1;
	}
	int cap = 1024;
	int count = 0;
	struct orphan_entry *arr = malloc(cap * sizeof(struct orphan_entry));
	if (!arr) { fclose(f); return -1; }

	char line[2048];
	while (fgets(line, sizeof(line), f)) {
		if (count >= cap) {
			cap *= 2;
			struct orphan_entry *tmp = realloc(arr, cap * sizeof(struct orphan_entry));
			if (!tmp) { free(arr); fclose(f); return -1; }
			arr = tmp;
		}
		int len = strlen(line);
		if (len > 0 && line[len-1] == '\n') line[--len] = '\0';
		if (len == 0) continue;

		/* Split by 0x01 */
		char *fields[5] = {0};
		int fcount = 0;
		fields[0] = line;
		char *p;
		for (p = line; *p && fcount < 5; p++) {
			if (*p == 0x01) {
				*p = '\0';
				fcount++;
				if (fcount < 5) fields[fcount] = p+1;
			}
		}
		if (fcount != 4) continue;

		struct orphan_entry *e = &arr[count];
		e->parent_ino = strtoull(fields[0], NULL, 10);
		e->dir_index = strtoull(fields[1], NULL, 10);
		e->namelen = (u16)strtoul(fields[2], NULL, 10);
		e->filetype = (u8)strtoul(fields[3], NULL, 10);
		strncpy(e->name, fields[4], 255);
		e->name[255] = '\0';
		count++;
	}
	fclose(f);
	*out = arr;
	*out_count = count;
	return 0;
}

/* Delete one orphan entry's DIR_ITEM + DIR_INDEX. Returns 0 on success.
 * ret > 0 means "not found, nothing to delete" (non-fatal). */
static int delete_orphan_entry(struct btrfs_trans_handle *trans,
			       struct btrfs_root *root,
			       const struct orphan_entry *e,
			       int *found_any)
{
	struct btrfs_path *path;
	struct btrfs_dir_item *di;
	int ret = 0;
	int found = 0;

	/* 1. DIR_ITEM lookup + delete */
	path = btrfs_alloc_path();
	if (!path) return -ENOMEM;

	di = btrfs_lookup_dir_item(trans, root, path, e->parent_ino,
				   e->name, e->namelen, -1);
	if (IS_ERR(di)) {
		ret = PTR_ERR(di);
		if (ret != -ENOENT) {
			btrfs_free_path(path);
			return ret;
		}
		ret = 0;
	} else if (di) {
		ret = btrfs_delete_one_dir_name(trans, root, path, di);
		if (ret) {
			btrfs_free_path(path);
			return ret;
		}
		found = 1;
	}
	btrfs_release_path(path);

	/* 2. DIR_INDEX lookup + delete */
	di = btrfs_lookup_dir_index_item(trans, root, path, e->parent_ino,
					 e->dir_index, e->name, e->namelen, -1);
	if (IS_ERR(di)) {
		ret = PTR_ERR(di);
		if (ret != -ENOENT) {
			btrfs_free_path(path);
			return ret;
		}
		ret = 0;
	} else if (di) {
		ret = btrfs_delete_one_dir_name(trans, root, path, di);
		if (ret) {
			btrfs_free_path(path);
			return ret;
		}
		found = 1;
	}
	btrfs_free_path(path);

	*found_any = found;
	return 0;
}

/* Update parent INODE_ITEM: decrement i_size by size_delta, decrement i_nlink
 * by nlink_delta (only if filetype dir was removed). Returns:
 *   0 on success
 *   -ENOENT if parent INODE_ITEM missing (orphan parent — skip)
 *   negative on other errors */
static int update_parent_inode(struct btrfs_trans_handle *trans,
			       struct btrfs_root *root,
			       u64 parent_ino,
			       u64 size_delta,
			       int nlink_delta)
{
	struct btrfs_path *path = btrfs_alloc_path();
	if (!path) return -ENOMEM;

	struct btrfs_key key = { parent_ino, BTRFS_INODE_ITEM_KEY, 0 };
	int ret = btrfs_search_slot(trans, root, &key, path, 0, 1);
	if (ret > 0) {
		btrfs_free_path(path);
		return -ENOENT;
	}
	if (ret < 0) {
		btrfs_free_path(path);
		return ret;
	}

	struct extent_buffer *leaf = path->nodes[0];
	struct btrfs_inode_item *ii = btrfs_item_ptr(leaf, path->slots[0],
						      struct btrfs_inode_item);

	u64 cur_size = btrfs_inode_size(leaf, ii);
	u32 cur_nlink = btrfs_inode_nlink(leaf, ii);

	u64 new_size = (cur_size > size_delta) ? cur_size - size_delta : 0;
	u32 new_nlink = cur_nlink;
	if ((u32)nlink_delta < cur_nlink) {
		new_nlink = cur_nlink - (u32)nlink_delta;
	}
	if (new_nlink < 2) new_nlink = 2; /* dir minimum . and parent */

	btrfs_set_inode_size(leaf, ii, new_size);
	btrfs_set_inode_nlink(leaf, ii, new_nlink);
	btrfs_mark_buffer_dirty(leaf);
	btrfs_free_path(path);
	return 0;
}

int main(int argc, char *argv[])
{
	if (argc < 3 || argc > 4) {
		fprintf(stderr, "Usage: %s <device> <orphan_tsv> [--write]\n", argv[0]);
		return 1;
	}

	int do_write = (argc == 4 && strcmp(argv[3], "--write") == 0);

	printf("=== clean_orphan_dir_entries %s ===\n", do_write ? "WRITE" : "DRY-RUN");
	printf("Device: %s\n", argv[1]);
	printf("Input:  %s\n", argv[2]);

	/* Phase 1: parse + validate */
	struct orphan_entry *entries = NULL;
	int total = 0;
	if (parse_orphan_tsv(argv[2], &entries, &total) < 0) {
		return 1;
	}
	printf("Parsed %d orphan entries\n", total);

	int protected_hits = 0;
	int i;
	for (i = 0; i < total; i++) {
		if (name_is_protected(entries[i].name)) {
			fprintf(stderr, "PROTECTED NAME DETECTED: parent=%llu name='%s' — REFUSING\n",
				(unsigned long long)entries[i].parent_ino, entries[i].name);
			protected_hits++;
		}
	}
	if (protected_hits > 0) {
		fprintf(stderr, "ABORT: %d protected name hit(s)\n", protected_hits);
		free(entries);
		return 3;
	}
	printf("Protected-name check: PASS (0 hits)\n");

	/* Phase 2: write plan file */
	const char *plan_file = "./fase2c_to_delete.txt";
	FILE *pf = fopen(plan_file, "w");
	if (pf) {
		fprintf(pf, "# Format: parent_inode|dir_index|namelen|filetype|name\n");
		for (i = 0; i < total; i++) {
			fprintf(pf, "%llu|%llu|%u|%u|%s\n",
				(unsigned long long)entries[i].parent_ino,
				(unsigned long long)entries[i].dir_index,
				entries[i].namelen, entries[i].filetype, entries[i].name);
		}
		fclose(pf);
		printf("Plan file: %s (%d lines)\n", plan_file, total);
	}

	if (!do_write) {
		printf("DRY-RUN complete. Review plan file, then run with --write.\n");
		free(entries);
		return 0;
	}

	/* Phase 3: open ctree WRITES */
	struct open_ctree_args oca = {
		.filename = argv[1],
		.flags = OPEN_CTREE_PARTIAL | OPEN_CTREE_ALLOW_TRANSID_MISMATCH |
			 OPEN_CTREE_SKIP_CSUM_CHECK | OPEN_CTREE_SKIP_LEAF_ITEM_CHECKS |
			 OPEN_CTREE_WRITES,
	};
	struct btrfs_fs_info *fs_info = open_ctree_fs_info(&oca);
	if (!fs_info) {
		fprintf(stderr, "ERROR: open_ctree\n");
		free(entries);
		return 1;
	}

	struct btrfs_root *root = fs_info->fs_root;
	if (!root || IS_ERR(root)) {
		fprintf(stderr, "ERROR: fs_root\n");
		close_ctree(fs_info->tree_root);
		free(entries);
		return 1;
	}

	/* Phase 4: chunked deletes */
	const int CHUNK_SIZE = 100;
	int deleted_total = 0;
	int skipped_total = 0;
	int chunks_done = 0;

	struct parent_delta *deltas = calloc(CHUNK_SIZE, sizeof(struct parent_delta));
	if (!deltas) {
		fprintf(stderr, "ERROR: alloc deltas\n");
		close_ctree(fs_info->tree_root);
		free(entries);
		return 1;
	}

	int start;
	for (start = 0; start < total; start += CHUNK_SIZE) {
		int end = start + CHUNK_SIZE;
		if (end > total) end = total;
		int batch = end - start;

		struct btrfs_trans_handle *trans = btrfs_start_transaction(root, batch * 8);
		if (IS_ERR(trans)) {
			fprintf(stderr, "ERROR: start_transaction chunk %d: %ld\n",
				chunks_done, PTR_ERR(trans));
			free(deltas); free(entries);
			close_ctree(fs_info->tree_root);
			return 1;
		}

		int chunk_deleted = 0;
		int chunk_skipped = 0;
		int pu_count = 0;
		int j;

		int idx;
		for (idx = start; idx < end; idx++) {
			int found = 0;
			int ret = delete_orphan_entry(trans, root, &entries[idx], &found);
			if (ret < 0) {
				fprintf(stderr, "WARN: entry %d (%llu/%s) ret=%d\n",
					idx, (unsigned long long)entries[idx].parent_ino,
					entries[idx].name, ret);
				chunk_skipped++;
				continue;
			}
			if (!found) {
				chunk_skipped++;
				continue;
			}
			chunk_deleted++;

			/* Aggregate deltas per parent */
			int exists = 0;
			for (j = 0; j < pu_count; j++) {
				if (deltas[j].ino == entries[idx].parent_ino) {
					deltas[j].size_delta += entries[idx].namelen;
					if (entries[idx].filetype == BTRFS_FT_DIR)
						deltas[j].nlink_delta_dirs++;
					exists = 1;
					break;
				}
			}
			if (!exists && pu_count < CHUNK_SIZE) {
				deltas[pu_count].ino = entries[idx].parent_ino;
				deltas[pu_count].size_delta = entries[idx].namelen;
				deltas[pu_count].nlink_delta_dirs =
					(entries[idx].filetype == BTRFS_FT_DIR) ? 1 : 0;
				pu_count++;
			}
		}

		/* Apply parent inode updates (skip ENOENT silently — orphan parents) */
		int updated_parents = 0;
		int skipped_parents = 0;
		for (j = 0; j < pu_count; j++) {
			int ret = update_parent_inode(trans, root, deltas[j].ino,
						       deltas[j].size_delta,
						       deltas[j].nlink_delta_dirs);
			if (ret == 0) updated_parents++;
			else if (ret == -ENOENT) skipped_parents++;
			else {
				fprintf(stderr, "WARN: update parent %llu ret=%d\n",
					(unsigned long long)deltas[j].ino, ret);
			}
		}

		/* Clear deltas for next chunk */
		memset(deltas, 0, CHUNK_SIZE * sizeof(struct parent_delta));

		int ret = btrfs_commit_transaction(trans, root);
		if (ret) {
			fprintf(stderr, "ERROR: commit chunk %d: %d\n", chunks_done, ret);
			btrfs_abort_transaction(trans, ret);
			free(deltas); free(entries);
			close_ctree(fs_info->tree_root);
			return 1;
		}

		chunks_done++;
		deleted_total += chunk_deleted;
		skipped_total += chunk_skipped;
		printf("chunk %d: deleted=%d skipped=%d parents_upd=%d parents_orphan=%d (cumul: %d/%d)\n",
		       chunks_done, chunk_deleted, chunk_skipped, updated_parents, skipped_parents,
		       deleted_total, total);
		fflush(stdout);
	}

	printf("\n=== SUCCESS: deleted %d of %d (skipped %d) ===\n",
	       deleted_total, total, skipped_total);
	free(deltas);
	free(entries);
	close_ctree(fs_info->tree_root);
	return 0;
}
