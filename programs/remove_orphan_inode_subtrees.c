/*
 * remove_orphan_inode_subtrees.c — Fase 2C-c / Opción A1
 *
 * Removes orphan inodes (both DIRs and REGs) from the FS_TREE to bring
 * btrfs check [5/8] to 0 errors. Input: target_orphan_inodes_886.txt with
 * format "inode|type|family_idx" where family_idx groups each orphan DIR
 * with its REG children for atomic per-family transaction processing.
 *
 * For each inode, this tool removes (in correct order):
 *   1. EXTENT_DATA items + btrfs_free_extent on referenced bytenrs
 *   2. INODE_REF + INODE_EXTREF items
 *   3. XATTR_ITEM items
 *   4. For DIRs: DIR_ITEM + DIR_INDEX entries (pointing to children already removed)
 *   5. INODE_ITEM itself
 *
 * Safety invariants:
 *   - Paranoid exclusion list hardcoded (refuses 256, 263, 269775, 285393, etc.)
 *   - Per-family transaction (atomicity of subtree removal)
 *   - Chunks of 50 for standalone REGs
 *   - Dry-run writes plan file with every operation to be performed
 *   - btrfs_free_extent errors other than ENOENT abort the transaction
 *
 * Usage:
 *   remove_orphan_inode_subtrees <device> <target_file> [--dryrun|--write]
 */

#include "kerncompat.h"
#include "kernel-shared/ctree.h"
#include "kernel-shared/disk-io.h"
#include "kernel-shared/transaction.h"
#include "kernel-shared/accessors.h"
#include "kernel-shared/extent-tree.h"
#include "kernel-shared/uapi/btrfs_tree.h"
#include "common/messages.h"
#include "common/open-utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <errno.h>
#include <sys/stat.h>

/* Paranoid exclusion list — refuse to touch any of these even if they
 * somehow appear in the input file. Matches known critical dir inodes. */
static const u64 EXCLUDED_INODES[] = {
	256,     /* FS_TREE root */
	259,     /* backups */
	260,     /* bkcache */
	262,     /* globaldisk */
	263,     /* top-level storage dir */
	264,     /* isos */
	269774,  /* downloads */
	269775,  /* top-level media dir */
	269887,  /* subdir example (fixed by fix_dir_inode_counts) */
	269969,  /* subdir example (fixed) */
	275299,  /* series */
	285393,  /* music */
	300658,  /* top-level network dir */
	305044,  /* nomad */
	305533,  /* PSP_ISOs */
	305534,  /* music subdir (fixed) */
	0        /* sentinel */
};

static int is_excluded(u64 inode)
{
	int i;
	if (inode < 500) return 1; /* system-reserved */
	for (i = 0; EXCLUDED_INODES[i]; i++) {
		if (EXCLUDED_INODES[i] == inode) return 1;
	}
	return 0;
}

struct target_entry {
	u64 inode;
	char type[4];  /* "DIR" or "REG" */
	int family_idx;  /* -1 for standalone */
};

struct dir_family {
	u64 dir_inode;
	u64 *children;
	size_t num_children;
};

/* Globals for dry-run logging */
static FILE *plan_log = NULL;
static int do_write = 0;

static void log_plan(const char *fmt, ...) {
	va_list ap;
	va_start(ap, fmt);
	if (plan_log) vfprintf(plan_log, fmt, ap);
	va_end(ap);
	va_start(ap, fmt);
	vprintf(fmt, ap);
	va_end(ap);
}

/* Walk items of a specific (inode, key_type, *) and delete them all.
 * For EXTENT_DATA_KEY, also calls btrfs_free_extent on each extent.
 * Returns # of items deleted, or negative on error. */
static int walk_and_delete_items(struct btrfs_trans_handle *trans,
				  struct btrfs_root *root, u64 inode, u8 key_type,
				  u64 *out_free_extent_errors)
{
	int deleted = 0;
	int iters = 0;
	struct btrfs_path *path;
	struct btrfs_key key;
	struct btrfs_key found;
	int ret;

	while (1) {
		if (++iters > 10000) {
			fprintf(stderr, "    ERROR walk_and_delete stuck: inode=%llu type=%u iters=%d deleted=%d\n",
				(unsigned long long)inode, key_type, iters, deleted);
			return -ELOOP;
		}
		path = btrfs_alloc_path();
		if (!path) return -ENOMEM;
		key.objectid = inode;
		key.type = key_type;
		key.offset = 0;
		ret = btrfs_search_slot(trans, root, &key, path, -1, 1);
		if (ret < 0) {
			btrfs_free_path(path);
			return ret;
		}
		/* If search landed past the last matching key, back up one */
		if (ret > 0) {
			if (path->slots[0] >= btrfs_header_nritems(path->nodes[0])) {
				ret = btrfs_next_leaf(root, path);
				if (ret) { btrfs_free_path(path); break; }
			}
		}
		btrfs_item_key_to_cpu(path->nodes[0], &found, path->slots[0]);
		if (found.objectid != inode || found.type != key_type) {
			btrfs_free_path(path);
			break;
		}

		/* For EXTENT_DATA, free the underlying extent via backref decrement */
		if (key_type == BTRFS_EXTENT_DATA_KEY && do_write) {
			struct btrfs_file_extent_item *fi = btrfs_item_ptr(
				path->nodes[0], path->slots[0],
				struct btrfs_file_extent_item);
			u8 ftype = btrfs_file_extent_type(path->nodes[0], fi);
			if (ftype != BTRFS_FILE_EXTENT_INLINE) {
				u64 disk_bytenr = btrfs_file_extent_disk_bytenr(
					path->nodes[0], fi);
				u64 disk_num_bytes = btrfs_file_extent_disk_num_bytes(
					path->nodes[0], fi);
				if (disk_bytenr != 0) {
					u64 fe_offset = btrfs_file_extent_offset(
						path->nodes[0], fi);
					u64 ref_offset = found.offset - fe_offset;
					int fret = btrfs_free_extent(trans,
						disk_bytenr, disk_num_bytes,
						0 /*parent*/,
						BTRFS_FS_TREE_OBJECTID,
						inode /*owner*/,
						ref_offset);
					if (fret && fret != -ENOENT) {
						fprintf(stderr, "    ERROR btrfs_free_extent bytenr=%llu ret=%d\n",
							(unsigned long long)disk_bytenr, fret);
						btrfs_free_path(path);
						return fret;
					}
					if (fret == -ENOENT) {
						(*out_free_extent_errors)++;
						fprintf(stderr, "    WARN btrfs_free_extent ENOENT inode=%llu bytenr=%llu num_bytes=%llu ref_offset=%llu\n",
							(unsigned long long)inode,
							(unsigned long long)disk_bytenr,
							(unsigned long long)disk_num_bytes,
							(unsigned long long)ref_offset);
					}
				}
			}
		}

		if (do_write) {
			ret = btrfs_del_item(trans, root, path);
			if (ret) {
				fprintf(stderr, "    ERROR btrfs_del_item type=%u ret=%d\n",
					key_type, ret);
				btrfs_free_path(path);
				return ret;
			}
		}
		deleted++;
		btrfs_free_path(path);

		if (!do_write) {
			/* Dry-run: prevent infinite loop (item wasn't deleted) */
			break;
		}
	}
	return deleted;
}

/* Remove a single inode fully (all associated items + INODE_ITEM).
 * Called inside an active transaction. Returns 0 on success, negative on error. */
static int remove_single_inode(struct btrfs_trans_handle *trans,
				struct btrfs_root *root, u64 inode,
				u64 *out_extent_errors)
{
	int ret;
	int ext_deleted = 0, ref_deleted = 0, extref_deleted = 0;
	int xattr_deleted = 0;

	/* Dry-run: count items without deleting */
	if (!do_write) {
		/* Just look up to verify INODE_ITEM exists */
		struct btrfs_path *path = btrfs_alloc_path();
		struct btrfs_key key = { inode, BTRFS_INODE_ITEM_KEY, 0 };
		ret = btrfs_search_slot(NULL, root, &key, path, 0, 0);
		if (ret != 0) {
			log_plan("    [dry] inode %llu: NO INODE_ITEM (skip)\n",
				(unsigned long long)inode);
			btrfs_free_path(path);
			return 0;
		}
		btrfs_free_path(path);
		log_plan("    [dry] inode %llu: would remove EXTENT_DATA + INODE_REF + INODE_EXTREF + XATTR + INODE_ITEM\n",
			(unsigned long long)inode);
		return 0;
	}

	/* WRITE PATH — in order: EXTENT_DATA (with free_extent), INODE_REF,
	 * INODE_EXTREF, XATTR, INODE_ITEM. */
	/* Pre-check: verify target INODE_ITEM exists. Pre-flight verified 886/886
	 * present; if one is missing now, something changed — ABORT, not skip. */
	{
		struct btrfs_path *_path = btrfs_alloc_path();
		struct btrfs_key _key = { inode, BTRFS_INODE_ITEM_KEY, 0 };
		int _ret = btrfs_search_slot(trans, root, &_key, _path, 0, 0);
		btrfs_free_path(_path);
		if (_ret != 0) {
			fprintf(stderr, "    ABORT: inode %llu missing INODE_ITEM in write phase (pre-flight said present)\n",
				(unsigned long long)inode);
			return -ENOENT;
		}
	}

	ret = walk_and_delete_items(trans, root, inode, BTRFS_EXTENT_DATA_KEY,
				     out_extent_errors);
	if (ret < 0) return ret;
	ext_deleted = ret;

	ret = walk_and_delete_items(trans, root, inode, BTRFS_INODE_REF_KEY,
				     out_extent_errors);
	if (ret < 0) return ret;
	ref_deleted = ret;

	ret = walk_and_delete_items(trans, root, inode, BTRFS_INODE_EXTREF_KEY,
				     out_extent_errors);
	if (ret < 0) return ret;
	extref_deleted = ret;

	ret = walk_and_delete_items(trans, root, inode, BTRFS_XATTR_ITEM_KEY,
				     out_extent_errors);
	if (ret < 0) return ret;
	xattr_deleted = ret;

	/* Finally the INODE_ITEM */
	struct btrfs_path *path = btrfs_alloc_path();
	struct btrfs_key key = { inode, BTRFS_INODE_ITEM_KEY, 0 };
	ret = btrfs_search_slot(trans, root, &key, path, -1, 1);
	if (ret == 0) {
		ret = btrfs_del_item(trans, root, path);
		if (ret) {
			fprintf(stderr, "    ERROR del INODE_ITEM %llu ret=%d\n",
				(unsigned long long)inode, ret);
			btrfs_free_path(path);
			return ret;
		}
	}
	btrfs_free_path(path);

	log_plan("    inode %llu: removed ext=%d ref=%d extref=%d xattr=%d\n",
		(unsigned long long)inode, ext_deleted, ref_deleted,
		extref_deleted, xattr_deleted);
	return 0;
}

/* Parse target file (SOH-less TSV with '|' delimiter) into families + standalone.
 * Returns: families array, families_count, standalone array, standalone_count */
static int parse_targets(const char *path, struct dir_family **out_fams,
			  int *out_fam_count, u64 **out_standalone,
			  int *out_standalone_count)
{
	FILE *f = fopen(path, "r");
	if (!f) return -errno;

	int max_fam = 64;
	struct dir_family *fams = calloc(max_fam, sizeof(struct dir_family));
	int fam_count = 0;
	u64 *stand = malloc(8192 * sizeof(u64));
	int stand_cap = 8192;
	int stand_count = 0;

	char line[256];
	while (fgets(line, sizeof(line), f)) {
		if (line[0] == '#') continue;
		u64 inode;
		char type[8];
		int fidx;
		if (sscanf(line, "%llu|%7[^|]|%d",
			   (unsigned long long *)&inode, type, &fidx) != 3) continue;

		if (fidx == -1) {
			if (stand_count >= stand_cap) {
				stand_cap *= 2;
				stand = realloc(stand, stand_cap * sizeof(u64));
			}
			stand[stand_count++] = inode;
		} else {
			if (fidx >= max_fam) {
				int new_max = fidx + 16;
				fams = realloc(fams, new_max * sizeof(struct dir_family));
				memset(&fams[max_fam], 0,
				       (new_max - max_fam) * sizeof(struct dir_family));
				max_fam = new_max;
			}
			if (fidx >= fam_count) fam_count = fidx + 1;

			if (strcmp(type, "DIR") == 0 && fams[fidx].dir_inode == 0) {
				fams[fidx].dir_inode = inode;
			} else {
				/* child — append */
				fams[fidx].children = realloc(fams[fidx].children,
					(fams[fidx].num_children + 1) * sizeof(u64));
				fams[fidx].children[fams[fidx].num_children++] = inode;
			}
		}
	}
	fclose(f);

	*out_fams = fams;
	*out_fam_count = fam_count;
	*out_standalone = stand;
	*out_standalone_count = stand_count;
	return 0;
}

int main(int argc, char *argv[])
{
	if (argc < 3 || argc > 4) {
		fprintf(stderr, "Usage: %s <device> <target_file> [--dryrun|--write]\n",
			argv[0]);
		return 1;
	}
	do_write = (argc == 4 && strcmp(argv[3], "--write") == 0);

	printf("=== remove_orphan_inode_subtrees %s ===\n",
		do_write ? "WRITE" : "DRY-RUN");
	printf("Device: %s\n", argv[1]);
	printf("Input:  %s\n\n", argv[2]);

	/* Parse targets */
	struct dir_family *fams = NULL;
	int fam_count = 0;
	u64 *standalone = NULL;
	int stand_count = 0;
	int ret = parse_targets(argv[2], &fams, &fam_count, &standalone, &stand_count);
	if (ret < 0) {
		fprintf(stderr, "ERROR parse: %d\n", ret);
		return 1;
	}

	int total_inodes = stand_count;
	int i;
	for (i = 0; i < fam_count; i++) {
		total_inodes += 1 + fams[i].num_children;
	}
	printf("Loaded %d families + %d standalone = %d total inodes\n",
		fam_count, stand_count, total_inodes);

	/* Paranoid exclusion check on all targets */
	int exc_hits = 0;
	for (i = 0; i < fam_count; i++) {
		if (is_excluded(fams[i].dir_inode)) {
			fprintf(stderr, "EXCLUSION HIT: family dir %llu is in exclusion list\n",
				(unsigned long long)fams[i].dir_inode);
			exc_hits++;
		}
		size_t j;
		for (j = 0; j < fams[i].num_children; j++) {
			if (is_excluded(fams[i].children[j])) {
				fprintf(stderr, "EXCLUSION HIT: child %llu in family %d\n",
					(unsigned long long)fams[i].children[j], i);
				exc_hits++;
			}
		}
	}
	for (i = 0; i < stand_count; i++) {
		if (is_excluded(standalone[i])) {
			fprintf(stderr, "EXCLUSION HIT: standalone %llu\n",
				(unsigned long long)standalone[i]);
			exc_hits++;
		}
	}
	if (exc_hits > 0) {
		fprintf(stderr, "ABORT: %d exclusion hits\n", exc_hits);
		return 3;
	}
	printf("Exclusion check: PASS (%d EXCLUDED_INODES + system-reserved <500)\n",
		(int)(sizeof(EXCLUDED_INODES)/sizeof(u64))-1);

	/* Plan log */
	const char *plan_path = do_write
		? "./orphan_removal_write.log"
		: "./orphan_removal_dryrun.log";
	plan_log = fopen(plan_path, "w");
	if (!plan_log) {
		fprintf(stderr, "WARN: cannot open plan log %s\n", plan_path);
	}
	log_plan("=== PLAN LOG %s ===\n", do_write ? "WRITE" : "DRY-RUN");

	/* Open ctree */
	struct open_ctree_args oca = {
		.filename = argv[1],
		.flags = OPEN_CTREE_PARTIAL | OPEN_CTREE_ALLOW_TRANSID_MISMATCH |
			 OPEN_CTREE_SKIP_CSUM_CHECK | OPEN_CTREE_SKIP_LEAF_ITEM_CHECKS |
			 (do_write ? OPEN_CTREE_WRITES : 0),
	};
	struct btrfs_fs_info *fs_info = open_ctree_fs_info(&oca);
	if (!fs_info) { fprintf(stderr, "ERROR open_ctree\n"); return 1; }
	struct btrfs_root *root = fs_info->fs_root;

	u64 total_extent_errors = 0;

	/* Phase 1: process DIR families */
	for (i = 0; i < fam_count; i++) {
		struct dir_family *fam = &fams[i];
		log_plan("\nFAMILY %d: dir=%llu children=%zu\n",
			i, (unsigned long long)fam->dir_inode, fam->num_children);

		struct btrfs_trans_handle *trans = NULL;
		if (do_write) {
			trans = btrfs_start_transaction(root,
				(fam->num_children + 5) * 8);
			if (IS_ERR(trans)) {
				fprintf(stderr, "ERROR start_trans family %d: %ld\n",
					i, PTR_ERR(trans));
				close_ctree(fs_info->tree_root);
				return 1;
			}
			/* RE-ACTIVATED after quick test: the previous run without these
			 * flags crashed on __btrfs_mod_ref assertion when btrfs_cow_block
			 * tried to decrement backrefs of stale pre-crash leaves (the
			 * 393K backpointer mismatch residual). These flags tell the
			 * code path to skip that accounting layer — same pattern that
			 * made remove_stale_ptrs_v2, rebuild_extent_tree_apply, and
			 * remove_extent_items_by_key work in earlier phases. Trade-off:
			 * btrfs_free_extent won't decrement bytes_used, so freed space
			 * remains as cosmetic residual (documented). */
			fs_info->rebuilding_extent_tree = 1;
			trans->reinit_extent_tree = true;
		}

		/* 2a: remove children */
		size_t j;
		for (j = 0; j < fam->num_children; j++) {
			u64 child = fam->children[j];
			ret = remove_single_inode(trans, root, child,
						   &total_extent_errors);
			if (ret < 0) {
				fprintf(stderr, "ERROR remove child %llu: %d\n",
					(unsigned long long)child, ret);
				if (trans) btrfs_abort_transaction(trans, ret);
				close_ctree(fs_info->tree_root);
				return 1;
			}
		}

		/* 2b: remove DIR's DIR_ITEM + DIR_INDEX entries */
		if (do_write) {
			int d1 = walk_and_delete_items(trans, root, fam->dir_inode,
				BTRFS_DIR_ITEM_KEY, &total_extent_errors);
			int d2 = walk_and_delete_items(trans, root, fam->dir_inode,
				BTRFS_DIR_INDEX_KEY, &total_extent_errors);
			if (d1 < 0 || d2 < 0) {
				btrfs_abort_transaction(trans, d1 < 0 ? d1 : d2);
				close_ctree(fs_info->tree_root);
				return 1;
			}
			log_plan("  DIR entries: %d DIR_ITEM + %d DIR_INDEX removed\n",
				d1, d2);
		}

		/* 2c+2d: remove DIR inode itself (INODE_REF, EXTREF, INODE_ITEM) */
		ret = remove_single_inode(trans, root, fam->dir_inode,
					   &total_extent_errors);
		if (ret < 0) {
			fprintf(stderr, "ERROR remove DIR %llu: %d\n",
				(unsigned long long)fam->dir_inode, ret);
			if (trans) btrfs_abort_transaction(trans, ret);
			close_ctree(fs_info->tree_root);
			return 1;
		}

		if (do_write) {
			log_plan("  CHECKPOINT: about to commit family %d dir=%llu\n",
				i, (unsigned long long)fam->dir_inode);
			ret = btrfs_commit_transaction(trans, root);
			if (ret) {
				fprintf(stderr, "ERROR commit family %d: %d\n", i, ret);
				close_ctree(fs_info->tree_root);
				return 1;
			}
			log_plan("  FAMILY %d committed OK\n", i);
		}
	}

	/* Phase 2: process standalone REGs in chunks of 50 */
	const int CHUNK = 50;
	for (i = 0; i < stand_count; i += CHUNK) {
		int end = i + CHUNK;
		if (end > stand_count) end = stand_count;
		int batch = end - i;

		log_plan("\nSTANDALONE chunk %d-%d (%d regs):\n", i, end-1, batch);

		struct btrfs_trans_handle *trans = NULL;
		if (do_write) {
			trans = btrfs_start_transaction(root, batch * 8);
			if (IS_ERR(trans)) {
				fprintf(stderr, "ERROR start_trans chunk %d: %ld\n",
					i/CHUNK, PTR_ERR(trans));
				close_ctree(fs_info->tree_root);
				return 1;
			}
			/* Same as family trans: bypass backref accounting to avoid
			 * __btrfs_mod_ref assertion on stale pre-crash leaves. */
			fs_info->rebuilding_extent_tree = 1;
			trans->reinit_extent_tree = true;
		}

		int k;
		for (k = i; k < end; k++) {
			ret = remove_single_inode(trans, root, standalone[k],
						   &total_extent_errors);
			if (ret < 0) {
				if (trans) btrfs_abort_transaction(trans, ret);
				close_ctree(fs_info->tree_root);
				return 1;
			}
		}

		if (do_write) {
			log_plan("  CHECKPOINT: about to commit standalone chunk %d (inodes %d-%d)\n",
				i/CHUNK, i, end-1);
			ret = btrfs_commit_transaction(trans, root);
			if (ret) {
				fprintf(stderr, "ERROR commit chunk %d: %d\n", i/CHUNK, ret);
				close_ctree(fs_info->tree_root);
				return 1;
			}
			log_plan("  chunk %d committed OK\n", i/CHUNK);
		}
	}

	if (plan_log) fclose(plan_log);
	printf("\n=== %s complete. Plan log: %s ===\n",
		do_write ? "WRITE" : "DRY-RUN", plan_path);
	printf("btrfs_free_extent ENOENT count: %llu\n",
		(unsigned long long)total_extent_errors);
	close_ctree(fs_info->tree_root);
	return 0;
}
