/*
 * rebuild_extent_tree_apply.c — Fase 3 writer
 *
 * Streams refs_folded.txt (already sorted by 5-tuple) and inserts EXTENT_ITEMs
 * into the extent tree for each physical extent that is not already present.
 *
 * Inputs:
 *   argv[1] — device
 *   argv[2] — refs_folded.txt path (sorted by bytenr,num_bytes,root,inode,ref_offset)
 *   argv[3] — to_insert_filtered.txt path (sorted bytenr|num_bytes, excludes stale collisions)
 *   argv[4] — watermark file path
 *   argv[5] — --write | --dryrun
 *
 * refs_folded.txt line format:
 *   disk_bytenr|disk_num_bytes|root|inode|ref_offset|count|gen|compression
 *
 * to_insert_filtered.txt line format:
 *   disk_bytenr|disk_num_bytes
 *
 * Algorithm:
 *   - Load to_insert_filtered into a sorted vector
 *   - Stream refs_folded accumulating refs per (bytenr,num_bytes) 2-tuple
 *   - When 2-tuple changes:
 *       - Binary search if it is in to_insert_filtered
 *       - If yes: enqueue this extent for insertion
 *       - Else: skip (either in existing, or it is a stale collision)
 *   - Every chunk_size extents: commit transaction, save watermark
 *
 * Layout written per extent (following kernel conventions):
 *   EXTENT_ITEM key (bytenr, EXTENT_ITEM=168, num_bytes)
 *     btrfs_extent_item { refs=sum(counts), generation=current, flags=DATA }
 *     inline ref: btrfs_extent_inline_ref overlaid with btrfs_extent_data_ref
 *                 of the FIRST ref (root, inode, ref_offset, count)
 *   For refs 2..N: separate items
 *     key (bytenr, EXTENT_DATA_REF_KEY=178, hash_extent_data_ref(root,inode,off))
 *     btrfs_extent_data_ref { root, inode, offset, count }
 *
 * Then btrfs_update_block_group(trans, bytenr, num_bytes, 1, 0) updates SB.
 */

#include "kerncompat.h"
#include "kernel-shared/ctree.h"
#include "kernel-shared/disk-io.h"
#include "kernel-shared/transaction.h"
#include "kernel-shared/accessors.h"
#include "kernel-shared/tree-checker.h"
#include "kernel-shared/extent-tree.h"
#include "kernel-shared/uapi/btrfs_tree.h"
#include "common/messages.h"
#include "common/open-utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>

#define MAX_REFS_PER_EXTENT 32
#define CHUNK_SIZE 5000
#define LOG_INTERVAL 1000
#define THROTTLE_INTERVAL 50000
#define THROTTLE_SLEEP_SEC 30

struct extent_ref {
	u64 root;
	u64 inode;
	u64 ref_offset;
	u32 count;
};

struct extent_entry {
	u64 bytenr;
	u64 num_bytes;
	int nrefs;
	u64 total_refs;
	struct extent_ref refs[MAX_REFS_PER_EXTENT];
};

/* Loaded from to_insert_filtered.txt. Sorted by bytenr asc. */
static u64 *filter_bytenrs = NULL;
static u64 *filter_numbytes = NULL;
static size_t filter_count = 0;
static size_t filter_cursor = 0;

static int load_filter(const char *path)
{
	FILE *fp = fopen(path, "r");
	if (!fp) { perror("filter open"); return -1; }

	size_t cap = 4 * 1024 * 1024;
	filter_bytenrs = malloc(cap * sizeof(u64));
	filter_numbytes = malloc(cap * sizeof(u64));
	if (!filter_bytenrs || !filter_numbytes) return -ENOMEM;

	char line[128];
	while (fgets(line, sizeof(line), fp)) {
		u64 b, n;
		if (sscanf(line, "%llu|%llu", (unsigned long long*)&b, (unsigned long long*)&n) == 2) {
			if (filter_count >= cap) {
				cap *= 2;
				filter_bytenrs = realloc(filter_bytenrs, cap * sizeof(u64));
				filter_numbytes = realloc(filter_numbytes, cap * sizeof(u64));
			}
			filter_bytenrs[filter_count] = b;
			filter_numbytes[filter_count] = n;
			filter_count++;
		}
	}
	fclose(fp);
	fprintf(stderr, "Loaded %zu filter entries\n", filter_count);
	return 0;
}

/* Since refs_folded and filter are both sorted, we walk filter_cursor in lockstep. */
static int filter_contains(u64 bytenr, u64 num_bytes)
{
	while (filter_cursor < filter_count) {
		u64 fb = filter_bytenrs[filter_cursor];
		u64 fn = filter_numbytes[filter_cursor];
		if (fb < bytenr || (fb == bytenr && fn < num_bytes)) {
			filter_cursor++;
			continue;
		}
		if (fb == bytenr && fn == num_bytes) {
			filter_cursor++;
			return 1;
		}
		return 0;
	}
	return 0;
}

/* hash_extent_data_ref is provided by kernel-shared/ctree.h (from extent-tree.c) */

static int insert_one_extent(struct btrfs_trans_handle *trans,
			      struct btrfs_root *extent_root,
			      struct extent_entry *ent)
{
	struct btrfs_path *path;
	struct btrfs_key key;
	struct extent_buffer *leaf;
	struct btrfs_extent_item *ei;
	struct btrfs_extent_inline_ref *iref;
	struct btrfs_extent_data_ref *dref;
	u32 item_size;
	int ret;

	path = btrfs_alloc_path();
	if (!path) return -ENOMEM;

	/* 1. Insert the main EXTENT_ITEM with inline first ref */
	key.objectid = ent->bytenr;
	key.type = BTRFS_EXTENT_ITEM_KEY;
	key.offset = ent->num_bytes;

	item_size = sizeof(struct btrfs_extent_item) +
		    btrfs_extent_inline_ref_size(BTRFS_EXTENT_DATA_REF_KEY);

	ret = btrfs_insert_empty_item(trans, extent_root, path, &key, item_size);
	if (ret) {
		if (ret == -EEXIST) {
			/* Someone else already inserted this extent — likely the
			 * to_insert_filtered is stale or we are on a resume.
			 * Skip silently. */
			btrfs_free_path(path);
			return 0;
		}
		fprintf(stderr, "ERR: insert EXTENT_ITEM bytenr=%llu: %d\n",
			(unsigned long long)ent->bytenr, ret);
		btrfs_free_path(path);
		return ret;
	}

	leaf = path->nodes[0];
	ei = btrfs_item_ptr(leaf, path->slots[0], struct btrfs_extent_item);
	btrfs_set_extent_refs(leaf, ei, ent->total_refs);
	btrfs_set_extent_generation(leaf, ei, trans->transid);
	btrfs_set_extent_flags(leaf, ei, BTRFS_EXTENT_FLAG_DATA);

	iref = (struct btrfs_extent_inline_ref *)(ei + 1);
	btrfs_set_extent_inline_ref_type(leaf, iref, BTRFS_EXTENT_DATA_REF_KEY);
	dref = (struct btrfs_extent_data_ref *)(&iref->offset);
	btrfs_set_extent_data_ref_root(leaf, dref, ent->refs[0].root);
	btrfs_set_extent_data_ref_objectid(leaf, dref, ent->refs[0].inode);
	btrfs_set_extent_data_ref_offset(leaf, dref, ent->refs[0].ref_offset);
	btrfs_set_extent_data_ref_count(leaf, dref, ent->refs[0].count);

	btrfs_mark_buffer_dirty(leaf);
	btrfs_release_path(path);

	/* 2. Additional refs as separate EXTENT_DATA_REF items */
	for (int i = 1; i < ent->nrefs; i++) {
		key.objectid = ent->bytenr;
		key.type = BTRFS_EXTENT_DATA_REF_KEY;
		key.offset = hash_extent_data_ref(ent->refs[i].root,
						   ent->refs[i].inode,
						   ent->refs[i].ref_offset);

		ret = btrfs_insert_empty_item(trans, extent_root, path, &key,
					       sizeof(struct btrfs_extent_data_ref));
		if (ret == -EEXIST) {
			btrfs_release_path(path);
			continue;
		}
		if (ret) {
			fprintf(stderr, "ERR: insert EXTENT_DATA_REF bytenr=%llu hash=%llu: %d\n",
				(unsigned long long)ent->bytenr,
				(unsigned long long)key.offset, ret);
			btrfs_free_path(path);
			return ret;
		}
		leaf = path->nodes[0];
		dref = btrfs_item_ptr(leaf, path->slots[0], struct btrfs_extent_data_ref);
		btrfs_set_extent_data_ref_root(leaf, dref, ent->refs[i].root);
		btrfs_set_extent_data_ref_objectid(leaf, dref, ent->refs[i].inode);
		btrfs_set_extent_data_ref_offset(leaf, dref, ent->refs[i].ref_offset);
		btrfs_set_extent_data_ref_count(leaf, dref, ent->refs[i].count);
		btrfs_mark_buffer_dirty(leaf);
		btrfs_release_path(path);
	}

	btrfs_free_path(path);

	/* 3. Update block group / SB bytes_used */
	ret = btrfs_update_block_group(trans, ent->bytenr, ent->num_bytes, 1, 0);
	if (ret) {
		fprintf(stderr, "WARN: update_block_group bytenr=%llu: %d\n",
			(unsigned long long)ent->bytenr, ret);
		/* Non-fatal for now; log and continue */
	}

	return 0;
}

static void save_watermark(const char *path, u64 bytenr, u64 num_bytes,
			    u64 inserted, u64 skipped)
{
	char tmp[1024];
	snprintf(tmp, sizeof(tmp), "%s.tmp", path);
	FILE *fp = fopen(tmp, "w");
	if (!fp) return;
	fprintf(fp, "bytenr=%llu\nnum_bytes=%llu\ninserted=%llu\nskipped=%llu\n",
		(unsigned long long)bytenr, (unsigned long long)num_bytes,
		(unsigned long long)inserted, (unsigned long long)skipped);
	fflush(fp);
	fsync(fileno(fp));
	fclose(fp);
	rename(tmp, path);
}

int main(int argc, char *argv[])
{
	if (argc < 6) {
		fprintf(stderr, "Usage: %s <device> <refs_folded> <filter> <watermark> --write|--dryrun\n", argv[0]);
		return 1;
	}

	int do_write = strcmp(argv[5], "--write") == 0;
	printf("=== rebuild_extent_tree_apply %s ===\n", do_write ? "WRITE" : "DRY-RUN");

	if (load_filter(argv[3]) < 0) return 1;

	FILE *rf = fopen(argv[2], "r");
	if (!rf) { perror("refs open"); return 1; }

	struct open_ctree_args oca = {
		.filename = argv[1],
		.flags = OPEN_CTREE_PARTIAL | OPEN_CTREE_ALLOW_TRANSID_MISMATCH |
			 OPEN_CTREE_SKIP_CSUM_CHECK | OPEN_CTREE_SKIP_LEAF_ITEM_CHECKS |
			 (do_write ? OPEN_CTREE_WRITES : 0),
	};
	struct btrfs_fs_info *fs_info = open_ctree_fs_info(&oca);
	if (!fs_info) { fprintf(stderr, "ERROR: open_ctree\n"); return 1; }

	struct btrfs_root *extent_root = btrfs_extent_root(fs_info, 0);
	if (!extent_root || IS_ERR(extent_root)) {
		fprintf(stderr, "ERROR: extent_root\n");
		close_ctree(fs_info->tree_root);
		return 1;
	}

	fs_info->rebuilding_extent_tree = 1;
	struct btrfs_trans_handle *trans = NULL;
	if (do_write) {
		trans = btrfs_start_transaction(extent_root, 1);
		if (IS_ERR(trans)) {
			fprintf(stderr, "ERROR: start_transaction\n");
			close_ctree(fs_info->tree_root);
			return 1;
		}
		trans->reinit_extent_tree = true;
	}

	struct extent_entry current = {0};
	int have_current = 0;
	u64 total_examined = 0, matched = 0, inserted = 0, skipped = 0;
	int chunk_count = 0;
	time_t start = time(NULL);

	char line[512];
	while (fgets(line, sizeof(line), rf)) {
		u64 b, n, r, i, off, g;
		int c, comp;
		if (sscanf(line, "%llu|%llu|%llu|%llu|%llu|%d|%llu|%d",
			   (unsigned long long*)&b, (unsigned long long*)&n,
			   (unsigned long long*)&r, (unsigned long long*)&i,
			   (unsigned long long*)&off, &c,
			   (unsigned long long*)&g, &comp) != 8)
			continue;
		total_examined++;

		if (have_current && (current.bytenr != b || current.num_bytes != n)) {
			/* Flush current */
			if (filter_contains(current.bytenr, current.num_bytes)) {
				matched++;
				if (do_write) {
					int ret = insert_one_extent(trans, extent_root, &current);
					if (ret == 0) inserted++;
					else skipped++;
				} else {
					inserted++;
				}
				if (do_write && (inserted % CHUNK_SIZE == 0) && inserted > 0) {
					chunk_count++;
					printf("CHUNK %d: inserted=%llu (of matched=%llu) examined=%llu elapsed=%lds\n",
					       chunk_count,
					       (unsigned long long)inserted,
					       (unsigned long long)matched,
					       (unsigned long long)total_examined,
					       (long)(time(NULL) - start));
					fflush(stdout);
					int cret = btrfs_commit_transaction(trans, extent_root);
					if (cret) {
						fprintf(stderr, "ERROR: commit chunk %d: %d\n", chunk_count, cret);
						close_ctree(fs_info->tree_root);
						return 1;
					}
					save_watermark(argv[4], current.bytenr, current.num_bytes, inserted, skipped);
					trans = btrfs_start_transaction(extent_root, 1);
					if (IS_ERR(trans)) {
						fprintf(stderr, "ERROR: restart transaction after chunk %d\n", chunk_count);
						close_ctree(fs_info->tree_root);
						return 1;
					}
					trans->reinit_extent_tree = true;
					if (inserted % THROTTLE_INTERVAL == 0) {
						printf("THROTTLE: sleeping %ds for SMR re-shingle\n", THROTTLE_SLEEP_SEC);
						fflush(stdout);
						sleep(THROTTLE_SLEEP_SEC);
					}
				}
			} else {
				skipped++;
			}
			memset(&current, 0, sizeof(current));
			have_current = 0;
		}

		if (!have_current) {
			current.bytenr = b;
			current.num_bytes = n;
			current.nrefs = 0;
			current.total_refs = 0;
			have_current = 1;
		}
		if (current.nrefs < MAX_REFS_PER_EXTENT) {
			current.refs[current.nrefs].root = r;
			current.refs[current.nrefs].inode = i;
			current.refs[current.nrefs].ref_offset = off;
			current.refs[current.nrefs].count = c;
			current.nrefs++;
			current.total_refs += c;
		}
	}

	/* Flush last */
	if (have_current) {
		if (filter_contains(current.bytenr, current.num_bytes)) {
			matched++;
			if (do_write) {
				int ret = insert_one_extent(trans, extent_root, &current);
				if (ret == 0) inserted++;
				else skipped++;
			} else {
				inserted++;
			}
		} else {
			skipped++;
		}
	}

	fclose(rf);

	if (do_write) {
		int cret = btrfs_commit_transaction(trans, extent_root);
		if (cret) {
			fprintf(stderr, "ERROR: final commit: %d\n", cret);
			close_ctree(fs_info->tree_root);
			return 1;
		}
		save_watermark(argv[4], 0, 0, inserted, skipped);
	}

	printf("\n=== STATS ===\n");
	printf("examined: %llu\n", (unsigned long long)total_examined);
	printf("matched filter: %llu\n", (unsigned long long)matched);
	printf("inserted: %llu\n", (unsigned long long)inserted);
	printf("skipped (pre-filtered or EEXIST): %llu\n", (unsigned long long)skipped);
	printf("elapsed: %ld seconds\n", (long)(time(NULL) - start));

	close_ctree(fs_info->tree_root);
	return 0;
}
