/*-
 * Copyright (c) 2014-2020 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static void __bm_method_set(WT_BM *, bool);

/*
 * __bm_readonly --
 *     General-purpose "writes not supported on this handle" function.
 */
static int
__bm_readonly(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_RET_MSG(
      session, ENOTSUP, "%s: write operation on read-only checkpoint handle", bm->block->name);
}

/*
 * __bm_addr_invalid --
 *     Return an error code if an address cookie is invalid.
 */
static int
__bm_addr_invalid(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
    return (__wt_block_addr_invalid(session, bm->block, addr, addr_size, bm->is_live));
}

/*
 * __bm_addr_string --
 *     Return a printable string representation of an address cookie.
 */
static int
__bm_addr_string(
  WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf, const uint8_t *addr, size_t addr_size)
{
    return (__wt_block_addr_string(session, bm->block, buf, addr, addr_size));
}

/*
 * __bm_block_header --
 *     Return the size of the block header.
 */
static u_int
__bm_block_header(WT_BM *bm)
{
    return (__wt_block_header(bm->block));
}

// 这个函数做了两件事：1. 写数据 2. checkpoint
// eg:
//  WT_ERR(checkpoint ? bm->checkpoint(bm, session, ip, btree->ckpt, data_checksum) :
//                         bm->write(bm, session, ip, addr, addr_sizep, data_checksum, checkpoint_io));
/*
 * __bm_checkpoint --
 *     Write a buffer into a block, creating a checkpoint.
 */
static int
__bm_checkpoint(
  WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf, WT_CKPT *ckptbase, bool data_checksum)
{
    return (__wt_block_checkpoint(session, bm->block, buf, ckptbase, data_checksum));
}

/*
 * __bm_checkpoint_last --
 *     Return information for the last known file checkpoint.
 */
static int
__bm_checkpoint_last(WT_BM *bm, WT_SESSION_IMPL *session, char **metadatap, char **checkpoint_listp,
  WT_ITEM *checkpoint)
{
    return (
      __wt_block_checkpoint_last(session, bm->block, metadatap, checkpoint_listp, checkpoint));
}

/*
 * __bm_checkpoint_readonly --
 *     Write a buffer into a block, creating a checkpoint; readonly version.
 */
static int
__bm_checkpoint_readonly(
  WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf, WT_CKPT *ckptbase, bool data_checksum)
{
    WT_UNUSED(buf);
    WT_UNUSED(ckptbase);
    WT_UNUSED(data_checksum);

    return (__bm_readonly(bm, session));
}

// __bm_checkpoint_load的一个调用栈
// #0  __bm_checkpoint_load (bm=0x631b90, session=0x7ffff7fab010,
//     addr=0x6324d0 "\001\205\201\344y\224\f0\206\201\344BA\a\264\207\201\344xW)\201\200\200\200\342o\300\317\300p_txn=-11,Q", addr_size=30,
//     root_addr=0x7fffffffd560 "access_pattern_hone),block_allocread_timestamp=n_timestamp=none,amp=none,durableint=none,allocat", root_addr_sizep=0x7fffffffd660,
//     checkpoint=false) at src/block/block_mgr.c:106
// #1  0x00007ffff77e4f65 in __wt_btree_open (session=0x7ffff7fab010, op_cfg=0x7fffffffdb60) at src/btree/bt_handle.c:151
// #2  0x00007ffff784ddee in __wt_conn_dhandle_open (session=0x7ffff7fab010, cfg=0x7fffffffdb60, flags=0) at src/conn/conn_dhandle.c:466
// #3  0x00007ffff793ac3f in __wt_session_get_dhandle (session=0x7ffff7fab010, uri=0x7ffff7983d36 "file:WiredTiger.wt", checkpoint=0x0, cfg=0x7fffffffdb60, flags=0)
//     at src/session/session_dhandle.c:504
// #4  0x00007ffff793abe8 in __wt_session_get_dhandle (session=0x7ffff7fab010, uri=0x7ffff7983d36 "file:WiredTiger.wt", checkpoint=0x0, cfg=0x7fffffffdb60, flags=0)
//     at src/session/session_dhandle.c:497
// #5  0x00007ffff793a46c in __wt_session_get_btree_ckpt (session=0x7ffff7fab010, uri=0x7ffff7983d36 "file:WiredTiger.wt", cfg=0x7fffffffdb60, flags=0)
//     at src/session/session_dhandle.c:320
// #6  0x00007ffff7872094 in __wt_curfile_open (session=0x7ffff7fab010, uri=0x7ffff7983d36 "file:WiredTiger.wt", owner=0x0, cfg=0x7fffffffdb60, cursorp=0x7fffffffdbc0)
//     at src/cursor/cur_file.c:805
// #7  0x00007ffff7927e89 in __session_open_cursor_int (session=0x7ffff7fab010, uri=0x7ffff7983d36 "file:WiredTiger.wt", owner=0x0, other=0x0, cfg=0x7fffffffdb60,
//     cursorp=0x7fffffffdbc0) at src/session/session_api.c:464
// #8  0x00007ffff7928215 in __wt_open_cursor (session=0x7ffff7fab010, uri=0x7ffff7983d36 "file:WiredTiger.wt", owner=0x0, cfg=0x7fffffffdb60, cursorp=0x7fffffffdbc0)
//     at src/session/session_api.c:528
// #9  0x00007ffff78db7cd in __wt_metadata_cursor_open (session=0x7ffff7fab010, config=0x0, cursorp=0x7fffffffdbc0) at src/meta/meta_table.c:66
// #10 0x00007ffff78db8e0 in __wt_metadata_cursor (session=0x7ffff7fab010, cursorp=0x0) at src/meta/meta_table.c:108
// #11 0x00007ffff7846ea1 in wiredtiger_open (home=0x400978 "./wt_meta", event_handler=0x0, config=0x400940 "create,io_capacity=(total=40MB),file_extend=(data=16MB)",
//     connectionp=0x7fffffffddd8) at src/conn/conn_api.c:2671
// #12 0x0000000000400774 in main () at test_wt.c:34

/*
 * 从上面的调用栈来看，只有一个btree是readonly的时候，checkpoint参数才为true。
 * 所以，wt的命名我真是服了。。。
 */
/*
 * __bm_checkpoint_load --
 *     Load a checkpoint.
 */
static int
__bm_checkpoint_load(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size,
  uint8_t *root_addr, size_t *root_addr_sizep, bool checkpoint)
{
    /* If not opening a checkpoint, we're opening the live system. */
    bm->is_live = !checkpoint;
    WT_RET(__wt_block_checkpoint_load(
      session, bm->block, addr, addr_size, root_addr, root_addr_sizep, checkpoint));

    if (checkpoint) {
        /*
         * Read-only objects are optionally mapped into memory instead of being read into cache
         * buffers.
         */
        WT_RET(__wt_block_map(session, bm->block, &bm->map, &bm->maplen, &bm->mapped_cookie));

        /*
         * If this handle is for a checkpoint, that is, read-only, there isn't a lot you can do with
         * it. Although the btree layer prevents attempts to write a checkpoint reference, paranoia
         * is healthy.
         */
        __bm_method_set(bm, true);
    }

    return (0);
}

/*
 * __bm_checkpoint_resolve --
 *     Resolve the checkpoint.
 */
static int
__bm_checkpoint_resolve(WT_BM *bm, WT_SESSION_IMPL *session, bool failed)
{
    return (__wt_block_checkpoint_resolve(session, bm->block, failed));
}

/*
 * __bm_checkpoint_resolve_readonly --
 *     Resolve the checkpoint; readonly version.
 */
static int
__bm_checkpoint_resolve_readonly(WT_BM *bm, WT_SESSION_IMPL *session, bool failed)
{
    WT_UNUSED(failed);

    return (__bm_readonly(bm, session));
}

/*
 * __bm_checkpoint_start --
 *     Start the checkpoint.
 */
static int
__bm_checkpoint_start(WT_BM *bm, WT_SESSION_IMPL *session)
{
    return (__wt_block_checkpoint_start(session, bm->block));
}

/*
 * __bm_checkpoint_start_readonly --
 *     Start the checkpoint; readonly version.
 */
static int
__bm_checkpoint_start_readonly(WT_BM *bm, WT_SESSION_IMPL *session)
{
    return (__bm_readonly(bm, session));
}

/*
 * __bm_checkpoint_unload --
 *     Unload a checkpoint point.
 */
static int
__bm_checkpoint_unload(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_DECL_RET;

    /* Unmap any mapped segment. */
    if (bm->map != NULL)
        WT_TRET(__wt_block_unmap(session, bm->block, bm->map, bm->maplen, &bm->mapped_cookie));

    /* Unload the checkpoint. */
    WT_TRET(__wt_block_checkpoint_unload(session, bm->block, !bm->is_live));

    return (ret);
}

/*
 * __bm_close --
 *     Close a file.
 */
static int
__bm_close(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_DECL_RET;

    if (bm == NULL) /* Safety check */
        return (0);

    ret = __wt_block_close(session, bm->block);

    __wt_overwrite_and_free(session, bm);
    return (ret);
}

/*
 * __bm_compact_end --
 *     End a block manager compaction.
 */
static int
__bm_compact_end(WT_BM *bm, WT_SESSION_IMPL *session)
{
    return (__wt_block_compact_end(session, bm->block));
}

/*
 * __bm_compact_end_readonly --
 *     End a block manager compaction; readonly version.
 */
static int
__bm_compact_end_readonly(WT_BM *bm, WT_SESSION_IMPL *session)
{
    return (__bm_readonly(bm, session));
}

/*
 * __bm_compact_page_skip --
 *     Return if a page is useful for compaction.
 */
static int
__bm_compact_page_skip(
  WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size, bool *skipp)
{
    return (__wt_block_compact_page_skip(session, bm->block, addr, addr_size, skipp));
}

/*
 * __bm_compact_page_skip_readonly --
 *     Return if a page is useful for compaction; readonly version.
 */
static int
__bm_compact_page_skip_readonly(
  WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size, bool *skipp)
{
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);
    WT_UNUSED(skipp);

    return (__bm_readonly(bm, session));
}

/*
 * __bm_compact_skip --
 *     Return if a file can be compacted.
 */
static int
__bm_compact_skip(WT_BM *bm, WT_SESSION_IMPL *session, bool *skipp)
{
    return (__wt_block_compact_skip(session, bm->block, skipp));
}

/*
 * __bm_compact_skip_readonly --
 *     Return if a file can be compacted; readonly version.
 */
static int
__bm_compact_skip_readonly(WT_BM *bm, WT_SESSION_IMPL *session, bool *skipp)
{
    WT_UNUSED(skipp);

    return (__bm_readonly(bm, session));
}

/*
 * __bm_compact_start --
 *     Start a block manager compaction.
 */
static int
__bm_compact_start(WT_BM *bm, WT_SESSION_IMPL *session)
{
    return (__wt_block_compact_start(session, bm->block));
}

/*
 * __bm_compact_start_readonly --
 *     Start a block manager compaction; readonly version.
 */
static int
__bm_compact_start_readonly(WT_BM *bm, WT_SESSION_IMPL *session)
{
    return (__bm_readonly(bm, session));
}

/*
 * __bm_free --
 *     Free a block of space to the underlying file.
 */
static int
__bm_free(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
    return (__wt_block_free(session, bm->block, addr, addr_size));
}

/*
 * __bm_free_readonly --
 *     Free a block of space to the underlying file; readonly version.
 */
static int
__bm_free_readonly(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);

    return (__bm_readonly(bm, session));
}

/*
 * __bm_is_mapped --
 *     Return if the file is mapped into memory.
 */
static bool
__bm_is_mapped(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_UNUSED(session);

    return (bm->map == NULL ? false : true);
}

/*
 * __bm_map_discard --
 *     Discard a mapped segment.
 */
static int
__bm_map_discard(WT_BM *bm, WT_SESSION_IMPL *session, void *map, size_t len)
{
    WT_FILE_HANDLE *handle;

    handle = bm->block->fh->handle;
    return (handle->fh_map_discard(handle, (WT_SESSION *)session, map, len, bm->mapped_cookie));
}

/*
 * __bm_salvage_end --
 *     End a block manager salvage.
 */
static int
__bm_salvage_end(WT_BM *bm, WT_SESSION_IMPL *session)
{
    return (__wt_block_salvage_end(session, bm->block));
}

/*
 * __bm_salvage_end_readonly --
 *     End a block manager salvage; readonly version.
 */
static int
__bm_salvage_end_readonly(WT_BM *bm, WT_SESSION_IMPL *session)
{
    return (__bm_readonly(bm, session));
}

/*
 * __bm_salvage_next_readonly --
 *     Return the next block from the file; readonly version.
 */
static int
__bm_salvage_next_readonly(
  WT_BM *bm, WT_SESSION_IMPL *session, uint8_t *addr, size_t *addr_sizep, bool *eofp)
{
    WT_UNUSED(addr);
    WT_UNUSED(addr_sizep);
    WT_UNUSED(eofp);

    return (__bm_readonly(bm, session));
}

/*
 * __bm_salvage_next --
 *     Return the next block from the file.
 */
static int
__bm_salvage_next(
  WT_BM *bm, WT_SESSION_IMPL *session, uint8_t *addr, size_t *addr_sizep, bool *eofp)
{
    return (__wt_block_salvage_next(session, bm->block, addr, addr_sizep, eofp));
}

/*
 * __bm_salvage_start --
 *     Start a block manager salvage.
 */
static int
__bm_salvage_start(WT_BM *bm, WT_SESSION_IMPL *session)
{
    return (__wt_block_salvage_start(session, bm->block));
}

/*
 * __bm_salvage_start_readonly --
 *     Start a block manager salvage; readonly version.
 */
static int
__bm_salvage_start_readonly(WT_BM *bm, WT_SESSION_IMPL *session)
{
    return (__bm_readonly(bm, session));
}

/*
 * __bm_salvage_valid --
 *     Inform salvage a block is valid.
 */
static int
__bm_salvage_valid(WT_BM *bm, WT_SESSION_IMPL *session, uint8_t *addr, size_t addr_size, bool valid)
{
    return (__wt_block_salvage_valid(session, bm->block, addr, addr_size, valid));
}

/*
 * __bm_salvage_valid_readonly --
 *     Inform salvage a block is valid; readonly version.
 */
static int
__bm_salvage_valid_readonly(
  WT_BM *bm, WT_SESSION_IMPL *session, uint8_t *addr, size_t addr_size, bool valid)
{
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);
    WT_UNUSED(valid);

    return (__bm_readonly(bm, session));
}

/*
 * __bm_stat --
 *     Block-manager statistics.
 */
static int
__bm_stat(WT_BM *bm, WT_SESSION_IMPL *session, WT_DSRC_STATS *stats)
{
    __wt_block_stat(session, bm->block, stats);
    return (0);
}

/*
 * __bm_sync --
 *     Flush a file to disk.
 */
static int
__bm_sync(WT_BM *bm, WT_SESSION_IMPL *session, bool block)
{
    return (__wt_fsync(session, bm->block->fh, block));
}

/*
 * __bm_sync_readonly --
 *     Flush a file to disk; readonly version.
 */
static int
__bm_sync_readonly(WT_BM *bm, WT_SESSION_IMPL *session, bool async)
{
    WT_UNUSED(async);

    return (__bm_readonly(bm, session));
}

/*
 * __bm_verify_addr --
 *     Verify an address.
 */
static int
__bm_verify_addr(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
    return (__wt_block_verify_addr(session, bm->block, addr, addr_size));
}

/*
 * __bm_verify_end --
 *     End a block manager verify.
 */
static int
__bm_verify_end(WT_BM *bm, WT_SESSION_IMPL *session)
{
    return (__wt_block_verify_end(session, bm->block));
}

/*
 * __bm_verify_start --
 *     Start a block manager verify.
 */
static int
__bm_verify_start(WT_BM *bm, WT_SESSION_IMPL *session, WT_CKPT *ckptbase, const char *cfg[])
{
    return (__wt_block_verify_start(session, bm->block, ckptbase, cfg));
}

/*
 * __bm_write --
 *     Write a buffer into a block, returning the block's address cookie.
 */
static int
__bm_write(WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf, uint8_t *addr, size_t *addr_sizep,
  bool data_checksum, bool checkpoint_io)
{
    __wt_capacity_throttle(
      session, buf->size, checkpoint_io ? WT_THROTTLE_CKPT : WT_THROTTLE_EVICT);
    return (
      __wt_block_write(session, bm->block, buf, addr, addr_sizep, data_checksum, checkpoint_io));
}

/*
 * __bm_write_readonly --
 *     Write a buffer into a block, returning the block's address cookie; readonly version.
 */
static int
__bm_write_readonly(WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf, uint8_t *addr,
  size_t *addr_sizep, bool data_checksum, bool checkpoint_io)
{
    WT_UNUSED(buf);
    WT_UNUSED(addr);
    WT_UNUSED(addr_sizep);
    WT_UNUSED(data_checksum);
    WT_UNUSED(checkpoint_io);

    return (__bm_readonly(bm, session));
}

// 如果要实际写入块中，块中的size可能比sizep本身所指的size大，因为有block header，字节对齐等，因此，
// 这个函数用来返回实际写入磁盘所占用的空间
/*
 * __bm_write_size --
 *     Return the buffer size required to write a block.
 */
static int
__bm_write_size(WT_BM *bm, WT_SESSION_IMPL *session, size_t *sizep)
{
    return (__wt_block_write_size(session, bm->block, sizep));
}

/*
 * __bm_write_size_readonly --
 *     Return the buffer size required to write a block; readonly version.
 */
static int
__bm_write_size_readonly(WT_BM *bm, WT_SESSION_IMPL *session, size_t *sizep)
{
    WT_UNUSED(sizep);

    return (__bm_readonly(bm, session));
}

/*
 * __bm_method_set --
 *     Set up the legal methods.
 */
static void
__bm_method_set(WT_BM *bm, bool readonly)
{
    bm->addr_invalid = __bm_addr_invalid; // ar (ar means aleady read)
    bm->addr_string = __bm_addr_string; // ar
    bm->block_header = __bm_block_header; // ar
    bm->checkpoint = __bm_checkpoint;  // TODO: 大概扫了下这个函数应该是上层做checkpoint时block层的实现。涉及的东西有点多，建议还是后面再看吧... 
    bm->checkpoint_last = __bm_checkpoint_last;  // TODO: reading here 2020-8-26-21:29
    bm->checkpoint_load = __bm_checkpoint_load;
    bm->checkpoint_resolve = __bm_checkpoint_resolve;
    bm->checkpoint_start = __bm_checkpoint_start;
    bm->checkpoint_unload = __bm_checkpoint_unload;
    bm->close = __bm_close; // ar
    bm->compact_end = __bm_compact_end; // ar
    bm->compact_page_skip = __bm_compact_page_skip; // du (du means dont understand)
    bm->compact_skip = __bm_compact_skip; // ar
    bm->compact_start = __bm_compact_start; // 初始化一些状态数据 (ar)
    bm->corrupt = __wt_bm_corrupt; // ar 
    bm->free = __bm_free; // ar reading here 2020-8-27-21:46
    bm->is_mapped = __bm_is_mapped; // du TODO: 可能和mmap相关，http://source.wiredtiger.com/3.2.1/group__wt.html#ga9e6adae3fc6964ef837a62795c7840ed
    bm->map_discard = __bm_map_discard; // TODO: 关于map的事情后面再看
    bm->preload = __wt_bm_preload; // ar, 不太重要，pass
    bm->read = __wt_bm_read; // 已读完
    bm->salvage_end = __bm_salvage_end; // TODO: salvage逻辑以后再看，不是核心业务逻辑
    bm->salvage_next = __bm_salvage_next;
    bm->salvage_start = __bm_salvage_start;
    bm->salvage_valid = __bm_salvage_valid;
    bm->size = __wt_block_manager_size; // ar
    bm->stat = __bm_stat; // ar
    bm->sync = __bm_sync; // ar
    bm->verify_addr = __bm_verify_addr; // TODO: 下面再读
    bm->verify_end = __bm_verify_end;
    bm->verify_start = __bm_verify_start;
    bm->write = __bm_write;  // 已读完
    bm->write_size = __bm_write_size; // ar

    if (readonly) {
        bm->checkpoint = __bm_checkpoint_readonly;
        bm->checkpoint_resolve = __bm_checkpoint_resolve_readonly;
        bm->checkpoint_start = __bm_checkpoint_start_readonly;
        bm->compact_end = __bm_compact_end_readonly;
        bm->compact_page_skip = __bm_compact_page_skip_readonly;
        bm->compact_skip = __bm_compact_skip_readonly;
        bm->compact_start = __bm_compact_start_readonly;
        bm->free = __bm_free_readonly;
        bm->salvage_end = __bm_salvage_end_readonly;
        bm->salvage_next = __bm_salvage_next_readonly;
        bm->salvage_start = __bm_salvage_start_readonly;
        bm->salvage_valid = __bm_salvage_valid_readonly;
        bm->sync = __bm_sync_readonly;
        bm->write = __bm_write_readonly;
        bm->write_size = __bm_write_size_readonly;
    }
}

/*
 * __wt_block_manager_open --
 *     Open a file. 这里的file就是*.wt文件, 所以，一个block manager对应一个文件
 */
int
__wt_block_manager_open(WT_SESSION_IMPL *session, const char *filename, const char *cfg[],
  bool forced_salvage, bool readonly, uint32_t allocsize, WT_BM **bmp)
{
    MY_PRINTF("yui %s\n", filename);
    WT_BM *bm;
    WT_DECL_RET;

    *bmp = NULL;

    WT_RET(__wt_calloc_one(session, &bm));
    __bm_method_set(bm, false);

    WT_ERR(
      __wt_block_open(session, filename, cfg, forced_salvage, readonly, allocsize, &bm->block));

    *bmp = bm;
    return (0);

err:
    WT_TRET(bm->close(bm, session));
    return (ret);
}

/*
 * __wt_block_panic --
 *     Report an error, then panic the handle and the system.
 */
int
__wt_block_panic(WT_SESSION_IMPL *session) WT_GCC_FUNC_ATTRIBUTE((cold))
{
    /* Switch the handle into read-only mode. */
    __bm_method_set(S2BT(session)->bm, true);

    return (__wt_panic(session));
}
