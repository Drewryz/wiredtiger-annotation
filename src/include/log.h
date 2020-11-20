/*-
 * Copyright (c) 2014-2020 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/* AUTOMATIC FLAG VALUE GENERATION START */
#define WT_LOGSCAN_FIRST 0x01u
#define WT_LOGSCAN_FROM_CKP 0x02u
#define WT_LOGSCAN_ONE 0x04u
#define WT_LOGSCAN_RECOVER 0x08u
#define WT_LOGSCAN_RECOVER_METADATA 0x10u
/* AUTOMATIC FLAG VALUE GENERATION STOP */

/* AUTOMATIC FLAG VALUE GENERATION START */
#define WT_LOG_BACKGROUND 0x01u
#define WT_LOG_DSYNC 0x02u
#define WT_LOG_FLUSH 0x04u // TODO: 这个标识没有看明白
#define WT_LOG_FSYNC 0x08u // transaction_sync=(method=fsync)
#define WT_LOG_SYNC_ENABLED 0x10u // transaction_sync=(enabled=true)
/* AUTOMATIC FLAG VALUE GENERATION STOP */

#define WT_LOGOP_IGNORE 0x80000000
#define WT_LOGOP_IS_IGNORED(val) ((val)&WT_LOGOP_IGNORE)

/*
 * WT_LSN --
 *	A log sequence number, representing a position in the transaction log.
 */
union __wt_lsn {
    struct {
#ifdef WORDS_BIGENDIAN
        uint32_t file;
        uint32_t offset;
#else
        uint32_t offset;
        uint32_t file;
#endif
    } l;
    /*
     *  63                     0
     *   ----------------------
     *  |   file  |   offset   |
     *   ----------------------
     */
    uint64_t file_offset;
};

#define WT_LOG_FILENAME "WiredTigerLog"     /* Log file name */
#define WT_LOG_PREPNAME "WiredTigerPreplog" /* Log pre-allocated name */
#define WT_LOG_TMPNAME "WiredTigerTmplog"   /* Log temporary name */

/* Logging subsystem declarations. */
#define WT_LOG_ALIGN 128

/*
 * Atomically set the two components of the LSN.
 */
#define WT_SET_LSN(l, f, o) (l)->file_offset = (((uint64_t)(f) << 32) + (o))

#define WT_INIT_LSN(l) WT_SET_LSN((l), 1, 0)

#define WT_MAX_LSN(l) WT_SET_LSN((l), UINT32_MAX, INT32_MAX)

#define WT_ZERO_LSN(l) WT_SET_LSN((l), 0, 0)

/*
 * Test for initial LSN. We only need to shift the 1 for comparison.
 */
#define WT_IS_INIT_LSN(l) ((l)->file_offset == ((uint64_t)1 << 32))
/*
 * Original tested INT32_MAX. But if we read one from an older release we may see UINT32_MAX.
 */
#define WT_IS_MAX_LSN(lsn) \
    ((lsn)->l.file == UINT32_MAX && ((lsn)->l.offset == INT32_MAX || (lsn)->l.offset == UINT32_MAX))
/*
 * Test for zero LSN.
 */
#define WT_IS_ZERO_LSN(l) ((l)->file_offset == 0)

/*
 * Macro to print an LSN.
 */
#define WT_LSN_MSG(lsn, msg) \
    __wt_msg(session, "%s LSN: [%" PRIu32 "][%" PRIu32 "]", (msg), (lsn)->l.file, (lsn)->l.offset)

/*
 * Both of the macros below need to change if the content of __wt_lsn ever changes. The value is the
 * following: txnid, record type, operation type, file id, operation key, operation value
 */
#define WT_LOGC_KEY_FORMAT WT_UNCHECKED_STRING(III)
#define WT_LOGC_VALUE_FORMAT WT_UNCHECKED_STRING(qIIIuu)

/*
 * Size range for the log files.
 */
#define WT_LOG_FILE_MAX ((int64_t)2 * WT_GIGABYTE)
#define WT_LOG_FILE_MIN (100 * WT_KILOBYTE)

#define WT_LOG_SKIP_HEADER(data) ((const uint8_t *)(data) + offsetof(WT_LOG_RECORD, record))
#define WT_LOG_REC_SIZE(size) ((size)-offsetof(WT_LOG_RECORD, record))

// 我们分配缓冲区大小，但当我们越过缓冲区的最大大小的一半时触发slot切换。如果一个记录超过了缓冲区的最大值，那么我们触发一个slot切换，并写入未缓冲的记录。
// 我们使用一个更大的缓冲区来提供溢出空间，以便在超过阈值时进行切换。
/*
 * WT_LOG_SLOT_BUF_SIZE是slot可以设置成的最大的buffer size
 * log->slot_buf_size是运行时真正的slot buffer size 
 * WT_LOG_SLOT_BUF_MAX是单个事务的log最大可以缓存在slot buffer中的size，超过需要unbuffered
 * 假设log->slot_buf_size = WT_LOG_SLOT_BUF_SIZE，则有下面的结构图
 *     31                         19                         18                     17                             0
 *  -------------------------------------------------------------------------------------------------------------------
 * |          | ...... |  WT_LOG_SLOT_UNBUFFERED   |  log->slot_buf_size   |   WT_LOG_SLOT_BUF_MAX   | ...... |        |
 *  -------------------------------------------------------------------------------------------------------------------
 * 
 */
/*
 * We allocate the buffer size, but trigger a slot switch when we cross the maximum size of half the
 * buffer. If a record is more than the buffer maximum then we trigger a slot switch and write that
 * record unbuffered. We use a larger buffer to provide overflow space so that we can switch once we
 * cross the threshold.
 */
#define WT_LOG_SLOT_BUF_SIZE (256 * 1024) /* Must be power of 2 */ // 2^8 * 2^10
#define WT_LOG_SLOT_BUF_MAX ((uint32_t)log->slot_buf_size / 2)
#define WT_LOG_SLOT_UNBUFFERED (WT_LOG_SLOT_BUF_SIZE << 1)

/*
 * Possible values for the consolidation array slot states:
 *
 * WT_LOG_SLOT_CLOSE - slot is in use but closed to new joins.
 *
 * WT_LOG_SLOT_FREE - slot is available for allocation.
 *
 * WT_LOG_SLOT_WRITTEN - slot is written and should be processed by worker.
 *
 * The slot state must be volatile: threads loop checking the state and can't cache the first value
 * they see.
 *
 * The slot state is divided into two 32 bit sizes. One half is the amount joined and the other is
 * the amount released. Since we use a few special states, reserve the top few bits for state. That
 * makes the maximum size less than 32 bits for both joined and released.
 */
/*
 * XXX The log slot bits are signed and should be rewritten as unsigned. For now, give the logging
 * subsystem its own flags macro.
 */
#define FLD_LOG_SLOT_ISSET(field, mask) (((field) & (uint64_t)(mask)) != 0)

/*
 * The high bit is reserved for the special states. If the high bit is set (WT_LOG_SLOT_RESERVED)
 * then we are guaranteed to be in a special state.
 * 只要slot.state设置为WT_LOG_SLOT_FREE或者WT_LOG_SLOT_WRITTEN，WT_LOG_SLOT_RESERVED都被设置成了1。牛逼，还能这么玩。
 */
#define WT_LOG_SLOT_FREE (-1)    /* Not in use */
#define WT_LOG_SLOT_WRITTEN (-2) /* Slot data written, not processed */

/*
 * If new slot states are added, adjust WT_LOG_SLOT_BITS and WT_LOG_SLOT_MASK_OFF accordingly for
 * how much of the top 32 bits we are using. More slot states here will reduce the maximum size that
 * a slot can hold unbuffered by half. If a record is larger than the maximum we can account for in
 * the slot state we fall back to direct writes.
 */
#define WT_LOG_SLOT_BITS 2
#define WT_LOG_SLOT_MAXBITS (32 - WT_LOG_SLOT_BITS)
#define WT_LOG_SLOT_CLOSE 0x4000000000000000LL    /* Force slot close */
#define WT_LOG_SLOT_RESERVED 0x8000000000000000LL /* Reserved states */

/*
 * Check if the unbuffered flag is set in the joined portion of the slot state.
 */
#define WT_LOG_SLOT_UNBUFFERED_ISSET(state) ((state) & ((int64_t)WT_LOG_SLOT_UNBUFFERED << 32))

#define WT_LOG_SLOT_MASK_OFF 0x3fffffffffffffffLL
#define WT_LOG_SLOT_MASK_ON ~(WT_LOG_SLOT_MASK_OFF)
#define WT_LOG_SLOT_JOIN_MASK (WT_LOG_SLOT_MASK_OFF >> 32)

/*
 * These macros manipulate the slot state and its component parts.
 */
// 取state(64位)的最高的两位
#define WT_LOG_SLOT_FLAGS(state) ((state)&WT_LOG_SLOT_MASK_ON)
#define WT_LOG_SLOT_JOINED(state) (((state)&WT_LOG_SLOT_MASK_OFF) >> 32)
#define WT_LOG_SLOT_JOINED_BUFFERED(state) \
    (WT_LOG_SLOT_JOINED(state) & (WT_LOG_SLOT_UNBUFFERED - 1))
#define WT_LOG_SLOT_JOIN_REL(j, r, s) (((j) << 32) + (r) + (s))
#define WT_LOG_SLOT_RELEASED(state) ((int64_t)(int32_t)(state)) // TODO: released what's meanming?
#define WT_LOG_SLOT_RELEASED_BUFFERED(state) \
    ((int64_t)((int32_t)WT_LOG_SLOT_RELEASED(state) & (WT_LOG_SLOT_UNBUFFERED - 1)))

/* Slot is in use */

#define WT_LOG_SLOT_ACTIVE(state) (WT_LOG_SLOT_JOINED(state) != WT_LOG_SLOT_JOIN_MASK)
/* Slot is in use, but closed to new joins */
// TODO: 为什么用这么复杂的条件判断slot是否closed
#define WT_LOG_SLOT_CLOSED(state)                                                              \
    (WT_LOG_SLOT_ACTIVE(state) && (FLD_LOG_SLOT_ISSET((uint64_t)(state), WT_LOG_SLOT_CLOSE) && \
                                    !FLD_LOG_SLOT_ISSET((uint64_t)(state), WT_LOG_SLOT_RESERVED)))
/* Slot is in use, all data copied into buffer */
#define WT_LOG_SLOT_INPROGRESS(state) (WT_LOG_SLOT_RELEASED(state) != WT_LOG_SLOT_JOINED(state)) // slot released != slot joined 就标识slot in progress
#define WT_LOG_SLOT_DONE(state) (WT_LOG_SLOT_CLOSED(state) && !WT_LOG_SLOT_INPROGRESS(state))
/* Slot is in use, more threads may join this slot */
#define WT_LOG_SLOT_OPEN(state)                                           \
    (WT_LOG_SLOT_ACTIVE(state) && !WT_LOG_SLOT_UNBUFFERED_ISSET(state) && \
      !FLD_LOG_SLOT_ISSET((uint64_t)(state), WT_LOG_SLOT_CLOSE) &&        \
      WT_LOG_SLOT_JOINED(state) < WT_LOG_SLOT_BUF_MAX) // joined的size小于slot buffer的一半

/*
 * __wt_logslot对应的实际日志文件一经确定，应该就不会再变动。
 * 这里会带来一个问题，每个redo log的size可能会超过用户设定的大小。  
 */
struct __wt_logslot {
    WT_CACHE_LINE_PAD_BEGIN
    volatile int64_t slot_state; /* Slot state */ // TODO: 这个变量是如何按位组织数据的
    int64_t slot_unbuffered;     /* Unbuffered data in this slot */ // 记录unbuffered日志的size
    int slot_error;              /* Error value */
    // 表示这个slot，开始写入日志文件时的文件偏移，表示要从日志文件的哪个位置开始写起。参见__wt_log_slot_activate函数
    wt_off_t slot_start_offset;  /* Starting file offset */
    // 表示这个slot，最后写入日志文件的偏移
    wt_off_t slot_last_offset;   /* Last record offset */
    /* 
     * 一般来说，slot_release_lsn都等于slot_start_lsn, 除非发生了日志file切换
     * TOOD: 那么为什么呢？
     */ 
    WT_LSN slot_release_lsn;     /* Slot release LSN */
    // 表示这个slot往日志文件中写日志的起始lsn，猜测slot_start_lsn.l.offset == slot_start_offset
    WT_LSN slot_start_lsn;       /* Slot starting LSN */
    // 表示这个slot结束时在日志文件中的lsn
    WT_LSN slot_end_lsn;         /* Slot ending LSN */
    WT_FH *slot_fh;              /* File handle for this group */
    WT_ITEM slot_buf;            /* Buffer for grouped writes */

/*
 * ???? 
 */
/* AUTOMATIC FLAG VALUE GENERATION START */
#define WT_SLOT_CLOSEFH 0x01u    /* Close old fh on release */
/*
 * 如果txn的txn_logsync设置了WT_LOG_FLUSH，那么slot设置该标志位 
 */
#define WT_SLOT_FLUSH 0x02u      /* Wait for write */
/*
 * 如果txn的txn_logsync设置了fsync，那么slot设置该标志位。 
 */
#define WT_SLOT_SYNC 0x04u       /* Needs sync on release */
/*
 * 如果txn的txn_logsync设置了fsync或者dsync，那么slot设置该标志位。 
 */
#define WT_SLOT_SYNC_DIR 0x08u   /* Directory sync on release */
// TODO: ？？？？
#define WT_SLOT_SYNC_DIRTY 0x10u /* Sync system buffers on release */
                                 /* AUTOMATIC FLAG VALUE GENERATION STOP */
    uint32_t flags;
    WT_CACHE_LINE_PAD_END
};

#define WT_SLOT_INIT_FLAGS 0

#define WT_SLOT_SYNC_FLAGS (WT_SLOT_SYNC | WT_SLOT_SYNC_DIR | WT_SLOT_SYNC_DIRTY)

#define WT_WITH_SLOT_LOCK(session, log, op)                                            \
    do {                                                                               \
        WT_ASSERT(session, !F_ISSET(session, WT_SESSION_LOCKED_SLOT));                 \
        WT_WITH_LOCK_WAIT(session, &(log)->log_slot_lock, WT_SESSION_LOCKED_SLOT, op); \
    } while (0)

struct __wt_myslot {
    WT_LOGSLOT *slot;    /* Slot I'm using */
    /* end_offset的唯一作用是用来判断记录的txn.record是否为unbuffered*/
    wt_off_t end_offset; /* My end offset in buffer */
    wt_off_t offset;     /* Slot buffer offset */ // txn日志在slot中的起始offset

/* AUTOMATIC FLAG VALUE GENERATION START */
#define WT_MYSLOT_CLOSE 0x1u         /* This thread is closing the slot */ // slot close后会置该标志位
#define WT_MYSLOT_NEEDS_RELEASE 0x2u /* This thread is releasing the slot */
#define WT_MYSLOT_UNBUFFERED 0x4u    /* Write directly */ // 如果slot有unbuffered的log，会在join的时候置该标志位
                                     /* AUTOMATIC FLAG VALUE GENERATION STOP */
    uint32_t flags;
};

#define WT_LOG_END_HEADER log->allocsize

struct __wt_log {
    uint32_t allocsize;    /* Allocation alignment size 分配调整size */
    uint32_t first_record; /* Offset of first record in file */
    wt_off_t log_written;  /* Amount of log written this period */
                           /*
                            * Log file information
                            */
    uint32_t fileid;       /* Current log file number */
    uint32_t prep_fileid;  /* Pre-allocated file number */
    uint32_t tmp_fileid;   /* Temporary file number */
    uint32_t prep_missed;  /* Pre-allocated file misses */
    WT_FH *log_fh;         /* Logging file handle */
    WT_FH *log_dir_fh;     /* Log directory file handle */
    WT_FH *log_close_fh;   /* Logging file handle to close */
    WT_LSN log_close_lsn;  /* LSN needed to close */

    /* 当前wt运行的WAL版本 */
    uint16_t log_version; /* Version of log file */

    /*
     * System LSNs
     */
    WT_LSN alloc_lsn;       /* Next LSN for allocation */ // 下一个要分配的LSN
    WT_LSN bg_sync_lsn;     /* Latest background sync LSN */
    WT_LSN ckpt_lsn;        /* Last checkpoint LSN */
    WT_LSN dirty_lsn;       /* LSN of last non-synced write */
    WT_LSN first_lsn;       /* First LSN */
    WT_LSN sync_dir_lsn;    /* LSN of the last directory sync */
    WT_LSN sync_lsn;        /* LSN of the last sync */
    WT_LSN trunc_lsn;       /* End LSN for recovery truncation */
    WT_LSN write_lsn;       /* End of last LSN written */
    WT_LSN write_start_lsn; /* Beginning of last LSN written */

    /*
     * Synchronization resources
     */
    WT_SPINLOCK log_lock;          /* Locked: Logging fields */
    WT_SPINLOCK log_fs_lock;       /* Locked: tmp, prep and log files */
    WT_SPINLOCK log_slot_lock;     /* Locked: Consolidation array */
    WT_SPINLOCK log_sync_lock;     /* Locked: Single-thread fsync */
    WT_SPINLOCK log_writelsn_lock; /* Locked: write LSN */

    WT_RWLOCK log_archive_lock; /* Archive and log cursors */

    /* Notify any waiting threads when sync_lsn is updated. */
    WT_CONDVAR *log_sync_cond;
    /* Notify any waiting threads when write_lsn is updated. */
    WT_CONDVAR *log_write_cond;

/*
 * Consolidation array information Our testing shows that the more consolidation we generate the
 * better the performance we see which equates to an active slot slot count of one.
 *
 * Note: this can't be an array, we impose cache-line alignment and gcc doesn't support that for
 * arrays.
 */
#define WT_SLOT_POOL 128
    WT_LOGSLOT *active_slot;            /* Active slot */ // 准备就绪且可以作为合并logrec的slotbuffer对象
    WT_LOGSLOT slot_pool[WT_SLOT_POOL]; /* Pool of all slots */ // 系统所有slot buffer对象数组，包括：正在合并的、准备合并和闲置的slot buffer。
    int32_t pool_index;                 /* Index into slot pool */
    size_t slot_buf_size;               /* Buffer size for slots */ // slot buffer size 不超过WT_LOG_SLOT_BUF_SIZE (256 * 1024)
#ifdef HAVE_DIAGNOSTIC
    uint64_t write_calls; /* Calls to log_write */
#endif

/* AUTOMATIC FLAG VALUE GENERATION START */
#define WT_LOG_FORCE_NEWFILE 0x1u   /* Force switch to new log file */ // 这个标识应该不是配置项
#define WT_LOG_OPENED 0x2u          /* Log subsystem successfully open */
#define WT_LOG_TRUNCATE_NOTSUP 0x4u /* File system truncate not supported */
                                    /* AUTOMATIC FLAG VALUE GENERATION STOP */
    uint32_t flags;
};

// WT引擎的操作日志对象（以下简称为logrec）对应的是提交的事务，事务的每个操作被记录成一个logop对象，一个logrec包含多个logop。
// logrec是一个通过精密序列化事务操作动作和参数得到的一个二进制buffer，这个buffer的数据是通过事务和操作类型来确定其格式的。
// WT_LOG_RECORD
struct __wt_log_record {
    uint32_t len;      /* 00-03: Record length including hdr */
    uint32_t checksum; /* 04-07: Checksum of the record */

/*
 * No automatic generation: flag values cannot change, they're written to disk.
 *
 * Unused bits in the flags, as well as the 'unused' padding, are expected to be zeroed; we check
 * that to help detect file corruption.
 */
#define WT_LOG_RECORD_COMPRESSED 0x01u /* Compressed except hdr */
#define WT_LOG_RECORD_ENCRYPTED 0x02u  /* Encrypted except hdr */
#define WT_LOG_RECORD_ALL_FLAGS (WT_LOG_RECORD_COMPRESSED | WT_LOG_RECORD_ENCRYPTED)
    uint16_t flags;    /* 08-09: Flags */
    uint8_t unused[2]; /* 10-11: Padding */
    uint32_t mem_len;  /* 12-15: Uncompressed len if needed */
    uint8_t record[0]; /* Beginning of actual data */
};

/*
 * __wt_log_record_byteswap --
 *     Handle big- and little-endian transformation of the log record header block.
 */
static inline void
__wt_log_record_byteswap(WT_LOG_RECORD *record)
{
#ifdef WORDS_BIGENDIAN
    record->len = __wt_bswap32(record->len);
    record->checksum = __wt_bswap32(record->checksum);
    record->flags = __wt_bswap16(record->flags);
    record->mem_len = __wt_bswap32(record->mem_len);
#else
    WT_UNUSED(record);
#endif
}

/*
 * WT_LOG_DESC --
 *	The log file's description.
 */
struct __wt_log_desc {
#define WT_LOG_MAGIC 0x101064u
    uint32_t log_magic; /* 00-03: Magic number */
                        /*
                         * NOTE: We bumped the log version from 2 to 3 to make it convenient for
                         * MongoDB to detect users accidentally running old binaries on a newer
                         * release. There are no actual log file format changes with version 2,
                         * version 3 and version 4.
                         * WT_LOG_VERSION 2 3 4 的日志格式都是一样的。也就是说当前wt只有两个版本的WAL日志
                         */
#define WT_LOG_VERSION 4
    uint16_t version;  /* 04-05: Log version */
    uint16_t unused;   /* 06-07: Unused */
    uint64_t log_size; /* 08-15: Log file size */
};

/*
 * 实际的WAL版本
 */
/*
 * This is the log version that introduced the system record.
 */
#define WT_LOG_VERSION_SYSTEM 2

/*
 * WiredTiger release version where log format version changed.
 */
#define WT_LOG_V2_MAJOR 3
#define WT_LOG_V2_MINOR 0
#define WT_LOG_V3_MAJOR 3
#define WT_LOG_V3_MINOR 1
#define WT_LOG_V4_MAJOR 3
#define WT_LOG_V4_MINOR 3

/*
 * __wt_log_desc_byteswap --
 *     Handle big- and little-endian transformation of the log file description block.
 */
static inline void
__wt_log_desc_byteswap(WT_LOG_DESC *desc)
{
#ifdef WORDS_BIGENDIAN
    desc->log_magic = __wt_bswap32(desc->log_magic);
    desc->version = __wt_bswap16(desc->version);
    desc->unused = __wt_bswap16(desc->unused);
    desc->log_size = __wt_bswap64(desc->log_size);
#else
    WT_UNUSED(desc);
#endif
}

/* Cookie passed through the transaction printlog routines. */
struct __wt_txn_printlog_args {
    WT_FSTREAM *fs;

/* AUTOMATIC FLAG VALUE GENERATION START */
#define WT_TXN_PRINTLOG_HEX 0x1u /* Add hex output */
                                 /* AUTOMATIC FLAG VALUE GENERATION STOP */
    uint32_t flags;
};

/*
 * WT_LOG_REC_DESC --
 *	A descriptor for a log record type.
 */
struct __wt_log_rec_desc {
    const char *fmt;
    int (*print)(WT_SESSION_IMPL *session, uint8_t **pp, uint8_t *end);
};

/*
 * WT_LOG_OP_DESC --
 *	A descriptor for a log operation type.
 */
struct __wt_log_op_desc {
    const char *fmt;
    int (*print)(WT_SESSION_IMPL *session, uint8_t **pp, uint8_t *end);
};
