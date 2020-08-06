#include "wt_internal.h"

/*
 * __wt_cursor_set_keyv --
 *     WT_CURSOR->set_key default implementation.
 */
void
__wt_cursor_set_keyv(WT_CURSOR *cursor, uint32_t flags, va_list ap)
{
    WT_DECL_RET;
    WT_ITEM *buf, *item, tmp;
    WT_SESSION_IMPL *session;
    size_t sz;
    const char *fmt, *str;
    va_list ap_copy;

    buf = &cursor->key;
    tmp.mem = NULL;

    CURSOR_API_CALL(cursor, session, set_key, NULL);
    if (F_ISSET(cursor, WT_CURSTD_KEY_SET) && WT_DATA_IN_ITEM(buf)) {
        tmp = *buf;
        buf->mem = NULL;
        buf->memsize = 0;
    }

    F_CLR(cursor, WT_CURSTD_KEY_SET);

    if (WT_CURSOR_RECNO(cursor)) {
        if (LF_ISSET(WT_CURSTD_RAW)) {
            item = va_arg(ap, WT_ITEM *);
            WT_ERR(__wt_struct_unpack(session, item->data, item->size, "q", &cursor->recno));
        } else
            cursor->recno = va_arg(ap, uint64_t);
        if (cursor->recno == WT_RECNO_OOB)
            WT_ERR_MSG(session, EINVAL, "%d is an invalid record number", WT_RECNO_OOB);
        buf->data = &cursor->recno;
        sz = sizeof(cursor->recno);
    } else {
        /* Fast path some common cases and special case WT_ITEMs. */
        fmt = cursor->key_format;
        if (LF_ISSET(WT_CURSOR_RAW_OK | WT_CURSTD_DUMP_JSON) || WT_STREQ(fmt, "u")) {
            item = va_arg(ap, WT_ITEM *);
            sz = item->size;
            buf->data = item->data;
        } else if (WT_STREQ(fmt, "S")) {
            str = va_arg(ap, const char *);
            sz = strlen(str) + 1;
            buf->data = (void *)str;
        } else {
            va_copy(ap_copy, ap);
            ret = __wt_struct_sizev(session, &sz, cursor->key_format, ap_copy);
            va_end(ap_copy);
            WT_ERR(ret);

            WT_ERR(__wt_buf_initsize(session, buf, sz));
            WT_ERR(__wt_struct_packv(session, buf->mem, sz, cursor->key_format, ap));
        }
    }
    if (sz == 0)
        WT_ERR_MSG(session, EINVAL, "Empty keys not permitted");
    else if ((uint32_t)sz != sz)
        WT_ERR_MSG(session, EINVAL, "Key size (%" PRIu64 ") out of range", (uint64_t)sz);
    cursor->saved_err = 0;
    buf->size = sz;
    F_SET(cursor, WT_CURSTD_KEY_EXT);
    if (0) {
err:
        cursor->saved_err = ret;
    }

    /*
     * If we copied the key, either put the memory back into the cursor, or if we allocated some
     * memory in the meantime, free it.
     */
    if (tmp.mem != NULL) {
        if (buf->mem == NULL) {
            buf->mem = tmp.mem;
            buf->memsize = tmp.memsize;
        } else
            __wt_free(session, tmp.mem);
    }
    API_END(session, ret);
}