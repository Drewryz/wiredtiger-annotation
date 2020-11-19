/*
 * @Author: yangzaorang 
 * @Date: 2020-10-23 15:03:57 
 * @Last Modified by: yangzaorang
 * @Last Modified time: 2020-10-23 16:18:53
 */

#include <wiredtiger.h>
#include <wiredtiger_ext.h>

void iterate_cursor(WT_CURSOR *cursor) {
    int ret;
    const char *key, *value;
    int count = 0;
    cursor->reset(cursor);
    while((ret = cursor->next(cursor)) == 0) {
        cursor->get_key(cursor, &key);
        cursor->get_value(cursor, &value);
        count++;
        printf("record: %s : %s\n", key, value);
    }
    printf("------------------get %d record------------------\n", count);
}

int main() {
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    const char *key, *value;
    int ret;
    /* Open a connection to the database, creating it if necessary. */
    wiredtiger_open("./wt_meta", NULL, "create,log=(enabled),transaction_sync=(enabled=true,method=fsync)", &conn);
    /* Open a session handle for the database. */
    conn->open_session(conn, NULL, NULL, &session);
    /* Create table. */
    session->create(session, "table:my_table", "key_format=S,value_format=S");
    session->open_cursor(session, "table:my_table", NULL, NULL, &cursor);
    cursor->set_key(cursor, "key");
    cursor->set_value(cursor, "val");
    cursor->insert(cursor);

    WT_SESSION *session1, *session2;
    WT_CURSOR *cursor1, *cursor2;

    conn->open_session(conn, NULL, NULL, &session1);
    session1->create(session1, "table:my_table", "key_format=S,value_format=S");
    session1->open_cursor(session1, "table:my_table", NULL, NULL, &cursor1);
    session1->begin_transaction(session1, "isolation=snapshot");
    conn->open_session(conn, NULL, NULL, &session2);
    session2->create(session2, "table:my_table", "key_format=S,value_format=S");
    session2->open_cursor(session2, "table:my_table", NULL, NULL, &cursor2);
    session2->begin_transaction(session2, "isolation=snapshot");

    /* 为了防止更新丢失，后置事务提交后，前置事务提交失败 */
    cursor2->set_key(cursor2, "key");
    cursor2->set_value(cursor2, "val2");
    switch (cursor2->insert(cursor2)) {
    case 0: /* Success */
        session2->commit_transaction(session2,NULL);
        // session2->commit_transaction(session2,"sync=on");
        break;
    case WT_ROLLBACK:
    default:
        session2->rollback_transaction(session2, NULL);
        break;
    }
    cursor1->set_key(cursor1, "key");
    cursor1->set_value(cursor1, "val1");
    switch (cursor1->insert(cursor1)) {
    case 0: /* Success */
        session1->commit_transaction(session1,NULL);
        break;
    case WT_ROLLBACK:
        printf("session1 rollback\n");
        break;
    default:
        printf("session1 default\n");
        session1->rollback_transaction(session1, NULL);
        break;
    }
    conn->close(conn, NULL);
}