/*
 * @Author: yangzaorang 
 * @Date: 2020-10-23 15:03:57 
 * @Last Modified by: yangzaorang
 * @Last Modified time: 2020-10-23 15:05:25
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
    wiredtiger_open("./wt_meta", NULL, "create", &conn);
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
    // 如果txn2没有事先遍历数据，txn1提交以后，txn2会读到txn1更新的数据，猜测：
    // 事务的快照是从其第一次开始读数据建立的。
    iterate_cursor(cursor2);

    cursor1->set_key(cursor1, "key");
    cursor1->set_value(cursor1, "val1");
    switch (cursor1->insert(cursor1)) {
    case 0: /* Success */
        session1->commit_transaction(session1,NULL);
        break;
    case WT_ROLLBACK:
    default:
        session1->rollback_transaction(session1, NULL);
        break;
    }
    // 即使txn1提交了，txn2读到的还是之前的值
    iterate_cursor(cursor2);
    conn->close(conn, NULL);
}