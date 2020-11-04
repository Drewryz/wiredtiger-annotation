/*
 * @Author: yangzaorang 
 * @Date: 2020-10-23 15:03:57 
 * @Last Modified by: yangzaorang
 * @Last Modified time: 2020-10-23 16:52:06
 */

#include <wiredtiger.h>
#include <wiredtiger_ext.h>

void iterate_cursor(WT_CURSOR *cursor) {
    int ret;
    const char *key;
    int *value;
    int count = 0;
    cursor->reset(cursor);
    while((ret = cursor->next(cursor)) == 0) {
        cursor->get_key(cursor, &key);
        cursor->get_value(cursor, &value);
        count++;
        printf("record: %s : %d\n", key, value);
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
    session->create(session, "table:my_table", "key_format=S,value_format=i");
    session->open_cursor(session, "table:my_table", NULL, NULL, &cursor);
    cursor->set_key(cursor, "key");
    cursor->set_value(cursor, 0);
    cursor->insert(cursor);

    WT_SESSION *session1, *session2;
    WT_CURSOR *cursor1, *cursor2;

    conn->open_session(conn, NULL, NULL, &session1);
    session1->create(session1, "table:my_table", "key_format=S,value_format=i");
    session1->open_cursor(session1, "table:my_table", NULL, NULL, &cursor1);
    session1->begin_transaction(session1, "isolation=snapshot");
    conn->open_session(conn, NULL, NULL, &session2);
    session2->create(session2, "table:my_table", "key_format=S,value_format=i");
    session2->open_cursor(session2, "table:my_table", NULL, NULL, &cursor2);
    session2->begin_transaction(session2, "isolation=snapshot");

    /* 前置事务先提交，后置事务再提交，后置事务同样提交失败 */
    cursor1->set_key(cursor1, "key");
    cursor1->set_value(cursor1, 1);
    switch (cursor1->insert(cursor1)) {
    case 0: /* Success */
        session1->commit_transaction(session1,NULL);
        break;
    case WT_ROLLBACK:
    default:
        printf("session1 rollback\n");
        session1->rollback_transaction(session1, NULL);
        break;
    }

    // 如果事务T想要更新键k，那么从事务T开始到事务T提交这段时间内，不能有对k的提交更新，否则T会出现更新失败而回滚。
    cursor2->set_key(cursor2, "key");
    cursor2->set_value(cursor2, 2);
    int status = 0;
    switch (status = cursor2->insert(cursor2)) {
    case 0: /* Success */
        session2->commit_transaction(session2,NULL);
        break;
    case WT_ROLLBACK:
        printf("mark1\n");
    default:
        printf("session2 rollback, error code = %d\n", status);
        session2->rollback_transaction(session2, NULL);
        break;
    }
    iterate_cursor(cursor2);
    conn->close(conn, NULL);
}