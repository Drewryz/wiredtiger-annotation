#include <wiredtiger.h>
#include <wiredtiger_ext.h>

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
    session->begin_transaction(session, "isolation=snapshot");
    cursor->set_key(cursor, "key3");
    cursor->set_value(cursor, "val3");
    switch (cursor->insert(cursor)) {
    case 0: /* Success */
        session->commit_transaction(session,NULL);
        break;
    case WT_ROLLBACK:
    default:
        session->rollback_transaction(session, NULL);
        break;
    }

    cursor->reset(cursor);

    while((ret = cursor->next(cursor)) == 0) {
        cursor->get_key(cursor, &key);
        cursor->get_value(cursor, &value);
        printf("Got record: %s : %s\n", key, value);
    }

    conn->close(conn, NULL);
}