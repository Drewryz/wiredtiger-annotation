#include <wiredtiger.h>
#include <wiredtiger_ext.h>
#include <test_util.h>


int main() {
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    const char *key, *value;
    int ret;
    /* Open a connection to the database, creating it if necessary. */
    error_check(wiredtiger_open("./", NULL, "create", &conn));
    /* Open a session handle for the database. */
    error_check(conn->open_session(conn, NULL, NULL, &session));
    error_check(session->create(session, "table:access", "key_format=S,value_format=S"));
    error_check(session->open_cursor(session, "table:access", NULL, NULL, &cursor));
    cursor->set_key(cursor, "key1"); /* Insert a record. */
    cursor->set_value(cursor, "value1");
    error_check(cursor->insert(cursor));

    error_check(cursor->reset(cursor)); /* Restart the scan. */
    while ((ret = cursor->next(cursor)) == 0) {
        error_check(cursor->get_key(cursor, &key));
        error_check(cursor->get_value(cursor, &value));
        printf("Got record: %s : %s\n", key, value);
    }
    scan_end_check(ret == WT_NOTFOUND); /* Check for end-of-table. */

    error_check(conn->close(conn, NULL)); /* Close all handles. */
}