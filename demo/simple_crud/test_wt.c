/*
 * @Author: yangzaorang 
 * @Date: 2020-08-12 10:35:58 
 * @Last Modified by: yangzaorang
 * @Last Modified time: 2020-08-12 11:47:51
 */

#include <wiredtiger.h>
#include <wiredtiger_ext.h>
#include <stdio.h>
#include <unistd.h>

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
    wiredtiger_open("./wt_meta", NULL, "create,io_capacity=(total=40MB),file_extend=(data=16MB)", &conn);
    /* Open a session handle for the database. */
    conn->open_session(conn, NULL, NULL, &session);
    /* Create table. */
    session->create(session, "table:my_table", "key_format=S,value_format=S");
    session->open_cursor(session, "table:my_table", NULL, NULL, &cursor);
    printf("Insert a record.\n");
    cursor->set_key(cursor, "key1");
    cursor->set_value(cursor, "val1");
    cursor->insert(cursor);
    printf("Inserting is success?\n");
    iterate_cursor(cursor);
    // printf("Delete a record.\n");
    // cursor->reset(cursor);
    // cursor->set_key(cursor, "key1");
    // cursor->remove(cursor);
    // printf("Deleting is success?\n");
    // iterate_cursor(cursor);
    // sleep(60);
    conn->close(conn, NULL);
}
