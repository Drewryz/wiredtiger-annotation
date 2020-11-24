/*
 * @Author: yangzaorang 
 * @Date: 2020-11-23 20:30:16 
 * @Last Modified by: mikey.zhaopeng
 * @Last Modified time: 2020-11-23 20:36:35
 */

#include <wiredtiger.h>
#include <wiredtiger_ext.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

void iterate_cursor(WT_CURSOR *cursor) {
    int ret;
    const char * key;
    const char *value;
    int count = 0;
    cursor->reset(cursor);
    while((ret = cursor->next(cursor)) == 0) {
        cursor->get_key(cursor, &key);
        cursor->get_value(cursor, &value);
        count++;
        printf("%s ||| %s\n", key, value);
    }
    printf("------------------get %d record------------------\n", count);
}

int main() {
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    const char *key, *value;
    wiredtiger_open("./wt_meta", NULL, "create,cache_size=1GB,log=(enabled,recover=on)", &conn);
    conn->open_session(conn, NULL, NULL, &session);
    session->create(session, "table:my_table", "key_format=i,value_format=S");
    session->open_cursor(session, "metadata:", NULL, NULL, &cursor);
    iterate_cursor(cursor);
    conn->close(conn, NULL);
}