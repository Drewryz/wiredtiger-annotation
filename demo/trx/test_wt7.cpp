/*
 * @Author: yangzaorang 
 * @Date: 2020-11-23 20:30:16 
 * @Last Modified by: mikey.zhaopeng
 * @Last Modified time: 2020-11-25 11:42:37
 */

#include <wiredtiger.h>
#include <wiredtiger_ext.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string>

using namespace std;

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

void insert_data(const char* wt_home, const char* tbl) {
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    const char *key, *value;
    std::string tbl_str = std::string("table:") + std::string(tbl);  
    wiredtiger_open(wt_home, NULL, "create,log=(enabled),transaction_sync=(enabled=true,method=fsync)", &conn);
    conn->open_session(conn, NULL, NULL, &session);
    session->create(session, tbl_str.c_str(), "key_format=S,value_format=S");
    session->open_cursor(session, tbl_str.c_str(), NULL, NULL, &cursor);

    cursor->set_key(cursor, "key-1");
    cursor->set_value(cursor, "val-1");
    cursor->insert(cursor);
    conn->close(conn, NULL);
}

int main() {
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    const char *key, *value;
    const char* wt_home = "./wt_meta";

    insert_data(wt_home, "t1");
    insert_data(wt_home, "t2");
    insert_data(wt_home, "t3");

    wiredtiger_open(wt_home, NULL, "create,log=(enabled),transaction_sync=(enabled=true,method=fsync)", &conn);
    conn->open_session(conn, NULL, NULL, &session);
    session->open_cursor(session, "metadata:", NULL, NULL, &cursor);
    iterate_cursor(cursor);
    conn->close(conn, NULL);
}