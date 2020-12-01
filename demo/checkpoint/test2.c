/*
 * @Author: yangzaorang 
 * @Date: 2020-11-30 19:27:38 
 * @Last Modified by: yangzaorang
 * @Last Modified time: 2020-12-01 16:38:18
 */

#include <wiredtiger.h>
#include <wiredtiger_ext.h>
#include <iostream>
#include <time.h>
#include <chrono>  

using namespace std;


void insert_one_record(WT_CONNECTION *conn, const char* table_name) {
    WT_SESSION *session;
    conn->open_session(conn, NULL, NULL, &session);
    string wt_tbl_url = string("table:").append(string(table_name));
    const char* wt_tbl_url_c = wt_tbl_url.c_str();
    session->create(session, wt_tbl_url_c, "key_format=S,value_format=S");
    WT_CURSOR *cursor;
    session->open_cursor(session,  wt_tbl_url_c, NULL, NULL, &cursor);
    cursor->set_key(cursor, "b");
    cursor->set_value(cursor, "B");
    cursor->insert(cursor);
    cursor->close(cursor);
    session->close(session, NULL);
}

void make_checkpoint(WT_CONNECTION *conn, const char* table_name) {
    WT_SESSION *session;
    conn->open_session(conn, NULL, NULL, &session);
    session->checkpoint(session, "name=for_test");
    session->close(session, NULL);
}

void iterate_tbl(WT_CONNECTION *conn, const char* table_name, const char* checkpoint) {
    int ret;
    const char *key, *value;
    int count = 0;
    WT_SESSION *session;
    WT_CURSOR *cursor;
    conn->open_session(conn, NULL, NULL, &session);
    string wt_tbl_url = string("table:").append(string(table_name));
    const char* wt_tbl_url_c = wt_tbl_url.c_str();
    session->create(session, wt_tbl_url_c, "key_format=S,value_format=S");
    if (checkpoint == NULL) {
        session->open_cursor(session, wt_tbl_url_c, NULL, NULL, &cursor);
    } else {
        string checkpoint_name = string("checkpoint=").append(string(checkpoint));
        session->open_cursor(session, wt_tbl_url_c, NULL, checkpoint_name.c_str(), &cursor);
    }
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
    int ret;
    const char *db_home = "checkpoint_test_home";
    const char *config = "create,log=(enabled=true,archive=false)";
    wiredtiger_open(db_home, NULL, config, &conn);
    insert_one_record(conn, "my_table");
    iterate_tbl(conn, "my_table", "for_test");
    conn->close(conn, NULL);
}