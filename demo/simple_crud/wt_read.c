/*
 * @Author: yangzaorang 
 * @Date: 2020-12-01 16:06:05 
 * @Last Modified by: yangzaorang
 * @Last Modified time: 2020-12-01 16:29:06
 */

/* 
 * 用于读取表的所有数据(只适用于key和value都是字符串类型的数据)，用法如下：
 * wt_read db_home table_name
 */

#include <wiredtiger.h>
#include <wiredtiger_ext.h>
#include <stdio.h>
#include <unistd.h>
#include <iostream>

using namespace std;

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

int main(int argc, char *argv[]) {
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    const char *key, *value;
    int ret;
    const char* db_home = argv[1];
    const char* table_name = argv[2];
    wiredtiger_open(db_home, NULL, "create,io_capacity=(total=40MB)", &conn);
    conn->open_session(conn, NULL, NULL, &session);
    string table_name_string = string("table:") + table_name;
    session->create(session, table_name_string.c_str(), "key_format=S,value_format=S");
    session->open_cursor(session, table_name_string.c_str(), NULL, NULL, &cursor);
    iterate_cursor(cursor);
    conn->close(conn, NULL);
}
