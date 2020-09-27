/*
 * @Author: yangzaorang 
 * @Date: 2020-08-20 11:27:51 
 * @Last Modified by: yangzaorang
 * @Last Modified time: 2020-08-20 14:35:09
 */

#include <wiredtiger.h>
#include <wiredtiger_ext.h>
#include <iostream>
#include <time.h>
#include <chrono>  

using namespace std;

std::time_t get_timestamp()
{
    auto tp = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());//获取当前时间点
    std::time_t timestamp =  tp.time_since_epoch().count(); //计算距离1970-1-1,00:00的时间长度
    return timestamp;
}

void insert_one_record(WT_CONNECTION *conn, const char* table_name) {
    WT_SESSION *session;
    conn->open_session(conn, NULL, NULL, &session);
    string wt_tbl_url = string("table:").append(string(table_name));
    const char* wt_tbl_url_c = wt_tbl_url.c_str();
    session->create(session, wt_tbl_url_c, "key_format=S,value_format=S");
    WT_CURSOR *cursor;
    session->open_cursor(session,  wt_tbl_url_c, NULL, NULL, &cursor);
    time_t now = get_timestamp();
    string key = to_string(now);
    string value = to_string(now);
    cursor->set_key(cursor, key.c_str());
    cursor->set_value(cursor, value.c_str());
    cursor->insert(cursor);
    cursor->reset(cursor);
    const char *key_tmp, *value_tmp;
    int ret = 0;
    while((ret = cursor->next(cursor)) == 0) {
        cursor->get_key(cursor, &key_tmp);
        cursor->get_value(cursor, &value_tmp);
        printf("Got record: %s : %s\n", key_tmp, value_tmp);
    }
    cursor->close(cursor);
    session->checkpoint(session, NULL);
    session->close(session, NULL);
}

int main() {
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    int ret;
    const char *db_home = "db_home";
    const char *config = "create,log=(enabled=true,archive=false,path=journal)";
    wiredtiger_open(db_home, NULL, config, &conn);
    insert_one_record(conn, "my_table1");
    insert_one_record(conn, "my_table2");
    conn->close(conn, NULL);
}
