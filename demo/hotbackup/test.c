/*
 * @Author: yangzaorang 
 * @Date: 2020-08-09 19:58:24 
 * @Last Modified by: yangzaorang
 * @Last Modified time: 2020-08-09 20:00:22
 */
// TODO: 这个demo出现过unknown compressor 'snappy': Invalid argument的问题，原因未知:(. 清理wt的数据文件后解决
// 关于wt的backup: http://source.wiredtiger.com/3.2.1/backup.html#backup_process


#include <wiredtiger.h>
#include <wiredtiger_ext.h>
#include <iostream>

using namespace std;

void create_table(WT_CONNECTION *conn, const char* table_name) {
    WT_SESSION *session;
    conn->open_session(conn, NULL, NULL, &session);
    string wt_tbl_url = string("table:").append(string(table_name));
    const char* wt_tbl_url_c = wt_tbl_url.c_str();
    session->create(session, wt_tbl_url_c, "key_format=S,value_format=S");
    WT_CURSOR *cursor;
    session->open_cursor(session,  wt_tbl_url_c, NULL, NULL, &cursor);
    cursor->set_key(cursor, "key1");
    cursor->set_value(cursor, "val1");
    cursor->insert(cursor);
    cursor->reset(cursor);
    int ret = 0;
    const char *key, *value;
    while((ret = cursor->next(cursor)) == 0) {
        cursor->get_key(cursor, &key);
        cursor->get_value(cursor, &value);
        printf("Got record: %s : %s\n", key, value);
    }
    cursor->close(cursor);
    session->close(session, NULL);
}

// TODO: wt backup原理
void backup(WT_CONNECTION *conn) {
    WT_SESSION *session;
    conn->open_session(conn, NULL, NULL, &session);
    WT_CURSOR *cursor;
    // cursor必须要在所有的文件拷贝完成以后才能被关闭
    // 对文件的拷贝顺序没有要求
    session->open_cursor(session,  "backup:", NULL, NULL, &cursor);
    int ret;
    char *filename;
    while ((ret = cursor->next(cursor)) == 0) {
        cursor->get_key(cursor, &filename);
        cout << filename << endl;
    }
    cursor->close(cursor);
    session->close(session, NULL);
}

int main() {
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    int ret;
    const char *db_home = "db_home";
    const char *config = "create,log=(enabled=true,archive=false,path=journal)";
    wiredtiger_open(db_home, NULL, config, &conn);
    create_table(conn, "my_table1");
    create_table(conn, "table2/my_table2");
    cout << "backup..." << endl;
    backup(conn);
    conn->close(conn, NULL);
}

/* 输出如下：
[root@9ce135f89f25 hotbackup]# ./test_wt
Got record: key1 : val1
Got record: key1 : val1
backup...
WiredTigerLog.0000000001
WiredTigerLog.0000000002
my_table1.wt
table2/my_table2.wt
WiredTiger.backup
WiredTiger.basecfg
WiredTiger

可以看到: cursor给出的journal缺少目录，因此在percona-server中就对其进行了补齐
*/