/*
 * @Author: yangzaorang 
 * @Date: 2020-12-01 16:48:33 
 * @Last Modified by:   yangzaorang 
 * @Last Modified time: 2020-12-01 16:48:33 
 */

#include <wiredtiger.h>
#include <wiredtiger_ext.h>
#include <iostream>
#include <fstream>

using namespace std;

#define BUFFER_SIZE 1024
char buff[BUFFER_SIZE];

void copy_file(const char* src, const char* dest) {
    ifstream in(src, ios::binary);
    if (!in) {
        cout << "open " << src << " error" << endl;
        exit(-1);
    }
    ofstream out(dest, ios::binary);
    if (!out) {
        cout << "open " << dest << " error" << endl;
        exit(-1);
    }
    while (!in.eof()) {
        in.read(buff, BUFFER_SIZE);
        out.write(buff, in.gcount());
    }
    in.close();
    out.close();
}

// TODO: wt backup原理
void backup(WT_CONNECTION *conn, const char* db_home, const char* dest_dir) {
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
        string db_file = string(db_home) + string("/") + string(filename);
        cout << db_file << endl;
        string dest_file = string(dest_dir) + string("/") + string(filename);
        cout << dest_file << endl;
        copy_file(db_file.c_str(), dest_file.c_str());
    }
    cursor->close(cursor);
    session->close(session, NULL);
}

int main() {
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    int ret;
    const char *db_home = "db_home";
    const char *config = "create,log=(enabled=true,archive=false)";
    wiredtiger_open(db_home, NULL, config, &conn);
    WT_SESSION *session;
    conn->open_session(conn, NULL, NULL, &session);
    const char* table_name = "my_table";
    string wt_tbl_url = string("table:").append(string(table_name));
    const char* wt_tbl_url_c = wt_tbl_url.c_str();
    session->create(session, wt_tbl_url_c, "key_format=S,value_format=S");

    session->open_cursor(session,  wt_tbl_url_c, NULL, NULL, &cursor);
    cursor->set_key(cursor, "key1");
    cursor->set_value(cursor, "val1");
    cursor->insert(cursor);
    session->checkpoint(session, NULL);

    cursor->set_key(cursor, "key2");
    cursor->set_value(cursor, "val2");
    cursor->insert(cursor);

    /*
     * 原表先插入key1:val1, 然后做checkpoint， 最后再插入key2:val2
     * 备份表可以读到key1:val1与key2:val2, 表明wt的备份不是通过最近的checkpoint来完成的
     * 具体做法看代码吧
     */

    cout << "backup..." << endl;
    backup(conn, db_home, "backup");

    cursor->close(cursor);
    session->close(session, NULL);

    conn->close(conn, NULL);
}