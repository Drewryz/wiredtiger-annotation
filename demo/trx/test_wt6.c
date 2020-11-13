/*
 * @Author: yangzaorang 
 * @Date: 2020-11-05 10:38:28 
 * @Last Modified by: yangzaorang
 * @Last Modified time: 2020-11-05 11:13:01
 */

/*
 * wt采用了redo机制作为恢复，所以，当有超大事务的时候，buffer被打满，事务就会被阻塞。
 */


#include <wiredtiger.h>
#include <wiredtiger_ext.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

void iterate_cursor(WT_CURSOR *cursor) {
    int ret;
    int key;
    const char *value;
    int count = 0;
    cursor->reset(cursor);
    while((ret = cursor->next(cursor)) == 0) {
        cursor->get_key(cursor, &key);
        cursor->get_value(cursor, &value);
        count++;
        printf("record: %d : %s\n", key, value);
    }
    printf("------------------get %d record------------------\n", count);
}

char* genRandomString(int length)
{
	int flag, i;
	char* string;
	if ((string = (char*) malloc(length)) == NULL )
	{
		printf("Malloc failed!flag:14\n");
		return NULL;
	}
 
	for (i = 0; i < length - 1; i++)
	{
		flag = rand() % 3;
		switch (flag)
		{
			case 0:
				string[i] = 'A' + rand() % 26;
				break;
			case 1:
				string[i] = 'a' + rand() % 26;
				break;
			case 2:
				string[i] = '0' + rand() % 10;
				break;
			default:
				string[i] = 'x';
				break;
		}
	}
	string[length - 1] = '\0';
	return string;
}

int main() {
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    const char *key, *value;
    wiredtiger_open("./wt_meta", NULL, "create,cache_size=1GB,log=(enabled,recover=on)", &conn);
    conn->open_session(conn, NULL, NULL, &session);
    session->create(session, "table:my_table", "key_format=i,value_format=S");
    session->open_cursor(session, "table:my_table", NULL, NULL, &cursor);
    session->begin_transaction(session, "isolation=snapshot");

    int record_num = 2048;
    srand((unsigned)time(NULL));
    for (int i=0; i<record_num; i++) {
        printf("number: %d\n", i);
        cursor->set_key(cursor, i);
        char* rand_str = genRandomString(1024*1024);
        cursor->set_value(cursor, rand_str);
        int ret = 0;
        switch (ret = cursor->insert(cursor)) {
        case 0: /* Success */
            break;
        default:
            // 报错：
            // oldest pinned transaction ID rolled back for eviction
            // insert error: -31800
            // number: 902
            printf("insert error: %d\n", ret);
            break;
        }
    }
    session->commit_transaction(session,NULL);

    iterate_cursor(cursor);

    conn->close(conn, NULL);

    sleep(1000);
}