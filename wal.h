#ifndef _WAL_H_
#define _WAL_H_
#include "internal.h"
typedef struct wal wal;

struct wal{
	char* name;
	char buf[WAL_BUF_SIZE];
	int size;
	int left;
	int type;
	int fd;
};

void wal_free(wal* w);
wal* wal_new(char* name,int type);
int wal_write(wal* w,int pos,char* buf,int len);
int wal_log(wal* w);
#endif
