#ifndef _SDB_H_
#define _SDB_H_
#include "sdb_cache.h"
typedef struct slice{
	char* buf;
	int len;
} slice;

typedef struct sdb sdb;


struct sdb{
	sdb_cache* dat_cache;
	sdb_cache* idx_cache;

	int last_ptr;
	int cur_ptr;
	int next_ptr;

	int chain;
	int cursor;
	int nextrec;

	int auto_commit;
	int init;
	char* path;
};

sdb* sdb_conn(char* path);
int sdb_insert(sdb* db,slice* key,slice* value);
int sdb_replace(sdb* db,slice* key,slice* value);
int sdb_delete(sdb* db,slice* key);
int sdb_get(sdb* db,slice* key,slice* value);   //should free value->buf
int sdb_scan(sdb* db);
void sdb_rewind(sdb* db);
int sdb_nextrec(sdb* db,slice* key,slice* value);   //should free key->buf,value->buf
int sdb_close(sdb* db);

void sdb_begin(sdb* db);
int sdb_commit(sdb* db);
#endif
