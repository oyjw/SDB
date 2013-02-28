#ifndef _SDB_CACHE_H_
#define _SDB_CACHE_H_
#include "wal.h"
typedef struct page page;
typedef struct sdb_cache sdb_cache;

struct page{
	char* data;
	int size;     //direct
	int no;

    int no_write;
	int dirty;
	struct page* dirty_next;

	struct page* next;
	struct page* prev;
};


struct sdb_cache{
	int fd;
	int cap;
	int used;
	struct sdb_map* map;
	page lru_;

    int page_size;
	int pages;

	struct page* dirty_pages;
	int wal_type;

	wal* w;
};


int sdb_cache_getdat(sdb_cache* sc,long off,int len,char* buffer);
int sdb_cache_append(sdb_cache* sc,char* buf,int len);
int sdb_cache_setdat(sdb_cache* sc,long off,char* buf,int len);
void sdb_cache_free(sdb_cache* sc);
sdb_cache* sdb_cache_new(int fd,int cap,int page_size,char* log_name,int wal_type);
int sdb_cache_flush(sdb_cache* cache);
#endif
