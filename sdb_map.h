#ifndef _SDB_MAP_H_
#define _SDB_MAP_H_

#define DEFAULT_SLOTS 8


typedef struct sdb_map sdb_map;


sdb_map* sdb_map_new();
void sdb_map_free(struct sdb_map* sm);
void* sdb_map_get(struct sdb_map* sm,int key);
int sdb_map_put(struct sdb_map* sm,int key,void* data);
int sdb_map_erase(struct sdb_map* sm,int key);
void map_dump(sdb_map* sm);
#endif
