map_test:sdb_map.o map_test.o internal.o
	gcc -Wall -g -o map_test sdb_map.c map_test.c internal.c
cache_test:sdb_map.o cache_test.o internal.o sdb_cache.o 
	gcc -Wall -g -o cache_test sdb_cache.c sdb_map.c cache_test.c internal.c
db_test:sdb_map.o wal.o sdb.o internal.o sdb_cache.o db_test.o 
	gcc -Wall -g -o db_test sdb.c sdb_cache.c sdb_map.c internal.c db_test.c wal.c
repair:internal.o repair.o
	gcc -Wall -g -o repair internal.c repair.c
.PHONY:clean
clean:
	rm db_test map_test cache_test repair *.o
