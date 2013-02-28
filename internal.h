#ifndef _INTERNAL_H_
#define _INTERNAL_H_

#define DEF_PAGES 100       //dat cache
#define DEF_PAGE_SIZE 512

#define DEF_IDX_HASHES 131     //idx file
#define BUCKET_LEN 4
#define HEADER_LEN 4
#define KEY_LEN 4

#define EXIST 0         //record header
#define DELETED 1

#define MODE (S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP)

#define WAL_IDX 0
#define WAL_DAT 1
#define WAL_BUF_SIZE 4096

void errdump(char* msg,...);

void tolowercase(char* s);
#endif
