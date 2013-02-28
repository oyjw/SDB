#include "sdb.h"
#include "internal.h"
#include "sdb_cache.h"
#include "wal.h"

#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>


static long hashfunc(char* buf,int len){
	int i;
	long hash=0;
	for(i=0;i<len;++i)
		hash+=(i+1)*buf[i];
    if(hash<0) errdump("hash:%ld\n",hash);
	return hash;
}

static int sdb_sync(sdb* db){
    wal* dat_w=db->dat_cache->w;
    wal* idx_w=db->idx_cache->w;
    if(-1==wal_log(idx_w)) return -1;
    if(-1==wal_log(dat_w)) return -1;
	if(-1==sdb_cache_flush(db->idx_cache)) return -1;
	if(-1==sdb_cache_flush(db->dat_cache)) return -1;
	if(-1==remove(dat_w->name)) return -1;
	return 0;
}

static struct sdb* sdb_alloc(char* path){
    int len=strlen(path);
	char* path_name=malloc(len+5);
	if(path_name==NULL) return NULL;
	strcpy(path_name,path);
	tolowercase(path_name);
	strcpy(path_name+len,".log");
	int fd=open(path_name,O_RDONLY);
	if(fd>0) {
        fprintf(stderr,"detect log file exist.you should repair it.\n");
        free(path_name);
        exit(1);
    }
	struct sdb* db=calloc(1,sizeof(sdb));
	if(db==NULL) return NULL;
	db->auto_commit=1;
	db->path=path_name;
	return db;
}

sdb* sdb_conn(char* path){
	sdb* db=sdb_alloc(path);
	if(db==NULL) return NULL;
	int path_len=strlen(path);
	strcpy(db->path+path_len,".dat");

	int dat_fd,idx_fd;
	dat_fd=open(db->path,O_RDWR);
	if(dat_fd<0) return db;
	strcpy(db->path+path_len,".idx");
	idx_fd=open(db->path,O_RDWR);
	if(idx_fd<0) {
		close(dat_fd);
		goto err1;
	}
	strcpy(db->path+path_len,".log");
	db->dat_cache=sdb_cache_new(dat_fd,DEF_PAGES,DEF_PAGE_SIZE,db->path,WAL_DAT);
	if(NULL==db->dat_cache) goto err2;
	db->idx_cache=sdb_cache_new(idx_fd,10,528,db->path,WAL_IDX);
	if(NULL==db->idx_cache) {
		sdb_cache_free(db->dat_cache);
		goto err2;
	}
	db->init=1;
	return db;
	err2:
        close(dat_fd);
        close(idx_fd);
	err1:
		free(db->path);
		free(db);
		return NULL;
}

static int init(sdb* db){
	int idx_len=HEADER_LEN+DEF_IDX_HASHES*BUCKET_LEN;
	char buf[idx_len];
	int tmp=-1;
	int len=strlen(db->path);
	strcpy(db->path+len-4,".dat");
	int dat_fd,idx_fd;
	dat_fd=open(db->path,O_RDWR | O_CREAT |O_EXCL,MODE);
	if(dat_fd<0) return -1;
	strcpy(db->path+len-4,".idx");
	idx_fd=open(db->path,O_RDWR | O_CREAT |O_EXCL,MODE);
	if(idx_fd<0) {
		close(dat_fd);
		return -1;
	}
	strcpy(db->path+len-4,".log");
	db->dat_cache=sdb_cache_new(dat_fd,3,5,db->path,WAL_DAT);
	if(NULL==db->dat_cache) goto err;
	db->idx_cache=sdb_cache_new(idx_fd,3,5,db->path,WAL_IDX);
	if(NULL==db->idx_cache) {
		sdb_cache_free(db->dat_cache);
		goto err;
	}
	memset(buf,0,idx_len);
	if(-1==sdb_cache_append(db->idx_cache,buf,idx_len)) goto err2;
	if(-1==sdb_cache_append(db->dat_cache,(char*)&tmp,4)) goto err2;
	if(-1==sdb_sync(db)) goto err2;
	db->init=1;
	return 0;
	err2:
        sdb_cache_free(db->dat_cache);
        sdb_cache_free(db->idx_cache);
	err:
        close(dat_fd);
        close(idx_fd);
        return -1;
}



static int sdb_idx_find(sdb* db,slice* key,int* rec_pos){
	int hash=hashfunc(key->buf,key->len)%DEF_IDX_HASHES;
	long off=HEADER_LEN+hash*BUCKET_LEN;
	sdb_cache* sc=db->idx_cache;
	char buffer[8];
	char* str=NULL;
	if(-1==sdb_cache_getdat(sc,off,4,buffer)) return -1;
	int cur_ptr=*((int*)buffer);
	if(cur_ptr==0) return -1;
	int next_ptr=0;
	int last_ptr=off;
	while(cur_ptr){
		sdb_cache_getdat(sc,cur_ptr,8,buffer);
		next_ptr=*((int*)buffer);
		int key_len=*((int*)buffer+1);
		if(key_len==key->len) {
			if(str==NULL) {
				str=malloc(key->len);
				if(str==NULL) return -1;
			}
			if(-1==sdb_cache_getdat(sc,cur_ptr+8,key->len,str)) return -1;
			if(memcmp(key->buf,str,key->len)==0) break;
		}
		if(next_ptr==0) return -1;
		else {
			last_ptr=cur_ptr;
			cur_ptr=next_ptr;
		}
	}
	if(str!=NULL) free(str);
	db->last_ptr=last_ptr;
	db->cur_ptr=cur_ptr;
	db->next_ptr=next_ptr;
	if(-1==sdb_cache_getdat(sc,cur_ptr+8+key->len,4,buffer)) return -1;
	*rec_pos=*((int*)buffer);
	return 0;
}

static int sdb_dat_insert(sdb*db,slice* key,slice* value){
	sdb_cache* sc=db->dat_cache;
	char *buffer=malloc(9+key->len+value->len);
	buffer[0]=EXIST;
	memcpy(buffer+1,&key->len,sizeof(int));
	memcpy(buffer+5,key->buf,key->len);
	memcpy(buffer+5+key->len,&value->len,sizeof(int));
	memcpy(buffer+9+key->len,value->buf,value->len);
	int pos=sdb_cache_append(sc,buffer,9+key->len+value->len);
	free(buffer);
	return pos;
}

static int sdb_idx_insert(sdb* db,slice* key,int pos){
	sdb_cache* sc=db->idx_cache;
	int hash=hashfunc(key->buf,key->len)%DEF_IDX_HASHES;
	long off=HEADER_LEN+hash*BUCKET_LEN;
	char buffer[4];
	char* toappend=malloc(13+key->len);
	if(toappend==NULL) return -1;
	if(-1==sdb_cache_getdat(sc,off,4,buffer)) return -1;
	toappend[0]=EXIST;
	memcpy(toappend+1,buffer,4);
	memcpy(toappend+5,(char*)&key->len,sizeof(int));
	memcpy(toappend+9,key->buf,key->len);
	memcpy(toappend+9+key->len,(char*)&pos,sizeof(int));
	int ptr=sdb_cache_append(sc,toappend,13+key->len);
	if(ptr==-1) goto err;
	ptr++;
	if(-1==sdb_cache_setdat(sc,off,(char*)&ptr,4)) goto err;
	free(toappend);
	return 0;
	err:
		free(toappend);
		return -1;
}

int sdb_insert(sdb* db,slice* key,slice* value){
	if(db->init==0) {
		if(init(db)==-1) return -1;
	}
	int rec_pos;
	int ret=sdb_idx_find(db,key,&rec_pos);
	if(ret==0) return -1;

	ret=sdb_dat_insert(db,key,value);
	if(ret==-1) return -1;
	ret=sdb_idx_insert(db,key,ret);
	if(ret==-1) return -1;
	if(db->auto_commit==1)
        if(-1==sdb_sync(db)) return -1;
	return 0;
}

void sdb_begin(sdb* db){
    db->auto_commit=0;
}

int sdb_commit(sdb* db){
    if(-1==sdb_sync(db)) return -1;
    db->auto_commit=1;
    return 0;
}

int sdb_replace(sdb* db,slice* key,slice* value){
	if(db->init==0) {
		if(init(db)==-1) return -1;
	}
	int ret,rec_pos;
	ret=sdb_idx_find(db,key,&rec_pos);
	if(ret==-1) return -1;

	char c=DELETED;
	if(sdb_cache_setdat(db->dat_cache,rec_pos,&c,1)==-1) return -1;
	ret=sdb_dat_insert(db,key,value);
	if(ret==-1) return -1;

	ret=sdb_cache_setdat(db->idx_cache,db->cur_ptr+8+key->len,(char*)&ret,sizeof(int));
	if(ret==-1) return -1;

	if(db->auto_commit==1)
        if(-1==sdb_sync(db)) return -1;
	return 0;
}

static int sdb_idx_delete(sdb* db,slice* key){
	sdb_cache* sc=db->idx_cache;
	char c=DELETED;
	if(sdb_cache_setdat(sc,db->cur_ptr-1,&c,1)==-1) return -1;
	if(sdb_cache_setdat(sc,db->last_ptr,(char*)&db->next_ptr,4)==-1) return -1;
	return 0;
}

int sdb_delete(sdb* db,slice* key){
	if(db->init==0) {
		return -1;
	}
	int ret,rec_pos;
	ret=sdb_idx_find(db,key,&rec_pos);
	//fprintf(stderr,"rec_pos:%d\n",rec_pos);
	if(ret==-1) return -1;

	char c=DELETED;
	if(sdb_cache_setdat(db->dat_cache,rec_pos,&c,1)==-1) return -1;
	ret=sdb_idx_delete(db,key);
	if(ret==-1) return -1;
	if(db->auto_commit==1)
        if(-1==sdb_sync(db)) return -1;
	return 0;
}



int sdb_get(sdb* db,slice* key,slice* value){
	if(db->init==0) {
		return -1;
	}
	int rec_pos;
	int ret=sdb_idx_find(db,key,&rec_pos);
	if(ret==-1) return -1;

	ret=sdb_cache_getdat(db->dat_cache,rec_pos+5+key->len,sizeof(int),(char*)&value->len);
	if(ret==-1) return -1;

	char* value_buf=malloc(value->len);
	if(value_buf==NULL) return -1;
	ret=sdb_cache_getdat(db->dat_cache,rec_pos+9+key->len,value->len,value_buf);
	if(ret==-1) {
        free(value_buf);
        return -1;
	}
	value->buf=value_buf;
	return 0;
}

int sdb_scan(sdb* db){
    if(db->init==0) {
		return -1;
	}
	int off,ret,pos,cursor,key_len;
	if(db->cursor==0){
        if(db->nextrec>0 && db->chain<(DEF_IDX_HASHES-1)) db->chain++;
		while(1){
			off=HEADER_LEN+BUCKET_LEN*db->chain;
			ret=sdb_cache_getdat(db->idx_cache,off,4,(char*)&pos);
			if(ret==-1) return -1;
			if(pos>0) break;
			if(pos==0 && db->chain>=DEF_IDX_HASHES-1) {
				return -1;
			}
			else if(db->chain<(DEF_IDX_HASHES-1)) {
				db->chain++;
			}else assert(0);
		}
	}else pos=db->cursor;

	char buffer[8];
	ret=sdb_cache_getdat(db->idx_cache,pos,8,buffer);
	if(ret==-1) return -1;
	cursor=*((int*)buffer);
	key_len=*((int*)buffer+1);
	ret=sdb_cache_getdat(db->idx_cache,pos+8+key_len,sizeof(int),(char*)&db->nextrec);
	if(ret==-1) return -1;
	db->cursor=cursor;
	return 0;
}

void sdb_rewind(sdb* db){
	db->cursor=0;
	db->chain=0;
	db->nextrec=0;
}

int sdb_nextrec(sdb* db,slice* key,slice* value){
	if(db->nextrec==0) return -1;
	int ret;
	ret=sdb_cache_getdat(db->dat_cache,db->nextrec+1,4,(char*)&key->len);
	if(ret==-1) return -1;
	key->buf=malloc(key->len);
	if(key->buf==NULL) return -1;
	ret=sdb_cache_getdat(db->dat_cache,db->nextrec+5,key->len,key->buf);
	if(ret==-1) goto err;

	ret=sdb_cache_getdat(db->dat_cache,db->nextrec+5+key->len,4,(char*)&value->len);
	if(ret==-1) goto err;
	value->buf=malloc(value->len);
	if(value->buf==NULL) goto err;
	ret=sdb_cache_getdat(db->dat_cache,db->nextrec+9+key->len,value->len,value->buf);
	if(ret==-1) {
        free(value->buf);
        goto err;
	}
	return 0;
	err:
        free(key->buf);
        return -1;
}

int sdb_close(sdb* db){
    if(db->init!=0) {
        sdb_cache_free(db->dat_cache);
        sdb_cache_free(db->idx_cache);
	}
	free(db->path);
	free(db);
	return 0;
}

