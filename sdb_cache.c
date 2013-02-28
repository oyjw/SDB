#include "sdb_cache.h"
#include "sdb_map.h"
#include "internal.h"
#include "wal.h"

#include <stdlib.h>
#include <assert.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
//#include <stdio.h>



static void append(sdb_cache* sc,page* p){
	p->next=&sc->lru_;
	p->prev=sc->lru_.prev;
	sc->lru_.prev->next=p;
	sc->lru_.prev=p;
}

static void del(page* p){
	p->prev->next=p->next;
	p->next->prev=p->prev;
}

void sdb_cache_free(sdb_cache* sc){
    assert(sc->dirty_pages==NULL);
	page* p=sc->lru_.next;
	while(p!=&sc->lru_){
		page* next=p->next;
		free(p->data);
		free(p);
		p=next;
	}
	wal_free(sc->w);
	sdb_map_free(sc->map);
	close(sc->fd);
	free(sc);
}

sdb_cache* sdb_cache_new(int fd,int cap,int page_size,char* log_name,int wal_type){
	sdb_cache* sc=malloc(sizeof(sdb_cache));
	if(sc==NULL) return NULL;
	sc->fd=fd;
	sc->used=0;
	sc->cap=cap;
	sc->lru_.next=&sc->lru_;
	sc->lru_.prev=&sc->lru_;
	sc->dirty_pages=NULL;
	sc->map=sdb_map_new();
	if(sc->map==NULL) {
		free(sc);
		return NULL;
	}
	sc->page_size=page_size;
	struct stat _stat;
	if(fstat(fd,&_stat)<0) goto err;
	sc->pages=_stat.st_size/page_size+1;

	sc->w=wal_new(log_name,wal_type);
	if(sc->w==NULL) goto err;
	return sc;
	err:
        free(sc);
        sdb_map_free(sc->map);
        return NULL;
}

static page* new_page(int page_size){
	page* p=calloc(1,sizeof(page));
	if(p==NULL) return NULL;
	p->data=calloc(1,page_size);
	if(p->data==NULL) {
		free(p);
		return NULL;
	}
	return p;
}

static int write_page(sdb_cache* sc,page* p){
	if(lseek(sc->fd,p->no*sc->page_size,SEEK_SET)<0) {
		return -1;
	}
	if(write(sc->fd,p->data,p->size)<0) {
		return -1;
	}
	return 0;
}

static void del_dirty_page(sdb_cache* sc,page* p){
	page* head=sc->dirty_pages;
	page* q=NULL;
	assert(sc->dirty_pages!=NULL);
	for(;head!=NULL;){
		if(head->no==p->no) {
			if(q==NULL) sc->dirty_pages=NULL;
			else q->dirty_next=head->dirty_next;
			p->dirty_next=NULL;             //can delete
			p->dirty=0;                     //
			break;
		}
		q=head;
		head=head->dirty_next;
	}
}

page* get_page(sdb_cache* sc,int no){
	page* p=sdb_map_get(sc->map,no);
	if(p!=NULL) return p;
	if(sc->used<sc->cap){
		p=new_page(sc->page_size);
		if(p==NULL) return NULL;
		sc->used++;
	}else {
	    int flag=0;
		p=sc->lru_.next;
		while(p->no_write==1 && p!=&sc->lru_) p=p->next;
		if(p==&sc->lru_) {
		    p=new_page(sc->page_size);
            if(p==NULL) return NULL;
		    sc->used++;
		    sc->cap++;
		    flag=1;
		}
		assert(p!=NULL);
		if(p->dirty) {
			if(write_page(sc,p)==-1) return NULL;
			del_dirty_page(sc,p);
			assert(p->dirty_next==NULL && p->dirty==0);             //can delete
		}
		if(flag==0){
		    assert(sdb_map_get(sc->map,p->no)!=NULL);
            del(p);
            int ret=sdb_map_erase(sc->map,p->no);
            assert(ret==0);
            memset(p->data,0,sc->page_size);
            char* temp=p->data;
            memset(p,0,sizeof(page));
            p->data=temp;
		}
	}

	int nread=sc->page_size;
	if(no<sc->pages){
		if(lseek(sc->fd,no*sc->page_size,SEEK_SET)<0) {
			goto err;
		}
		if((nread=read(sc->fd,p->data,sc->page_size))<0) {
			goto err;
		}
		p->size=nread;   //the last page may not have 4096B
	} else {
		sc->pages++;
	}
	p->no=no;
	append(sc,p);
	if(-1==sdb_map_put(sc->map,p->no,p)) goto err;
	return p;
	err:
		free(p->data);
		free(p);
		return NULL;
}

static void add_dirty_page(sdb_cache* sc,page* p){
	p->dirty=1;
	page* q=sc->dirty_pages;
	sc->dirty_pages=p;
	p->dirty_next=q;
	p=sc->dirty_pages;
}

static int page_getdat(sdb_cache* sc,page* p,int len,long page_off,char* final_data){
	int left_len=p->size-page_off;
	assert(left_len>0);
	int ptr=0;
	while(len>0){
		if(left_len<len) {
			memcpy(final_data+ptr,p->data+page_off,left_len);
			len-=left_len;
			ptr+=left_len;
			assert(p->no!=sc->pages-1);
			p=get_page(sc,p->no+1);
			if(p==NULL) return -1;
			assert(p->size>0);
			left_len=p->size;
			page_off=0;
		}else {
			memcpy(final_data+ptr,p->data+page_off,len);
			break;
		}
	}
	return 0;
}

int sdb_cache_getdat(sdb_cache* sc,long off,int len,char* buffer){
	int no=off/sc->page_size;
	page *p=get_page(sc,no);
	if(p==NULL) return -1;
	off=off%sc->page_size;
	if(page_getdat(sc,p,len,off,buffer)==-1) return -1;
	return 0;
}

static int page_setdat(sdb_cache* sc,page* p,int len,long off,char* buf,int flag){
	int left_len;
	if(flag==1) left_len=sc->page_size-off;
	else left_len=p->size-off;
	int ptr=0;
	while(len>0){
		if(left_len<len) {
		    if(flag==0) assert(p->no!=sc->pages-1);
			memcpy(p->data+off,buf+ptr,left_len);
            p->no_write=1;
			len-=left_len;
			ptr+=left_len;
			if(flag==1) p->size+=left_len;

			assert(p->size==sc->page_size);
			if(p->dirty==0) add_dirty_page(sc,p);
			p=get_page(sc,p->no+1);
			if(p==NULL) return -1;
			left_len=sc->page_size;
			off=0;
		}else {
			memcpy(p->data+off,buf+ptr,len);
            p->no_write=1;
			if(flag==1) p->size+=len;
			if(p->dirty==0) add_dirty_page(sc,p);
			break;
		}
	}
    return 0;
}

int sdb_cache_append(sdb_cache* sc,char* buf,int len){
	page* p=get_page(sc,sc->pages-1);
	if(p==NULL) return -1;
	int left_len=sc->page_size-p->size;
	if(left_len==0) {
		p=get_page(sc,sc->pages);
		if(p==NULL) return -1;
	}
    int ret_off=p->size+p->no*sc->page_size;
	if(page_setdat(sc,p,len,p->size,buf,1)==-1) return -1;
    if(-1==wal_write(sc->w,ret_off,buf,len)) return -1;
	return ret_off;
}

int sdb_cache_setdat(sdb_cache* sc,long off,char* buf,int len){
    if(-1==wal_write(sc->w,off,buf,len)) return -1;
	int no=off/sc->page_size;
	off=off%sc->page_size;
	page *p=get_page(sc,no);
	if(p==NULL) return -1;
	if(page_setdat(sc,p,len,off,buf,0)==-1) return -1;
	return 0;
}

int sdb_cache_flush(sdb_cache* sc){
	page* p=sc->dirty_pages,*q;
	while(p!=NULL) {
		assert(p->dirty==1);
		if(write_page(sc,p)==-1) return -1;
		p->dirty=0;
		q=p->dirty_next;
		p->dirty_next=NULL;
		p=q;
	}
	sc->dirty_pages=NULL;
	if(-1==fsync(sc->fd)) return -1;
	return 0;
}
