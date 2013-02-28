#include "wal.h"
#include "internal.h"

#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

void wal_free(wal* w){
    free(w->name);
    free(w);
}

static int writebuf(wal* w,char* toappend,int buf_len){
	int ptr=0;
	int off=w->size;
	int flag=0;
	while(buf_len>0){
		if(w->left<buf_len){
			memcpy(w->buf+off,toappend+ptr,w->left);
			ptr+=w->left;
			buf_len-=w->left;
			int fd;
			if(w->fd>0) fd=w->fd;
			else {
			    fd=open(w->name,O_WRONLY|O_APPEND);
                if(fd<0) {
                    fd=open(w->name,O_WRONLY|O_APPEND|O_CREAT|O_EXCL,MODE);
                    if(fd<0) return -1;
                }
                w->fd=fd;
			}
			if(-1==write(fd,w->buf,WAL_BUF_SIZE)) return -1;
			w->left=WAL_BUF_SIZE;
			w->size=0;
			off=0;
			flag=1;
		}else {
			memcpy(w->buf+off,toappend+ptr,buf_len);
			if(flag) {
			    assert(w->fd>0);
			    if(-1==write(w->fd,w->buf,buf_len)) return -1;
            }else{
                w->size+=buf_len;
                w->left-=buf_len;
            }
			break;
		}
	}
	return 0;
}
wal* wal_new(char* name,int type){
	wal* w=calloc(1,sizeof(wal));
	if(w==NULL) return NULL;
	w->left=WAL_BUF_SIZE;
	w->type=type;
	w->name=malloc(strlen(name)+1);
	if(w->name==NULL) {
		free(w);
		return NULL;
	}
	strcpy(w->name,name);
	return w;
}

int wal_write(wal* w,int pos,char* buf,int len){
	char *toappend=malloc(9+len+1);
	if(toappend==NULL) return -1;
	toappend[0]=w->type;
	memcpy(toappend+1,(char*)&pos,sizeof(int));
	memcpy(toappend+5,(char*)&len,sizeof(int));
	memcpy(toappend+9,buf,len);
	toappend[9+len]='>';
	int buf_len=10+len;
	int ret;
	if(writebuf(w,toappend,buf_len)==-1) {
        free(toappend);
        ret=-1;
	}
	free(toappend);
	ret=0;
	return ret;
}


int wal_log(wal* w){
    int fd;
    if(w->type==WAL_DAT) writebuf(w,"commit",strlen("commit"));
    if(w->fd>0) fd=w->fd;
    else {
        fd=open(w->name,O_WRONLY|O_APPEND);
        if(fd<0) {
            fd=open(w->name,O_WRONLY|O_APPEND|O_CREAT|O_EXCL,MODE);
            if(fd<0) return -1;
        }
        w->fd=fd;
    }
    if(-1==write(fd,w->buf,w->size)) return -1;
    memset(w->buf,0,WAL_BUF_SIZE);
    w->size=0;
    w->left=WAL_BUF_SIZE;
    if(w->type==WAL_DAT)
        if(-1==fsync(w->fd)) return -1;
    if(-1==close(w->fd)) return -1;
    w->fd=0;
    return 0;
}

