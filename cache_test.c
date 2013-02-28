#include "sdb_cache.h"
#include "internal.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>



void dump(sdb_cache *sc){
    printf("sc->cap:%d\nsc->used:%d\nsc->pages:%d\n",sc->cap,sc->used,sc->pages);
    page* p=sc->dirty_pages;
	while(p!=NULL) {
	    printf("%d %d\n",p->no,p->dirty);
		p=p->dirty_next;
	}
}

void test2(){
    struct stat st;
    int fd=open("1",O_RDWR);
    if(fd<0) {
        fprintf(stderr,"creating 1\n");
        fd=open("1",O_RDWR|O_CREAT|O_EXCL,MODE);
        if(fd<0) errdump("creating 1 fail\n");
    }

    if(fstat(fd,&st)<0) errdump("stat fail");
    printf("file size:%ld\n",st.st_size);

    int pos=st.st_size;

    char buf[4096];

    sdb_cache* sc=sdb_cache_new(fd,3,5);
    if(sc==NULL) errdump("sdb_cache alloc fail\n");
    sprintf(buf,"%s","ssf3434");
    int ret=sdb_cache_append(sc,buf,strlen(buf));
    if(ret>=0) printf("cache append success\n");
    else errdump("cache append fail\n");
    dump(sc);
    ret=sdb_cache_append(sc,buf,strlen(buf));
    if(ret>=0) printf("cache append success\n");
    ret=sdb_cache_append(sc,buf,strlen(buf));
    if(ret>=0) printf("cache append success\n");

    dump(sc);
    char buf2[4096];
    ret=sdb_cache_getdat(sc,pos+3,strlen(buf)*2,buf2);
    if(ret==0) printf("cache getdat result:%s\n",buf2);
    else errdump("cache  getdat fail ret:%d\n",ret);

	dump(sc);

    ret=sdb_cache_flush(sc);
    if(ret==0) printf("cache flush success\n");
    else errdump("cache  flush fail\n");

    int k;
    scanf("%d",&k);
    ret=sdb_cache_setdat(sc,pos+2,"666666666666",12);
    if(ret==0 && sdb_cache_getdat(sc,pos,strlen(buf)*3,buf2)==0) {
        printf("cache setdat result:%s\n",buf2);
    }else errdump("cache  setdat fail ret:%d\n",ret);
    dump(sc);

    if(sc->dirty_pages==NULL) printf("yes\n");


    ret=sdb_cache_flush(sc);
    if(ret==0) printf("cache flush success\n");
    else errdump("cache  flush fail\n");
    return;
}

int main(){
    test2();
    return 0;
}
