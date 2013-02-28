#include "sdb_cache.h"
#include "sdb.h"
#include "internal.h"
#include <stdio.h>
#include <string.h>
#include <sys/time.h>

long get_duration(struct timeval begin,struct timeval end){
    return (1000000*end.tv_sec+end.tv_usec)-(1000000*begin.tv_sec+begin.tv_usec);
}
void test2(){
    struct timeval begin,end,end2;
    gettimeofday(&begin,NULL);
    int ret;
    sdb* db=sdb_conn("oyjw");
	if(db==NULL) errdump("db conn fail\n");
	else printf("db conn success\n");
	slice key,value;
	char buffer[10];
	memset(buffer,0,10);
	key.buf=buffer;
	int i;
	char* str="sldkfjsldfjs";
	int len=strlen(str);
	for(i=1;i<=100;++i) {
	    snprintf(buffer,10,"%d",i);
	    key.len=strlen(buffer)+1;
	    value.buf=str+i%len;
	    value.len=len-i%len+1;
		ret=sdb_insert(db,&key,&value);
		if(ret==-1) errdump("db insert fail\n");
        else printf("db insert success\n");
	}
	//if(-1==sdb_commit(db)) errdump("db transaction fail\n");
	gettimeofday(&end,NULL);
	slice s;
	key.buf[0]='1';
	int k=0;
	for(i=1;i<=100;++i) {
	    snprintf(buffer,10,"%d",i);
	    key.len=strlen(buffer)+1;
        ret=sdb_get(db,&key,&s);
        if(ret==-1) errdump("db get fail\n");
        else printf("db get success\nkey.buf:%s  key.len:%d   s.buf:%s  s.len:%d\n",key.buf,key.len,s.buf,s.len);
        k++;
	}
	printf("get %d record\n",k);
	gettimeofday(&end2,NULL);
	long duration=get_duration(begin,end);
    printf("insert duration is %ld.%ld\n",duration/1000000,duration%1000000);
    duration=get_duration(end,end2);
    printf("get duration is %ld.%ld\n",duration/1000000,duration%1000000);
	ret=sdb_close(db);
	if(ret==-1) errdump("db close fail\n");
	else printf("db close success\n");
}

void test3(){
    int ret;
    sdb* db=sdb_conn("oyjw");
	if(db==NULL) errdump("db conn fail\n");
	else printf("db conn success\n");
	slice key;
	slice s;
	int k=0;
	while(sdb_scan(db)==0){
	    ret=sdb_nextrec(db,&key,&s);
        if(ret==-1) errdump("db nextrec fail\n");
        else printf("db nextrec success\nkey.buf:%s  key.len:%d\
                    s.buf:%s  s.len:%d\n",key.buf,key.len,s.buf,s.len);
        k++;
	}

	printf("get %d record\n",k);
	ret=sdb_close(db);
	if(ret==-1) errdump("db close fail\n");
	else printf("db close success\n");
}

int main(){
    test4();
	return 0;
}
