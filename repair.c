#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "internal.h"

char *array[]={"IDX","DAT"};

int writefile(FILE* fp,int pos,char* buffer,long len){
    if(0!=fseek(fp,pos,SEEK_SET)) return -1;
    if(fwrite(buffer,len,1,fp)!=1) return -1;
    return 0;
}
int restore(FILE *fp,char* name,int len){
    strcpy(name+len,".dat");
    FILE* dat_fp=fopen(name,"rb+");
    if(dat_fp==NULL) errdump("dat file open fail\n");
    strcpy(name+len,".idx");
    FILE* idx_fp=fopen(name,"rb+");
    if(idx_fp==NULL) errdump("idx file open fail\n");
    int nread;
    char c,flag;
    int pos,data_len;
    char* str;
    FILE* filep;
    if(fseek(fp,0,SEEK_SET)!=0) return -1;
    int success=0;
    while(1){
        if(1!=fread(&c,1,1,fp)) break;
        if(c=='c') {
            fprintf(stderr,"transaction commit,redo it\n");
            success=1;
            break;
        }
        if(1!=fread(&pos,sizeof(int),1,fp)) break;

        if(1!=fread(&data_len,sizeof(int),1,fp)) break;

        str=malloc(data_len);
        if(str==NULL) return -1;
        if(data_len!=(nread=fread(str,1,data_len,fp))) break;
        if(1!=fread(&flag,1,1,fp)) break;
        fprintf(stderr,"%s  pos:%d  dat_len:%d  str:%s\n",array[(int)c],pos,data_len,str);
        assert(flag=='>');
        if(c==WAL_DAT) filep=dat_fp;
        else filep=idx_fp;
        if(writefile(filep,pos,str,data_len)==-1) return -1;

    }
    if(success==1) return 0;
    if(ferror(fp)) return -1;
    assert(0);
}

int iscommit(FILE* fp){
    if(fseek(fp,-6,SEEK_END)!=0) return -1;
    char buf[6];
    if(fread(buf,6,1,fp)!=1) return -1;
    if(memcmp(buf,"commit",6)!=0) return -1;
    fprintf(stderr,"%s\n",buf);
    return 0;
}

int main(int argc,char* argv[]){
    if(argc!=2)  errdump("usage:repair dbname(no suffix)\n");
    int len=strlen(argv[1]);
    char* name=malloc(len+5);
    if(name==NULL) errdump("mem alloc fail\n");
    strcpy(name,argv[1]);
    tolowercase(name);
    strcpy(name+len,".log");
    fprintf(stderr,"%s\n",name);
    FILE* fp=fopen(name,"rb");
    if(fp==NULL) errdump("log file doesn't exist\n");
    if(iscommit(fp)==0)
        if(restore(fp,name,len)==-1) errdump("repair fail\n");
    printf("repair successfully\n");
    strcpy(name+len,".log");
    if(-1==remove(name)) errdump("remove %s fail\n",name);
    else printf("remove %s successfully\n",name);
    return 0;
}
