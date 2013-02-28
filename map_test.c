#include "sdb_map.h"
#include "internal.h"

int main(){
	struct sdb_map* sm=sdb_map_new();
	if(sm==NULL) errdump("map alloc fail");
	char *s="sdlsdfj";
	if(sdb_map_put(sm,234,(void*)s)==-1) errdump("map put fail");
	if(sdb_map_get(sm,234)==NULL) errdump("map get fail");
	if(sdb_map_get(sm,234)!=s) errdump("map get(234)!=s");
	int i;
	char* str="sldkfjsldfjsdleiojufowfejsjfiwejfeifjwefjsdlfslf";
	int len=strlen(str);
	for(i=0;i<100;++i) {
		sdb_map_put(sm,i,str+i%len);
	}
	for(i=0;i<100;++i) {
		printf("%d	%s\n",i,(char*)sdb_map_get(sm,i));
	}
	for(i=0;i<50;++i) {
		sdb_map_erase(sm,i);
	}
	for(i=0;i<100;++i) {
		printf("%d	%s\n",i,(char*)sdb_map_get(sm,i));
	}
	//printf("slots:%d size:%d\n",sm->slots,sm->size);
	sdb_map_free(sm);
	return 0;
}
