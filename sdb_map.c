#include "sdb_map.h"
#include <stdlib.h>
#include <stdio.h>
struct node{
	void* data;
	int key;
	struct node* next;
};

struct sdb_map{
	int slots;
	int size;
	struct node** buckets;
};


void sdb_map_free(struct sdb_map* sm){
	int i;
	for(i=0;i<sm->slots;++i){
		if(sm->buckets[i]!=NULL){
			struct node *n=sm->buckets[i];
			do{
				struct node* next=n->next;
				free(n);
				n=next;
			}while(n!=NULL);
		}
	}
	free(sm->buckets);
	free(sm);
}

struct sdb_map* sdb_map_new(){
	struct sdb_map *sm=malloc(sizeof(struct sdb_map));
	if(sm==NULL) return NULL;
	sm->slots=DEFAULT_SLOTS;
	sm->size=0;
	sm->buckets=calloc(1,sizeof(struct node*)*sm->slots);
	if(sm->buckets==NULL) {
		free(sm);
		return NULL;
	}
	return sm;
}



static int resize(struct sdb_map* sm,int newslots){
	struct node** newbuckets=calloc(1,sizeof(struct node*)*newslots);
	if(newbuckets==NULL) return -1;
	int i;
	for(i=0;i<sm->slots;++i){
		if(sm->buckets[i]!=NULL){
			struct node *n=sm->buckets[i];
			do{
				int hash=(n->key)%newslots;
				struct node* next=n->next;
				n->next=newbuckets[hash];
				newbuckets[hash]=n;
				n=next;
			}while(n!=NULL);
		}
	}
	struct node** old=sm->buckets;
	sm->buckets=newbuckets;
	sm->slots=newslots;
	free(old);
	return 0;
}

void* sdb_map_get(struct sdb_map* sm,int key){
	int hash=key%sm->slots;
	struct node* n=sm->buckets[hash];
	while(n!=NULL){
		if(n->key==key) return n->data;
		else n=n->next;
	}
	return NULL;
}

int sdb_map_put(struct sdb_map* sm,int key,void* data){       //doesn't do key compare
	if(sm->size>=sm->slots) {
		if(resize(sm,sm->slots*2)<0) return -1;
	}
	int hash=key%sm->slots;
	struct node* n=malloc(sizeof(struct node));
	if(n==NULL) return -1;
	n->next=sm->buckets[hash];
	n->key=key;
	n->data=data;
	sm->buckets[hash]=n;
	sm->size++;
	return 0;
}

int sdb_map_erase(struct sdb_map* sm,int key){
	int hash=key%sm->slots;
	struct node* n=sm->buckets[hash];
	struct node* prev=n;
	while(n!=NULL){
		if(n->key==key) {
			if(prev==n) {
				sm->buckets[hash]=n->next;
			}else {
				prev->next=n->next;
			}
			free(n);
			return 0;
		}else {
			prev=n;
			n=n->next;
		}
	}
	return -1;
}

void map_dump(sdb_map* sm){
    int i;
	for(i=0;i<sm->slots;++i){
		if(sm->buckets[i]!=NULL){
			struct node *n=sm->buckets[i];
			while(n!=NULL){
			    fprintf(stderr,"n->key:%d  ",n->key);
                n=n->next;
			}
		}
	}
	fprintf(stderr,"\n");
}
