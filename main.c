#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "hash1.h"
#include "partition.h"
#include "hashtable.h"

#define SAMPLE_SIZE 400

int main(void){    
    tuple* tuples = malloc(SAMPLE_SIZE * sizeof(tuple));
    for(int i=0;i<SAMPLE_SIZE;i++){
        tuples[i].key=i;
        tuples[i].payload=rand()%1000;
        if(hash1(tuples[i].payload,2)==4){
            printf("---------------------DING----------------------------\n");
        }
    }
    relation relA;
    relA.num_tuples=SAMPLE_SIZE;
    relA.tuples=tuples;
    
    partition_relation(relA,2);

    hashbucket p= inithashbucket(4);
    printf("print hashbucket%d %d %d %d\n",p.bitmap[1], p.bitmap[2], p.bitmap[0], p.bitmap[3]);

    hashtable *ht= inithashtable(10,3);
    printf(" %d %d %d\n", ht->htbuckets[1].bitmap[1],ht->htbuckets[2].bitmap[3],ht->htbuckets[1].bitmap[3]);
    p.bitmap[1] =1;
    p.rowid = 12;
    ht->htbuckets[1].bitmap[1] = 5;
    printf("%d\n",ht->htbuckets[1].bitmap[1]);
    expandhashtable(&ht);
    printf("%d\n",ht->htbuckets[1].bitmap[1]);
    return 0;
}