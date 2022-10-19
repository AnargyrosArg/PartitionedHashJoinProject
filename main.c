#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "hash1.h"
#include "partition.h"

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
    printf("created commit test");
    return 0;
}