#include <stdio.h>
#include "hash1.h"
#include <stdlib.h>
#include <time.h>


int main(void){
    printf("hash1 result: %d\n",hash1(-139,2));
    
    tuple* tuples = malloc(1000 * sizeof(tuple));
    for(int i=0;i<1000;i++){
        tuples[i].key=i;
        tuples[i].payload=rand()%1000;
    }
    relation relA;
    relA.num_tuples=1000;
    relA.tuples=tuples;

    partition_relation(relA,3);
    return 0;
}