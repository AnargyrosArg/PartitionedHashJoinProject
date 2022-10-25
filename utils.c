#include "utils.h"


/*
Various utility functions
*/

int power(int base,int exp){
    int ret=base;
    for(int i=0;i<exp-1;i++){
        ret *= base;
    }
    return ret;
}

int pseudo_log2(int val){
    int count=0;
    while(val/2){
        count++;
        val=val/2;
    }
    return count;
}

void init_relation(relation* rel,int num_tuples){
    rel->tuples = malloc(num_tuples * sizeof(tuple));
    rel->num_tuples = num_tuples;
}

void delete_relation(relation rel){
    free(rel.tuples);
}

void init_array(int* array,int size,int value){
    for(int i=0;i < size ;i++){
        array[i]=value;
    }
}