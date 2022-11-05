#include "relations.h"

#include <stdio.h>


void init_result(result* res){
    res->result_size=0;
    res->capacity=INITIAL_RESULT_CAPACITY;
    res->pairs = malloc(INITIAL_RESULT_CAPACITY * sizeof(pair));
}

void add_result(result* res,pair p){
    //if we reached max capacity , reallocate new,larger space and copy old values over
    if(res->capacity == res->result_size){
        pair* new = malloc( (res->capacity + 100) * sizeof(pair));
        for(int i=0;i<res->result_size;i++){
            new[i].key1 = res->pairs[i].key1;
            new[i].key2 = res->pairs[i].key2;
            new[i].payload = res->pairs[i].payload;
        }
        free(res->pairs);
        res->pairs = new;
        res->capacity += 100;
    }
    //add new pair
    res->pairs[res->result_size].key1 = p.key1;
    res->pairs[res->result_size].key2 = p.key2;
    res->pairs[res->result_size].payload = p.payload;
    //increment current number of elements
    res->result_size = res->result_size +1;
    
    return;
}