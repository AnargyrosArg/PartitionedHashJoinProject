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


// get value of n-th bit of bitmap (n=0 refers to MOST significant bit of the integer)
int bitmap_get_bit(unsigned long long bitmap, int n) {
    return ((bitmap << n) >> ((sizeof(unsigned long long)*__CHAR_BIT__)-1));
}

// set n-th bit of bitmap (n=0 refers to MOST significant bit of the integer)
void bitmap_set_bit(unsigned long long* bitmap, int n, int value) {
    if (value == 1) *bitmap = (1 << ((sizeof(unsigned long long)*__CHAR_BIT__)-n-1)) | (*bitmap);
    if (value == 0) *bitmap = *bitmap & (~(1 << ((sizeof(unsigned long long)*__CHAR_BIT__)-n-1)));
}

// returns 0 or 1 depending on whether bitmap is full (aka when everything is 1)
int bitmap_full(unsigned long long bitmap, int size) {
    for (int i=0; i<size; i++)
        if (!bitmap_get_bit(bitmap, i))
            return 0;
    return 1;
}


// insert element to array, expand array if it is full
int* insert_array(int* array, int* pos, int* size, int data) {
    if ((*pos) <= ((*size)-1)) { // if array is not full
        array[*pos] = data;
        (*pos) += 1;
        return array;
    }
    else { // if array is full
        int* new_array = malloc(sizeof(int) * (*size)*2);
        for (int i=0; i<(*size); i++)
            new_array[i] = array[i];
        new_array[*pos] = data;
        (*pos) += 1;
        (*size) *= 2;
        free(array);
        return new_array;
    }
}