#include "relations.h"
#include <stdlib.h>

int power(int base,int exp);
int pseudo_log2(int val);
void init_relation(relation* rel,int num_tuples);
void delete_relation(relation rel);
void init_array(int* array,int size,int value);
int bitmap_get_bit(unsigned long long bitmap, int n);
void bitmap_set_bit(unsigned long long* bitmap, int n, int value);
int bitmap_full(unsigned long long bitmap, int size);
int* insert_array(int* array, int* pos, int* size, int data);