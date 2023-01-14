#include "relations.h"
#include <stdlib.h>
#include <stdio.h>

void read_file(relation *rela,char *name);

int power(int base,int exp);
int pseudo_log2(int val);
int factorial(int x);

void init_relation(relation* rel,int num_tuples);
void delete_relation(relation rel);

void init_array(int* array,int size,int value);

void print_result(result* res);

int bitmap_get_bit(unsigned long long bitmap, int n);
void bitmap_set_bit(unsigned long long* bitmap, int n, int value);
int bitmap_full(unsigned long long bitmap, int size);

int* insert_array(int* array, int* pos, int* size, int data);

uint array_to_int(int* array, size_t size); // strings all ints of an array into one int
uint concat_uints(uint x, uint y); // concatenates two uints (2,3 = 23)

int combinations(int arr[], int size, int r, int** ret); // calculate all possible combinations of r elements in an array
int combinations_util(int arr[], int data[], int start, int end, int index, int r, int** ret, int count);

int in(int* arr, size_t size, int x); // returns 1 if x is in arr, 0 otherwise