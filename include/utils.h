#include "relations.h"
#include <stdlib.h>

int power(int base,int exp);
int pseudo_log2(int val);
void init_relation(relation* rel,int num_tuples);
void delete_relation(relation rel);
void init_array(int* array,int size,int value);