#include <stdio.h>
#include <stdlib.h>

#include "relations.h"

typedef struct Intermediates Intermediates;
typedef struct Intermediate Intermediate;

// array of intermediate results
struct Intermediates {
    size_t relation_count;
    Intermediate** intermediates;
};

// array of rowid arrays
struct Intermediate {
    size_t relation_count;
    size_t rowids_count;
    int** rowids;
};

Intermediates* init_intermediates(size_t relation_count);
Intermediate* init_intermediate(size_t relation_count);
void delete_intermediates(Intermediates** intermediates);
void delete_intermediate(Intermediate** intermediate);
Intermediates* insert_intermediates_filter(Intermediates* intermediates, relation* rel, uint relation_index);
void print_intermediates(Intermediates* intermediates);
void get_intermediates(Intermediates* intermediates, uint relation_index, int** ret, int* ret_len);
Intermediates* insert_intermediates_join(Intermediates* intermediates, result* result, uint* indexes);