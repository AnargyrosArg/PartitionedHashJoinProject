#include <stdlib.h>
#include "inttypes.h"
#include <sys/mman.h>
#include <stdio.h>
#include <limits.h>
#include <string.h>

#ifndef MYTYPES_H
#define MYTYPES_H


#define INITIAL_RESULT_CAPACITY 100000
#define RESULT_CAPACITY_INCREMENT 50000


typedef struct tuple tuple;

typedef struct table table;

typedef struct relation relation;

typedef struct pair pair;

typedef struct result result;

typedef struct stats stats;



struct pair{
    int key1;
    int key2;
    int payload;
};

struct tuple{
    int key;
    int payload;
};

struct relation{
    tuple* tuples;
    int num_tuples;
};

struct result{
    pair* pairs;
    unsigned int result_size;
    int capacity;
};

struct stats {
    int lower;
    int upper;
    uint count;
    uint distinct;
};


struct table{
    uint64_t num_tuples;
    size_t num_colums;
    stats* statistics;
    //2 dimensional array;Used as table[column][tuple]
    uint64_t** table;
};

#endif

void init_result(result* res);
void add_result(result* res, pair p);
void delete_result(result* res);

table load_relation(const char* filename);
void delete_table(table*);
void print_table(table, int only_stats);