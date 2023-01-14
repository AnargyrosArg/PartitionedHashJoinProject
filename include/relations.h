#include <stdlib.h>
#include "inttypes.h"
#include <sys/mman.h>
#include <stdio.h>
#include <limits.h>
#include <string.h>

#ifndef MYTYPES_H
#define MYTYPES_H


#define INITIAL_RESULT_CAPACITY 500000
#define RESULT_CAPACITY_INCREMENT 50000
#define MAX_DISTINCT_SIZE 5000000


typedef struct tuple tuple;

typedef struct table table;

typedef struct relation relation;

typedef struct pair pair;

typedef struct result result;

typedef struct stats stats;
typedef struct query_stats query_stats;



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
    int64_t lower;
    int64_t upper;
    uint64_t count;
    uint64_t distinct;
};

// statistics for each column of each relation in a query
struct query_stats {
    size_t num_query_rels; // number of relations involved in query
    size_t* cols_per_rel;  // number of columns for each relation
    stats** statistics;    // statistics for each column of each relation
    int** connections;     // array for each relation, showing which relations it has been joined with (we assume it is joined with itself by default)
};

struct table{
    uint64_t num_tuples;
    size_t num_colums;
    stats* statistics; // stores initial statistics for each column of table
    //2 dimensional array;Used as table[column][tuple]
    uint64_t** table;
};

#endif

void init_result(result* res);
void add_result(result* res, pair p);
void delete_result(result* res);

table load_relation(const char* filename, int optimize); // if optimize is non-zero, stats for each column will be calculated on load
void delete_table(table*);
void print_table(table, int only_stats);