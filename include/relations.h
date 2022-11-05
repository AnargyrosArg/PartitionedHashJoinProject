#include <stdlib.h>

#ifndef MYTYPES_H
#define MYTYPES_H


#define INITIAL_RESULT_CAPACITY 100



typedef struct tuple tuple;

typedef struct relation relation;

typedef struct pair pair;

typedef struct result result;



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

#endif

void init_result(result* res);
void add_result(result* res, pair p);


