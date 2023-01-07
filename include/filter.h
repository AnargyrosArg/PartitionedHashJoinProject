#ifndef FILTER
#define FILTER

#include <stdio.h>

#include "relations.h"
#include "intermediates.h"
#include "jobscheduler.h"

#define MAX_FILTER_JOBS 6

// underscores are to distinguish from other enumerator with same values in parser.h
enum operation { _EQUALS , _GREATER , _LESS, _GREATER_EQUALS, _LESS_EQUALS };

//we make a basic  list that holds ints
typedef struct list {
    int row_id;
    int payload;
    struct list *next;
    struct list *tail;
} list;

typedef struct filter_job_args{
    int operation;
    int target;
    Intermediate* r;
    int start;
    int stop;
    int actualid;
    int colid;
    int relid;
    int* completed;
    table* tabl;
    int* ret;
    pthread_mutex_t* filter_mutex;
    pthread_cond_t* filter_cond;
    int* n_results;
} filter_job_args;

//basic list functions
list *init_list();
void list_append(list *l, size_t id, size_t pd);
void delete_list(list *l);
void print_list(list *l);


// r is the relation to be filtered, resulting relation will be placed in ret
void filter_function(relation* r, relation* ret, int operation, int target);
void better_filter_function(relation* r, relation* ret, int operation, int target);
void filter_intermediate(Intermediate* r,Intermediate** ret,int operation,int target,int relid,int colid,table* tabl,int actualid,jobscheduler* scheduler);