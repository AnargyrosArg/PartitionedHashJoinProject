#include <stdio.h>

#include <stdlib.h>

#include "relations.h"
#include "hash1.h"
#include "utils.h"
#include "hashtable.h"
#include "partition.h"
#include "intermediates.h"
#include "parser.h"
#include "jobscheduler.h"

void* joinjob(void* p);
result joinfunction(relation r, relation s,jobscheduler* scheduler);
Intermediate* join_intermediates(Intermediate* inter1,Intermediate* inter2,QueryInfo* query,int rel1,int col1,int rel2,int col2,table* tabl,jobscheduler* scheduler);
Intermediate* parallel_join(Intermediate* inter1,Intermediate* inter2,QueryInfo* query,int rel1,int col1,int rel2,int col2,table* tabl,jobscheduler* scheduler);

typedef struct join_job_args join_job_args;

struct join_job_args{
    int start;
    int stop;
    //sync between threads
    pthread_cond_t* cond;
    pthread_mutex_t* mutex;

    int* completed;

    //hashtable for relation r
    hashtable* tableR;

    //part info for relation 1 aka R
    partition_result part_info;
    //part info for relation 2 aka S
    partition_result part_info2;
    
    relation r;
    result* ret;
    int partition_index;
};