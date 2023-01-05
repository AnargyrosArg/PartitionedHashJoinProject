#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "hash1.h"
#include "utils.h"
#include "relations.h"
#include "jobscheduler.h"

#define MAX_PASSES 10
#define MAX_ORDER_JOBS 4
#define MAX_HISTOGRAM_JOBS 4

typedef struct partition_result partition_result;

typedef struct partition_info partition_info;

typedef struct histogram_job_args histogram_job_args;

typedef struct order_job_args order_job_args;
partition_info partition_relations(relation , relation , int,jobscheduler*);

void delete_part_info(partition_info);
//void delete_part_result(partition_result);



struct partition_result{
    relation ordered_rel;
    int* prefix_sum;
    int* partition_sizes;
    int histogram_size;
    int depth;
};


struct partition_info{
    partition_result relA_info;
    partition_result relB_info;
};

struct histogram_job_args{
    int start;
    int stop;
    int histogram_size;
    int *histogram;
    relation* rel;
    int depth;
    int* job_counter;
    pthread_mutex_t* histogram_mutex;
    pthread_cond_t* histogram_cond;
};


struct order_job_args
{
    int start;
    int stop;
    relation rel;
    relation ordered_rel;
    int depth;
    int* prefix_sum;
    int* offsets;
    pthread_mutex_t* order_mutex;
    pthread_cond_t* order_cond;
    int* completed;
};
