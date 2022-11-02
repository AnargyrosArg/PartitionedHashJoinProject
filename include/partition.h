#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "hash1.h"
#include "utils.h"
#include "relations.h"


typedef struct partition_result partition_result;

typedef struct partition_info partition_info;


partition_info partition_relations(relation , relation , int);

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