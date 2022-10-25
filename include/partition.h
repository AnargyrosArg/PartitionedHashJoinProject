#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "hash1.h"
#include "utils.h"
#include "relations.h"


typedef struct partition_result partition_result;

partition_result partition_relation(relation,int);

struct partition_result{
    relation ordered_rel;
    int* prefix_sum;
    int histogram_size;
};