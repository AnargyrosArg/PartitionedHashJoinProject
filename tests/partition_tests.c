#include <stdio.h>
#include <stdlib.h>
#include "partition.h"
#include "acutest.h"
#include "relations.h"
#include "utils.h"

void one_pass_partition(void){
    int test_size = 16;
    relation relA;
    init_relation(&relA , test_size);
    for(int i=0;i<test_size;i++){
        relA.tuples[i].payload = i;
        relA.tuples[i].key = i;
    }
    partition_result partition_info = partition_relation(relA, 2);

    //Assert that for elements are in order of ascending hash values
    for(int i=0;i < (partition_info.ordered_rel.num_tuples-1);i++){
        TEST_ASSERT(hash1(partition_info.ordered_rel.tuples[i].payload,2) <=hash1(partition_info.ordered_rel.tuples[i+1].payload,2) );
    }

    delete_relation(relA);
    delete_relation(partition_info.ordered_rel);
    free(partition_info.prefix_sum);
}


TEST_LIST = {
    { "Basic 1 pass functionality",one_pass_partition},
    { NULL, NULL }
};