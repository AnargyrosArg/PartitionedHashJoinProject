#include <stdio.h>
#include <stdlib.h>
#include "partition.h"
#include "acutest.h"
#include "relations.h"
#include "utils.h"

void partition_test(void){
    int test_size = 16;
    relation relA;
    relation relB;
    
    init_relation(&relA , test_size);
    init_relation(&relB, test_size);

    for(int i=0;i<test_size;i++){
        relA.tuples[i].payload = i;
        relA.tuples[i].key = i;

        relB.tuples[i].key = i;
        relB.tuples[i].payload = i%4;
    }
    partition_info info = partition_relations(relA, relB,2);

    TEST_ASSERT(info.relA_info.histogram_size == info.relB_info.histogram_size);
    TEST_ASSERT(info.relA_info.depth == info.relB_info.depth);
    
    int sumA=0,sumB=0;
    for(int i=0 ; i<info.relA_info.histogram_size;i++){
        sumA+= info.relA_info.partition_sizes[i];
        sumB += info.relB_info.partition_sizes[i];
    }
    TEST_ASSERT((info.relA_info.ordered_rel.num_tuples == sumA ) && (sumA == relA.num_tuples));
    TEST_ASSERT((info.relB_info.ordered_rel.num_tuples == sumB ) && (sumB == relB.num_tuples));


    delete_relation(relA);
    delete_relation(relB);
    delete_part_info(info);
    delete_relation(info.relA_info.ordered_rel);
    delete_relation(info.relB_info.ordered_rel);

}


TEST_LIST = {
    { "Partition test",partition_test},
    { NULL, NULL }
};