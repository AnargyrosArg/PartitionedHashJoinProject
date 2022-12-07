#include <stdio.h>
#include <stdlib.h>
#include "join.h"
#include "relations.h"
#include "intermediates.h"
#include "acutest.h"

//test for the init_intermediate function
void init_intermediate_test(void) {
    Intermediate* intermediate = init_intermediate(1);
    TEST_ASSERT(intermediate->relation_count == 1);
    TEST_ASSERT(intermediate->rowids_count == 0);
    TEST_ASSERT(intermediate->rowids[0] == NULL);
    delete_intermediate(&intermediate);
}

//test for the init_intermediates function
void init_intermediates_test(void) {
    Intermediates* intermediates = init_intermediates(1);
    TEST_ASSERT(intermediates->relation_count == 1);
    TEST_ASSERT(intermediates->intermediates[0]->relation_count == 1);
    TEST_ASSERT(intermediates->intermediates[0]->rowids_count == 0);
    TEST_ASSERT(intermediates->intermediates[0]->rowids[0] == NULL);
    delete_intermediates(&intermediates);
}

void delete_intermediates_test(void) {
    Intermediates* intermediates = init_intermediates(1);
    delete_intermediates(&intermediates);
    TEST_ASSERT(intermediates == NULL);
}

void delete_intermediate_test(void) {
    Intermediate* intermediate = init_intermediate(1);
    delete_intermediate(&intermediate);
    TEST_ASSERT(intermediate == NULL);
}

void insert_intermediates_filter_test(void) {
    relation rel;
    read_file(&rel,"./relations/1pass.txt");
    Intermediates* intermediates = init_intermediates(1);
    intermediates = insert_intermediates_filter(intermediates, &rel, 0);
    TEST_ASSERT(intermediates->intermediates[0]->rowids_count == rel.num_tuples);
    TEST_ASSERT(intermediates->intermediates[0]->rowids[0][0] == 0);
    TEST_ASSERT(intermediates->intermediates[0]->rowids[0][1] == 1);
    delete_intermediates(&intermediates);
}

void insert_intermediates_join_test(void) {
    relation rel1;
    read_file(&rel1,"./relations/1pass.txt");
    relation rel2;
    read_file(&rel2,"./relations/1pass.txt");
    Intermediates* intermediates = init_intermediates(3);
    result res = joinfunction(rel1,rel2);
    uint indexes[2] = {0,1};
    intermediates = insert_intermediates_join(intermediates, &res, indexes);
    TEST_ASSERT(intermediates->intermediates[0]->rowids_count == res.result_size);
    TEST_ASSERT(intermediates->intermediates[0]->rowids[0][0] == 0);
    TEST_ASSERT(intermediates->intermediates[0]->rowids[0][1] == 1);


    //now we test with rel3 relation
    relation rel3;
    read_file(&rel3,"./relations/1pass.txt");
    result res2 = joinfunction(rel1,rel3);
    uint indexes2[2] = {0,2};
    intermediates = insert_intermediates_join(intermediates, &res2, indexes2);
    TEST_ASSERT(intermediates->intermediates[0]->rowids_count == res2.result_size);
    TEST_ASSERT(intermediates->intermediates[0]->rowids[0][0] == 0);
    
    delete_intermediates(&intermediates);
    delete_result(&res);
    delete_result(&res2);
}


TEST_LIST = {
    { "basic init_intermediate test", init_intermediate_test },
    { "basic init_intermediates test", init_intermediates_test },
    { "basic delete_intermediates test", delete_intermediates_test },
    { "basic delete_intermediate test", delete_intermediate_test },
    { "basic insert_intermediates_filter test", insert_intermediates_filter_test },
    { "basic insert_intermediates_join test", insert_intermediates_join_test },
    { NULL, NULL }
};