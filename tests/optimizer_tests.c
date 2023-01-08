#include <stdio.h>
#include <stdlib.h>
#include "stats.h"
#include "hashtable.h"
#include "sort.h"
#include "acutest.h"
#include "optimizer.h"

// basic query stats calls
void helper_functions(void) {
    table tables[4];
    tables[0] = load_relation("../workloads/small/r0", 1);
    tables[1] = load_relation("../workloads/small/r1", 1);
    tables[2] = load_relation("../workloads/small/r2", 1);
    tables[3] = load_relation("../workloads/small/r3", 1);

    QueryInfo* query = malloc(sizeof(QueryInfo));
    parse_query("3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1", query);

    // can_join functions
    int default_sequence[3] = {0, 1, 2};
    int wrong_sequence[3] = {1, 2, 0};
    int other_sequence[3] = {0, 2, 1};

    TEST_ASSERT(can_join(query, default_sequence, 3, -1) == 1);
    TEST_ASSERT(can_join(query, wrong_sequence, 3, -1) == 0);
    TEST_ASSERT(can_join_util(query, default_sequence, 1, 2) == 1);

    // best_sequence() and get_best_tree_max_size()
    TEST_ASSERT(get_best_tree_max_size() == 1234);
    int nsize = MAX_RELS_PER_QUERY; // neighborhood size
    hashtable* best_tree_hashtable = init_hashtable(get_best_tree_max_size(), nsize, hash_simple);

    int* ret;

    int set[] = {0};
    ret = best_sequence(query, best_tree_hashtable, set, 1);
    TEST_ASSERT(ret[0] == 0);

    int set2[] = {1, 0};
    ret = best_sequence(query, best_tree_hashtable, set2, 2);
    TEST_ASSERT(ret == NULL);
    best_tree_hashtable = insert_hashtable(best_tree_hashtable, 12, 0);
    best_tree_hashtable = insert_hashtable(best_tree_hashtable, 12, 1);
    ret = best_sequence(query, best_tree_hashtable, set2, 2);
    TEST_ASSERT(ret[0] == 0 && ret[1] == 1);

    // cost function
    TEST_ASSERT(sequence_cost(tables, query, default_sequence, 3) == 2584);
    TEST_ASSERT(sequence_cost(tables, query, other_sequence, 3) == 2564);

    delete_hashtable(best_tree_hashtable);
    delete_table(&(tables[0]));
    delete_table(&(tables[1]));
    delete_table(&(tables[2]));
    delete_table(&(tables[3]));
    query_info_delete(query);
    free(query);
}

// update query stats
void optimizer(void) {
    table tables[4];
    tables[0] = load_relation("../workloads/small/r0", 1);
    tables[1] = load_relation("../workloads/small/r1", 1);
    tables[2] = load_relation("../workloads/small/r2", 1);
    tables[3] = load_relation("../workloads/small/r3", 1);

    QueryInfo* query = malloc(sizeof(QueryInfo));
    parse_query("3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1", query);

    int result[2];
    optimize_query(tables, query, result);
    TEST_ASSERT(result[0] == 1 && result[1] == 0);

    delete_table(&(tables[0]));
    delete_table(&(tables[1]));
    delete_table(&(tables[2]));
    delete_table(&(tables[3]));
    query_info_delete(query);
    free(query);
}

TEST_LIST = {
    { "helper functions", helper_functions },
    { "optimize_query", optimizer },
    { NULL, NULL }
};