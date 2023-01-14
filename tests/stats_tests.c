#include "acutest.h"
#include <stdio.h>
#include <stdlib.h>
#include "stats.h"
#include "parser.h"


// basic query stats calls
void simple_query_stats(void) {
    table tables[4];
    tables[0] = load_relation("../workloads/small/r0", 1);
    tables[1] = load_relation("../workloads/small/r1", 1);
    tables[2] = load_relation("../workloads/small/r2", 1);
    tables[3] = load_relation("../workloads/small/r3", 1);

    QueryInfo* query = malloc(sizeof(QueryInfo));
    query_info_init(query);
    parse_query("3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1", query);

    // init_query_stats
    query_stats* stat = init_query_stats(query, tables);
    TEST_ASSERT(stat->num_query_rels == query->num_rels);
    for (int i=0; i<query->num_rels; i++) {
        TEST_ASSERT(stat->cols_per_rel[i] == tables[query->rel_ids[i]].num_colums);
        TEST_ASSERT(stat->connections[i][0] == i);
        table* current_table = &(tables[query->rel_ids[i]]);   // for every table
        for (int j=0; j<current_table->num_colums; j++)
            TEST_ASSERT(stat->statistics[i][j].count == current_table->statistics[j].count);
    }

    // connections
    TEST_ASSERT(no_connections(stat, 0) == 1);
    TEST_ASSERT(no_connections(stat, 1) == 1);
    TEST_ASSERT(connected(stat, 0, 1) == 0);
    connect(stat, 0, 1);
    TEST_ASSERT(connected(stat, 0, 1) == 1);

    // helpers
    update_stats(&(stat->statistics[0][0]), 1, 2, 3, 4);
    TEST_ASSERT(stat->statistics[0][0].lower == 1);
    TEST_ASSERT(stat->statistics[0][0].upper == 2);
    TEST_ASSERT(stat->statistics[0][0].count == 3);
    TEST_ASSERT(stat->statistics[0][0].distinct == 4);

    TEST_ASSERT(value_exists_in_column(tables, query, 0, 0, 5) == 1);
    TEST_ASSERT(value_exists_in_column(tables, query, 0, 0, 6) == 0);
    TEST_ASSERT(value_exists_in_column(tables, query, 0, 0, 7) == 1);

    // delete_query_stats
    delete_query_stats(&stat);
    TEST_ASSERT(stat == NULL);

    delete_table(&(tables[0]));
    delete_table(&(tables[1]));
    delete_table(&(tables[2]));
    delete_table(&(tables[3]));
    query_info_delete(query);
    free(query);
}

// update query stats
void update_query_stats(void) {
    table tables[4];
    tables[0] = load_relation("../workloads/small/r0", 1);
    tables[1] = load_relation("../workloads/small/r1", 1);
    tables[2] = load_relation("../workloads/small/r2", 1);
    tables[3] = load_relation("../workloads/small/r3", 1);

    QueryInfo* query = malloc(sizeof(QueryInfo));
    query_info_init(query);
    parse_query("3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1", query);

    query_stats* stat = init_query_stats(query, tables);

    // filter update
    int64_t start_upper = stat->statistics[0][2].upper;
    stat = update_query_stats_filter(tables, stat, query);
    TEST_ASSERT(stat->statistics[0][2].lower == 3500);
    TEST_ASSERT(stat->statistics[0][2].upper == start_upper);

    // first join update
    uint64_t cost;
    stat = update_query_stats_join(tables, stat, query, 0, &cost);
    TEST_ASSERT(cost == 1944);

    // seconds join update
    stat = update_query_stats_join(tables, stat, query, 1, &cost);
    TEST_ASSERT(cost == 640);

    delete_query_stats(&stat);
    delete_table(&(tables[0]));
    delete_table(&(tables[1]));
    delete_table(&(tables[2]));
    delete_table(&(tables[3]));
    query_info_delete(query);
    free(query);
}



TEST_LIST = {
    {"simple stats functions", simple_query_stats},
    {"update stats functions", update_query_stats},
    { 0, 0 }
};