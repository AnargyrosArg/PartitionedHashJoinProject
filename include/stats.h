#include <stdlib.h>
#include <stdio.h>
#include "relations.h"
#include "filter.h"
#include "parser.h"
#include <math.h>

// NOTE: stats structs defined in relations.h

#define max(x,y) (((x) >= (y)) ? (x) : (y))
#define min(x,y) (((x) <= (y)) ? (x) : (y))

// basic functions for query stats
query_stats* init_query_stats(QueryInfo* query, table* tables);
void delete_query_stats(query_stats** stat);
void print_query_stats(query_stats* stat);

int connected(query_stats* stat, int rel1, int rel2);
void connect(query_stats* stat, int rel1, int rel2);
int no_connections(query_stats* stat, int rel);

// applies all filters of query to update stats
query_stats* update_query_stats_filter(table* tables, query_stats* stat, QueryInfo* query);

// applies a single join to update stats (cost of join stored in cost)
query_stats* update_query_stats_join(table* tables, query_stats* stat, QueryInfo* query, uint join_index, uint64_t* cost);

// helpers
stats* update_stats(stats* stat, int lower, int upper, uint count, uint distinct);
int value_exists_in_column(table* tables, QueryInfo* query, int rel, int col, int value);