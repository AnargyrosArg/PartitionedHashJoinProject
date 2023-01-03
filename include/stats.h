#include <stdlib.h>
#include <stdio.h>
#include "relations.h"
#include "filter.h"
#include "parser.h"
#include <math.h>

// NOTE: stats structs defined in relations.h

#define max(x,y) (((x) >= (y)) ? (x) : (y))
#define min(x,y) (((x) <= (y)) ? (x) : (y))

query_stats* init_query_stats(QueryInfo* query, table* tables);
void delete_query_stats(query_stats** stat);
void print_query_stats(query_stats* stat);

query_stats* update_query_stats_filter(table* tables, query_stats* stat, QueryInfo* query);
query_stats* update_query_stats_join(table* tables, query_stats* stat, QueryInfo* query, uint join_index);

stats* update_stats(stats* stat, int lower, int upper, uint count, uint distinct);
int value_exists_in_column(table* tables, QueryInfo* query, int rel, int col, int value);
int connected(query_stats* stat, int rel1, int rel2);
void connect(query_stats* stat, int rel1, int rel2);
int no_connections(query_stats* stat, int rel);