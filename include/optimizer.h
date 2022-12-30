#include <stdlib.h>
#include <stdio.h>
#include "relations.h"
#include "filter.h"
#include "parser.h"

query_stats* init_query_stats(QueryInfo* query, table* tables);
void delete_query_stats(query_stats** stat);
void print_query_stats(query_stats* stat);

query_stats* update_query_stats_filter(query_stats* stat, FilterInfo* info);
query_stats* update_query_stats_join(query_stats* stat, JoinInfo* info);

size_t get_max_columns_query(QueryInfo* info, table* tables);