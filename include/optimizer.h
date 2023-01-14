#include "stats.h"
#include "hashtable.h"
#include "sort.h"
#include <limits.h>

// returns array of ints, that show optimal join execution order for given query
void optimize_query(table* tables, QueryInfo* query, int* result_sequence);

// helpers
int can_join(QueryInfo* query, int* sequence, size_t size, int extra_rel);
int can_join_util(QueryInfo* query, int* sequence, size_t size, int rel);

int* best_sequence(QueryInfo* query, hashtable* best_tree, int* set, size_t size);
uint64_t sequence_cost(table* tables, QueryInfo* query, int* sequence, size_t size);
uint get_best_tree_max_size();