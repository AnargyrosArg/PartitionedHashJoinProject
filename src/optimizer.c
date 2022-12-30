#include "optimizer.h"

// creates 2D array of statistics for each column of each relation in a query
// statistics are the initial ones created during relation loading
query_stats* init_query_stats(QueryInfo* query, table* tables) {
    // allocate 2D stats array as in: stat[relation][column] (sizes are dynamic)
    query_stats* qstats = malloc(sizeof(query_stats));
    stats** stat = malloc(query->num_rels * sizeof(stats*));
    size_t* cols_per_rel = malloc(query->num_rels * sizeof(size_t));

    // for each relation i, allocate exactly as many stats as relation i's columns, and store all the sizes
    for (int i=0; i<query->num_rels; i++) {
        cols_per_rel[i] = tables[query->rel_ids[i]].num_colums;
        stat[i] = malloc(cols_per_rel[i] * sizeof(stats));
    }

    // copy the table statistics over
    for (int i=0; i<query->num_rels; i++) {
        table* current_table = &(tables[query->rel_ids[i]]);   // for every table
        for (int j=0; j<current_table->num_colums; j++)
            stat[i][j] = current_table->statistics[j];       // copy every column's stats over
    }
    qstats->num_query_rels = query->num_rels;
    qstats->cols_per_rel = cols_per_rel;
    qstats->statistics = stat;
    return qstats;
}

// evaluates new statistics for every column of a query, given a filter
// stats** stat:        should be 2D array of statistics as in: stat[relation][column], from init_stats
// FilterInfo info:     will ignore list component of FilterInfo struct
query_stats* update_query_stats_filter(query_stats* stat, FilterInfo* info) {
    printf("hello mistah\n");
    return NULL;
}



// frees memory of stats structure
void delete_query_stats(query_stats** stat) {
    for (int i=0; i<(*stat)->num_query_rels; i++) {
        free((*stat)->statistics[i]);
        (*stat)->statistics[i] = NULL;
    }
    free((*stat)->cols_per_rel);
    (*stat)->cols_per_rel = NULL;
    free((*stat)->statistics);
    (*stat)->statistics = NULL;
    free(*stat);
    *stat = NULL;
}

// prints query statistics
void print_query_stats(query_stats* stat) {
    for (int i=0; i<stat->num_query_rels; i++) {
        printf("====== table %d statistics: ======\nlower\tupper\tcount\tdistinct\n", i);
        for (int j=0; j<stat->cols_per_rel[i]; j++) {
            stats tmp = stat->statistics[i][j];
            printf("%d\t%d\t%u\t%u\n", tmp.lower, tmp.upper, tmp.count, tmp.distinct);
        }
        printf("\n");
    }
}