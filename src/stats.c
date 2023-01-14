#include "stats.h"

// ============================================ statistics update functions ============================================

// evaluates new statistics for every column of a query, after applying ALL filters
// stats** stat:        should be 2D array of statistics as in: stat[relation][column], from init_stats
// QueryInfo* query:    takes query instead of filter because for a specific calculation (value_exists_in_column), the query's rel_ids are needed
query_stats* update_query_stats_filter(table* tables, query_stats* stat, QueryInfo* query) {
    FilterInfo* info = query->filters;

    while (info != NULL) {
        int rel = info->sel.rel_id;
        int col = info->sel.col_id;
        stats* current = &(stat->statistics[rel][col]); // stats of column that will get filtered
        uint count_start = current->count; // initial row count of above column

        // equals filter (x == k)
        if (info->type == EQUALS) {
            // filtered column
            int exists = value_exists_in_column(tables, query, rel, col, info->constant);
            int tmp = (int) round((double) (((double) current->count) / ((double) current->distinct)));
            if (current->distinct == 0) tmp = 0; // careful for division with 0
            update_stats(current, info->constant, info->constant, exists*tmp, exists);
        }
        // range filter (k1 < x < k2)
        else {
            // convert just greater or just less to range filter
            int low = -1, up = -1;
            if (info->type == LESS)    { low = current->lower; up = info->constant-1; }
            if (info->type == GREATER) { low = info->constant+1; up = current->upper; }

            // fit low and up within boundaries
            if (low < current->lower) low = current->lower;
            if (up > current->upper) up = current->upper;

            // filtered column
            if (up-low != 0) { 
                double tmp = ((double) (up-low)) / ((double) (current->upper - current->lower));
                if (current->upper - current->lower == 0) tmp = 0; // careful for division with 0
                int tmp2 = (int) round(tmp * (double)(current->count));
                int final_distinct = (int) round(tmp * (double)(current->distinct));
                update_stats(current, low, up, tmp2, final_distinct);
            }
            else { // if k1 == k2 just apply equals filter
                int constant = up*(info->type == LESS) + low*(info->type == GREATER);
                int exists = value_exists_in_column(tables, query, rel, col, constant);
                int tmp = (int) round((double) (((double) current->count) / ((double) current->distinct)));
                if (current->distinct == 0) tmp = 0; // careful for division with 0
                update_stats(current, constant, constant, exists*tmp, exists);
            }
        }

        // other columns
        for (int i=0; i<stat->cols_per_rel[rel]; i++) {
            current = &(stat->statistics[rel][i]);
            if (i != col) {
                double base = 1.0 - ((double) (((double) stat->statistics[rel][col].count) / ((double) count_start)));
                if (count_start == 0) base = 1.0; // careful for division with 0
                double exp = (double) (((double) current->count) / ((double) current->distinct));
                if (current->distinct == 0) exp = 0.0; // careful for division with 0
                int final_distinct = (int) round(((double) current->distinct) * (1.0 - pow(base, exp)));
                update_stats(current, current->lower, current->upper, stat->statistics[rel][col].count, final_distinct);
            }
        }
        info = info->next;
    }
    return stat;
}

// evaluates new statistics for every column of a query, after applying a SINGLE join
// stats** stat:        should be 2D array of statistics as in: stat[relation][column], from init_stats
// QueryInfo* query:    takes query as input instead of join, because query is an argument needed for update_query_stats_filter
//                      refer to update_query_stats_filter above to see why it takes in a query instead of a filter
// uint join_index:     the index of the query's join to evaluate (index of 1 will evaluate query's 2nd join)
// uint64_t* cost:      the cost of the join will be stored in this variable
query_stats* update_query_stats_join(table* tables, query_stats* stat, QueryInfo* query, uint join_index, uint64_t* cost) {
    // find correct join
    JoinInfo* info = query->joins;
    uint current_index = 0;
    while (info != NULL && current_index != join_index) {
        info = info->next;
        current_index++;
    }

    // error check
    if (current_index != join_index) {
        fprintf(stderr, "update_query_stats_join: invalid join index\n");
        return NULL;
    }

    int rel1 = info->left.rel_id; int rel2 = info->right.rel_id;
    int col1 = info->left.col_id; int col2 = info->right.col_id;
    int tmp;
    stats* current;

    // self join
    if ((query->rel_ids[rel1] == query->rel_ids[rel2]) && (col1 == col2) && (no_connections(stat, rel1)) && (no_connections(stat, rel2))) {
        // affected columns
        stats* current1 = &(stat->statistics[rel1][col1]);
        stats* current2 = &(stat->statistics[rel2][col2]);
        tmp = (int) round(((double)(current1->count * current1->count)) / ((double)(current1->upper - current1->lower + 1)));
        update_stats(current1, current1->lower, current1->upper, tmp, current1->distinct);
        update_stats(current2, current1->lower, current1->upper, tmp, current1->distinct);

        // other columns
        for (int i=0; i<stat->cols_per_rel[rel1]; i++) {
            current = &(stat->statistics[rel1][i]);
            if (i != col1) update_stats(current, current->lower, current->upper, tmp, current->distinct);
            current = &(stat->statistics[rel2][i]);
            if (i != col2) update_stats(current, current->lower, current->upper, tmp, current->distinct);
        }
    }
    // join between same relation, not same column (R.A = R.B)
    else if (connected(stat, rel1, rel2)) {
        // affected columns
        stats* current1 = &(stat->statistics[rel1][col1]);
        stats* current2 = &(stat->statistics[rel1][col2]);

        int new_lower = max(current1->lower, current2->lower);
        int new_upper = min(current1->upper, current2->upper);

        int start_count = current1->count;
        tmp = (int) round(((double)(start_count)) / ((double)(new_upper - new_lower + 1)));

        double base = 1.0 - ((double) (((double) tmp) / ((double) start_count)));
        if (start_count == 0) base = 1.0; // careful for division with 0
        double exp = (double) (((double) start_count) / ((double) current1->distinct));
        if (current1->distinct == 0) exp = 0.0; // careful for division with 0
        int final_distinct = (int) round(((double) current1->distinct) * (1.0 - pow(base, exp)));

        update_stats(current1, new_lower, new_upper, tmp, final_distinct);
        update_stats(current2, new_lower, new_upper, tmp, final_distinct);

        // other columns
        for (int j=0; j<stat->num_query_rels; j++) { // for every connected relation
            int rel = stat->connections[rel1][j];
            if (rel == -1) break;
            for (int i=0; i<stat->cols_per_rel[rel]; i++) { // for every column
                current = &(stat->statistics[rel][i]);
                if ((rel == rel1 && i == col1) || (rel == rel2 && i == col2)) // dont touch columns we already updated above
                    continue;

                base = 1.0 - ((double) (((double) tmp) / ((double) start_count)));
                if (start_count == 0) base = 1.0; // careful for division with 0
                exp = (double) (((double) current->count) / ((double) current->distinct));
                if (current->distinct == 0) exp = 0.0; // careful for division with 0
                final_distinct = (int) round(((double) current->distinct) * (1.0 - pow(base, exp)));
                update_stats(current, current->lower, current->upper, tmp, final_distinct);
            }
        }
    }
    // join between different relations (R.A = S.B)
    else {
        stats* current1 = &(stat->statistics[rel1][col1]);
        stats* current2 = &(stat->statistics[rel2][col2]);

        // update stats with filters so that both relations have same lower and upper
        // create temporary query with filter we want to apply (because update_query_stats_filter takes a query as input)
        SelectionInfo sel; FilterInfo filter; QueryInfo tmp_query;
        filter.next = NULL;
        for (int i=0; i<MAX_RELS_PER_QUERY; i++)
            tmp_query.rel_ids[i] = query->rel_ids[i]; // copy rel_ids to tmp_query

        // greater filter
        if (current1->lower != current2->lower) {
            filter.type = GREATER;
            if (current1->lower > current2->lower) {
                sel.rel_id = rel2;
                sel.col_id = col2;
                filter.constant = (current1->lower)-1;
            } 
            else if (current2->lower > current1->lower) {
                sel.rel_id = rel1;
                sel.col_id = col1;
                filter.constant = (current2->lower)-1;
            }
            filter.sel = sel;
            tmp_query.filters = &filter;
            stat = update_query_stats_filter(tables, stat, &tmp_query);
            // print_query_stats(stat);
        }
        // less filter
        if (current1->upper != current2->upper) {
            filter.type = LESS;
            if (current1->upper < current2->upper) {
                sel.rel_id = rel2;
                sel.col_id = col2;
                filter.constant = (current1->upper)+1;
            } 
            else if (current2->upper < current1->upper) {
                sel.rel_id = rel1;
                sel.col_id = col1;
                filter.constant = (current2->upper)+1;
            }
            filter.sel = sel;
            tmp_query.filters = &filter;
            stat = update_query_stats_filter(tables, stat, &tmp_query);
            // print_query_stats(stat);
        }
        // affected columns
        int start_distinct1 = current1->distinct, start_distinct2 = current2->distinct;
        tmp = (int) round(((double)(current1->count * current2->count)) / ((double)(current1->upper - current1->lower + 1)));
        int tmp2 = (int) round((double)(start_distinct1 * start_distinct2) / ((double)(current1->upper - current1->lower + 1)));
        update_stats(current1, current1->lower, current1->upper, tmp, tmp2);
        update_stats(current2, current1->lower, current1->upper, tmp, tmp2);

        // other columns of 1st relation
        for (int j=0; j<stat->num_query_rels; j++) { // for every connected relation
            int rel = stat->connections[rel1][j];
            if (rel == -1) break;
            for (int i=0; i<stat->cols_per_rel[rel]; i++) { // for every column
                current = &(stat->statistics[rel][i]);
                if (rel == rel1 && i == col1) // dont touch columns we already updated above
                    continue;

                double base = 1.0 - ((double) (((double) tmp2) / ((double) start_distinct1)));
                if (start_distinct1 == 0) base = 1.0; // careful for division with 0
                double exp = (double) (((double) current->count) / ((double) current->distinct));
                if (current->distinct == 0) exp = 0.0; // careful for division with 0
                int final_distinct = (int) round(((double) current->distinct) * (1.0 - pow(base, exp)));
                update_stats(current, current->lower, current->upper, tmp, final_distinct);
            }
        }

        // other columns of 2nd relation
        for (int j=0; j<stat->num_query_rels; j++) { // for every connected relation
            int rel = stat->connections[rel2][j];
            if (rel == -1) break;
            for (int i=0; i<stat->cols_per_rel[rel]; i++) { // for every column
                current = &(stat->statistics[rel][i]);
                if (rel == rel2 && i == col2) // dont touch columns we already updated above
                    continue;

                double base = 1.0 - ((double) (((double) tmp2) / ((double) start_distinct2)));
                if (start_distinct2 == 0) base = 1.0; // careful for division with 0
                double exp = (double) (((double) current->count) / ((double) current->distinct));
                if (current->distinct == 0) exp = 0.0; // careful for division with 0
                int final_distinct = (int) round(((double) current->distinct) * (1.0 - pow(base, exp)));
                update_stats(current, current->lower, current->upper, tmp, final_distinct);
            }
        }

        // update connections
        connect(stat, rel1, rel2);
    }

    // return
    *cost = tmp;
    return stat;
}


// ============================================ basic statistics functions ============================================

// creates 2D array of statistics for each column of each relation in a query
// statistics are the initial ones created during relation loading
query_stats* init_query_stats(QueryInfo* query, table* tables) {
    // allocate 2D stats array as in: stat[relation][column] (sizes are dynamic)
    query_stats* qstats = malloc(sizeof(query_stats));
    stats** stat = malloc(query->num_rels * sizeof(stats*));
    size_t* cols_per_rel = malloc(query->num_rels * sizeof(size_t));
    int** connections = malloc(query->num_rels * sizeof(int*));

    // for each relation i, allocate exactly as many stats as relation i's columns, and store all the sizes
    for (int i=0; i<query->num_rels; i++) {
        cols_per_rel[i] = tables[query->rel_ids[i]].num_colums;
        stat[i] = malloc(cols_per_rel[i] * sizeof(stats));
        connections[i] = malloc(query->num_rels * sizeof(int));
        connections[i][0] = i; // init connections so that each relation i is connected with itself
        for (int j=1; j<query->num_rels; j++)
            connections[i][j] = -1;
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
    qstats->connections = connections;
    return qstats;
}

// frees memory of stats structure
void delete_query_stats(query_stats** stat) {
    for (int i=0; i<(*stat)->num_query_rels; i++) {
        free((*stat)->statistics[i]);
        (*stat)->statistics[i] = NULL;
        free((*stat)->connections[i]);
        (*stat)->connections[i] = NULL;
    }
    free((*stat)->cols_per_rel);
    (*stat)->cols_per_rel = NULL;
    free((*stat)->statistics);
    (*stat)->statistics = NULL;
    free((*stat)->connections);
    (*stat)->connections = NULL;
    free(*stat);
    *stat = NULL;
}

// prints query statistics
void print_query_stats(query_stats* stat) {
    for (int i=0; i<stat->num_query_rels; i++) {
        printf("====== table %d statistics: ======\nlower\tupper\tcount\tdistinct\n", i);
        for (int j=0; j<stat->cols_per_rel[i]; j++) {
            stats tmp = stat->statistics[i][j];
            printf("%ld\t%ld\t%lu\t%lu\n", tmp.lower, tmp.upper, tmp.count, tmp.distinct);
        }
        for (int j=0; j<stat->num_query_rels; j++)
            printf("%d ", stat->connections[i][j]);
        printf("\n");
    }
    printf("=================================================================\n");
}

// connects statistics of two relations
void connect(query_stats* stat, int rel1, int rel2) {
    int pos1 = -1, pos2 = -1; // first empty position in connection array
    int num = stat->num_query_rels;
    int tmp1[num], tmp2[num];

    for (int i=0; i<num; i++) {
        if (stat->connections[rel1][i] == -1 && pos1 == -1) pos1 = i;
        if (stat->connections[rel2][i] == -1 && pos2 == -1) pos2 = i;
        tmp1[i] = stat->connections[rel1][i]; // keep copy of rel1 connections
        tmp2[i] = stat->connections[rel2][i]; // keep copy of rel2 connections
    }
    // connect rel1 to rel2
    for (int i=0; i<num; i++) {
        if (tmp2[i] == -1 || pos1 == -1) break;
        for (int j=0; j<num; j++) {
            if (tmp1[j] == -1) break;
            stat->connections[tmp1[j]][pos1] = tmp2[i];
        }
        pos1++;
    }
    // connect rel2 to rel1
    for (int i=0; i<num; i++) {
        if (tmp1[i] == -1 || pos2 == -1) break; // use tmp we stored (we cannot use already updated rel1 connections to update rel2)
        for (int j=0; j<num; j++) {
            if (tmp2[j] == -1) break;
            stat->connections[tmp2[j]][pos2] = tmp1[i];
        }
        pos2++;
    }
}

// checks if two relations have been joined together
int connected(query_stats* stat, int rel1, int rel2) {
    for (int i=0; i<stat->num_query_rels; i++)
        if (stat->connections[rel1][i] == rel2)
            return 1;
    return 0;
}

// ============================================ helper functions ============================================

// checks if a relation has been joined or not
int no_connections(query_stats* stat, int rel) {
    if (stat->num_query_rels < 2) return 1;
    if (stat->connections[rel][1] != -1) return 0;
    return 1;
}

// helper function to find if a value exists in a column
int value_exists_in_column(table* tables, QueryInfo* query, int rel, int col, int value) {
    int table_index = query->rel_ids[rel];
    for (int i=0; i<tables[table_index].num_tuples; i++)
        if (tables[table_index].table[col][i] == value)
            return 1;
    return 0;
}

// helper function to update stats
stats* update_stats(stats* stat, int lower, int upper, uint count, uint distinct) {
    if (count != 0 && distinct == 0) distinct = 1; // prevent distinct from being 0 if count is non-zero
    if (count == 0 && distinct != 0) distinct = 0; // prevent distinct from being non-0 if count is zero
    stat->lower = lower;
    stat->upper = upper;
    stat->count = count;
    stat->distinct = distinct;
    return stat;
}