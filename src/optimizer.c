#include "optimizer.h"

// finds show optimal join execution order for given query, and stores it in given result_sequence array
// careful: returns JOIN SEQUENCE, not relation sequence
// result_sequence array should have size equal to [number of joins]
void optimize_query(table* tables, QueryInfo* query, int* result_sequence) {
    // use hashtable to store best sequence for a set of relations
    // a key is the concatenation of the (1-indexed) relations in the set, after the latter has been ordered
    // chaining is used to store the sequence of a given key
    // hash function used is hash_simple (key of value i -> goes to pos i)
    // the sequences for sets of size 1 are not stored
    int nsize = MAX_RELS_PER_QUERY; // neighborhood size
    hashtable* best_tree_hashtable = init_hashtable(get_best_tree_max_size(), nsize, hash_simple);

    int num_rels = query->num_rels, num_joins = get_join_count(query);
    int max_count = factorial(num_rels), count = 0, join_index;
    int* tmp; int* tmp_subset; int* tree;
    uint key;

    // setup 2D array to store subsets of relations
    int** subsets = malloc(max_count * sizeof(int*));
    for (int i=0; i<max_count; i++)
        subsets[i] = malloc(max_count * sizeof(int));

    // array with all query rels
    int rels[num_rels];
    for (int i=0; i<num_rels; i++) rels[i] = i;

    // dynamic programming
    for (int i=1; i<num_rels; i++) {
        count = combinations(rels, num_rels, i, subsets);
        for (int S=0; S<count; S++) {
            for (int Rj=0; Rj<num_rels; Rj++) {
                if (!in(subsets[S], i, Rj)) {
                    if (!can_join(query, subsets[S], i, Rj)) continue;
                    
                    // construct tree = bestTree(S) + Rj
                    tmp = best_sequence(query, best_tree_hashtable, subsets[S], i); // bestTree(S)
                    tree = malloc((i+1) * sizeof(int));                             // tree
                    for (int j=0; j<i; j++) tree[j] = tmp[j];                       // copy bestTree(S) to tree
                    tree[i] = Rj;                                                   // add Rj to tree

                    // construct S' = tmp_subset (set consisting of tree's elements)
                    tmp_subset = malloc((i+1) * sizeof(int));
                    for (int j=0; j<i+1; j++) tmp_subset[j] = tree[j]; // tmp_subset is a copy of tree

                    // Check if current tree is better than stored one, and if so replace
                    tmp = best_sequence(query, best_tree_hashtable, tmp_subset, i+1); // bestTree(S')

                    if ((tmp == NULL) || (sequence_cost(tables, query, tmp, i+1) > sequence_cost(tables, query, tree, i+1))) {
                        for (int j=0; j<i+1; j++) tmp_subset[j] += 1;
                        key = array_to_int(tmp_subset, i+1);
                        for (int j=0; j<i+1; j++) tmp_subset[j] -= 1;
                        
                        // reset bucket before we insert to remove previous data
                        free(best_tree_hashtable->htbuckets[key]->rowids);
                        free(best_tree_hashtable->htbuckets[key]);
                        best_tree_hashtable->htbuckets[key] = init_hashbucket(nsize);

                        for (int j=0; j<i+1; j++)
                            // inserting to the same key multiple times to chain the sequence into the bucket's data
                            insert_hashtable(best_tree_hashtable, key, tree[j]);
                    }
                    free(tree);
                    free(tmp_subset);
                }
            }
        }
    }

    // find join sequence from relation sequence we got
    tmp = best_sequence(query, best_tree_hashtable, rels, num_rels);
    for (int i=1; i<num_rels; i++) {
        join_index = can_join_util(query, tmp, i, tmp[i]); // find the next valid join
        if (join_index == -1) {
            fprintf(stderr, "optimize_query: could not resolve relation sequence to join sequence\n");
            break;
        }
        result_sequence[i-1] = join_index;
        count = i; // used in for loop below
    }

    // attach any extra missed joins to the join sequence
    for (int i=0; i<num_joins; i++)
        if (!in(result_sequence, num_joins, i))
            result_sequence[count++] = i;

    // freedom
    delete_hashtable(best_tree_hashtable);
    for (int i=0; i<max_count; i++)
        free(subsets[i]);
    free(subsets);
}


// given a set of relations, gets the best sequence
int* best_sequence(QueryInfo* query, hashtable* best_tree, int* set, size_t size) {
    int ret_size;

    for (int i=0; i<size; i++) set[i] += 1; // make set 1-indexed to eliminate any 0 in the set
    sort(set, size);
    uint key = array_to_int(set, size); // unique key for every set
    for (int i=0; i<size; i++) set[i] -= 1; // back to normal

    if (size == 1) return set;
    if (size == 2) {
        search_hashtable(best_tree, key, &ret_size);
        if ((can_join(query, set, size, -1)) && (ret_size != 0)) return set;
        else return NULL;
    }
    return search_hashtable(best_tree, key, &ret_size);
}


// checks if given relation can be joined into sequence of already joined relations
// if it can, returns the index of the join that need to be used
int can_join_util(QueryInfo* query, int* sequence, size_t size, int rel) {
    int counter = 0, found = 0;
    JoinInfo* current_join = query->joins;

    // input check
    if (size < 1) {
        // fprintf(stderr, "can_join: invalid size\n");
        return -1;
    }

    // go through join list to find the next join
    while(current_join != NULL) {
        // look for a join that includes the relation we want to add (rel)
        if (current_join->left.rel_id != rel && current_join->right.rel_id != rel) {
            current_join = current_join->next;
            counter++; continue;
        }

        // now check if this join could connect with current intermediate (aka it contains a relation already in the intermediate)
        // the relations already in the intermediate are: sequence[j] where j<size
        for (int j=0; j<size; j++) {
            if (current_join->left.rel_id == sequence[j] || current_join->right.rel_id == sequence[j]) {
                found = 1;
                break;
            }
        }
        if (found) break;
        current_join = current_join->next;
        counter++;
    }

    if (!found) {
        // fprintf(stderr, "can_join: could not join given relation into given sequence\n");
        return -1;
    }
    return counter;
}


// checks if sequence of relations can be joined together
// extra_rel: if set to a non-negative, can_join will check if extra_rel can be joined onto sequence as well
int can_join(QueryInfo* query, int* sequence, size_t size, int extra_rel) {
    int join_index;

    // find plausible join sequence from relation sequence
    for (int i=1; i<size; i++) {
        // find the next valid join
        join_index = can_join_util(query, sequence, i, sequence[i]);

        if (join_index == -1) {
            // fprintf(stderr, "can_join: could not resolve relation sequence to join sequence\n");
            return 0;
        }
    }

    // extra rel (one more check)
    if (extra_rel >= 0) {
        join_index = can_join_util(query, sequence, size, extra_rel);
        if (join_index == -1) {
            // fprintf(stderr, "can_join: could not resolve relation sequence to join sequence\n");
            return 0;
        }
    }
    return 1;
}


// calculates and returns cost of given relation sequence
uint64_t sequence_cost(table* tables, QueryInfo* query, int* sequence, size_t size) {
    int join_index;
    uint64_t cost, max_cost = ULLONG_MAX, total_cost = 0;
    
    query_stats* stat = init_query_stats(query, tables);
    stat = update_query_stats_filter(tables, stat, query); // apply filters first

    // input check
    if (size <= 1) { fprintf(stderr, "sequence_cost: invalid size\n"); return max_cost; }

    // find plausible join sequence from relation sequence
    for (int i=1; i<size; i++) {
        // find the next valid join
        join_index = can_join_util(query, sequence, i, sequence[i]);

        if (join_index == -1) {
            fprintf(stderr, "sequence_cost: could not resolve relation sequence to join sequence\n");
            return max_cost;
        }

        // update intermediate statistics by applying the join we found
        stat = update_query_stats_join(tables, stat, query, join_index, &cost);
        total_cost += cost; // cost is the sum of intermediate result sizes
    }
    return total_cost;
}


// returns the maximum index for best tree hashtable
// the max is the concat of all (1-indexed) relations, in order
// for MAX_RELS_PER_QUERY = 4, that is concat([1, 2, 3, 4]) = 1234
uint get_best_tree_max_size() {
    int max_rel_array[MAX_RELS_PER_QUERY];
    for (int i=0; i<MAX_RELS_PER_QUERY; i++)
        max_rel_array[i] = i+1;
    return array_to_int(max_rel_array, MAX_RELS_PER_QUERY);
}
