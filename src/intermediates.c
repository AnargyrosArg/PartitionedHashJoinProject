#include "intermediates.h"

Intermediates* init_intermediates(size_t relation_count) {
    Intermediates* intermediates;
    intermediates = malloc(sizeof(Intermediates));
    intermediates->relation_count = relation_count;
    intermediates->intermediates = malloc(relation_count * sizeof(Intermediate*));
    for (int i=0; i<relation_count; i++)
        intermediates->intermediates[i] = init_intermediate(relation_count);
    return intermediates;
}

void delete_intermediates(Intermediates* intermediates) {
    for (int i=0; i<intermediates->relation_count; i++)
        delete_intermediate(intermediates->intermediates[i]);
    free(intermediates->intermediates);
    free(intermediates);
}

Intermediate* init_intermediate(size_t relation_count) {
    Intermediate* intermediate;
    intermediate = malloc(sizeof(Intermediate));
    intermediate->relation_count = relation_count;
    intermediate->rowids = malloc(relation_count * sizeof(int*));
    return intermediate;
}

void delete_intermediate(Intermediate* intermediate) {
    for (int i=0; i<intermediate->relation_count; i++)
        if (intermediate->rowids[i] != NULL)
            free(intermediate->rowids[i]);
    free(intermediate->rowids);
    free(intermediate);
}

void print_intermediates(Intermediates* intermediates) {
    printf("\n");
    for (int i=0; i<intermediates->relation_count; i++) {
        printf("=============== Intermediate %d: ===============\n", i);
        if (intermediates->intermediates[i]->rowids_count != 0) {
            for (int j=0; j<intermediates->intermediates[i]->relation_count; j++) {
                if (intermediates->intermediates[i]->rowids[j] != NULL) {
                    printf("rowids for relation with index %d:\n", j);
                    for (int k=0; k<intermediates->intermediates[i]->rowids_count; k++)
                        printf("%d ", intermediates->intermediates[i]->rowids[j][k]);
                    printf("\n");
                }
            }
        }
    }
    printf("\n");
}

// ============================================================================================

// retrieves stored rowids of a specific relation
void get_intermediates(Intermediates* intermediates, uint relation_index, int** ret, int* ret_size) {
    for (int i=0; i<intermediates->relation_count; i++) {
        if (intermediates->intermediates[i]->rowids[relation_index] != NULL) {
            *ret = intermediates->intermediates[i]->rowids[relation_index];
            *ret_size = intermediates->intermediates[i]->rowids_count;
            return;
        }
    }
    //if the relation is not in the intermediates we return NULL at ret and 0 as ret_size
    *ret = NULL;
    *ret_size = 0;
}

// inserts array of rowids into the data structure.
// relation_index refers to the relation to which the rowids belong (ex. 0 -> 1st relation, which could be r1 for example)
// keep in mind, rel is not an actual relation, it's just elements of a column of the [relation_index]-th relation, along with rowids
// used for storing results of filters
Intermediates* insert_intermediates_filter(Intermediates* intermediates, relation* rel, uint relation_index) {
    // extract rowids from rel
    int* rowids = malloc((rel->num_tuples) * sizeof(int));
    for (int i=0; i<rel->num_tuples; i++)
        rowids[i] = rel->tuples[i].key;

    // check if we already have rowids for the given relation, and if we do, replace them
    for (int i=0; i<intermediates->relation_count; i++) {
        if (intermediates->intermediates[i]->rowids[relation_index] != NULL) {
            free(intermediates->intermediates[i]->rowids[relation_index]);
            intermediates->intermediates[i]->rowids[relation_index] = rowids;
            intermediates->intermediates[i]->rowids_count = rel->num_tuples;
            return intermediates;
        }
    }
    // if we don't have rowids already stored for the given relation, place the rowids on the first empty intermediate
    for (int i=0; i<intermediates->relation_count; i++) {
        if (intermediates->intermediates[i]->rowids_count == 0) { // if we found empty intermediate
            intermediates->intermediates[i]->rowids[relation_index] = rowids;
            intermediates->intermediates[i]->rowids_count = rel->num_tuples;
            return intermediates;
        }
    }
    fprintf(stderr, "insert_intermediates_filter: Could not place rowids in intermediates.\n");
    return intermediates;
}

// inserts join result into the data structure
// result is the join result, indexes is an array of size 2, containing the indexes of the involved relations
// make sure the order of the given indexes match the order in the join result (if join between 0 and 1, index1 = 0, index2 = 1)
Intermediates* insert_intermediates_join(Intermediates* intermediates, result* result, uint* indexes) {
    // extract rowids from join result
    int* join_rowids[2];
    join_rowids[0] = malloc((result->result_size) * sizeof(int));
    join_rowids[1] = malloc((result->result_size) * sizeof(int));
    for (int i=0; i<result->result_size; i++) {
        join_rowids[0][i] = result->pairs[i].key1;
        join_rowids[1][i] = result->pairs[i].key2;
    }

    // do one pass to check which of the involved relations are already have rowids stored
    int inter_indexes[2]; // inter_index[0]/[1] is the index of the intermediate where the rowids of the 1st/2nd relation are stored
    inter_indexes[0] = -1; inter_indexes[1] = -1; // -1 means that there are no rowids stored in any intermediate
    for (int i=0; i<intermediates->relation_count; i++) {
        if (intermediates->intermediates[i]->rowids[indexes[0]] != NULL) inter_indexes[0] = i;
        if (intermediates->intermediates[i]->rowids[indexes[1]] != NULL) inter_indexes[1] = i;
        if (inter_indexes[0] != -1 && inter_indexes[1] != -1) break;
    }

    // ============== CASE 1: no stored rowids for any of the two relations ==============
    if (inter_indexes[0] == -1 && inter_indexes[1] == -1) {
        // find the first empty intermediate and place the join result rowids there
        for (int i=0; i<intermediates->relation_count; i++) {
            if (intermediates->intermediates[i]->rowids_count == 0) { // if we found empty intermediate
                intermediates->intermediates[i]->rowids[indexes[0]] = join_rowids[0];
                intermediates->intermediates[i]->rowids[indexes[1]] = join_rowids[1];
                intermediates->intermediates[i]->rowids_count = result->result_size;
                return intermediates;
            }
        }
    }
    // ============== CASE 2: only one of the two relations has stored rowids ==============
    else if ((inter_indexes[0] != -1 && inter_indexes[1] == -1) || (inter_indexes[0] == -1 && inter_indexes[1] != -1)) {
        // distinguish which of the two relations is the one that has rowids stored
        int stored_index = (inter_indexes[0] != -1) ? 0 : 1;
        int other_index = !stored_index, counter = 0;
        size_t final_size = 0, rel_count = 1; // rel_count starts at 1 to account for the new relation being introduced by join

        // first pass to find final rowids size
        for (int i=0; i<result->result_size; i++) {
            for (int j=0; j<intermediates->intermediates[inter_indexes[stored_index]]->rowids_count; j++) {
                if (intermediates->intermediates[inter_indexes[stored_index]]->rowids[indexes[stored_index]][j] == join_rowids[stored_index][i])
                    final_size++;
            }
        }

        // find number of relations involved in the final rowids
        for (int i=0; i<intermediates->relation_count; i++)
            rel_count += (intermediates->intermediates[inter_indexes[stored_index]]->rowids[i] != NULL);

        // initialize final rowids arrays
        int** final_rowids = malloc(rel_count * sizeof(int*));
        for (int i=0; i<rel_count; i++)
            final_rowids[i] = malloc(final_size * sizeof(int));

        // second pass to constuct final rowids
        for (int i=0; i<result->result_size; i++) {
            for (int j=0; j<intermediates->intermediates[inter_indexes[stored_index]]->rowids_count; j++) {
                if (intermediates->intermediates[inter_indexes[stored_index]]->rowids[indexes[stored_index]][j] == join_rowids[stored_index][i]) {
                    for (int k=0; k<intermediates->relation_count; k++) {
                        if ((intermediates->intermediates[inter_indexes[stored_index]]->rowids[k] != NULL) && (k != indexes[other_index]))
                            final_rowids[k][counter] = intermediates->intermediates[inter_indexes[stored_index]]->rowids[k][j];
                        if (k == indexes[other_index])
                            final_rowids[k][counter] = join_rowids[other_index][i];
                    }
                    counter++;
                } 
            }
        }

        // replace old rowids with new ones
        for (int i=0; i<intermediates->relation_count; i++) {
            if (intermediates->intermediates[inter_indexes[stored_index]]->rowids[i] != NULL) {
                free(intermediates->intermediates[inter_indexes[stored_index]]->rowids[i]);
                intermediates->intermediates[inter_indexes[stored_index]]->rowids[i] = final_rowids[i];
            } else if (i == indexes[other_index]) {
                intermediates->intermediates[inter_indexes[stored_index]]->rowids[i] = final_rowids[i];
            }
        }
        intermediates->intermediates[inter_indexes[stored_index]]->rowids_count = final_size;
        free(join_rowids[0]);
        free(join_rowids[1]);
    }
    // ============== CASE 3: both relations already have stored rowids (similar to case 2) ==============
    else {
        size_t final_size = 0, rel_count = 0; // rel_count starts at 0 because no all-new relations are being introduced
        int counter = 0;

        // first pass to find final rowids size
        for (int i=0; i<result->result_size; i++) {
            for (int j=0; j<intermediates->intermediates[inter_indexes[0]]->rowids_count; j++) {
                if (intermediates->intermediates[inter_indexes[0]]->rowids[indexes[0]][j] == join_rowids[0][i]) {
                    for (int k=0; k<intermediates->intermediates[inter_indexes[1]]->rowids_count; k++) {
                        if (intermediates->intermediates[inter_indexes[1]]->rowids[indexes[1]][k] == join_rowids[1][i])
                            final_size++;
                    }
                }
            }
        }

        // find number of relations involved in the final rowids
        for (int i=0; i<intermediates->relation_count; i++) {
            rel_count += (intermediates->intermediates[inter_indexes[0]]->rowids[i] != NULL);
            rel_count += (intermediates->intermediates[inter_indexes[1]]->rowids[i] != NULL);
        }

        // initialize final rowids arrays
        int** final_rowids = malloc(rel_count * sizeof(int*));
        for (int i=0; i<rel_count; i++)
            final_rowids[i] = malloc(final_size * sizeof(int));

        // second pass to constuct final rowids
        for (int i=0; i<result->result_size; i++) {
            for (int j=0; j<intermediates->intermediates[inter_indexes[0]]->rowids_count; j++) {
                if (intermediates->intermediates[inter_indexes[0]]->rowids[indexes[0]][j] == join_rowids[0][i]) {
                    for (int k=0; k<intermediates->intermediates[inter_indexes[1]]->rowids_count; k++) {
                        if (intermediates->intermediates[inter_indexes[1]]->rowids[indexes[1]][k] == join_rowids[1][i]) {
                            for (int l=0; l<intermediates->relation_count; l++) {
                                if ((intermediates->intermediates[inter_indexes[0]]->rowids[l] != NULL) && (l != indexes[0]))
                                    final_rowids[l][counter] = intermediates->intermediates[inter_indexes[0]]->rowids[l][j];
                                if (l == indexes[0])
                                    final_rowids[l][counter] = join_rowids[0][i];
                                if ((intermediates->intermediates[inter_indexes[1]]->rowids[l] != NULL) && (l != indexes[1]))
                                    final_rowids[l][counter] = intermediates->intermediates[inter_indexes[1]]->rowids[l][k];
                                if (l == indexes[1])
                                    final_rowids[l][counter] = join_rowids[1][i];
                            }
                            counter++;
                        }
                    }
                }
            }
        }

        // replace old rowids with new ones (new intermediate in place of first intermediate)
        for (int i=0; i<intermediates->relation_count; i++) {
            if (intermediates->intermediates[inter_indexes[0]]->rowids[i] != NULL) {
                free(intermediates->intermediates[inter_indexes[0]]->rowids[i]);
                intermediates->intermediates[inter_indexes[0]]->rowids[i] = final_rowids[i];
            } else if (i == indexes[0] || i == indexes[1]) {
                intermediates->intermediates[inter_indexes[0]]->rowids[i] = final_rowids[i];
            }

            // free anything from the second intermediate
            if (intermediates->intermediates[inter_indexes[1]]->rowids[i] != NULL) {
                free(intermediates->intermediates[inter_indexes[1]]->rowids[i]);
                intermediates->intermediates[inter_indexes[1]]->rowids[i] = NULL;
            }
        }
        intermediates->intermediates[inter_indexes[0]]->rowids_count = final_size;
        intermediates->intermediates[inter_indexes[1]]->rowids_count = 0;
        free(join_rowids[0]);
        free(join_rowids[1]);
    }
    return intermediates;
}

