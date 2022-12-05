#include "execqueries.h" 


//function that prints the sum of one projection
void printsum(int rel, int column, Intermediates* inter, table *tabl){
    uint64_t sum =0;

    //first we need to get the relation from the intermediates
    int *rowidarray;
    int numrows;

    get_intermediates(inter, rel, &rowidarray, &numrows);
    //printf("relation %d has %d tuples:",rel,numrows);
    
    //now for every id in the rowidarray we need to get the value of the column
    int i=0;
    for(i=0;i<numrows;i++){
        uint rowid = rowidarray[i];
        uint64_t value = tabl->table[column][rowid];
        sum += value;
        //printf("value %d is %lu ",i,sum);
    }

    //if the sum is equal to 0 then we print NULL
    if(sum == 0){
        printf("NULL ");
    }
    else{
        printf("%lu ",sum);
    }

    return;
}

void exec_query(QueryInfo *query, table* tabl){
    FilterInfo* current_filter = query->filters;
    JoinInfo* current_join = query->joins;
    SelectionInfo* current_proj = query->projections;

    // debug tools
    int print_filter = 0;
    int print_join_index = -1;
    int join_counter = 0;

    //we create the intermediate struct that we will use to store the results
    Intermediates *intermediates = init_intermediates(query->num_rels);

    //we first need to filter the relations
    //we pass all the filter list till the end
    while(current_filter!=NULL){
        int rel = current_filter->sel.rel_id;
        int col = current_filter->sel.col_id;
        int op = current_filter->type;
        long long value = current_filter->constant;
        //we have to get the actual realtion from the table, remember that the int rel is the id of the relation in the query, not the actual id of the relation in the table
        int actualid = query->rel_ids[rel];
        
        //now we create the relation that will be filtered
        //first we check if it exists in the intermediates
        int *rowidarray;
        int numrows;
        get_intermediates(intermediates, rel, &rowidarray, &numrows);

        relation prefiltered_relation;
        relation filtered_relation;
        //if it doesn't exist we create it from the table
        if(rowidarray == NULL){
            tuple *tuples = malloc(tabl[actualid].num_tuples * sizeof(tuple));
            for (int i=0;i<tabl[actualid].num_tuples;i++){
                tuples[i].key = i;
                tuples[i].payload = tabl[actualid].table[col][i];
            }
            prefiltered_relation.tuples = tuples;
            prefiltered_relation.num_tuples = tabl[actualid].num_tuples;


            //now we filter the relation
            better_filter_function(&prefiltered_relation,&filtered_relation, op, value);
            //now we add the filtered relation to the intermediates
            intermediates = insert_intermediates_filter(intermediates,& filtered_relation, rel);
            if (print_filter) print_intermediates(intermediates);
        }

        //if the relation already exists in the intermediates we get the ids from there
        else{
            //we create the relation from the rowidarray
            tuple *tuples = malloc(numrows * sizeof(tuple));
            for (int i=0;i<numrows;i++){
                tuples[i].key = rowidarray[i];
                tuples[i].payload = tabl[actualid].table[col][rowidarray[i]];
            }
            prefiltered_relation.tuples = tuples;
            prefiltered_relation.num_tuples = numrows;

            //now we filter the relation
            better_filter_function(&prefiltered_relation,&filtered_relation, op, value);
            //now we add the filtered relation to the intermediates
            intermediates = insert_intermediates_filter(intermediates, &filtered_relation, rel);
            if (print_filter) print_intermediates(intermediates);
        }

        current_filter = current_filter->next;

        //we free the memory of relations each time
        free(prefiltered_relation.tuples);
        free(filtered_relation.tuples);

    }

    //now we continue with the joins
    //we pass all the join list till the end
    while(current_join !=NULL){
        int rel1 = current_join->left.rel_id;
        int rel2 = current_join->right.rel_id;
        int col1 = current_join->left.col_id;
        int col2 = current_join->right.col_id;

        //we have to get the actual realtion from the table, remember that the int rel is the id of the relation in the query, not the actual id of the relation in the table
        int actualid1 = query->rel_ids[rel1];
        int actualid2 = query->rel_ids[rel2];

        //now we create the relations that will be joined
        //first we check if they exist in the intermediates
        int *rowidarray1;
        int numrows1;
        get_intermediates(intermediates, rel1, &rowidarray1, &numrows1);

        int *rowidarray2;
        int numrows2;
        get_intermediates(intermediates, rel2, &rowidarray2, &numrows2);

        relation prejoined_relation1;
        relation prejoined_relation2;
        
        //if they don't exist at the intermediate we create them from the table
        //first for the first relation
        int i=0;
        if(rowidarray1 == NULL){
            tuple *tuples1 = malloc(tabl[actualid1].num_tuples * sizeof(tuple));
            for (i=0;i<tabl[actualid1].num_tuples;i++){
                tuples1[i].key = i;
                tuples1[i].payload = tabl[actualid1].table[col1][i];
            }
            prejoined_relation1.tuples = tuples1;
            prejoined_relation1.num_tuples = tabl[actualid1].num_tuples;
        }
        else { // if rowids already exist in intermediate results, use them to create relation
            tuple *tuples1 = malloc(numrows1 * sizeof(tuple));
            int* ret;
            int counter = 0, found = 0, ret_size;
            hashtable* htable = init_hashtable(numrows1, 16); // WE DONT WANT DUPLICATE ROWIDS ON RELATION WE WILL CREATE
                                                              // so we use a hashtable to store all the rowids we have already used
            for (i=0; i<numrows1; i++) {
                ret = search_hashtable(htable, tabl[actualid1].table[col1][rowidarray1[i]], &ret_size); // check if this rowid has already been used
                for (int j=0; j<ret_size; j++)
                    if (ret[j] == rowidarray1[i])
                        found = 1;

                if (!found) { // if we have not put this rowid in the relation yet, put it in and also insert it to the hashtable to avoid it in the future
                    tuples1[counter].key = rowidarray1[i];
                    tuples1[counter].payload = tabl[actualid1].table[col1][rowidarray1[i]];
                    htable = insert_hashtable(htable, tuples1[counter].payload, tuples1[counter].key);
                    counter++;
                }
                found = 0;
            }
            prejoined_relation1.tuples = tuples1;
            prejoined_relation1.num_tuples = counter;
            delete_hashtable(htable);
        }

        //now for the second relation
        if(rowidarray2 == NULL){
            tuple *tuples2 = malloc(tabl[actualid2].num_tuples * sizeof(tuple));
            for (i=0;i<tabl[actualid2].num_tuples;i++){
                tuples2[i].key = i;
                tuples2[i].payload = tabl[actualid2].table[col2][i];
            }
            prejoined_relation2.tuples = tuples2;
            prejoined_relation2.num_tuples = tabl[actualid2].num_tuples;
        }
        else { // if rowids already exist in intermediate results, use them to create relation
            tuple *tuples2 = malloc(numrows2 * sizeof(tuple));
            int* ret;
            int counter = 0, found = 0, ret_size;
            hashtable* htable = init_hashtable(numrows2, 16); // WE DONT WANT DUPLICATE ROWIDS ON RELATION WE WILL CREATE
                                                              // so we use a hashtable to store all the rowids we have already used
            for (i=0; i<numrows2; i++){
                ret = search_hashtable(htable, tabl[actualid2].table[col2][rowidarray2[i]], &ret_size); // check if this rowid has already been used
                for (int j=0; j<ret_size; j++)
                    if (ret[j] == rowidarray2[i])
                        found = 1;

                if (!found) { // if we have not put this rowid in the relation yet, put it in and also insert it to the hashtable to avoid it in the future
                    tuples2[counter].key = rowidarray2[i];
                    tuples2[counter].payload = tabl[actualid2].table[col2][rowidarray2[i]];
                    htable = insert_hashtable(htable, tuples2[counter].payload, tuples2[counter].key);
                    counter++;
                }
                found = 0;
            }
            prejoined_relation2.tuples = tuples2;
            prejoined_relation2.num_tuples = counter;
            delete_hashtable(htable);
        }

        //now we join the relations
        result joinres = joinfunction(prejoined_relation1, prejoined_relation2);
        uint indexes[] = {rel1, rel2};
        //we add the joined relation to the intermediates
        intermediates = insert_intermediates_join(intermediates, &joinres, indexes);
        if (print_join_index != -1 && join_counter == print_join_index)
            print_intermediates(intermediates);

        //we coninue to the next join
        current_join = current_join->next;
        join_counter++;

        //we free the memory of relations each time
        free(prejoined_relation1.tuples);
        free(prejoined_relation2.tuples);

        delete_result(&joinres);
    }

    //now that we finished with the joins and the filter all we have to do is do the projections and print the sum
    //we pass all the projection list till the end
    while(current_proj != NULL){
        int projrel = current_proj->rel_id;
        int projcol = current_proj->col_id;
        //we get the actual relation from the table
        int actualid = query->rel_ids[projrel];
        //now we just run the sum function for every projection
        printsum(projrel, projcol, intermediates,&tabl[actualid]);
        //we move on to the next projection
        current_proj = current_proj->next;
    }

    printf("\n");

    delete_intermediates(&intermediates);
    return;
}

//function that executes all the queries
void exec_all_queries(QueryInfo *queries,table *tabl,uint num_queries){

    for (int i=0;i<num_queries;i++){
        exec_query(&queries[i],tabl);
    }
    return;
}