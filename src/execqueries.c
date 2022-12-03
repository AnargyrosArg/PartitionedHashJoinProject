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


    //we create the intermediate struct that we will use to store the results
    Intermediates *intermediates = init_intermediates(query->num_rels);

    //we first need to filter the relations
    //we pass all the filter list till the end
    while(query->filters !=NULL){

        int rel = query->filters->sel.rel_id;
        int col = query->filters->sel.col_id;
        int op = query->filters->type;
        long long value = query->filters->constant;
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
            filter_function(&prefiltered_relation,&filtered_relation, op, value);
            //now we add the filtered relation to the intermediates
            intermediates = insert_intermediates_filter(intermediates,& filtered_relation, rel);
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
            filter_function(&prefiltered_relation,&filtered_relation, op, value);
            //now we add the filtered relation to the intermediates
            intermediates = insert_intermediates_filter(intermediates, &filtered_relation, rel);
        }

        query->filters = query->filters->next;

        //we free the memory of relations each time
        free(prefiltered_relation.tuples);
        free(filtered_relation.tuples);

    }

    //now we continue with the joins
    //we pass all the join list till the end
    while(query->joins !=NULL){
        int rel1 = query->joins->left.rel_id;
        int rel2 = query->joins->right.rel_id;
        int col1 = query->joins->left.col_id;
        int col2 = query->joins->right.col_id;

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
        else{
            tuple *tuples1 = malloc(numrows1 * sizeof(tuple));
            for (i=0;i<numrows1;i++){
                tuples1[i].key = rowidarray1[i];
                tuples1[i].payload = tabl[actualid1].table[col1][rowidarray1[i]];
            }
            prejoined_relation1.tuples = tuples1;
            prejoined_relation1.num_tuples = numrows1;
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
        else{
            tuple *tuples2 = malloc(numrows2 * sizeof(tuple));
            for (i=0;i<numrows2;i++){
                tuples2[i].key = rowidarray2[i];
                tuples2[i].payload = tabl[actualid2].table[col2][rowidarray2[i]];
            }
            prejoined_relation2.tuples = tuples2;
            prejoined_relation2.num_tuples = numrows2;
        }

        //now we join the relations
        result joinres = joinfunction(prejoined_relation1, prejoined_relation2);
        uint indexes[] = {rel1, rel2};
        //we add the joined relation to the intermediates
        intermediates = insert_intermediates_join(intermediates, &joinres, indexes);

        //we coninue to the next join
        query->joins = query->joins->next;

        //we free the memory of relations each time
        free(prejoined_relation1.tuples);
        free(prejoined_relation2.tuples);
    }

    //now that we finished with the joins and the filter all we have to do is do the projections and print the sum
    //we pass all the projection list till the end
    while(query->projections != NULL){
        int projrel = query->projections->rel_id;
        int projcol = query->projections->col_id;
        //we get the actual relation from the table
        int actualid = query->rel_ids[projrel];
        //now we just run the sum function for every projection
        printsum(projrel, projcol, intermediates,&tabl[actualid]);
        //we move on to the next projection
        query->projections = query->projections->next;
    }

    printf("\n");


    delete_intermediates(intermediates);
    return;
}