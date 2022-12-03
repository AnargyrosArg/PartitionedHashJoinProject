#include "execqueries.h" 



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

    printf("sum of column %d of relation %d is %lu\n",column,rel,sum);

    //we free the memory of the rowidarray
    free(rowidarray);

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

        print_intermediates(intermediates);
    }
    delete_intermediates(intermediates);
    return;
}