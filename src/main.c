#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "relations.h"
#include "utils.h"
#include "execqueries.h"

#define MAX_LINE_SIZE 50
#define MAX_N_TABLES 25
#define MAX_N_QUERIES 50

int main(int argc, char** argv) {
    //consider making these global for easy access
    //array that keeps the input relations
    table* tables;
    size_t n_tables=0;
    //array with info for input queries
    QueryInfo* queries;
    size_t n_queries=0;

    //allocate table array
    tables = malloc(MAX_N_TABLES * sizeof(table));
    queries = malloc(MAX_N_QUERIES * sizeof(QueryInfo));

    //buffer for reading input
    char* line = malloc(MAX_LINE_SIZE*sizeof(char));
    size_t line_max_size = MAX_LINE_SIZE;
    int n_read;

    //parse relation names and load them into memory
    while((n_read=getline(&line,&line_max_size,stdin))>0){
        //remove newline and carriage return for the last 2 chars
        for (int i=0; i<2; i++)
            if ((line[strlen(line)-1] == 10) || (line[strlen(line)-1] == 13))
                line[strlen(line)-1] = 0;
        if (strcmp(line,"Done")==0) break;
        //store table
        tables[n_tables++]=load_relation(line);
    }

    //parse batch of queries
    while ((n_read=getline(&line,&line_max_size,stdin))>0) {
        //remove newline and carriage return for the last 2 chars
        for (int i=0; i<2; i++)
            if ((line[strlen(line)-1] == 10) || (line[strlen(line)-1] == 13))
                line[strlen(line)-1] = 0;
        if (strcmp(line,"F")==0) continue; // End of a batch
        parse_query(line,&(queries[n_queries++]));
    }

    for(int i =0;i<n_queries;i++){
        print_query_info(queries[i]);
    }
    free(line);

    // // ================================= process queries =================================
    // // preparations
    // relation relations[n_tables];
    // tuple** tuples = malloc(n_tables * sizeof(tuple*));
    // uint columns[] = {1, 1, 0, 0}; // hard coded, shows the column to use for each relation

    // for (int i=0; i<n_tables; i++) {
    //     tuples[i] = malloc(tables[i].num_tuples * sizeof(tuple));
    //     for (int j=0; j<tables[i].num_tuples; j++) {
    //         tuples[i][j].key = j;
    //         tuples[i][j].payload = tables[i].table[columns[i]][j];
    //     }
    //     relations[i].tuples = tuples[i];
    //     relations[i].num_tuples = tables[i].num_tuples;
    // }

    // // insert tests
    // Intermediates* intermediates = init_intermediates(n_tables);
    // print_intermediates(intermediates);

    // // FILTER r0
    // relation filtered_relation;
    // filter_function(&relations[0], &filtered_relation, LESS, 4500);
    // intermediates = insert_intermediates_filter(intermediates, &filtered_relation, 0);
    // print_intermediates(intermediates);

    // // JOIN r0 and r1
    // uint indexes[] = {0, 1};
    // result joinres = joinfunction(relations[0], relations[1]); // NOTICE! here we use raw r0 and r1 for join, ignoring the intermediate results for r0
    // //print_result(&joinres);                                  // end result will be the same, but it is less optimal
    // intermediates = insert_intermediates_join(intermediates, &joinres, indexes);
    // print_intermediates(intermediates);

    // // FILTER r2
    // relation filtered_relation2;                                        // CAREFUL: don't filter on relation that is already in intermediate results
    // filter_function(&relations[2], &filtered_relation2, LESS, 10000);   // doing all the filters first is the simplest way to go
    // intermediates = insert_intermediates_filter(intermediates, &filtered_relation2, 2);
    // print_intermediates(intermediates);

    // // JOIN r1 and r2
    // uint indexes3[] = {1, 2};
    // result joinres3 = joinfunction(relations[1], relations[2]); // NOTICE! same thing as first notice. join uses raw r1 and r2 but should take 
    // //print_result(&joinres3);                                  // intermediate results into consideration
    // intermediates = insert_intermediates_join(intermediates, &joinres3, indexes3);
    // print_intermediates(intermediates);


    // // cleanup
    // delete_intermediates(intermediates);
    // for (int i=0; i<n_tables; i++)
    //     delete_relation(relations[i]);
    // delete_relation(filtered_relation);
    // delete_relation(filtered_relation2);
    // free(tuples);

    //===================================================================================
    
    exec_query(&queries[0],tables);

    //free tables mem
    for(int i =0;i<n_tables;i++){
        delete_table(&(tables[i]));
    }
    free(tables);
    
    //free queries mem
    for(int i=0;i<n_queries;i++){
        query_info_delete(&(queries[i]));
    }
    free(queries);

    return 0;
}