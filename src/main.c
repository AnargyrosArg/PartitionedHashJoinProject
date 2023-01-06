#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "relations.h"
#include "utils.h"
#include "execqueries.h"
#include "optimizer.h"

#define MAX_LINE_SIZE 100
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

    // set to true (non-zero) to enable optimizer
    // WARNING: optimizer does not work for a certain type of query
    // the problematic queries of small.work are listed in optimizer.c, and have been removed from test_input.txt
    // ./runTestharness DOES NOT WORK with optimizer, unless you change small.work and small.result
    int optimize = 0;

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

    int current = 0;
    //parse batch of queries
    while ((n_read=getline(&line,&line_max_size,stdin))>0) {
        //remove newline and carriage return for the last 2 chars
        for (int i=0; i<2; i++)
            if ((line[strlen(line)-1] == 10) || (line[strlen(line)-1] == 13))
                line[strlen(line)-1] = 0;
        if (strcmp(line,"F")==0) continue; // End of a batch
        parse_query(line,&(queries[n_queries++]));

        if (optimize) {
            int optimal[get_join_count(&queries[current])]; // stores optimal join sequence
            optimize_query(tables, &queries[current], optimal);

            // printf("current query is %d\n", current);
            // for (int i=0; i<get_join_count(&queries[current]); i++)
            //     printf("%d ", optimal[i]);
            // printf("\n");

            exec_query(&queries[current++],tables, optimal);
        } else 
            exec_query(&queries[current++],tables, NULL);
    }
    free(line);

    //harness waits for result before sending next batch , so we have to execute queries as they come    
    //exec_all_queries(queries, tables, n_queries);

    //free tables mem
    for(int i =0;i<n_tables;i++){
        // print_table(tables[i], 1);
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