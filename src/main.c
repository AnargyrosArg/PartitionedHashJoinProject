#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "relations.h"
#include "utils.h"
#include "execqueries.h"

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

    //allocate table array
    tables = malloc(MAX_N_TABLES * sizeof(table));
    queries = malloc(MAX_N_QUERIES * sizeof(QueryInfo));

    //init job scheduler
    jobscheduler* scheduler = malloc(sizeof(jobscheduler));
    init_scheduler(scheduler);
    
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
        // End of a batch,we have to execute all the queries
        if (strcmp(line,"F")==0) {
            exec_all_queries(queries, tables, n_queries,scheduler);
            
            //we have to reset the queries array
            for(int i=0;i<n_queries;i++){
                query_info_delete(&(queries[i]));
            }
            n_queries = 0;
            continue;
        }
        parse_query(line,&(queries[n_queries++]));
        //exec_query(&queries[current++],tables,scheduler);
    }
    free(line);


    //harness waits for result before sending next batch , so we have to execute queries as they come    
    //exec_all_queries(queries, tables, n_queries);
    
    //free tables mem
    for(int i =0;i<n_tables;i++){
        delete_table(&(tables[i]));
    }
    free(tables);
    
    //free queries mem
    free(queries);

    //TODO free and destroy scheduler
    return 0;
}