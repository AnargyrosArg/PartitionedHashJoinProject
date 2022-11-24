#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "relations.h"
#include "parser.h"

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
        //remove newline
        line[strlen(line)-1] = 0;
        if (strcmp(line,"Done")==0) break;
        //store table
        tables[n_tables++]=load_relation(line);
    }

    //parse batch of queries
    while ((n_read=getline(&line,&line_max_size,stdin))>0) {
        //remove newline
        line[strlen(line)-1] = 0;
        if (strcmp(line,"F")==0) continue;; // End of a batch
        parse_query(line,&(queries[n_queries++]));
    }

    for(int i =0;i<n_queries;i++){
        print_query_info(queries[i]);
    }
 
    //free input buffer
    free(line);
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