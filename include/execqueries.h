#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "intermediates.h"
#include "parser.h"
#include "filter.h"
#include "join.h"
#include "jobscheduler.h"

#define MAX_QUERY_THREADS 2

//we need this struct for the thread function
typedef struct {
  QueryInfo* query;
  table* tabl;
  jobscheduler* scheduler;
} ThreadArgs;

//struct that holds the information for the projections of each query
typedef struct{
    int numofprojections;
    uint64_t *sums;
    int numofquery; //this is the number of the query, we need it to print the results in order
} exec_result;


uint64_t printsum(int , int , Intermediates* ,table *,int );
exec_result* exec_query(QueryInfo*, table*,jobscheduler*);
void exec_all_queries(QueryInfo*, table*, uint,jobscheduler*);

void *thread_function(void *);