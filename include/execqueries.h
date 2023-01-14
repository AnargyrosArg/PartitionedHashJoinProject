#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "intermediates.h"
#include "parser.h"
#include "filter.h"
#include "join.h"
#include "stats.h"
#include "jobscheduler.h"
#include "optimizer.h"

#define MAX_QUERY_THREADS 6



//struct that holds the information for the projections of each query
typedef struct{
    int numofprojections;
    uint64_t *sums;
    int numofquery; //this is the number of the query, we need it to print the results in order
} exec_result;

//we need this struct for the thread function
typedef struct {
  QueryInfo* query;
  int num_queries;
  table* tabl;
  jobscheduler* scheduler;
  exec_result** results;
  int optimize; // if optimize is non-zero, optimizer is used
} ThreadArgs;


uint64_t printsum(int , int , Intermediates* ,table *,int );
exec_result* exec_query(QueryInfo*, table*,jobscheduler*, int optimize);
void exec_all_queries(QueryInfo*, table*, uint,jobscheduler*, int optimize); // if optimize is non-zero, optimizer is used

void *thread_function(void *);