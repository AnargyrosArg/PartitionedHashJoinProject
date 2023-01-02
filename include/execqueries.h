#include <stdio.h>
#include <stdlib.h>

#include "intermediates.h"
#include "parser.h"
#include "filter.h"
#include "join.h"
#include "jobscheduler.h"

void printsum(int , int , Intermediates* ,table *,int );
void exec_query(QueryInfo*, table*,jobscheduler*);
void exec_all_queries(QueryInfo*, table*, uint,jobscheduler*);