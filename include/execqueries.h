#include <stdio.h>
#include <stdlib.h>

#include "intermediates.h"
#include "parser.h"
#include "filter.h"
#include "join.h"


void printsum(int , int , Intermediates* ,table *);
void exec_query(QueryInfo*, table*);