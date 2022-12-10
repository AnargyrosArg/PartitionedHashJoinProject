#include <stdio.h>

#include <stdlib.h>

#include "relations.h"
#include "hash1.h"
#include "utils.h"
#include "hashtable.h"
#include "partition.h"
#include "intermediates.h"
#include "parser.h"

result joinfunction(relation r, relation s);
Intermediate* join_intermediates(Intermediate* inter1,Intermediate* inter2,QueryInfo* query,int rel1,int col1,int rel2,int col2,table* tabl);