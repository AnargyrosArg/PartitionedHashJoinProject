#include <stdio.h>
#include <stdlib.h>
#include "filter.h"
#include "relations.h"
#include "utils.h"
#include "acutest.h"


void filter_test(void){

    relation r;
    read_file(&r,"./relations/filterte.txt");
    relation ret;
    filter_function(&r,&ret,_GREATER,0);
    int result = ret.num_tuples;


    TEST_ASSERT(result == 5);
}

TEST_LIST = {
    { "Basic Filter Functionality",filter_test},
    { NULL, NULL }
};

