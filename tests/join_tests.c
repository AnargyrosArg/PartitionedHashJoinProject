#include <stdio.h>
#include <stdlib.h>
#include "join.h"
#include "acutest.h"

void join_test(void){
    relation relA;
    read_file(&relA,"./relations/1pass.txt");
    relation relB;
    read_file(&relB,"./relations/2pass_required.txt");
    result res = joinfunction(relA,relB);
    TEST_ASSERT(res.result_size == 1593);
    delete_relation(relA);
    delete_relation(relB);
    delete_result(&res);
    return;
}

TEST_LIST = {
    { "Basic Join Functionality",join_test},
    { NULL, NULL }
};