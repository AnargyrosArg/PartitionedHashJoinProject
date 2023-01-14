#include <stdio.h>
#include <stdlib.h>
#include "join.h"
#include "acutest.h"

void join_test(void){
    jobscheduler* scheduler = malloc(sizeof(jobscheduler));
    init_scheduler(scheduler);
    relation relA;
    read_file(&relA,"./relations/1pass.txt");
    relation relB;
    read_file(&relB,"./relations/2pass_required.txt");
    result res = joinfunction(relA,relB,scheduler);
    TEST_ASSERT(res.result_size == 1593);
    delete_relation(relA);
    delete_relation(relB);
    delete_result(&res);
    delete_scheduler(scheduler);
    free(scheduler);

    return;
}

TEST_LIST = {
    { "Basic Join Functionality",join_test},
    { NULL, NULL }
};