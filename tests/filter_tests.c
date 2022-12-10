#include <stdio.h>
#include <stdlib.h>
#include "filter.h"
#include "relations.h"
#include "utils.h"
#include "acutest.h"
#include "intermediates.h"


void list_init_test(void) {
    list* l = init_list();
    TEST_ASSERT(l->row_id == -1);
    TEST_ASSERT(l->payload == -1);
    TEST_ASSERT(l->next == NULL);
    TEST_ASSERT(l->tail == l);
    delete_list(l);
}

void list_append_test(void) {
    list* l = init_list();
    list_append(l, 1, 2);
    TEST_ASSERT(l->row_id == 1);
    TEST_ASSERT(l->payload == 2);
    TEST_ASSERT(l->next == NULL);
    TEST_ASSERT(l->tail == l);
    list_append(l, 3, 4);
    TEST_ASSERT(l->row_id == 1);
    TEST_ASSERT(l->payload == 2);
    TEST_ASSERT(l->next->row_id == 3);
    TEST_ASSERT(l->next->payload == 4);
    TEST_ASSERT(l->next->next == NULL);
    TEST_ASSERT(l->tail->row_id == 3);
    TEST_ASSERT(l->tail->payload == 4);
    TEST_ASSERT(l->tail->next == NULL);
    delete_list(l);
}



void filter_test(void){

    relation r;
    read_file(&r,"./relations/filterte.txt");
    relation ret;
    filter_function(&r,&ret,_GREATER,0);
    int result = ret.num_tuples;
    TEST_ASSERT(result == 5);
    relation ret2;
    better_filter_function(&r,&ret2,_LESS,1);
    int result2 = ret2.num_tuples;
    TEST_ASSERT(result2 == 5);
}

TEST_LIST = {
    { "Basic Filter Functionality",filter_test},
    { "Test List Init", list_init_test },
    { "Test List Append", list_append_test },
    { NULL, NULL }
};

