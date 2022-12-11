#include "acutest.h"
#include "intermediates.h"


void basic_intermediate_functionality(void){
    Intermediates* inters = init_intermediates();
    bool val_rels[4] = {true , false , false ,true};
    bool val_rels2[4] = {true , true , false ,false};
    
    table* tabl=NULL;


    Intermediate* int1= malloc(sizeof(Intermediate));
    init_intermediate(int1);
    set_intermediate(int1,100,val_rels);
    insert_intermediate(int1,inters);

    Intermediate* int2 = malloc(sizeof(Intermediate));
    init_intermediate(int2);
    set_intermediate(int2,100,val_rels2);
    insert_intermediate(int2,inters);

    Intermediate* test;
    get_intermediates(inters,0,0, &test,tabl);

    TEST_ASSERT(test->rowids == int1->rowids);
    Intermediate* temp;

    TEST_ASSERT(in_same_intermediate_relation(inters ,0,3,&temp) == 0);
    TEST_ASSERT(in_same_intermediate_relation(inters ,0,1,&temp) == 1);
    TEST_ASSERT(in_same_intermediate_relation(inters ,0,2,&temp) == -1);

    delete_intermediates(inters);
}



TEST_LIST = {
    {"Basic intermediates Functionality", basic_intermediate_functionality},
    {NULL, NULL}
};