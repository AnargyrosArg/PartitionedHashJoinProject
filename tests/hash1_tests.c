#include <stdio.h>
#include <stdlib.h>
#include "hash1.h"
#include "acutest.h"

void simple_hash1_calls(void){
    TEST_ASSERT(hash1(3,3) == 3);
    TEST_ASSERT(hash1(3,4) == 3);
    TEST_ASSERT(hash1(3,5) == 3);
    TEST_ASSERT(hash1(3,1) == 1);
}

void large_depth(void){
    int r = rand()%100;
    TEST_ASSERT( hash1(r,40) == r );
}

void negative_depth(void){
    int r = rand()%100;
    TEST_ASSERT(hash1(r,-10) == r );
}


TEST_LIST = {
    { "Basic Functionality",simple_hash1_calls},
    { "Shifting value larger than int width",large_depth},
    { "Negative depth",negative_depth},
    { NULL, NULL }
};