#include "stdio.h"
#include "acutest.h"

void d(void){
    TEST_ASSERT(1);
}


TEST_LIST = {
    { "Mock test",d},
    { NULL, NULL }
};