#include <stdio.h>
#include <stdlib.h>
#include "parser.h"
#include "acutest.h"

void parser_test(void){
    QueryInfo qinfo ;
    query_info_init(&qinfo);

    char* str = "9 0 2|0.1=1.0&0.3=3.0&1.0=2.2&0.0>12472|1.0 0.3 0.4 1.0 0.3 0.4 1.0 0.3 0.4";
    char* query = str;
    parse_query(query,&qinfo);
}

TEST_LIST = {
    { "Basic Parser Functionality",parser_test},
    { 0, 0 }
};