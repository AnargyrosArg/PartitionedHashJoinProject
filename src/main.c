#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "join.h"


table* tables;


int main(int argc, char** argv) {
    
    table ret = load_relation("workloads/small/r1");

    for(uint64_t tuple=0;tuple<ret.num_tuples;tuple++){
        for(uint64_t column=0;column<ret.num_colums;column++){
            printf("%ld|",ret.table[column][tuple]);
        }
        printf("\n");
    }

    delete_table(&ret);
}