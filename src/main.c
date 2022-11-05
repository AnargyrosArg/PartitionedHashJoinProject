#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "join.h"

int main(int argc, char** argv) {
    if(argc != 3){
        printf("Invalid Arguements\n");
        return 1;
    }
    
    relation relA;
    read_file(&relA,argv[1]);
    
    relation relB;
    read_file(&relB,argv[2]);

    joinfunction(relA,relB);

    delete_relation(relA);
    delete_relation(relB);
    return 0;
}