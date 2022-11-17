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

    result res = joinfunction(relA,relB);

    for(int i=0; i<res.result_size ;i++){
        printf("%d - %d with value: %d\n",res.pairs[i].key1,res.pairs[i].key2,res.pairs[i].payload);
    }
    printf("result capacity %d ,  result size %d\n",res.capacity,res.result_size);
    delete_result(&res);
    delete_relation(relA);
    delete_relation(relB);
    return 0;
}