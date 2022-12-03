#include "execqueries.h" 



void printsum(int rel, int column, Intermediates* inter, table *tabl){
    uint64_t sum =0;

    //first we need to get the relation from the intermediates
    int *rowidarray;
    int numrows;

    get_intermediates(inter, rel, &rowidarray, &numrows);
    //printf("relation %d has %d tuples:",rel,numrows);
    
    //now for every id in the rowidarray we need to get the value of the column
    int i=0;
    for(i=0;i<numrows;i++){
        uint rowid = rowidarray[i];
        uint64_t value = tabl->table[column][rowid];
        sum = sum + value;
    }

    printf("sum of column %d of relation %d is %lu\n",column,rel,sum);

    return;
}