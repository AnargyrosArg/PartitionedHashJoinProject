#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "hash1.h"
#include "partition.h"
#include "hashtable.h"

#define SAMPLE_SIZE 400

int main(void) {
    tuple* tuples = malloc(SAMPLE_SIZE * sizeof(tuple));
    for (int i=0; i<SAMPLE_SIZE; i++) {
        tuples[i].key = i;
        tuples[i].payload = rand() % 1000;
        //printf("hash for %d: %u\n", tuples[i].payload, hash2(tuples[i].payload, 100)); // test for hash2

        if (hash1(tuples[i].payload, 2) == 4)
            printf("---------------------DING----------------------------\n");
    }
    relation relA;
    relA.num_tuples = SAMPLE_SIZE;
    relA.tuples = tuples;
    partition_relation(relA, 2);

    int ret_rowid, data; // data represents elements of the column we want to join (ex. an element of R.a)
    hashtable* table = init_hashtable(10, 4); // init creates a 2*n size hash table (in this case, size of 20)

    for (int i=0; i<20; i++) {
        data = rand() % 1000;
        table = insert_hashtable(table, data, i); // use data as key to store rowid (in this case i) in hash table
        print_hashtable(table);
        search_hashtable(table, data, &ret_rowid); // search using data as key, get rowid back
        printf("found %d\n", ret_rowid);
    }
    print_hashtable(table);
    delete_hashtable(table);

    // recreating example from ekfonisi to test hopscotch
    
    // hashtable* table2 = init_hashtable(4, 4);

    // // random rowid values
    // table2->htbuckets[0].rowid = 10;
    // table2->htbuckets[1].rowid = 2;
    // table2->htbuckets[2].rowid = 5;
    // table2->htbuckets[3].rowid = 8;
    // table2->htbuckets[4].rowid = 3;
    // table2->htbuckets[5].rowid = 48;
    // table2->htbuckets[6].rowid = 32;
    // table2->htbuckets[7].rowid = -1;

    // // bitmaps from ekfonisi
    // table2->htbuckets[0].bitmap[0] = 1;
    // table2->htbuckets[0].bitmap[1] = 0;
    // table2->htbuckets[0].bitmap[2] = 1;
    // table2->htbuckets[0].bitmap[3] = 0;

    // table2->htbuckets[2].bitmap[0] = 0;
    // table2->htbuckets[2].bitmap[1] = 1;
    // table2->htbuckets[2].bitmap[2] = 0;
    // table2->htbuckets[2].bitmap[3] = 0;

    // table2->htbuckets[4].bitmap[0] = 0;
    // table2->htbuckets[4].bitmap[1] = 1;
    // table2->htbuckets[4].bitmap[2] = 0;
    // table2->htbuckets[4].bitmap[3] = 0;

    // print_hashtable(table2);
    // insert_hashtable(table2, 528, 69); // trying to insert 69 to pos 0, but only empty slot is at pos 7
    // print_hashtable(table2);
    // delete_hashtable(table2);

    return 0;
}