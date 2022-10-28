#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "hash1.h"
#include "partition.h"
#include "hashtable.h"

#define SAMPLE_SIZE 400

int main(void) {
    // tuple* tuples = malloc(SAMPLE_SIZE * sizeof(tuple));
    // for (int i=0; i<SAMPLE_SIZE; i++) {
    //     tuples[i].key = i;
    //     tuples[i].payload = rand() % 1000;
    //     //printf("hash for %d: %u\n", tuples[i].payload, hash2(tuples[i].payload, 100)); // test for hash2
    // }
    // relation relA;
    // relA.num_tuples = SAMPLE_SIZE;
    // relA.tuples = tuples;
    // partition_result partition_info = partition_relation(relA, 2);
    // for(int i=0;i<partition_info.histogram_size;i++){
    //     printf("partition %d begins at %d\n",i,partition_info.prefix_sum[i]);
    // }

    // delete_relation(relA);
    // delete_relation(partition_info.ordered_rel);
    // free(partition_info.prefix_sum);
    
    //=================================================================================================================
    int ret_rowid, data; // data represents elements of the column we want to join (ex. an element of R.a)
    hashtable* table = init_hashtable(SAMPLE_SIZE, 32); // init creates a 2*n size hash table (in this case, size of 20)

    for (int i=0; i<SAMPLE_SIZE; i++) {
        data = rand() % 1000;
        table = insert_hashtable(table, data, i); // use data as key to store rowid (in this case i) in hash table
        //print_hashtable(table);
        //search_hashtable(table, data, &ret_rowid); // search using data as key, get rowid back
        //printf("found %d\n", ret_rowid);
    }
    print_hashtable(table);
    delete_hashtable(table);
    //=================================================================================================================

    // recreating example from ekfonisi to test hopscotch, but its CYCLICAL
    // hashtable* table2 = init_hashtable(4, 4);

    // // random rowid values
    // table2->htbuckets[0].rowid = 32;
    // table2->htbuckets[1].rowid = -1;
    // table2->htbuckets[2].rowid = 10;
    // table2->htbuckets[3].rowid = 2;
    // table2->htbuckets[4].rowid = 5;
    // table2->htbuckets[5].rowid = 8;
    // table2->htbuckets[6].rowid = 3;
    // table2->htbuckets[7].rowid = 48;

    // // bitmaps from ekfonisi
    // bitmap_set_bit(&table2->htbuckets[2].bitmap, 0, 1);
    // bitmap_set_bit(&table2->htbuckets[2].bitmap, 1, 0);
    // bitmap_set_bit(&table2->htbuckets[2].bitmap, 2, 1);
    // bitmap_set_bit(&table2->htbuckets[2].bitmap, 3, 0);

    // bitmap_set_bit(&table2->htbuckets[4].bitmap, 0, 0);
    // bitmap_set_bit(&table2->htbuckets[4].bitmap, 1, 1);
    // bitmap_set_bit(&table2->htbuckets[4].bitmap, 2, 0);
    // bitmap_set_bit(&table2->htbuckets[4].bitmap, 3, 0);

    // bitmap_set_bit(&table2->htbuckets[6].bitmap, 0, 0);
    // bitmap_set_bit(&table2->htbuckets[6].bitmap, 1, 1);
    // bitmap_set_bit(&table2->htbuckets[6].bitmap, 2, 0);
    // bitmap_set_bit(&table2->htbuckets[6].bitmap, 3, 0);

    // print_hashtable(table2);
    // insert_hashtable(table2, 8962, 69); // trying to insert 69 to pos 2, but only empty slot is at pos 1
    // print_hashtable(table2);
    // delete_hashtable(table2);

    return 0;
}