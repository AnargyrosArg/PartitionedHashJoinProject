#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "join.h"

#define SAMPLE_SIZE 500

int main(void) {
    relation relA;
    read_file(&relA,"relation_a.txt");
    
    relation relB;
    read_file(&relB,"relation_b.txt");

    joinfunction(relA,relB);

    //=================================================================================================================

    // int data; // data represents elements of the column we want to join (ex. an element of R.a)
    // int ret_size, range = 5; // range limits range of random numbers (LOW NUMBER = LOTS OF DUPLICATES, so if you want to test chaining, have it low)
    // int* ret;
    // hashtable* table = init_hashtable(10, 4); // init creates a 2*n size hash table (in this case, size of 20)
    
    // for (int i=0; i<20; i++) {
    //     data = rand() % range;
    //     table = insert_hashtable(table, data, i); // use data as key to store rowid (in this case i) in hash table
    //     print_hashtable(table);
    //     ret = search_hashtable(table, data, &ret_size); // search using data as key, get rowid back

    //     printf("found ");
    //     for (int j=0; j<ret_size; j++)
    //         printf("%d ", ret[j]);
    //     printf("\n");
    // }
    // print_hashtable(table);
    // delete_hashtable(table);

    //=================================================================================================================

    // recreating example from ekfonisi to test hopscotch, but its CYCLICAL
    // hashtable* table2 = init_hashtable(4, 4);

    // // manually add random rowid values (also have to manually increase counter)
    // table2->htbuckets[0]->rowids[0] = 32; table2->htbuckets[0]->rowids_pos++;
    // table2->htbuckets[1]->rowids[0] = -1; table2->htbuckets[1]->rowids_pos++;
    // table2->htbuckets[2]->rowids[0] = 10; table2->htbuckets[2]->rowids_pos++;
    // table2->htbuckets[3]->rowids[0] = 2; table2->htbuckets[3]->rowids_pos++;
    // table2->htbuckets[4]->rowids[0] = 5; table2->htbuckets[4]->rowids_pos++;
    // table2->htbuckets[5]->rowids[0] = 8; table2->htbuckets[5]->rowids_pos++;
    // table2->htbuckets[6]->rowids[0] = 3; table2->htbuckets[6]->rowids_pos++;
    // table2->htbuckets[7]->rowids[0] = 48; table2->htbuckets[7]->rowids_pos++;

    // // manually add bitmaps from ekfonisi
    // bitmap_set_bit(&table2->htbuckets[2]->bitmap, 0, 1);
    // bitmap_set_bit(&table2->htbuckets[2]->bitmap, 1, 0);
    // bitmap_set_bit(&table2->htbuckets[2]->bitmap, 2, 1);
    // bitmap_set_bit(&table2->htbuckets[2]->bitmap, 3, 0);

    // bitmap_set_bit(&table2->htbuckets[4]->bitmap, 0, 0);
    // bitmap_set_bit(&table2->htbuckets[4]->bitmap, 1, 1);
    // bitmap_set_bit(&table2->htbuckets[4]->bitmap, 2, 0);
    // bitmap_set_bit(&table2->htbuckets[4]->bitmap, 3, 0);

    // bitmap_set_bit(&table2->htbuckets[6]->bitmap, 0, 0);
    // bitmap_set_bit(&table2->htbuckets[6]->bitmap, 1, 1);
    // bitmap_set_bit(&table2->htbuckets[6]->bitmap, 2, 0);
    // bitmap_set_bit(&table2->htbuckets[6]->bitmap, 3, 0);

    // print_hashtable(table2);
    // insert_hashtable(table2, 8962, 69); // trying to insert 69 to pos 2, but only empty slot is at pos 1
    // print_hashtable(table2);
    // delete_hashtable(table2);

    //=================================================================================================================
    delete_relation(relA);
    delete_relation(relB);
    return 0;
}