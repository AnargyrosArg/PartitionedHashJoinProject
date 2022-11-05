#include "hashtable.h"
#include <limits.h>

// hash function for hash table. Max limits output range
// credit: https://stackoverflow.com/questions/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key
unsigned int hash2(unsigned int x, unsigned int max) {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x % max;
}


// prints hash table, for debug purposes
void print_hashtable(hashtable* table) {
    printf("\n---------------------------\n");
    for (int i=0; i<table->tablesize; i++) {
        printf("%d: [", i);
        for (int j=0; j<(table->htbuckets[i]->rowids_pos); j++)
            printf("%d, ", table->htbuckets[i]->rowids[j]);
        printf("]  ");
        for (int j=0; j<table->nbsize; j++)
            printf("%d", bitmap_get_bit(table->htbuckets[i]->bitmap, j));
        printf("\n");
    }
}


// creates a bucket, n is the size of the bitmap
hashbucket* init_hashbucket(int n) {
    hashbucket* bucket;
    bucket = malloc(sizeof(hashbucket));
    bucket->key = 0;
    bucket->bitmap = 0;
    bucket->rowids = malloc(sizeof(int) * n);
    bucket->rowids[0] = -1;
    bucket->rowids_pos = 0;
    bucket->rowids_size = n;
    return bucket;
}


// initialize hash table
hashtable *init_hashtable(int n, int H) {
    // input check
    if (H > n) {
        //printf("init_hashtable error: hash table size cannot be greater than neighbouhood\n");
        //return NULL;
        H = n;
    }

    if (H > (sizeof(unsigned long long)*__CHAR_BIT__)) {
        printf("init_hashtable error: hash table size cannot be greater than %ld\n", (sizeof(unsigned long long)*__CHAR_BIT__));
        return NULL;
    }

    hashtable *ht;
    ht = malloc(sizeof(hashtable));

    // size of hashtable starts as 2*n, it may expand later
    ht->tablesize = 2*n;
    ht->nbsize = H;
    ht->htbuckets = malloc((2*n)*sizeof(hashbucket*)); // FREE THIS PLS

    for (int i=0; i<ht->tablesize; i++)
        ht->htbuckets[i] = init_hashbucket(H);
    return ht;
}


// swaps payload of buckets and updates bitmap (assumes first bucket is empty, so this should only be used for hopscotch)
// "bit" shows which bit of the bitmap to nullify
void bucket_swap(hashtable* table, int empty_bucket, int other_bucket, int bit) {
    int final_bucket = ((other_bucket + bit) % table->tablesize);
    int final_bit = empty_bucket-other_bucket;
    if (final_bit < 0) final_bit = (table->tablesize - other_bucket) + empty_bucket;
    int* empty_rowids = table->htbuckets[empty_bucket]->rowids;

    table->htbuckets[empty_bucket]->rowids = table->htbuckets[final_bucket]->rowids;
    table->htbuckets[empty_bucket]->key = table->htbuckets[final_bucket]->key;
    table->htbuckets[empty_bucket]->rowids_pos = table->htbuckets[final_bucket]->rowids_pos;
    table->htbuckets[empty_bucket]->rowids_size = table->htbuckets[final_bucket]->rowids_size;

    table->htbuckets[final_bucket]->rowids = empty_rowids;
    table->htbuckets[final_bucket]->key = 0;
    table->htbuckets[final_bucket]->rowids_pos = 0;
    table->htbuckets[final_bucket]->rowids_size = table->nbsize;

    bitmap_set_bit(&table->htbuckets[other_bucket]->bitmap, bit, 0);
    bitmap_set_bit(&table->htbuckets[other_bucket]->bitmap, final_bit, 1);
}


// takes key, and searches hash table. If search succeeds, the data (rowid) is stored in "ret"
int* search_hashtable(hashtable* table, int key, int* ret_size) {
    int table_size = table->tablesize;
    unsigned int hash_value = hash2(key, table_size);

    for (int i=0; i<table->nbsize; i++) { // search neighborhood
        if (table->htbuckets[((hash_value+i) % table_size)]->key == key) {
            //printf("found rowid %d at index %d\n", table->htbuckets[((hash_value+i) % table_size)].rowid, hash_value);
            *ret_size = (table->htbuckets[((hash_value+i) % table_size)]->rowids_pos);
            return table->htbuckets[((hash_value+i) % table_size)]->rowids;
        }
    }
    //printf("could not find any data from key %d\n", key);
    return NULL;
}


// hashes key to insert data to hash table using hopscotch hashing
hashtable* insert_hashtable(hashtable* table, int key, int data) {
    int hashtable_full = 1, swapped, bit_num, pos, tmp_pos, tmp_j;
    int table_size = table->tablesize;
    int neighborhood_size = table->nbsize;
    unsigned int hash_value = hash2(key, table_size);
    int map_full = bitmap_full(table->htbuckets[hash_value]->bitmap, neighborhood_size);

    //printf("\ninserting %d using key %d, mapping to index %u\n", data, key, hash_value);

    // linear search for empty space 
    for (int i=0; i<table_size; i++) {
        tmp_pos = hash_value + i; // tmp_pos can go out of bounds of the hashtable. It is useful to measure distance when we loop back to the start

        if (table->htbuckets[(tmp_pos % table_size)]->rowids[0] == -1) { // if we found an empty position
            pos = tmp_pos % table_size; // turn tmp_pos into actual position that does not go out of bounds
            hashtable_full = 0;

            while ((tmp_pos - hash_value) >= neighborhood_size) { // loop until we find position within neighborhood
                bit_num = neighborhood_size-1;
                swapped = 0;

                for (int j=pos-neighborhood_size+1; j<pos; j++) { // for the previous H-1 positions,
                    if (swapped) break;
                    tmp_j = j;
                    if (j < 0) tmp_j = table_size + j; // loop back to end of hash table if we go out of bounds

                    for (int k=0; k<bit_num; k++) { // for every bitmap bit that matters (as we move through positions, less bits matter)
                        //printf("empty at %d, checking bucket %d, bit %d\n", pos, tmp_j, k);
                        if (bitmap_get_bit(table->htbuckets[tmp_j]->bitmap, k)) { // check for swap
                            bucket_swap(table, pos, tmp_j, k);
                            //printf("\nafter swap:\n");
                            //print_hashtable(table);
                            pos = ((tmp_j + k) % table_size);
                            tmp_pos = tmp_j + k;
                            swapped = 1;
                            break;
                        }
                    }
                    bit_num--; // with every position we check, less and less bits matter in the bitmaps we check
                }
                if (!swapped) { // if there are no swap candidates, rehash
                    rehash_hashtable(&table, key, data);
                    return table;
                }
            }
            // finally, insert rowid
            table->htbuckets[pos]->rowids = insert_array(table->htbuckets[pos]->rowids, &(table->htbuckets[pos]->rowids_pos),
                                                         &(table->htbuckets[pos]->rowids_size), data);
            table->htbuckets[pos]->key = key;

            tmp_pos = pos-hash_value;
            if (tmp_pos < 0) tmp_pos = (table_size - hash_value) + pos;
            bitmap_set_bit(&table->htbuckets[hash_value]->bitmap, tmp_pos, 1);
            break;
        }
        else if ((i >= neighborhood_size-1) && (map_full)) { // if we have not found an empty position yet, check if we should give up early:
            rehash_hashtable(&table, key, data);             // if the bitmap is already full, and the key we are trying to insert is not
            return table;                                    // already in the neighborhood (meaning we cannot chain it), then rehash
        }
        // check if we can chain (the key we were given is already in the neighborhood)
        else if (table->htbuckets[(tmp_pos % table_size)]->key == key) {
            pos = tmp_pos % table_size;
            table->htbuckets[pos]->rowids = insert_array(table->htbuckets[pos]->rowids, &(table->htbuckets[pos]->rowids_pos),
                                                         &(table->htbuckets[pos]->rowids_size), data);
            return table;
        }
    }
    if (hashtable_full) { // if hash table is completely full, rehash
        rehash_hashtable(&table, key, data);
        return table; 
    }
    return table;
}


// double size of table and re-insert everything ("cause value is the value that caused the rehash. It will be inserted after")
void rehash_hashtable(hashtable **ht, int cause_key, int cause_data) {
    int original_size = (*ht)->tablesize;
    printf("\nRehashing\n");

    hashtable *ht2 = init_hashtable(original_size, (*ht)->nbsize); // "init_hashtable" initializes table with 2*original_size

    for (int i=0; i<original_size; i++) { // re-insert everything to new hash table
        if ((*ht)->htbuckets[i]->rowids[0] != -1) { // for every bucket
            for (int j=0; j<((*ht)->htbuckets[i]->rowids_pos); j++) // for every rowid of that bucket
                ht2 = insert_hashtable(ht2, (*ht)->htbuckets[i]->key, (*ht)->htbuckets[i]->rowids[j]);
        }
    }
    delete_hashtable(*ht);
    *ht = ht2;

    if (cause_key != -1) // try to insert the value that caused the rehash again
        *ht = insert_hashtable(*ht, cause_key, cause_data);
}


void delete_hashtable(hashtable *ht) {
    for (int i=0; i<ht->tablesize; i++) {
        free(ht->htbuckets[i]->rowids);
        free(ht->htbuckets[i]);
    }
    free(ht->htbuckets);
    free(ht);
}