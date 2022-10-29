#include "hashtable.h"

// NOTES:
// - on rehash, neighborhood size does not increase. May change
// - hash function used for hash table seems kinda bad, but the rehashes might be because of the small, static neighborhood
// - hopscotch has not been tested extremely thoroughly, but seems to work fine

// hash function for hash table. Max limits output range
unsigned int hash2(unsigned int x, unsigned int max) {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x % max;
}


// get value of n-th bit of bitmap (n=0 refers to MOST significant bit of the integer)
int bitmap_get_bit(unsigned int bitmap, int n) {
    return ((bitmap << n) >> (__INT_WIDTH__-1));
}

// set n-th bit of bitmap (n=0 refers to MOST significant bit of the integer)
void bitmap_set_bit(unsigned int* bitmap, int n, int value) {
    if (value == 1) *bitmap = (1 << (__INT_WIDTH__-n-1)) | (*bitmap);
    if (value == 0) *bitmap = *bitmap & (~(1 << (__INT_WIDTH__-n-1)));
}

// returns 0 or 1 depending on whether bitmap is full (aka when everything is 1)
int bitmap_full(unsigned int bitmap, int size) {
    for (int i=0; i<size; i++)
        if (!bitmap_get_bit(bitmap, i))
            return 0;
    return 1;
}


// prints hash table, for debug purposes
void print_hashtable(hashtable* table) {
    printf("\n---------------------------\n");
    for (int i=0; i<table->tablesize; i++) {
        printf("%d: %d, ", i, table->htbuckets[i].rowid);
        for (int j=0; j<table->nbsize; j++)
            printf("%d", bitmap_get_bit(table->htbuckets[i].bitmap, j));
        printf("\n");
    }
}


// creates a bucket, n is the size of the bitmap
hashbucket init_hashbucket(int n) {
    hashbucket bucket;
    bucket.rowid = -1;
    bucket.key = 0;
    bucket.bitmap = 0;
    return bucket;   
}


// swaps payload of buckets and updates bitmap (assumes first bucket is empty, so this should only be used for hopscotch)
// "bit" shows which bit of the bitmap to nullify
void bucket_swap(hashtable* table, int empty_bucket, int other_bucket, int bit) {
    int final_bucket = ((other_bucket + bit) % table->tablesize);
    int final_bit = empty_bucket-other_bucket;
    if (final_bit < 0) final_bit = (table->tablesize - other_bucket) + empty_bucket;

    table->htbuckets[empty_bucket].rowid = table->htbuckets[final_bucket].rowid;
    table->htbuckets[empty_bucket].key = table->htbuckets[final_bucket].key;
    table->htbuckets[final_bucket].rowid = -1;
    table->htbuckets[final_bucket].key = 0;
    bitmap_set_bit(&table->htbuckets[other_bucket].bitmap, bit, 0);
    bitmap_set_bit(&table->htbuckets[other_bucket].bitmap, final_bit, 1);
}


// initialize hash table
hashtable *init_hashtable(int n, int H) {
    // input check
    if (H > n) {
        printf("init_hashtable error: hash table size cannot be greater than neighbouhood\n");
        return NULL;
    }

    hashtable *ht;
    ht = malloc(sizeof(hashtable));

    // size of hashtable starts as 2*n, it may expand later
    ht->tablesize = 2*n;
    ht->nbsize = H;
    ht->htbuckets = malloc((2*n)*sizeof(hashbucket));

    for (int i=0; i<ht->tablesize; i++)
        ht->htbuckets[i] = init_hashbucket(H);
    return ht;
}


// takes key, and searches hash table. If search succeeds, the data (rowid) is stored in "ret"
void search_hashtable(hashtable* table, int key, int* ret) {
    int table_size = table->tablesize;
    unsigned int hash_value = hash2(key, table_size);
    int counter =0;

    for (int i=0; i<table->nbsize; i++) { // search neighborhood
        if (table->htbuckets[((hash_value+i) % table_size)].key == key) {
            //printf("found rowid %d at index %d\n", table->htbuckets[((hash_value+i) % table_size)].rowid, hash_value);
            ret[counter] = table->htbuckets[((hash_value+i) % table_size)].rowid;
            counter++;
        }
        
    }
    //printf("could not find any data from key %d\n", key);
    return;
}


// hashes key to insert data to hash table using hopscotch hashing
hashtable* insert_hashtable(hashtable* table, int key, int data) {
    int hashtable_full = 1, swapped, bit_num, pos, tmp_pos, tmp_j;
    int table_size = table->tablesize;
    int neighborhood_size = table->nbsize;
    unsigned int hash_value = hash2(key, table_size);

    //printf("\ninserting %d using key %d, mapping to index %u\n", data, key, hash_value);

    // if bitmap for desired position is full, rehash
    if (bitmap_full(table->htbuckets[hash_value].bitmap, neighborhood_size)) {
        rehash_hashtable(&table, key, data);
        return table;
    }

    // linear search for empty space 
    for (int i=0; i<table_size; i++) {
        tmp_pos = hash_value + i; // mod this value so we search the entire hash table cyclically from inital hash position x to x-1
        if (table->htbuckets[(tmp_pos % table_size)].rowid == -1) { // if we found an empty position
            pos = tmp_pos % table_size;
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
                        if (bitmap_get_bit(table->htbuckets[tmp_j].bitmap, k)) { 
                            bucket_swap(table, pos, tmp_j, k);
                            //printf("\nafter swap:\n");
                            //print_hashtable(table);
                            pos = ((tmp_j + k) % table_size);
                            tmp_pos = tmp_j + k;
                            swapped = 1;
                            break;
                        }
                    }
                    bit_num--;
                }
                if (!swapped) { // if there are no swap candidates, rehash
                    rehash_hashtable(&table, key, data);
                    return table;
                }
            }
            table->htbuckets[pos].rowid = data; // done
            table->htbuckets[pos].key = key;

            tmp_pos = pos-hash_value;
            if (tmp_pos < 0) tmp_pos = (table_size - hash_value) + pos;
            bitmap_set_bit(&table->htbuckets[hash_value].bitmap, tmp_pos, 1);
            break;
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

    for (int i=0; i<original_size; i++) // re-insert everything to new hash table
        if ((*ht)->htbuckets[i].rowid != -1)
            ht2 = insert_hashtable(ht2, (*ht)->htbuckets[i].key, (*ht)->htbuckets[i].rowid);
    delete_hashtable(*ht);
    *ht = ht2;

    if (cause_key != -1) // try to insert the value that caused the rehash again
        *ht = insert_hashtable(*ht, cause_key, cause_data);
}


void delete_hashtable(hashtable *ht) {
    free(ht->htbuckets);
    free(ht);
}