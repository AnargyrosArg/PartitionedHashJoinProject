#include <stdio.h>
#include <stdlib.h>
#include "utils.h"

#include "relations.h"

unsigned int hash2(unsigned int x, unsigned int max);
unsigned int hash_simple(unsigned int x, unsigned int max);

typedef struct hashbucket hashbucket;
typedef struct hashtable hashtable;

struct hashbucket {
    int key; // the key that when hashed, leads to this bucket
    int* rowids; // the data the bucket holds (all the rowids related to the key)
    int rowids_size; // size of rowid array
    int rowids_pos; // shows where the next empty position is in the array
    unsigned long long bitmap; // bitmap showing which elements in the neighborhood originally hashed to this bucket
};

struct hashtable {
    hashbucket **htbuckets;
    int tablesize; // size of the table
    int nbsize; // size of the neighbourhood
    unsigned int (*hash_ptr)(unsigned int, unsigned int); // hash function pointer
};

// bitmap functions
int bitmap_get_bit(unsigned long long bitmap, int n);
void bitmap_set_bit(unsigned long long* bitmap, int n, int value);
int bitmap_full(unsigned long long bitmap, int size);

// hash table functions
hashbucket* init_hashbucket(int n); // creates a bucket, n is the size of the bitmap
void bucket_swap(hashtable* table, int x, int y, int z);

// creates an empty hastable, the size is going to be 2*n and H the size of neighbourhood
hashtable *init_hashtable(int n, int H, unsigned int (*hash_ptr)(unsigned int, unsigned int));

hashtable* insert_hashtable(hashtable* table, int key, int data); // hashes key to insert data to hash table using hopscotch hashing
void print_hashtable(hashtable* table);
void delete_hashtable(hashtable *ht);
int* search_hashtable(hashtable* table, int key, int* ret_size); // returns all rowids related to given key (ret_size shows size of returned array)

// doubles size of table, places everything again, and re-inserts value that caused rehash
void rehash_hashtable(hashtable **ht, int cause_key, int cause_data);
