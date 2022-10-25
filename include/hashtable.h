#include <stdio.h>
#include <stdlib.h>

#include "relations.h"

unsigned int hash2(unsigned int x, unsigned int max);

typedef struct hashbucket hashbucket;
typedef struct hashtable hashtable;

struct hashbucket {
    int rowid; // the data the bucket holds
    int key; // the key that when hashed, leads to this bucket
    unsigned int bitmap; // bitmap showing which elements in the neighborhood originally hashed to this bucket
};

struct hashtable {
    hashbucket *htbuckets;
    int tablesize; // size of the table
    int nbsize; // size of the neighbourhood
};

// bitmap functions
int bitmap_get_bit(unsigned int bitmap, int n);
void bitmap_set_bit(unsigned int* bitmap, int n, int value);
int bitmap_full(unsigned int bitmap, int size);

// hash table functionsZ
hashbucket init_hashbucket(int n); // creates a bucket, n is the size of the bitmap
void bucket_swap(hashtable* table, int x, int y, int z);

hashtable *init_hashtable(int n, int H); // creates an empty hastable, the size is going to be 2*n and H the size of neighbourhood
hashtable* insert_hashtable(hashtable* table, int key, int data); // hashes key to insert data to hash table using hopscotch hashing
void print_hashtable(hashtable* table);
void delete_hashtable(hashtable *ht);
void search_hashtable(hashtable* table, int key, int* ret);

// doubles size of table, places everything again, and re-inserts value that caused rehash
void rehash_hashtable(hashtable **ht, int cause_key, int cause_data);
