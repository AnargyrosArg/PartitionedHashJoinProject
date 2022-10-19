#include <stdio.h>
#include <stdlib.h>

#include "relations.h"

typedef struct hashbucket hashbucket;
typedef struct hashtable hashtable;


struct hashbucket{
    int rowid;
    int *bitmap;
};

struct hashtable{
    hashbucket *htbuckets;
    //size of the table
    int tablesize;
    //size of the neighbourhood
    int nbsize;
};

// creates a bucket, n is the size of the bitmap
hashbucket inithashbucket(int n);
//creates an empty hastable, the size is going to be 2*n and H the size of neighbourhood
hashtable *inithashtable(int n, int H);


void deletehashtable(hashtable *ht);

void expandhashtable(hashtable **ht);
