#include "hashtable.h"

// creates a bucket, n is the size of the bitmap
hashbucket inithashbucket(int n){
    hashbucket bucket;
    bucket.rowid = -1;
    bucket.bitmap = malloc(n*sizeof(int));
    int i = 0;
    for(i=0; i<n;i++){
        bucket.bitmap[i]=0;
    }
    return bucket;
    
}

hashtable *inithashtable(int n, int H){
    hashtable *ht;
    ht = malloc(sizeof(hashtable));
    //size of hashtable starts as 2*n, it may expand later...
    ht->tablesize = 2*n;
    ht->nbsize = H;
    ht->htbuckets = malloc((2*n)*sizeof(hashbucket));
    int i=0;
    for(i=0; i<ht->tablesize; i++){
        ht->htbuckets[i] = inithashbucket(H);
    }

    return ht;
}

void expandhashtable(hashtable **ht){
    //the new size of the hashtable will be double the old size and the size of the neighbourhood will be expanded as well
    int new_size = (*ht)->tablesize *2;
    int new_nbsize = (*ht)->nbsize *2;

    hashtable *ht2 = inithashtable(new_size,new_nbsize);

    //transfer all the data as it was before 
    for(int i=0;i<new_size;i++){
        if(i< (*ht)->tablesize){
            ht2->htbuckets[i].rowid = (*ht)->htbuckets[i].rowid;
            int j;
            for(j=0; j< (*ht)->nbsize; j++){
                ht2->htbuckets[i].bitmap[j] = (*ht)->htbuckets[i].bitmap[j];
            }
        }
    }

    free(*ht);
    *ht = ht2;
    return;
}

void deletehashtable(hashtable *ht){
    int i=0;
    for(i=0; i< ht->tablesize; i++){
       free(ht->htbuckets[i].bitmap);
    }
    free(ht);
    return;
}