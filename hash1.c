#include "hash1.h"

unsigned int hash1(unsigned int value,int n){
    return (value << (__INT_WIDTH__ - n)) >> (__INT_WIDTH__ - n);
}

unsigned int hopscotch_search(relation relation,int key ,unsigned int H){

}

//print histogram for debug purposes
void print_histogram(int* histogram,int size){
    for(int i=0;i<size;i++){
        printf("Bucket %d has %d elements\n",i,histogram[i]);
    }
}

//Initializes histogram values to 0
void init_histogram(int* histogram,int histogram_size){
    for(int i=0;i < histogram_size ;i++){
        histogram[i]=0;
    }
}



void init_relation(relation* rel,int num_tuples){
    rel->tuples = malloc(num_tuples * sizeof(tuple));
    rel->num_tuples = num_tuples;
}


//partition the original relation in 2^n buckets 
relation* partition_relation(relation rel,int n){
    //Our histograms are int arrays where the index is the bucket hash value and the number pointed to
    //by the index is the number of occurences
    int histogram_size = 2<<(n-1);
    int histogram[histogram_size];
    //init array values to 0
    init_histogram(histogram,histogram_size);

    //fill histogram by hashing the payloads of the relation tuples and counting the results
    for(int i=0;i<rel.num_tuples;i++){
        histogram[hash1(rel.tuples[i].payload,n)]++;
    }   

    //debug line
    print_histogram(histogram,histogram_size);

    int prefix_sum[histogram_size];
    int sum=0;
    //calculate prefix sums for each bucket
    for(int i=0;i<histogram_size;i++){
        prefix_sum[i]=sum;
        sum += histogram[i];       
    }
    relation ordered_rel;
    init_relation(&ordered_rel,rel.num_tuples);
    printf("rel inited\n");
    //a table to keep the last index of each bucket that an element was inserted in
    int offsets[histogram_size];
    init_histogram(offsets,histogram_size);
    for(int i=0;i<rel.num_tuples;i++){
        int bucket = hash1(rel.tuples[i].payload,n);
        ordered_rel.tuples[prefix_sum[bucket] + offsets[bucket]]= rel.tuples[i];
        offsets[bucket]++;
    }
    
    for(int i=0;i<ordered_rel.num_tuples;i++){
        printf("Element with payload %d and hashed value %d\n",ordered_rel.tuples[i].payload,hash1(ordered_rel.tuples[i].payload,n));
    }

}