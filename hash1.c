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


//partition the original relation in 2^n buckets 
relation* partition_relation(relation relation,int n){
    //Our histograms are int arrays where the index is the bucket hash value and the number pointed to
    //by the index is the number of occurences
    int histogram_size = 2<<(n-1);
    int histogram[histogram_size];
    //init array values to 0
    init_histogram(histogram,histogram_size);

    //fill histogram by hashing the payloads of the relation tuples and counting the results
    for(int i=0;i<relation.num_tuples;i++){
        histogram[hash1(relation.tuples[i].payload,n)]++;
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
    
}