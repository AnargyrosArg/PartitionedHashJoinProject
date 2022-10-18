#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "partition.h"
#include "hash1.h"

/*
Source file containing logic for the partitioning the relations
*/

//print histogram for debug purposes
void print_histogram(int* histogram,int size){
    for(int i=0;i<size;i++){
        printf("Bucket %d has %d elements\n",i,histogram[i]);
    }
}


void init_relation(relation* rel,int num_tuples){
    rel->tuples = malloc(num_tuples * sizeof(tuple));
    rel->num_tuples = num_tuples;
}

//Initializes histogram values to 0
void init_array(int* histogram,int histogram_size,int value){
    for(int i=0;i < histogram_size ;i++){
        histogram[i]=value;
    }
}

// //partition the original relation in 2^n buckets 
// relation* partition_relation_old(relation rel,int n){

//     int prefix_sum[histogram_size];
//     int sum=0;
//     //calculate prefix sums for each bucket
//     for(int i=0;i<histogram_size;i++){
//         prefix_sum[i]=sum;
//         sum += histogram[i];       
//     }

//     relation ordered_rel;
//     init_relation(&ordered_rel,rel.num_tuples);
//     //printf("rel inited\n");
//     //a table to keep the last index of each bucket that an element was inserted in
//     int offsets[histogram_size];
//     init_histogram(offsets,histogram_size);
//     for(int i=0;i<rel.num_tuples;i++){
//         int bucket = hash1(rel.tuples[i].payload,n);
//         ordered_rel.tuples[prefix_sum[bucket] + offsets[bucket]]= rel.tuples[i];
//         offsets[bucket]++;
//     }

//     //check size of each partition to see if it fits in L2 Cache
//     for(int partition=0;partition<histogram_size;partition++){
//         if(histogram[partition]*sizeof(int)>L2_SIZE_BYTES){
//             //if a partition doesnt fit the L2 cache we need to split that partition into two
            
//             //depth table keeps how many bits we have used to hash the values in each partition
//             int depth_table[histogram_size];
//             for(int i=0;i<histogram_size;i++){
//                 depth_table[i]=n;
//             }
            
//             //----- init relation
//             relation test;
//             test.num_tuples=histogram[partition];
//             tuple* test_data = malloc((prefix_sum[partition+1]-prefix_sum[partition])*sizeof(tuple));
//             test.tuples=test_data;
//             //-----
//             for(int element=0;element<test.num_tuples;element++){
                
//             }
//         }
//     }



    
//     for(int i=0;i<ordered_rel.num_tuples;i++){
//       // printf("Element with payload %d and hashed value %d\n",ordered_rel.tuples[i].payload,hash1(ordered_rel.tuples[i].payload,n));
//     }
// }

//doubles histogram in size and copies over previous values
void expand_histogram(int** histogram,int* histogram_size){
    int* new_histogram = malloc(2*(*histogram_size)*sizeof(int));

    for(int i=0;i<2*(*histogram_size);i++){
        if(i< *histogram_size){
            new_histogram[i]=(*histogram)[i];
        }else{
            new_histogram[i]=0;
        }
    }
    *histogram_size = 2*(*histogram_size);
    //print_histogram(new_histogram,*histogram_size);
    free(*histogram);
    *histogram = new_histogram;
}

void expand_depth_table(int** depth_table,int* size){
    int *new_depth_table = malloc((*size)*sizeof(int));
    for(int i=0;i<(*size);i++){
        if(i<(*size)/2){
            new_depth_table[i]=(*depth_table)[i];
        }else{
            new_depth_table[i]= ((*size)>>2)+1;
        }
    }
    free(*depth_table);
    *depth_table = new_depth_table;
}



//this function checks if any partition exceeds the L2 cache size 
void repartition(relation* rel,int** histogram,int** depth_table,int* histogram_size,long  L2_SIZE_BYTES){
    for(int partition=0; partition< *histogram_size; partition++){
        if((*histogram)[partition]*sizeof(int)>L2_SIZE_BYTES){
          //  printf("-----REPARTITIONING partition:%d-----\n",partition);
           // printf("local depth: %d global depth: %d n_elements %d\n",(*depth_table)[partition],(*histogram_size)>>2,(*histogram)[partition]);
            if(((*depth_table)[partition]<<2) > *histogram_size){
            //    printf("Expasion needed local depth: %d global depth: %d\n",(*depth_table)[partition],(*histogram_size)>>2);
                expand_histogram(histogram,histogram_size);
                expand_depth_table(depth_table,histogram_size);
            }
            (*histogram)[partition] = 0;
            (*depth_table)[partition]++;

            //iterate over relation
            for(int element=0;element<rel->num_tuples;element++){
                //if element hashed into bucket that was full,rehash with greater depth
                //printf("prev hash:%d , partition %d, depth %d\n",hash1(rel->tuples[element].payload,(*depth_table)[partition]-1),partition,(*depth_table)[partition]-1);
                if(hash1(rel->tuples[element].payload,(*depth_table)[partition]-1)==partition){
                  //  printf("element with payload: %d depth %d hash value %d , now hashed to %d\n",rel->tuples[element].payload,(*depth_table)[partition]-1,hash1(rel->tuples[element].payload,(*depth_table)[partition]-1),hash1(rel->tuples[element].payload,(*depth_table)[partition]));
                    (*histogram)[hash1(rel->tuples[element].payload,(*depth_table)[partition])]++;
                } 
            }
          //  printf("-----------------------------------\n");
           // print_histogram(*histogram,*histogram_size);
            repartition(rel,histogram,depth_table,histogram_size,L2_SIZE_BYTES);
            break;
        }
    }
    return;
}


relation* partition_relation(relation rel,int n){
    //L2 cache size,in bytes,per core
    long L2_SIZE_BYTES = 256;//sysconf(_SC_LEVEL2_CACHE_SIZE);
    
    //Our histograms are int arrays where the index is the bucket hash value and the number pointed to
    //by the index is the number of occurences
    int histogram_size = 2<<(n-1);
    int* histogram = malloc(histogram_size*sizeof(int));
    
    //init histogram values to 0
    init_array(histogram,histogram_size,0);

    //depth table keeps how many bits we have used in hashing for each partition
    int* depth_table= malloc(histogram_size*sizeof(int));
    init_array(depth_table,histogram_size,n);
    

    //fill initial histogram by hashing (using n bits of the value) the payloads of the relation tuples and counting the results
    for(int i=0;i<rel.num_tuples;i++){
        histogram[hash1(rel.tuples[i].payload,n)]++;
    }
    printf("-----------pass 0 partition----------\n");
    print_histogram(histogram,histogram_size);
    repartition(&rel,&histogram,&depth_table,&histogram_size,L2_SIZE_BYTES);
    printf("----------pass n partition-----------\n");
    print_histogram(histogram,histogram_size);
    printf("---------------------\n");

    int sum=0;
    for(int i=0;i<histogram_size;i++){
        sum += histogram[i];
    }
    printf("%d\n",sum);
}
