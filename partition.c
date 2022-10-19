#include "partition.h"
/*
Source file containing logic for the partitioning the relations
*/

//print histogram for debug purposes
void print_histogram(int* histogram,int size,int* depths){
    for(int i=0;i<size;i++){
        printf("Partition %d has %d elements %d depth, size %d\n",i,histogram[i],depths[i],size);
    }
}


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
    free(*histogram);
    *histogram = new_histogram;
}

void expand_depth_table(int** depth_table,int* size){
    int *new_depth_table = malloc((*size)*sizeof(int));
    for(int i=0;i<(*size);i++){
        if(i<(*size)/2){
            new_depth_table[i]=(*depth_table)[i];
        }else{
            new_depth_table[i]= pseudo_log2((*size));
        }
    }
    free(*depth_table);
    *depth_table = new_depth_table;
}



//this function checks if any partition exceeds the L2 cache size and splits it accordingly into smaller ones
void repartition(relation* rel,int** histogram,int** depth_table,int* histogram_size,int** partition_map,long  L2_SIZE_BYTES){
    //iterate over partitions
    for(int partition=0; partition< *histogram_size; partition++){
        //check is size smaller than L2 cache
        if((*histogram)[partition]*sizeof(int)>L2_SIZE_BYTES){
            //check if we need to increase global depth
            if(  (*depth_table)[partition] >= pseudo_log2((*histogram_size))){
                expand_histogram(histogram,histogram_size);
                expand_depth_table(depth_table,histogram_size);
            }
            (*histogram)[partition] = 0;
            (*depth_table)[partition]++;

            //iterate over relation
            for(int element=0;element<rel->num_tuples;element++){
                //if element hashed into bucket that was full,rehash with greater depth
                if(hash1(rel->tuples[element].payload,(*depth_table)[partition]-1)==partition){
                    (*histogram)[hash1(rel->tuples[element].payload,(*depth_table)[partition])]++;
                    (*partition_map)[element]=hash1(rel->tuples[element].payload,(*depth_table)[partition]);
                } 
            }

            //recursive call
            repartition(rel,histogram,depth_table,histogram_size,partition_map,L2_SIZE_BYTES);
            break;
        }
    }
    return;
}



void partition_relation(relation rel,int n){
    //L2 cache size,in bytes,per core
    long L2_SIZE_BYTES = 256;//sysconf(_SC_LEVEL2_CACHE_SIZE);
    
    //Our histograms are int arrays where the index is the bucket hash value and the number pointed to
    //by the index is the number of occurences , size is 2^n
    int histogram_size = power(2,n);
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

    //this maps elements of the relation to the partition they belong to
    int* partition_map = malloc(rel.num_tuples*sizeof(int));
    //it is initialized with the default depth hash values of each element and corrected inside the repartition function
    //if we expand
    for(int i=0;i<rel.num_tuples;i++){
        partition_map[i] = hash1(rel.tuples[i].payload,n);
    }

    repartition(&rel,&histogram,&depth_table,&histogram_size,&partition_map,L2_SIZE_BYTES);
    
    int prefix_sum[histogram_size];
    int sum=0;
    //calculate prefix sums for each bucket
    for(int i=0;i<histogram_size;i++){
        prefix_sum[i]=sum;
        sum += histogram[i];       
    }

    //allocate room for ordered relation
    relation ordered_rel;
    init_relation(&ordered_rel,rel.num_tuples);

    //a table to keep the last index of each bucket that an element was inserted in
    int offsets[histogram_size];
    init_array(offsets,histogram_size,0);
    for(int i=0;i<rel.num_tuples;i++){
        int bucket = partition_map[i];
        ordered_rel.tuples[prefix_sum[bucket] + offsets[bucket]]= rel.tuples[i];
        offsets[bucket]++;
    }
    
    print_histogram(histogram,histogram_size,depth_table);
    printf("------------------------------------------------\n");
    for(int i=0;i<histogram_size;i++){
        printf("Partition %d begins at index: %d\n",i,prefix_sum[i]);
    }
    //TODO
    // return ordered relation and the prefix sum array
}



