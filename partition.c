#include "partition.h"
/*
Source file containing logic for the partitioning the relations
*/

//print histogram for debug purposes
void print_histogram(int* histogram,int size){
    for(int i=0;i<size;i++){
        printf("Partition %d has %d elements, size %d\n",i,histogram[i],size);
    }
}



//frees memory for partition_results , does not free the relation
void delete_part_result(partition_result result){
    free(result.partition_sizes);
    free(result.prefix_sum);
}

void delete_part_info(partition_info info){
    delete_part_result(info.relA_info);
    delete_part_result(info.relB_info);
}



//doubles histogram in size and copies over previous values
void expand_histogram(int** histogram,int* histogram_size){
    int* new_histogram = malloc(2*(*histogram_size)*sizeof(int));

    // for(int i=0;i<2*(*histogram_size);i++){
    //     if(i< *histogram_size){
    //         new_histogram[i]=(*histogram)[i];
    //     }else{
    //         new_histogram[i]=0;
    //     }
    // }
    *histogram_size = 2*(*histogram_size);
    free(*histogram);
    *histogram = new_histogram;
}

//this function checks if any partition exceeds the L2 cache size and splits it accordingly into smaller ones
void repartition(relation* rel,int** histogram,int *depth,int* histogram_size,long  L2_SIZE_BYTES,int max_passes){
    //init histogram values to 0
    init_array(*histogram,*histogram_size,0);

    //fill histogram by hashing (using depth bits of the value) the payloads of the relation tuples and counting the results
    for(int i=0;i<rel->num_tuples;i++){
        (*histogram)[hash1(rel->tuples[i].payload,*depth)]++;
    }

    //iterate over partitions
    for(int partition=0; partition< *histogram_size; partition++){
        //check is size larger than L2 cache
        if((*histogram)[partition]*sizeof(int)>L2_SIZE_BYTES){
            expand_histogram(histogram,histogram_size);       
            
            //increment depth
            (*depth)++;
            if(max_passes==0){
                printf("ERROR: Maximum amount of partition passes reached, relation probably does not fit in L2 cache!\n");
                exit(-1);
            }
            //repartition with incremented depth
            repartition(rel,histogram,depth,histogram_size,L2_SIZE_BYTES,max_passes-1);
            break;
        }
    }
    return;
}



//returns an ordered version of relA and a prefix sum table
//IMPORTANT: remember to free ordered relation tuples
partition_result partition_relation_internal(relation rel,int depth){
    //L2 cache size,in bytes,per core
    long L2_SIZE_BYTES = 256;//sysconf(_SC_LEVEL2_CACHE_SIZE);

    //Our histograms are int arrays where the index is the bucket hash value and the number pointed to
    //by the index is the number of occurences , size is 2^n
    int histogram_size = power(2,depth);
    int* histogram = malloc(histogram_size*sizeof(int));
    
    repartition(&rel,&histogram,&depth,&histogram_size,L2_SIZE_BYTES,MAX_PASSES);
    
    int *prefix_sum = malloc(histogram_size * sizeof(int));
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
        int bucket = hash1(rel.tuples[i].payload,depth);
        ordered_rel.tuples[prefix_sum[bucket] + offsets[bucket]]= rel.tuples[i];
        offsets[bucket]++;
    }
    
    partition_result result;
    result.ordered_rel = ordered_rel;
    result.prefix_sum=prefix_sum;
    result.partition_sizes = histogram;
    result.histogram_size = histogram_size;
    result.depth= depth;
    return result;
}


partition_info partition_relations(relation relA , relation relB, int depth){
    partition_info result;

    //partition first relation with given depth
    partition_result result1 = partition_relation_internal(relA,depth);
    //partition second relation with same depth as first 
    partition_result result2 = partition_relation_internal(relB,result1.depth);

    //if second relation requires more depth, repartition first relation with matching depth
    if(result2.depth > result1.depth){
        delete_part_result(result1);
        delete_relation(result1.ordered_rel);
        result1 = partition_relation_internal(relA,result2.depth);
    }

    result.relA_info = result1;
    result.relB_info = result2;
    return result;
}
