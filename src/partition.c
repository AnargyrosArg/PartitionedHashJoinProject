#include "partition.h"
/*
Source file containing logic for the partitioning the relations
*/

void* histogram_job(void* args);

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
void repartition(relation* rel,int** histogram,int *depth,int* histogram_size,long  L2_SIZE_BYTES,int max_passes,jobscheduler* scheduler){
    //init histogram values to 0
    init_array(*histogram,*histogram_size,0);
    //fill histogram by hashing (using depth bits of the value) the payloads of the relation tuples and counting the results
    // for(int i=0;i<rel->num_tuples;i++){
    //     (*histogram)[hash1(rel->tuples[i].payload,*depth)]++;
    // }

    pthread_mutex_t histogram_mutex;
    pthread_mutex_init(&histogram_mutex,NULL);
    pthread_cond_t histogram_cond;
    pthread_cond_init(&histogram_cond,NULL);
    int completed = 0;

    for(int i=0;i<N_WORKERS;i++){
        histogram_job_args* args = malloc(sizeof(histogram_job_args));
        args->histogram_size = *histogram_size;
        args->start = i * (rel->num_tuples / N_WORKERS);
        args->stop = (i+1) * (rel->num_tuples / N_WORKERS);
        if(i == N_WORKERS-1){
            args->stop = rel->num_tuples;
        }
        if(args->start-args->stop == 0){
            free(args);
            completed++;
            continue;
        }
        args->rel = rel;
        args->histogram = *histogram;
        args->depth = *depth;
        args->histogram_mutex = &histogram_mutex;
        args->histogram_cond = &histogram_cond;
        args->job_counter = &completed;
        job j = {HISTOGRAM_JOB ,histogram_job ,(void*)args};
        schedule_job(scheduler,j);
    }

    pthread_mutex_lock(&histogram_mutex);
    //while there are jobs being executed , wait
    while(completed < N_WORKERS){
        pthread_cond_wait(&histogram_cond,&histogram_mutex);
    }

    pthread_mutex_destroy(&histogram_mutex);
    pthread_cond_destroy(&histogram_cond);
    
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
            repartition(rel,histogram,depth,histogram_size,L2_SIZE_BYTES,max_passes-1,scheduler);
            break;
        }
    }
    return;
}



//returns an ordered version of relA and a prefix sum table
//IMPORTANT: remember to free ordered relation tuples
partition_result partition_relation_internal(relation rel,int depth,jobscheduler* scheduler){
    //L2 cache size,in bytes,per core
    long L2_SIZE_BYTES = sysconf(_SC_LEVEL2_CACHE_SIZE);//256;

    //Our histograms are int arrays where the index is the bucket hash value and the number pointed to
    //by the index is the number of occurences , size is 2^n
    int histogram_size = power(2,depth);
    int* histogram = malloc(histogram_size*sizeof(int));
    
    repartition(&rel,&histogram,&depth,&histogram_size,L2_SIZE_BYTES,MAX_PASSES,scheduler);
    
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


partition_info partition_relations(relation relA , relation relB, int depth , jobscheduler* scheduler){
    partition_info result;

    //partition first relation with given depth
    partition_result result1 = partition_relation_internal(relA,depth , scheduler);
    //partition second relation with same depth as first 
    partition_result result2 = partition_relation_internal(relB,result1.depth , scheduler);

    //if second relation requires more depth, repartition first relation with matching depth
    if(result2.depth > result1.depth){
        delete_part_result(result1);
        delete_relation(result1.ordered_rel);
        result1 = partition_relation_internal(relA,result2.depth , scheduler);
    }

    result.relA_info = result1;
    result.relB_info = result2;
    return result;
}



void* histogram_job(void* a){
    histogram_job_args* args = (histogram_job_args* ) a;
    int start = args->start;
    int stop = args->stop;
    int depth = args->depth;
    relation* rel = args->rel;
    int histogram_size = args->histogram_size;    
    

   // fprintf(stderr,"start = %d , stop = %d/%d \n",start,stop,rel->num_tuples);
    int temp_histogram[histogram_size];
    init_array(temp_histogram,histogram_size,0);
    for(int i=start;i<stop;i++){
        temp_histogram[hash1(rel->tuples[i].payload,depth)]++;
    }

    pthread_mutex_lock(args->histogram_mutex);
    for(int i=0;i<histogram_size;i++){
        args->histogram[i] += temp_histogram[i];
    }
    *(args->job_counter) = *(args->job_counter)+1;
    pthread_mutex_unlock(args->histogram_mutex);
    pthread_cond_broadcast(args->histogram_cond);
    free(args);
    return NULL;
} 