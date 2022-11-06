#include "join.h"

void joinfunction(relation r, relation s){

    //first we create partitions for relationship r
    partition_result partition_info = partition_relation(r, 2);
    for(int i=0;i<partition_info.histogram_size;i++){
        printf("partition %d begins at %d\n",i,partition_info.prefix_sum[i]);
    }

    printf("\n\n");

    //we do the same for relationship s
    partition_result partition_info2 = partition_relation(s, 2);
    for(int i=0;i<partition_info2.histogram_size;i++){
        printf("partition %d begins at %d\n",i,partition_info2.prefix_sum[i]);
    }


    //we are going to compare the items in every partition
    int i=0;
    for(i=0;i<partition_info.histogram_size;i++){

        int bucketsizeA = r.num_tuples - partition_info.prefix_sum[i];
        int bucketsizeB = s.num_tuples - partition_info2.prefix_sum[i];

        //first we have to check if bucket is empty on one or both of relationships, if yes there is nothing to compare and we go to the next one
        //if we are at the last bucket the number of items inside the bucket is equal to the number of tuples minus the prefix sum[i]
        if( i < (partition_info.histogram_size -1)){
            bucketsizeA = partition_info.prefix_sum[i+1] - partition_info.prefix_sum[i];
            bucketsizeB = partition_info2.prefix_sum[i+1] - partition_info2.prefix_sum[i];
        }
            
       // printf("%d %d\n", bucketsizeA ,bucketsizeB);

        if(bucketsizeA == 0 || bucketsizeB == 0 ){
            //printf("empty bucket! Continuing...\n");
            continue;
        }

        //now we have to create hashtable for the i-th partition of r
        //size of neighbour starts at 8
        //must free memory at the end 
        int H = 8;
        hashtable *tableR = init_hashtable(bucketsizeA,H);
        //print_hashtable(tableR);

        //now that we created our hashtable we want to insert every tuple of the partition into the hashtable 
        int j;
        int stop = partition_info.prefix_sum[i+1];
        //if we are at the last bucket loop will stop at the end of the array
        if( i == (partition_info.histogram_size -1)){
            stop = r.num_tuples;
        }

        //we start putting the j-th item of our ordered r, as the prefix sum shows
        for( j = partition_info.prefix_sum[i]; j<stop; j++ ){
            int rel_tobeinserted = partition_info.ordered_rel.tuples[j].payload;
            tableR = insert_hashtable(tableR, rel_tobeinserted, partition_info.ordered_rel.tuples[j].key);
        }
        //print_hashtable(tableR);


        //if we are at the last bucket loop will stop at the end of the array
        if( i == (partition_info2.histogram_size -1)){
            stop = s.num_tuples;
        }
        //now that our hashtable is ready, all we have to do is the join for every tuple in bucket in rel S
        for(j = partition_info2.prefix_sum[i]; j<stop; j++){
            int data = partition_info2.ordered_rel.tuples[j].payload, size = 0;
            int* p;

            p = search_hashtable(tableR, data, &size);
            
            if(p == NULL)
            {
                continue;
            }

            //printf("for payload:%d\n", data);
            for(int k=0; k<size; k++) {
                int rowid_s = partition_info2.ordered_rel.tuples[j].key;
                int rowid_r = r.tuples[p[k]].key;
                printf("%d %d\n", rowid_s,rowid_r);
            }
            printf("\n");
        }



        //at the end of each loop we have to free the memory of the hashtable
        delete_hashtable(tableR);
    }

    //we free all the memory at last
    delete_relation(partition_info.ordered_rel);
    delete_relation(partition_info2.ordered_rel);
    free(partition_info.prefix_sum);
    free(partition_info2.prefix_sum);
    return;

    
}