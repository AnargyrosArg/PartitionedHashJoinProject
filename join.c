#include "join.h"

void joinfunction(relation r, relation s){

    //first we create partitions for relationship r
    partition_info info = partition_relations(r, s,2);
    partition_result partition_info = info.relA_info;
    partition_result partition_info2 = info.relB_info;

    for(int i=0;i<partition_info.histogram_size;i++){
        printf("partition %d begins at %d and has %d elements\n",i,partition_info.prefix_sum[i],partition_info.partition_sizes[i]);
    }

    printf("\n\n");

    //we do the same for relationship s
    for(int i=0;i<partition_info2.histogram_size;i++){
        printf("partition %d begins at %d and has %d elements\n",i,partition_info2.prefix_sum[i],partition_info2.partition_sizes[i]);
    }


    //we are going to compare the items in every partition
    int i=0;
    for(i=0;i<partition_info.histogram_size;i++){
        

        int bucketsizeA = partition_info.partition_sizes[i];//r.num_tuples - partition_info.prefix_sum[i];
        int bucketsizeB = partition_info2.partition_sizes[i];//s.num_tuples - partition_info2.prefix_sum[i];
        //skip empty buckets
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
        
        int stop = partition_info.prefix_sum[i] + partition_info.partition_sizes[i];

        //we start putting the j-th item of our ordered r, as the prefix sum shows
        for(int j = partition_info.prefix_sum[i]; j<stop; j++ ){
            int rel_tobeinserted = partition_info.ordered_rel.tuples[j].payload;
            tableR = insert_hashtable(tableR, rel_tobeinserted, partition_info.ordered_rel.tuples[j].key);
        }
        //print_hashtable(tableR);
        
        stop = partition_info2.prefix_sum[i] + partition_info2.partition_sizes[i];
       
        //now that our hashtable is ready, all we have to do is the join for every tuple in bucket in rel S
        for(int j = partition_info2.prefix_sum[i]; j<stop; j++){
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
                printf("%d %d\n",rowid_r+1,rowid_s+1);
            }
            printf("\n");
        }



        //at the end of each loop we have to free the memory of the hashtable
        delete_hashtable(tableR);
    }

    //we free all the memory at last
    delete_relation(partition_info.ordered_rel);
    delete_relation(partition_info2.ordered_rel);
    delete_part_info(info);
    return;

}