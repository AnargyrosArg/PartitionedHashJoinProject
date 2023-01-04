#include "join.h"


result joinfunction(relation r, relation s){
    result ret;
    init_result(&ret);

    //first we create partitions for relationship r,s
    partition_info info = partition_relations(r, s , 2);
    partition_result partition_info = info.relA_info;
    partition_result partition_info2 = info.relB_info;

    // for(int i=0;i<partition_info.histogram_size;i++){
    //     printf("partition %d begins at %d and has %d elements\n",i,partition_info.prefix_sum[i],partition_info.partition_sizes[i]);
    // }
    // printf("\n\n");
    // for(int i=0;i<partition_info2.histogram_size;i++){
    //     printf("partition %d begins at %d and has %d elements\n",i,partition_info2.prefix_sum[i],partition_info2.partition_sizes[i]);
    // }

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
        hashtable *tableR = init_hashtable(bucketsizeA,H,hash2);
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
                pair pair;

                pair.key2 = partition_info2.ordered_rel.tuples[j].key;
                pair.key1 = p[k];
                pair.payload = r.tuples[p[k]].payload;
                add_result(&ret,pair);
            }
        }



        //at the end of each loop we have to free the memory of the hashtable
        delete_hashtable(tableR);
    }

    //we free all the memory at last
    delete_relation(partition_info.ordered_rel);
    delete_relation(partition_info2.ordered_rel);
    delete_part_info(info);

    return ret;

}




Intermediate* join_intermediates(Intermediate* inter1,Intermediate* inter2,QueryInfo* query,int rel1,int col1,int rel2,int col2,table* tabl){
    relation r = intermediate_to_relation(inter1,rel1,col1,tabl,query);
    relation s = intermediate_to_relation(inter2,rel2,col2,tabl,query);
    result ret;
    init_result(&ret);
    
    //first we create partitions for relationship r,s
    partition_info info = partition_relations(r, s , 2);
    partition_result partition_info = info.relA_info;
    partition_result partition_info2 = info.relB_info;


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
        hashtable *tableR = init_hashtable(bucketsizeA,H,hash2);
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
                pair pair;
                pair.key2 = partition_info2.ordered_rel.tuples[j].key;
                pair.key1 = p[k];
                pair.payload = r.tuples[p[k]].payload;
                add_result(&ret,pair);
            }
        }



        //at the end of each loop we have to free the memory of the hashtable
        delete_hashtable(tableR);
    }

    //we free all the memory at last
    delete_relation(partition_info.ordered_rel);
    delete_relation(partition_info2.ordered_rel);
    delete_part_info(info);

    //in this case our result is the pairs of rowids in the intermediate relation!
    //we can construct a new intermediate result in O(n) time
    Intermediate* final = malloc(sizeof(Intermediate));
    init_intermediate(final);
    bool valid_rels[MAX_RELS_PER_QUERY];
    
    //new valid rels is union of 2 prev 
    for(int i=0;i<MAX_RELS_PER_QUERY;i++){
        valid_rels[i] = (inter1->valid_rels[i] || inter2->valid_rels[i]);
    }
    //prepare intermediate for loading
    set_intermediate(final,ret.result_size,valid_rels);
    
    //load data into intermediate
    for(int i=0;i<ret.result_size;i++){
        for(int k=0;k<MAX_RELS_PER_QUERY;k++){
            if(inter1->valid_rels[k]){
                final->rowids[i][k] = inter1->rowids[ret.pairs[i].key1][k];
            }
            if(inter2->valid_rels[k]){
                final->rowids[i][k] = inter2->rowids[ret.pairs[i].key2][k];
            }
            if(inter2->valid_rels[k] && inter1->valid_rels[k]){
                //this should never happen , if both relations are valid at an intermediate a selfjoin should have been used instead
                fprintf(stderr,"Stinky situation!\n");
            }
        }
    }

    delete_result(&ret);
    delete_relation(r);
    delete_relation(s);
    return final;
}