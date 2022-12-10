#include "execqueries.h" 


//function that prints the sum of one projection
void printsum(int rel, int column, Intermediates* inter, table *tabl,int actualid){
    uint64_t sum =0;
    //first we need to get the relation from the intermediates
    Intermediate* intermediate;

    get_intermediates(inter,rel,actualid,&intermediate,tabl);
    //now for every id in the rowidarray we need to get the value of the column
    int i=0;
    for(i=0;i<intermediate->rowids_count;i++){
        uint rowid = intermediate->rowids[i][rel];
        uint64_t value = tabl->table[column][rowid];
        sum += value;
        //printf("value %d is %lu ",i,sum);
    }

    //if the sum is equal to 0 then we print NULL
    if(sum == 0){
        printf("NULL");

    }
    else{
        printf("%lu",sum);
    }

    return;
}


void selfjoin(int rel1,int rel2,uint col1,uint col2,Intermediate* inter,table* tabl,QueryInfo* query,Intermediates* intermediates,Intermediate** res){
        //we have to get the actual realtion from the table, remember that the int rel is the id of the relation in the query, not the actual id of the relation in the table
        int actualid1 = query->rel_ids[rel1];
        int actualid2 = query->rel_ids[rel2];
        result joinres;
        init_result(&joinres);
        for(int i=0;i<inter->rowids_count;i++){
            if(tabl[actualid1].table[col1][inter->rowids[i][rel1]] == tabl[actualid2].table[col2][inter->rowids[i][rel2]]){
                pair temp;
                temp.key1 = inter->rowids[i][rel1];
                temp.key2 = inter->rowids[i][rel2];
                temp.payload = tabl[actualid1].table[col1][inter->rowids[i][rel1]];
                add_result(&joinres,temp);
            }
        }

        init_intermediate(*res);
        set_intermediate(*res,joinres.result_size,inter->valid_rels);
        int counter=0;
        for(int i=0;i<inter->rowids_count;i++){
            if(inter->rowids[i][rel1] == joinres.pairs[counter].key1 && inter->rowids[i][rel2] == joinres.pairs[counter].key2){
                for(int k=0;k<MAX_RELS_PER_QUERY;k++){
                    (*res)->rowids[counter][k] = inter->rowids[i][k];
                }
                counter++;
            }
        }
        //we free the memory of relations each time
        delete_result(&joinres);
}

void exec_query(QueryInfo *query, table* tabl){
    FilterInfo* current_filter = query->filters;
    JoinInfo* current_join = query->joins;
    SelectionInfo* current_proj = query->projections;
    // debug tools
    int join_counter = 0;

    //we create the intermediate struct that we will use to store the results
    Intermediates* intermediates = init_intermediates();

    //we first need to filter the relations
    //we pass all the filter list till the end
    while(current_filter!=NULL){
        int rel = current_filter->sel.rel_id;
        int col = current_filter->sel.col_id;
        int op = current_filter->type;
        long long value = current_filter->constant;
        //we have to get the actual realtion from the table, remember that the int rel is the id of the relation in the query, not the actual id of the relation in the table
        int actualid = query->rel_ids[rel];
        
        Intermediate* intermediate;
        get_intermediates(intermediates,rel,actualid,&intermediate,tabl);
        
        Intermediate* filter_result;
        filter_intermediate(intermediate,&filter_result,op,value,rel,col,tabl,actualid);
        remove_intermediate(intermediate,intermediates);
        insert_intermediate(filter_result,intermediates);
        free(filter_result);
        current_filter = current_filter->next;
    }

    //now we continue with the joins
    //we pass all the join list till the end
    while(current_join !=NULL){
        int rel1 = current_join->left.rel_id;
        int rel2 = current_join->right.rel_id;
        int col1 = current_join->left.col_id;
        int col2 = current_join->right.col_id;
        int actualid1 = query->rel_ids[rel1];
        int actualid2 = query->rel_ids[rel2];

     
        //now we create the relations that will be joined
        //first we check if they exist in the intermediates
        Intermediate* inter1;
        Intermediate* inter2;

        int index;
        if((index = in_same_intermediate_relation(intermediates,rel1,rel2)) != -1){
            //get common intermediate , and perform join as a filter on it
            inter1 = &(intermediates->intermediates[index]);            
            Intermediate* selfjoin_result;
            selfjoin(rel1,rel2,col1,col2,inter1,tabl,query,intermediates,&selfjoin_result);
            remove_intermediate(inter1,intermediates);
            insert_intermediate(selfjoin_result,intermediates);
            free(selfjoin_result);
        }else{
            get_intermediates(intermediates,rel1,actualid1,&inter1,tabl);
            get_intermediates(intermediates,rel2,actualid2,&inter2,tabl);
            //execute join operation normally
            Intermediate* joinres = join_intermediates(inter1,inter2,query,rel1,col1,rel2,col2,tabl);
            remove_intermediate(inter1,intermediates);
            remove_intermediate(inter2,intermediates);
            insert_intermediate(joinres,intermediates);
            free(joinres);
       }
       

        //we coninue to the next join
        current_join = current_join->next;
        join_counter++;

        
    }

    //now that we finished with the joins and the filter all we have to do is do the projections and print the sum
    //we pass all the projection list till the end
    while(current_proj != NULL){
        int projrel = current_proj->rel_id;
        int projcol = current_proj->col_id;
        //we get the actual relation from the table
        int actualid = query->rel_ids[projrel];
        //now we just run the sum function for every projection
        printsum(projrel, projcol, intermediates,&tabl[actualid],actualid);
        if(current_proj->next!=NULL){
            printf(" ");
        }
        //we move on to the next projection
        current_proj = current_proj->next;
    }
    printf("\n");
    fflush(stdout);
    delete_intermediates(intermediates);
    return;
}

//function that executes all the queries
void exec_all_queries(QueryInfo *queries,table *tabl,uint num_queries){

    for (int i=0;i<num_queries;i++){
        exec_query(&queries[i],tabl);
    }
    return;
}