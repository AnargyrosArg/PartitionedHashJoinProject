#include "intermediates.h"

Intermediates* init_intermediates() {
    Intermediates* inter_array = malloc(sizeof(Intermediates));
    inter_array->count = 0;
    inter_array->capacity = START_N_INTERMEDIATES;
    inter_array->intermediates = malloc(START_N_INTERMEDIATES * sizeof(Intermediate*));
    for(int i=0;i<START_N_INTERMEDIATES;i++){
        inter_array->intermediates[i]= NULL;
    }
    return inter_array;
}

void delete_intermediates(Intermediates* inter_array) {
    for(int i=0;i<inter_array->capacity;i++){
        if(inter_array->intermediates[i]!=NULL){
            delete_intermediate(inter_array->intermediates[i]);
            free(inter_array->intermediates[i]);
        } 
    }
    free(inter_array->intermediates);
    free(inter_array);
    return;
}

void init_intermediate(Intermediate* intermediate){
    for(int k =0;k<MAX_RELS_PER_QUERY;k++){
        intermediate->valid_rels[k] = false;
    }    
    intermediate->rowids_count =0;
    intermediate->rowids = NULL;
    return;
}

void set_intermediate(Intermediate* inter,int rowid_count,bool rels[MAX_RELS_PER_QUERY]){
    for(int i=0;i<MAX_RELS_PER_QUERY;i++){
        inter->valid_rels[i]=rels[i];
    }
    inter->rowids_count = rowid_count;
    inter->rowids = malloc(rowid_count*sizeof(int*));
    for(int i=0;i<rowid_count;i++){
        inter->rowids[i]=malloc(sizeof(int) * MAX_RELS_PER_QUERY);
    }
}

void delete_intermediate(Intermediate* inter){
    for(int i=0;i<inter->rowids_count;i++){
        free(inter->rowids[i]);
    }
    free(inter->rowids);
}

//creates a relation from given intermediate, but this time the keys in the relation correspond to intermediate indexes , not actual relation keys
relation intermediate_to_relation(Intermediate* inter,int rel , int col,table* tabl,QueryInfo* query){
    if(!inter->valid_rels[rel]){
        fprintf(stderr,"Invalid intermediate to instantiate relation\n");
        exit(-1);
    }
    int actualid1 = query->rel_ids[rel];

    relation result;
    init_relation(&result,inter->rowids_count);

    for(int i=0;i<inter->rowids_count;i++){
        tuple temp;
        temp.key = i;
        temp.payload = tabl[actualid1].table[col][inter->rowids[i][rel]];
        result.tuples[i] = temp;
    }
    return result;
}


void get_intermediates(Intermediates* intermediates, uint relation_index,int actualid, Intermediate** ret,table* tabl) {
    for (int i=0; i<intermediates->count; i++){
        if (intermediates->intermediates[i]!=NULL && intermediates->intermediates[i]->valid_rels[relation_index]) {
            (*ret) = intermediates->intermediates[i];
            return;
        }
    } 
    //if the relation is not in the intermediates create intermediate from relation and return it;
    *ret = malloc(sizeof(Intermediate));    
    relation_to_intermediate(tabl,relation_index,actualid,ret);
    insert_intermediate(*ret,intermediates);
}
 

void relation_to_intermediate(table* tabl,int rel,int actualid,Intermediate** result){
    bool valid_ids[MAX_RELS_PER_QUERY];
    for(int i=0;i<MAX_RELS_PER_QUERY;i++){
        valid_ids[i]=(i == rel);
    }
    init_intermediate(*result);

    set_intermediate(*result,tabl[actualid].num_tuples,valid_ids);
    for(int i=0;i<tabl[actualid].num_tuples;i++){
        //store rowid of rels into intermediate
        (*result)->rowids[i][rel] = i;
    }
    return;
}


int in_same_intermediate_relation(Intermediates* inter ,int rel1 , int rel2,Intermediate** ret){
    for(int i=0;i<inter->count;i++){
        if(inter->intermediates[i]!=NULL && inter->intermediates[i]->valid_rels[rel1] && inter->intermediates[i]->valid_rels[rel2]){
            *ret = inter->intermediates[i];
            return i;
        }
    }
    *ret =NULL;
    return -1;
}


void insert_intermediate(Intermediate* res,Intermediates* intermediates){
    for(int i=0;i<intermediates->capacity;i++){
        if(intermediates->intermediates[i]==NULL){
            intermediates->intermediates[i] = res;
            intermediates->count++;
            return;
        }
    }
    fprintf(stderr,"could not insert intermediate\n");
    exit(-1);
}

void remove_intermediate(Intermediate* res,Intermediates* intermediates){
    for(int i=0;i<intermediates->capacity;i++){
        if(intermediates->intermediates[i]!=NULL && intermediates->intermediates[i]->rowids == res->rowids){
            delete_intermediate(intermediates->intermediates[i]);
            free(intermediates->intermediates[i]);
            intermediates->intermediates[i] = NULL;
            intermediates->count--;
            return;
        }
    }
    fprintf(stderr,"could not remove intermediate\n");
    exit(-1);
}