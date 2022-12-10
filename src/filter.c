#include "filter.h"




// r is the relation to be filtered, resulting relation will be placed in ret
void filter_function(relation* r, relation* ret, int operation, int target) {
    size_t counter = 0;

    // input check
    if (operation < _EQUALS && operation > _LESS_EQUALS) {
        fprintf(stderr, "filter_function: Invalid operation.\n");
        return;
    }

    // do two passes (could be done in one pass for optimization)
    // first pass to find length of relation
    for (int i=0; i<r->num_tuples; i++) {
        int payload_tmp = r->tuples[i].payload;
        switch(operation) {
            case _EQUALS:            counter += (payload_tmp == target);     break;
            case _GREATER:           counter += (payload_tmp > target);      break;
            case _LESS:              counter += (payload_tmp < target);      break;
            case _GREATER_EQUALS:    counter += (payload_tmp >= target);     break;
            case _LESS_EQUALS:       counter += (payload_tmp <= target);     break;
        }
    }

    // allocate memory for tuples
    ret->num_tuples = counter;
    tuple* tuples = malloc(counter * sizeof(tuple));
    counter = 0;

    // second pass to construct resulting relation
    for (int i=0; i<r->num_tuples; i++) {
        int payload_tmp = r->tuples[i].payload;
        if ((operation == _EQUALS && payload_tmp == target)
            || (operation == _GREATER && payload_tmp > target)
            || (operation == _LESS && payload_tmp < target)
            || (operation == _GREATER_EQUALS && payload_tmp >= target)
            || (operation == _LESS_EQUALS && payload_tmp <= target)) {
                tuples[counter].key = r->tuples[i].key;
                tuples[counter].payload = payload_tmp;
                counter++;
        }
    }
    ret->tuples = tuples;
}


//better filter it uses list so we dont have to do 2 passes to the main table, i suppose it is faster than the first one
void better_filter_function(relation* r, relation* ret, int operation, int target) {
    size_t counter = 0;

    // input check
    if (operation < _EQUALS && operation > _LESS_EQUALS) {
        fprintf(stderr, "filter_function: Invalid operation.\n");
        return;
    }

    //we initialize the list
    list *l = init_list();

    for (int i=0; i<r->num_tuples; i++) {
        int payload_tmp = r->tuples[i].payload;
        int key_tmp = r->tuples[i].key;
        switch(operation) {
            case _EQUALS:        
                if(payload_tmp == target){
                    list_append(l, key_tmp, payload_tmp);
                    counter++;
                }      
            break;
            case _GREATER:
                if(payload_tmp > target){
                    list_append(l, key_tmp, payload_tmp);
                    counter++;
                }      
            break;
            case _LESS:  
                if(payload_tmp < target){
                    list_append(l, key_tmp, payload_tmp);
                    counter++;
                }      
                break;
            case _GREATER_EQUALS:
                if(payload_tmp >= target){
                    list_append(l, key_tmp, payload_tmp);
                    counter++;
                }      
                break;
            case _LESS_EQUALS:
                if(payload_tmp <= target){
                    list_append(l, key_tmp, payload_tmp);
                    counter++;
                }      
                break;
        }
    }


    // allocate memory for tuples
    ret->num_tuples = counter;
    tuple* tuples = malloc(counter * sizeof(tuple));

    list *curr=l;
    for(size_t j=0;j<counter;j++){
        if(curr->row_id ==-1){
            printf("error");
        }
        tuples[j].key = curr->row_id;
        tuples[j].payload = curr->payload;
        curr=curr->next;
    }
    ret->tuples = tuples;
    delete_list(l);
    return;
}



//basic list functions
list *init_list(){
    list *l = (list*)malloc(sizeof(list));
    l->row_id = -1;
    l->payload = -1;
    l->next = NULL;
    l->tail = l;
    return l;
}

void list_append(list *l, size_t value,size_t pd){
    //first we check if the list is empty
    if(l->row_id == -1){
        l->row_id = value;
        l->payload = pd;
    }
    else if(l->next == NULL){
        list *current= (list*)malloc(sizeof(list));
        current->row_id = value;
        current->payload = pd;
        current->next = NULL;
        l->next = current;
        l->tail = current;
        current->tail = current;
    }
    else{
        list *current= (list*)malloc(sizeof(list));
        current->row_id = value;
        current->payload = pd;
        current->next = NULL;
        l->tail->next = current;
        l->tail = current;
        current->tail = current;
    }
}

void delete_list(list *l){
    list *current = l;
    list *next;
    while(current != NULL){
        next = current->next;
        free(current);
        current = next;
    }
}

void print_list(list *l){
    list *current = l;
    while(current != NULL && current->row_id != -1){
        printf("%d ", current->row_id);
        printf("%d ", current->payload);
        current = current->next;
    }
    printf("\n");
}

void filter_intermediate(Intermediate* r,Intermediate** ret,int operation,int target,int relid,int colid,table* tabl,int actualid){

 size_t counter = 0;

    // input check
    if (operation < _EQUALS && operation > _LESS_EQUALS) {
        fprintf(stderr, "filter_function: Invalid operation.\n");
        exit(-1);
    }
    if(!r->valid_rels[relid]){
        fprintf(stderr, "filter: given relation not valid in intermediate.\n");
        exit(-1);
    }
    fflush(stderr);
    // do two passes (could be done in one pass for optimization)
    // first pass to find length of relation
    for (int i=0; i<r->rowids_count; i++) {
        int payload_tmp = tabl[actualid].table[colid][r->rowids[i][relid]];
        switch(operation) {
            case _EQUALS:            counter += (payload_tmp == target);     break;
            case _GREATER:           counter += (payload_tmp > target);      break;
            case _LESS:              counter += (payload_tmp < target);      break;
            case _GREATER_EQUALS:    counter += (payload_tmp >= target);     break;
            case _LESS_EQUALS:       counter += (payload_tmp <= target);     break;
        }
    }

    //create intermediate result
    (*ret) = malloc(sizeof(Intermediate));
    init_intermediate((*ret));
    set_intermediate((*ret),counter,r->valid_rels);
    
    counter = 0;
    
    // second pass to construct resulting relation
    for (int i=0; i<r->rowids_count; i++) {
        int payload_tmp = tabl[actualid].table[colid][r->rowids[i][relid]];
        if ((operation == _EQUALS && payload_tmp == target)
            || (operation == _GREATER && payload_tmp > target)
            || (operation == _LESS && payload_tmp < target)
            || (operation == _GREATER_EQUALS && payload_tmp >= target)
            || (operation == _LESS_EQUALS && payload_tmp <= target)) {
                (*ret)->rowids[counter][relid] = r->rowids[i][relid];
                counter++;
        }
    }
}