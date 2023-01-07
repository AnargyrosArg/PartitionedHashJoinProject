#include "filter.h"

void* filter_job(void* a);


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

void filter_intermediate(Intermediate* r,Intermediate** ret,int operation,int target,int relid,int colid,table* tabl,int actualid,jobscheduler* scheduler){

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
    // do two passes (could be done in one pass for optimization)
    // first pass to find length of relation
    // for (int i=0; i<r->rowids_count; i++) {
    //     int payload_tmp = tabl[actualid].table[colid][r->rowids[i][relid]];
    //     switch(operation) {
    //         case _EQUALS:            counter += (payload_tmp == target);     break;
    //         case _GREATER:           counter += (payload_tmp > target);      break;
    //         case _LESS:              counter += (payload_tmp < target);      break;
    //         case _GREATER_EQUALS:    counter += (payload_tmp >= target);     break;
    //         case _LESS_EQUALS:       counter += (payload_tmp <= target);     break;
    //     }
    // }
    // fprintf(stderr,"total %ld\n",counter);
    int completed =0;
    int* rets[MAX_FILTER_JOBS];
    int n_results[MAX_FILTER_JOBS] = {0};
    pthread_mutex_t filter_mutex;
    pthread_mutex_init(&filter_mutex,NULL);
    pthread_cond_t filter_cond;
    pthread_cond_init(&filter_cond,NULL);

    for(int i=0;i< MAX_FILTER_JOBS;i++){
        filter_job_args* args = malloc(sizeof(filter_job_args));
        args->n_results = &(n_results[i]);
        args->completed = &completed;
        args->filter_cond = &filter_cond;
        args->filter_mutex = &filter_mutex;
        args->actualid = actualid;
        args->tabl = tabl;
        args->colid = colid;
        args->relid = relid;
        args->r = r;
        args->target = target;
        args->operation = operation;
        args->start = i * (r->rowids_count / MAX_FILTER_JOBS);
        args->stop = (i+1) * (r->rowids_count / MAX_FILTER_JOBS);
        if(i == MAX_FILTER_JOBS-1){
            args->stop = r->rowids_count;
        }
        if(args->start-args->stop == 0){
            free(args);
            completed++;
            continue;
        }
        rets[i] = malloc((args->stop-args->start)*sizeof(int));
        args->ret = rets[i];
        job j = {FILTER_JOB , filter_job , (void*)args};
        schedule_job(scheduler,j);
    }

    pthread_mutex_lock(&filter_mutex);
    while(completed < MAX_FILTER_JOBS){
        pthread_cond_wait(&filter_cond,&filter_mutex);
    }
    pthread_mutex_destroy(&filter_mutex);
    pthread_cond_destroy(&filter_cond);

    for(int i=0;i<MAX_FILTER_JOBS;i++){
        counter += n_results[i];
    }
    //create intermediate result
    (*ret) = malloc(sizeof(Intermediate));
    init_intermediate((*ret));
    set_intermediate((*ret),counter,r->valid_rels);

    counter = 0;
    for(int i=0;i<MAX_FILTER_JOBS;i++){
        for(int j=0;j<n_results[i];j++){
            (*ret)->rowids[counter][relid] = rets[i][j];
            counter++;
        }
        free(rets[i]);
    }
}


void* filter_job(void* a){
    filter_job_args* args = (filter_job_args*) a;
    int operation = args->operation;
    int target = args->target;
    Intermediate* r = args->r;
    int start = args->start;
    int stop = args->stop;
    int actualid = args->actualid;
    int colid = args->colid;
    int relid = args->relid;
    table* tabl= args->tabl;
   // args->ret = malloc((stop-start)*sizeof(int));

    int counter = 0;
    for (int i=start; i<stop; i++) {
        int payload_tmp = tabl[actualid].table[colid][r->rowids[i][relid]];
        switch(operation) {
            case _EQUALS:            
                if(payload_tmp == target){
                    args->ret[counter] = r->rowids[i][relid];
                    counter++;
                }
                break;
            case _GREATER:            
                if(payload_tmp > target){
                    args->ret[counter] = r->rowids[i][relid];
                    counter++;
                }
                break;
            case _LESS:            
                if(payload_tmp < target){
                    args->ret[counter] = r->rowids[i][relid];
                    counter++;
                }
                break;
            case _GREATER_EQUALS:            
                if(payload_tmp >= target){
                    args->ret[counter] = r->rowids[i][relid];
                    counter++;
                }
                break;
            case _LESS_EQUALS:            
                if(payload_tmp <= target){
                    args->ret[counter] = r->rowids[i][relid];
                    counter++;
                }
                break;
        }
    }

    *(args->n_results) = counter; 

    pthread_mutex_lock(args->filter_mutex);
    *(args->completed) = *(args->completed)+1;
    pthread_mutex_unlock(args->filter_mutex);
    pthread_cond_broadcast(args->filter_cond);
    free(args);
    return NULL;
}