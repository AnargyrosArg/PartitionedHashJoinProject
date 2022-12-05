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


//TODO: it does not return the correct result if i run it with the exeute query function
//better filter it uses list so we dont have to do 2 passes to the main table
// void better_filter_function(relation* r, relation* ret, int operation, int target) {
//     size_t counter = 0;

//     // input check
//     if (operation < _EQUALS && operation > _LESS_EQUALS) {
//         fprintf(stderr, "filter_function: Invalid operation.\n");
//         return;
//     }

//     //we initialize the list
//     list *l = init_list();

//     for (int i=0; i<r->num_tuples; i++) {
//         int payload_tmp = r->tuples[i].payload;
//         switch(operation) {
//             case _EQUALS:            counter += (payload_tmp == target);   list_append(l,i,payload_tmp);    break;
//             case _GREATER:           counter += (payload_tmp > target);    list_append(l,i,payload_tmp);   break;
//             case _LESS:              counter += (payload_tmp < target);    list_append(l,i,payload_tmp);   break;
//             case _GREATER_EQUALS:    counter += (payload_tmp >= target);   list_append(l,i,payload_tmp);   break;
//             case _LESS_EQUALS:       counter += (payload_tmp <= target);   list_append(l,i,payload_tmp);   break;
//         }
//     }

//     // allocate memory for tuples
//     ret->num_tuples = counter;
//     tuple* tuples = malloc(counter * sizeof(tuple));

//     list *curr=l;
//     for(size_t j=0;j<counter;j++){
//         if(curr->row_id ==-1){
//             printf("error");
//         }
//         tuples[j].key = curr->row_id;
//         tuples[j].payload = curr->payload;
//         curr=curr->next;
//     }
//     ret->tuples = tuples;
//     delete_list(l);
//     return;
// }



//basic list functions
list *init_list(){
    list *l = (list*)malloc(sizeof(list));
    l->row_id = -1;
    l->payload = -1;
    l->next = NULL;
    return l;
}

void list_append(list *l, size_t value,size_t pd){
    //first we check if the list is empty
    if(l->row_id == -1){
        l->row_id = value;
        l->payload = pd;
    }
    else{
        list *current = l;
        while(current->next != NULL && current->row_id != -1){
            current = current->next;
        }
        current->next = (list*)malloc(sizeof(list));
        current->next->row_id = value;
        current->next->payload = pd;
        current->next->next = NULL;
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
        printf("%ld ", current->row_id);
        printf("%ld ", current->payload);
        current = current->next;
    }
    printf("\n");
}
