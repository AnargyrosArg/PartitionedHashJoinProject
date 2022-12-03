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