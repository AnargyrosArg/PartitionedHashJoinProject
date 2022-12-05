#include <stdio.h>

#include "relations.h"

// underscores are to distinguish from other enumerator with same values in parcer.h
enum operation { _EQUALS , _GREATER , _LESS, _GREATER_EQUALS, _LESS_EQUALS };

//we make a basic  list that holds ints
typedef struct list {
    size_t row_id;
    size_t payload;
    struct list *next;
} list;

//basic list functions
list *init_list();
void list_append(list *l, size_t id, size_t pd);
void delete_list(list *l);
void print_list(list *l);


// r is the relation to be filtered, resulting relation will be placed in ret
void filter_function(relation* r, relation* ret, int operation, int target);
void better_filter_function(relation* r, relation* ret, int operation, int target);