#include <stdio.h>

#include "relations.h"

// underscores are to distinguish from other enumerator with same values in parcer.h
enum operation { _EQUALS , _GREATER , _LESS, _GREATER_EQUALS, _LESS_EQUALS };

// r is the relation to be filtered, resulting relation will be placed in ret
void filter_function(relation* r, relation* ret, int operation, int target);