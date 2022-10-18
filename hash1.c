#include "hash1.h"

unsigned int hash1(unsigned int value,int n){
    return (value << (__INT_WIDTH__ - n)) >> (__INT_WIDTH__ - n);
}



