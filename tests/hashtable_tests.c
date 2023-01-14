#include <stdio.h>
#include <stdlib.h>
#include "hashtable.h"
#include "acutest.h"

// basic hash2 calls
void simple_hash2_calls(void) {
    TEST_ASSERT(hash2(4, 10) == 5);
    TEST_ASSERT(hash2(123, 100000) == 6228);
    TEST_ASSERT(hash2(10, 0) == 0); // if we give range of 0, the return is always 0
}

// test that hash2 return value is always within given range
void hash2_range(void) {
    int range = 10, iterations = 1000;
    unsigned int ret;
    for (int i=0; i<iterations; i++) {
        ret = hash2(rand() % 1000, range);
        TEST_ASSERT((ret < range) && (ret >= 0));
    }
}

// bitmap basic calls
void bitmap_calls(void) {
    int bit_num = sizeof(unsigned long long)*__CHAR_BIT__;
    unsigned long long num = 3;
    TEST_ASSERT(bitmap_get_bit(num, bit_num-1) == 1);
    TEST_ASSERT(bitmap_get_bit(num, bit_num-2) == 1);
    TEST_ASSERT(bitmap_get_bit(num, bit_num-3) == 0);

    TEST_ASSERT(bitmap_get_bit(num, 0) == 0);
    bitmap_set_bit(&num, 0, 1);
    TEST_ASSERT(bitmap_get_bit(num, 0) == 1);
    bitmap_set_bit(&num, 0, 0);
    TEST_ASSERT(bitmap_get_bit(num, 0) == 0);

    TEST_ASSERT(bitmap_full(num, bit_num) == 0);
    TEST_ASSERT(bitmap_full(18446744073709551615ull, bit_num) == 1);
}

// hashtable init tests
void hashtable_init(void) {
    int nbsize = 4, size = 10;
    hashtable* table = init_hashtable(size, nbsize, hash2);
    TEST_ASSERT(table->nbsize == nbsize);
    TEST_ASSERT(table->tablesize == 2*size);
    delete_hashtable(table);
}

// hashtable insert tests
void hashtable_insert(void) {
    int data, ret_size, start_range = 10, final_range = 100, iterations = 100, nbsize = 8, start_size = 10;
    int* ret;

    for (int range=start_range; range<final_range; range+=90) { // once with range 10 and once with range 100
        hashtable* table = init_hashtable(start_size, nbsize, hash2);
        
        for (int i=0; i<iterations; i++) { // insert [iterations] elements
            data = rand() % range;
            table = insert_hashtable(table, data, i); 
            ret = search_hashtable(table, data, &ret_size);
            TEST_ASSERT(ret_size > 0);
        }
        delete_hashtable(table);
    }
}

TEST_LIST = {
    {"Basic hash2 Functionality", simple_hash2_calls},
    {"Hash2 Return Range", hash2_range},
    {"Bitmap Basic Functionality", bitmap_calls},
    {"Hashtable Init", hashtable_init},
    {"Hashtable Insert", hashtable_insert},
    {NULL, NULL}
};