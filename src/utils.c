#include "utils.h"


/*
Various utility functions
*/

void read_file(relation *rela,char *name){
    //we open the fle 
    FILE *fl;
	fl = fopen ( name , "r");	

	if ( fl == NULL ) {
		perror("fopen");
		exit(1);
	}
    
    //now we need to check how many tuples there are in the file 
    int counter =0;
    int num;
    //while there are still numbers to be read, we increment the counter
    while(fscanf(fl,"%d", &num) == 1){
        counter++;
    }
    fclose(fl);

    //we create the relation with size of the counter
    rela->num_tuples= counter;

    //now we have to create the tuples for our relation
    tuple* tuples = malloc(counter * sizeof(tuple));

    //we open the file again
	fl = fopen ( name , "r");	

	if ( fl == NULL ) {
		perror("fopen");
		exit(1);
	}

    int i=0;
    for(i=0;i<counter;i++){
        fscanf(fl,"%d", &num);
        tuples[i].key = i;
        tuples[i].payload = num;
    }

    rela->tuples = tuples;
    //we close the file
    fclose(fl);
    return;
}

int power(int base,int exp){
    int ret=base;
    for(int i=0;i<exp-1;i++){
        ret *= base;
    }
    return ret;
}

int pseudo_log2(int val){
    int count=0;
    while(val/2){
        count++;
        val=val/2;
    }
    return count;
}

int factorial(int x) {
    if (x < 0) return -1;
    int fact = 1;
    for (int i=1; i<=x; i++)
        fact *= i;
    return fact;
}

int in(int* arr, size_t size, int x) {
    for (int i=0; i<size; i++)
        if (arr[i] == x) return 1;
    return 0;
}

void init_relation(relation* rel,int num_tuples){
    rel->tuples = malloc(num_tuples * sizeof(tuple));
    rel->num_tuples = num_tuples;
}

void delete_relation(relation rel){
    if (rel.tuples != NULL)
        free(rel.tuples);
}

void init_array(int* array,int size,int value){
    for(int i=0;i < size ;i++){
        array[i]=value;
    }
}

void print_result(result* res) {
    for (int i=0; i<res->result_size; i++)
        printf("(%d, %d) ", res->pairs[i].key1, res->pairs[i].key2);
    printf("\n\n");
}


// get value of n-th bit of bitmap (n=0 refers to MOST significant bit of the integer)
int bitmap_get_bit(unsigned long long bitmap, int n) {
    return ((bitmap << n) >> ((sizeof(unsigned long long)*__CHAR_BIT__)-1));
}

// set n-th bit of bitmap (n=0 refers to MOST significant bit of the integer)
void bitmap_set_bit(unsigned long long* bitmap, int n, int value) {
    if (value == 1) *bitmap = (1 << ((sizeof(unsigned long long)*__CHAR_BIT__)-n-1)) | (*bitmap);
    if (value == 0) *bitmap = *bitmap & (~(1 << ((sizeof(unsigned long long)*__CHAR_BIT__)-n-1)));
}

// returns 0 or 1 depending on whether bitmap is full (aka when everything is 1)
int bitmap_full(unsigned long long bitmap, int size) {
    for (int i=0; i<size; i++)
        if (!bitmap_get_bit(bitmap, i))
            return 0;
    return 1;
}


// insert element to array, expand array if it is full
int* insert_array(int* array, int* pos, int* size, int data) {
    if ((*pos) <= ((*size)-1)) { // if array is not full
        array[*pos] = data;
        (*pos) += 1;
        return array;
    }
    else { // if array is full
        int* new_array = malloc(sizeof(int) * (*size)*2);
        for (int i=0; i<(*size); i++)
            new_array[i] = array[i];
        new_array[*pos] = data;
        (*pos) += 1;
        (*size) *= 2;
        free(array);
        return new_array;
    }
}

// strings all ints of an array into one int
uint array_to_int(int* array, size_t size) {
    if (size < 1) return 0;
    uint x = (uint) array[0];

    for (int i=1; i<size; i++)
        x = concat_uints(x, (uint) array[i]);
    return x;
}

// concatenates two uints (2,3 = 23)
uint concat_uints(uint x, uint y) {
    uint pow = 10;
    while(y >= pow) pow *= 10;
    return x*pow + y;
}

// calculate all possible combinations of r elements in an array
int combinations(int arr[], int size, int r, int** ret) {
    int data[r]; // temp array to store all combinations one by one
    return combinations_util(arr, data, 0, size-1, 0, r, ret, 0);
}

// recursive function to calculate all possible combinations of r elements in an array
int combinations_util(int arr[], int data[], int start, int end, int index, int r, int** ret, int count) {
    // Current combination is ready
    if (index == r) {
        for (int j=0; j<r; j++)
            ret[count][j] = data[j];
        return ++count;
    }
 
    // replace index with all possible elements. The condition
    // "end-i+1 >= r-index" makes sure that including one element
    // at index will make a combination with remaining elements
    // at remaining positions
    for (int i=start; i<=end && end-i+1 >= r-index; i++) {
        data[index] = arr[i];
        count = combinations_util(arr, data, i+1, end, index+1, r, ret, count);
    }
    return count;
}