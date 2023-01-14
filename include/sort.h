#include <stdio.h>
#include <stdlib.h>

// merge sort (used by optimizer)

// just calls the other functions
void sort(int array[], size_t size);

// merge sort functions
void merge_sort(int arr[], int l, int r);
void merge(int arr[], int l, int m, int r);