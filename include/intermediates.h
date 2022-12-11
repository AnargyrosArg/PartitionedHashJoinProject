
#ifndef INTERMEDIATES
#define INTERMEDIATES


#include <stdio.h>
#include <stdlib.h>
#include "stdbool.h"
#include "relations.h"
#include "utils.h"
#include "parser.h"

#define START_N_INTERMEDIATES 20
#define MAX_RELS_PER_QUERY 4 //As per the sigmod 2018 contest website


typedef struct Intermediates Intermediates;
typedef struct Intermediate Intermediate;

// array of intermediate results
struct Intermediates {
    size_t count;
    size_t capacity;
    Intermediate** intermediates;
};

// array of rowid arrays
struct Intermediate {
    //array of tuples of rowids used as rowids[n_row][relid];
    int** rowids;
    //total n of rows held
    size_t rowids_count;
    //table of bools showing which for which relations this intermediate result holds rowids
    bool valid_rels[MAX_RELS_PER_QUERY];
    
};

Intermediates* init_intermediates();
void init_intermediate(Intermediate* intermediate);
relation intermediate_to_relation(Intermediate* inter,int rel , int col,table* tabl,QueryInfo* query);
void delete_intermediate(Intermediate* inter);
void delete_intermediates(Intermediates* inter_array);
void relation_to_intermediate(table* tabl,int rel,int actualid,Intermediate** result);
void set_intermediate(Intermediate* inter,int rowid_count,bool rels[MAX_RELS_PER_QUERY]);
void get_intermediates(Intermediates* intermediates, uint relation_index,int actualid, Intermediate** ret,table* tabl);
int in_same_intermediate_relation(Intermediates* inter ,int rel1 , int rel2,Intermediate** ret);
void insert_intermediate(Intermediate* joinres,Intermediates* intermediates);
void remove_intermediate(Intermediate* res,Intermediates* intermediates);
#endif