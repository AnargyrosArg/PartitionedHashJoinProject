#ifndef PARSER
#define PARSER

#define MAX_RELS_PER_QUERY 4 //As per the sigmod 2018 contest website

#include <stdbool.h>
#include <math.h>
#include <ctype.h>



typedef struct QueryInfo QueryInfo;

typedef struct JoinInfo JoinInfo;

typedef struct SelectionInfo SelectionInfo;

typedef struct FilterInfo FilterInfo;

typedef struct PredicateInfo PredicateInfo;

enum Operator{ EQUALS , GREATER , LESS};
enum PredicateType {JOIN,FILTER};

struct QueryInfo{
    //array of relation ids involved in query
    unsigned rel_ids[MAX_RELS_PER_QUERY];
    unsigned num_rels;

    //list of join operation involed in query
    JoinInfo* joins;

    //list of filter operations involved in query
    FilterInfo* filters;

    //list of selections chosen for projection
    SelectionInfo* projections;

};




//a relation id along with the collumn its payloads represent in the actuall database table
struct SelectionInfo{
    long long rel_id;
    long long col_id;

    SelectionInfo* next;
};

struct JoinInfo{
    SelectionInfo left;
    SelectionInfo right;
    JoinInfo* next;
};

struct FilterInfo{
    int type;
    SelectionInfo sel;
    //the constant the values of the relation are compared to
    long long constant;

    FilterInfo* next;
};

struct PredicateInfo{
    int predicateType;
    SelectionInfo left;
    SelectionInfo right;
    int operator_type;
    long long constant;
};



void parse_query(char* query,QueryInfo* qinfo);
void query_info_init(QueryInfo* qinfo);
void query_info_delete(QueryInfo* qinfo);
void print_query_info(QueryInfo qinfo);
size_t get_join_count(QueryInfo* query);

#endif