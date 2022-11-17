#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "parser.h"

int number_of_digits(int num){
    if(num < 10 ) return 1;
    if(num < 100 ) return 2;
    if(num < 1000 ) return 3;
    if(num < 10000 ) return 4;
    if(num < 100000 ) return 5;
    if(num < 1000000 ) return 6;
    if(num < 10000000 ) return 7;
    if(num < 100000000 ) return 8;
    if(num < 1000000000 ) return 9;
    return 10;
}

void print_query_info(QueryInfo qinfo){
    printf("===== Involved Relations =====\n");
    for(int i=0;i<qinfo.num_rels;i++){
        printf("Relation %d with ID %d\n",qinfo.rel_ids[i],i);
    }
    printf("===== Join Operations  =====\n");
    JoinInfo* joins = qinfo.joins;
    while(joins!=NULL){
        printf(" %lld.%lld = %lld.%lld\n",joins->left.rel_id,joins->left.col_id,joins->right.rel_id,joins->right.col_id);
        joins=joins->next;
    }
    printf("===== Filter Operations  =====\n");
    FilterInfo* filters = qinfo.filters;
    while(filters!=NULL){
        printf("%lld.%lld with constant %lld\n",filters->sel.rel_id,filters->sel.col_id , filters->constant);
        filters=filters->next;
    }
    printf("===== Projections =====\n");
    SelectionInfo* projections = qinfo.projections;
    while(projections!=NULL){
        printf("%lld.%lld\n",projections->rel_id,projections->col_id);
        projections=projections->next;
    }
}


//TODO LIST APPENDS
void append_to_list_filter(FilterInfo** head,FilterInfo* node){
   FilterInfo* tmp = *head;
    if(*head == NULL) {
    	*head = node;
      	return;
    }
  	while(tmp->next != NULL){
    	tmp = tmp->next;
  	}
  	tmp->next = node;
  	return;
}
void append_to_list_join(JoinInfo** head,JoinInfo* node){
    JoinInfo* tmp = *head;
    if(*head == NULL) {
    	*head = node;
      	return;
    }
  	while(tmp->next != NULL){
    	tmp = tmp->next;
  	}
  	tmp->next = node;
  	return;
}
void append_to_list_select(SelectionInfo** head,SelectionInfo* node){
    SelectionInfo* tmp = *head;
    if(*head == NULL) {
    	*head = node;
      	return;
    }
  	while(tmp->next != NULL){
    	tmp = tmp->next;
  	}
  	tmp->next = node;
  	return;
}

void query_info_delete(QueryInfo* qinfo){
    qinfo->num_rels = 0 ;
    
    while(qinfo->joins){
        JoinInfo* temp = qinfo->joins;
        qinfo->joins = qinfo->joins->next;
        free(temp);
    }
    while(qinfo->filters){
        FilterInfo* temp = qinfo->filters;
        qinfo->filters = qinfo->filters->next;
        free(temp);
    }
    while(qinfo->projections){
        SelectionInfo* temp = qinfo->projections;
        qinfo->projections = qinfo->projections->next;
        free(temp);
    }
    return;
}


void query_info_init(QueryInfo* qinfo){
    qinfo->filters=NULL;
    qinfo->joins=NULL;
    qinfo->projections=NULL;

    qinfo->num_rels = 0;
}

int parse_relation(char* query,QueryInfo* qinfo,int index){
    if(isdigit(query[index])){
        int num = atoi(query+index);
        qinfo->rel_ids[qinfo->num_rels] = num;
        qinfo->num_rels = qinfo->num_rels +1;
        return number_of_digits(num);
    }else{
        printf("error parsing relation\n");
        exit(-1);
    }
}

int parse_relations(char* query,QueryInfo* qinfo,int index){
    int start = index;
    while(query[index] != '|'){
        if(query[index]==' '){
            index++;
            continue;
        }
        if(!isdigit(query[index])){
            printf("error parsing relations\n");
            exit(-1);
        }
        index += parse_relation(query,qinfo,index);
    }
    return index - start;
}

int parse_selection(char* query,QueryInfo* qinfo,int index,SelectionInfo* sel){
    int start = index;
    if(!isdigit(query[index])){
        printf("error parsing selection at index %d character %c\n",index,query[index]);
        exit(-1);
    }
    int rel = atoi(query+index);
    index += number_of_digits(rel);
    sel->rel_id=rel;
    if(query[index]!='.'){
        printf("error parsing selection at index %d character  %c\n",index,query[index]);
        exit(-1);
    }
    index++;
    int col = atoi(query+index);
    index += number_of_digits(col);
    sel->col_id=col;
    sel->next=NULL;
    return index - start;
}


int parse_operator(char* query,QueryInfo* qinfo,int index,int* op_id){
    switch (query[index])
    {
    case '=':
        (*op_id)=EQUALS;
        break;
    case '>':
        (*op_id)=GREATER;
        break;
    case '<':
        (*op_id)=LESS;
        break;
    default:
        printf("Error parsing operator at index %d char:%c\n",index,query[index]);
        exit(-1);
        break;
    }
    return 1;
}

int parse_constant(char* query,QueryInfo* qinfo,int index,int* num){
    if(!isdigit(query[index])){
        printf("error parsing constant\n");
        exit(-1);
    }
    (*num) = atoi(query+index);
    return number_of_digits(*num);
}

int parse_predicate(char* query,QueryInfo* qinfo,int index,PredicateInfo* predicate_info){
    int start = index;
    if(!isdigit(query[index])){
        printf("error parsing predicate at index:%d character:%c\n",index,query[index]);
        exit(-1);
    }else{
        while(query[index]!='&'){
            if(query[index]=='|'){
                break;
            }
            SelectionInfo sel1;
            index += parse_selection(query,qinfo,index,&sel1);
            int op_id;
            index += parse_operator(query,qinfo,index,&op_id);
            if(op_id!=EQUALS){
                //parse filter predicate
                predicate_info->predicateType = FILTER;
                predicate_info->left = sel1;
                predicate_info->operator_type = op_id;
                int constant_val;
                index+=parse_constant(query,qinfo,index,&constant_val);
                predicate_info->constant = constant_val;
            }else{
                //parse join predicate OR EQUALS filter!
                int temp = index;
                while(isdigit(query[temp])){
                    temp++;
                }
                if(query[temp]=='.'){
                    SelectionInfo sel2;
                    index += parse_selection(query,qinfo,index,&sel2);
                    predicate_info->predicateType = JOIN;
                    predicate_info->left = sel1;
                    predicate_info->right = sel2;
                    predicate_info->operator_type = EQUALS;
                }else{
                    int constant_val;
                    index += parse_constant(query,qinfo,index,&constant_val);
                    predicate_info->predicateType = FILTER;
                    predicate_info->left = sel1;
                    predicate_info->operator_type = op_id;
                    predicate_info->constant = constant_val;
                }
            }
        }
    }
    return index - start;
}

int parse_predicates(char* query,QueryInfo* qinfo,int index){
    int start = index;
    if(query[index] != '|'){
        printf("error parsing predicates\n");
        exit(-1);
    }else{
        //skip | character
        index++;
        while(query[index] != '|'){
            PredicateInfo pred_info;
            index += parse_predicate(query,qinfo,index,&pred_info);
            if(pred_info.predicateType==FILTER){                
                FilterInfo* filternode = malloc(sizeof(FilterInfo));
                filternode->constant=pred_info.constant;
                filternode->next=NULL;
                filternode->sel=pred_info.left;
                filternode->type=pred_info.operator_type;
                append_to_list_filter(&(qinfo->filters),filternode);
            }else if( pred_info.predicateType==JOIN){
                JoinInfo* joinnode = malloc(sizeof(JoinInfo));
                joinnode->left=pred_info.left;
                joinnode->right=pred_info.right;
                joinnode->next =NULL;
                append_to_list_join(&(qinfo->joins),joinnode);
            }

            if(query[index]=='&') index++;
        }
        return index - start;
    }
}

int parse_projections(char* query,QueryInfo* qinfo,int index){
    int start = index;
    //skip | character
    if(query[index]=='|'){
        index++;
    }
    while(true){
        SelectionInfo* sel = malloc(sizeof(SelectionInfo));
        index += parse_selection(query,qinfo,index,sel);
        append_to_list_select(&(qinfo->projections),sel);
        if(query[index]==' '){
            index++;
        }else if(!query[index]){
            break;
        }else{
            printf("error parsing projections\n");
            exit(-1);
        }
    }
    return index-start;
}

void parse_query(char* query ,QueryInfo *qinfo){
    //index to the current character we are parsing , designed to be modified by parse functions
    int index = 0;
    index += parse_relations(query,qinfo,index);
    index += parse_predicates(query,qinfo,index);
    index += parse_projections(query,qinfo,index);
    return;    
}

