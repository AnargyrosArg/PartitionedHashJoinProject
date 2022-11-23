#include "relations.h"


void init_result(result* res){
    res->result_size=0;
    res->capacity=INITIAL_RESULT_CAPACITY;
    res->pairs = malloc(INITIAL_RESULT_CAPACITY * sizeof(pair));
}

void delete_result(result* res){
    free(res->pairs);
}

void add_result(result* res,pair p){
    //if we reached max capacity , reallocate new,larger space and copy old values over
    if(res->capacity == res->result_size){
        pair* new = malloc( (res->capacity + 100) * sizeof(pair));
        for(int i=0;i<res->result_size;i++){
            new[i].key1 = res->pairs[i].key1;
            new[i].key2 = res->pairs[i].key2;
            new[i].payload = res->pairs[i].payload;
        }
        free(res->pairs);
        res->pairs = new;
        res->capacity += 100;
    }
    //add new pair
    res->pairs[res->result_size].key1 = p.key1;
    res->pairs[res->result_size].key2 = p.key2;
    res->pairs[res->result_size].payload = p.payload;
    //increment current number of elements
    res->result_size = res->result_size +1;
    
    return;
}


table load_relation(const char* filename){
    FILE* fp = fopen(filename, "r");
    if (fp==NULL) {
        perror("fopen");
        exit(-1);
    }
    printf("opened file!\n");
    //go to end of file
    fseek(fp, 0L, SEEK_END);
    //position is size
    int size = ftell(fp);
    //go back to beginning 
    fseek(fp, 0L, SEEK_SET);
    printf("got size!\n");

    char* addr=(char*)(mmap(NULL,size,PROT_READ,MAP_PRIVATE,fileno(fp),0u));
    if (addr==MAP_FAILED) {
        perror("cannot mmap ");
        exit(-1);
    }

    if (size<16) {
        printf("Could not parse input relation, no header\n");
        exit(-1);
    }
    
    table ret;

    ret.num_tuples = *(uint64_t*)(addr);
    addr += sizeof(uint64_t);
    ret.num_colums = *(size_t*)(addr);
    addr += sizeof(size_t);
    ret.table = malloc(ret.num_colums*sizeof(uint64_t*));
    for (unsigned i=0;i<ret.num_colums;++i) {
        ret.table[i] =(uint64_t*)(addr); 
        addr+=ret.num_tuples*sizeof(uint64_t);
    }


    fclose(fp);
    return ret;
}

void delete_table(table* table){
    free(table->table);
}