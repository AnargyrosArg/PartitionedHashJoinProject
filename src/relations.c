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
       // fprintf(stderr,"before %d now %d\n",res->capacity,res->capacity+RESULT_CAPACITY_INCREMENT);
        res->pairs = realloc(res->pairs,(res->capacity + RESULT_CAPACITY_INCREMENT) * sizeof(pair));    
        res->capacity += RESULT_CAPACITY_INCREMENT;
    }
    //add new pair
    res->pairs[res->result_size].key1 = p.key1;
    res->pairs[res->result_size].key2 = p.key2;
    res->pairs[res->result_size].payload = p.payload;
    //increment current number of elements
    res->result_size = res->result_size +1;
    
    return;
}





// void add_result(result* res,pair p){
//     //if we reached max capacity , reallocate new,larger space and copy old values over
//     if(res->capacity == res->result_size){
//         pair* new = malloc( (res->capacity + RESULT_CAPACITY_INCREMENT) * sizeof(pair));
//         for(int i=0;i<res->result_size;i++){
//             new[i].key1 = res->pairs[i].key1;
//             new[i].key2 = res->pairs[i].key2;
//             new[i].payload = res->pairs[i].payload;
//         }
//         free(res->pairs);
//         res->pairs = new;
//         res->capacity += RESULT_CAPACITY_INCREMENT;
//     }
//     //add new pair
//     res->pairs[res->result_size].key1 = p.key1;
//     res->pairs[res->result_size].key2 = p.key2;
//     res->pairs[res->result_size].payload = p.payload;
//     //increment current number of elements
//     res->result_size = res->result_size +1;
    
//     return;
// }


table load_relation(const char* filename){
    FILE* fp = fopen(filename, "r");
    if (fp==NULL) {
        perror("fopen");
        exit(-1);
    }
    //go to end of file
    fseek(fp, 0L, SEEK_END);
    //position is size
    int size = ftell(fp);
    //go back to beginning 
    fseek(fp, 0L, SEEK_SET);
    char* addr=(char*)(mmap(NULL,size,PROT_READ,MAP_PRIVATE,fileno(fp),0u));
    if (addr==MAP_FAILED) {
        perror("cannot mmap ");
        exit(-1);
    }

    if (size<16) {
        printf("Could not parse input relation, no header\n");
        exit(-1);
    }

    // setup table
    table ret;
    ret.num_tuples = *(uint64_t*)(addr);
    addr += sizeof(uint64_t);

    ret.num_colums = *(size_t*)(addr);
    addr += sizeof(size_t);

    ret.statistics = malloc(ret.num_colums * sizeof(stats));
    ret.table = malloc(ret.num_colums * sizeof(uint64_t*));
    
    for (unsigned i=0; i<ret.num_colums; ++i) {
        uint lowest = UINT_MAX, highest = 0;

        ret.table[i] = (uint64_t*)(addr); 
        addr += ret.num_tuples * sizeof(uint64_t);

        // statistics
        ret.statistics[i].count = ret.num_tuples;
        for (int j=0; j<ret.num_tuples; j++) {
            if (ret.table[i][j] < lowest)  lowest = ret.table[i][j];
            if (ret.table[i][j] > highest) highest = ret.table[i][j];
        }
        ret.statistics[i].lower = lowest;
        ret.statistics[i].upper = highest;
        
        uint N = 50000000, size = highest-lowest+1, distinct_count = 0;
        size_t distinct_size = (size > N) ? N : size;
        char distinct_table[distinct_size];
        memset(distinct_table, 0, distinct_size);

        for (int j=0; j<ret.num_tuples; j++)
            distinct_table[(ret.table[i][j] - lowest) % distinct_size] = 1;
        for (int j=0; j<distinct_size; j++)
            distinct_count += distinct_table[j];
        ret.statistics[i].distinct = distinct_count;
    }


    fclose(fp);
    return ret;
}

void delete_table(table* table){
    free(table->table);
    free(table->statistics);
}

// prints table (if only_stats is true, only table statistics will be printed)
void print_table(table t, int only_stats) {
    if (!only_stats) {
        for(uint64_t tuple=0;tuple<t.num_tuples;tuple++){
            for(uint64_t column=0;column<t.num_colums;column++){
                printf("%ld|",t.table[column][tuple]);
            }
            printf("\n");
        }
    }

    // print statistics
    printf("====== statistics: ======\nlower\tupper\tcount\tdistinct\n");
    for (int i=0; i<t.num_colums; i++) {
        stats stat = t.statistics[i];
        printf("%d\t%d\t%u\t%u\n", stat.lower, stat.upper, stat.count, stat.distinct);
    }
    printf("\n");
}