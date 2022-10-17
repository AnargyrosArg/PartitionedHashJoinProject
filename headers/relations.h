typedef struct tuple tuple;

typedef struct relation relation;

typedef struct result result;


struct tuple{
    int key;
    int payload;
};

struct relation{
    tuple* tuples;
    int num_tuples;
};

struct result{
    tuple* results;
    unsigned int result_size;
};
