#ifndef JOBSCHEDULER
#define JOBSCHEDULER

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#define MAX_QUEUE_SIZE 4096
#define N_WORKERS 4

//enum to determine what kind of job the scheduler just pulled out of the queue
enum JobType {JOIN_JOB};

typedef struct jobscheduler jobscheduler;
typedef struct job job;
typedef struct jobqueue jobqueue;



//a single job to be executed by a thread
struct job
{
    enum JobType type;
    //pointer to the function that does the work
    void* (*work_funct)(void*);
    //passed to work_funct as parameter, it should point to a job specific data structure the function knows how to interpret 
    void* parameters;
};

//queue of jobs waiting to be assigned to threads
struct jobqueue
{
    //jobs stored in queue, static capacity for now , might need to make dynamic
    job queue[MAX_QUEUE_SIZE];
    
    int pop_index;
    int push_index;

    //mutex lock for locking queue
    pthread_mutex_t queue_lock;

};

//keeps the cond variables,mutexes and other data required to do jobscheduling
struct jobscheduler
{
    //queue of jobs to be assigned to threads
    jobqueue jobsqueue;
    //array of worker threads
    pthread_t workers[N_WORKERS];
    //condition variable that signals worker threads that work is available
    pthread_cond_t work_cond;
    pthread_mutex_t work_mutex;
    
    bool DONE;
};


bool isFull(jobqueue*);
bool isEmpty(jobqueue*);
void schedule_job(jobscheduler* scheduler,job j);
void delete_scheduler(jobscheduler* scheduler);
void init_scheduler(jobscheduler* scheduler);

#endif