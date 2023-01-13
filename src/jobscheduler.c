#include "jobscheduler.h"

void* Worker(void* sch);

//========================== Queue Implementation ==========================
void init_queue(jobqueue* queue){
    queue->pop_index=0;
    queue->push_index=0;
    pthread_mutex_init(&queue->queue_lock,NULL);
}

bool isFull(jobqueue* queue){
    return ((queue->pop_index - queue->push_index) == 1);
}

bool isEmpty(jobqueue* queue){
    return (queue->pop_index == queue->push_index);
}

void push(jobqueue* queue ,job value){
    if(isFull(queue)){
        fprintf(stderr,"Push on full queue\n");
        exit(-1);
    }
    queue->queue[queue->push_index] = value;
    queue->push_index = (queue->push_index+1)%MAX_QUEUE_SIZE;

}

job pop(jobqueue* queue){
    if(isEmpty(queue)){
        fprintf(stderr,"Pop on empty queue\n");
        exit(-1);
    }
    job ret = queue->queue[queue->pop_index];
    queue->pop_index = (queue->pop_index+1)%MAX_QUEUE_SIZE;
    return ret;
}

//=================================================================


//========================== Scheduler ==========================

//TODO delete scheduler
void delete_scheduler(jobscheduler* scheduler){
    //signal execution is done
    scheduler->DONE = true;
    //wake up threads
    //collect threads
    pthread_cond_broadcast(&(scheduler->work_cond));
    for(int i=0;i<N_WORKERS;i++){
        pthread_join(scheduler->workers[i],NULL);
    }
    //destroy mutex / cond variable
    pthread_cond_destroy(&(scheduler->work_cond));
    pthread_mutex_destroy(&(scheduler->work_mutex));

    //TODO replace with a queue_destroy function!
    pthread_mutex_destroy(&(scheduler->jobsqueue.queue_lock));
}

void init_scheduler(jobscheduler* scheduler){
    init_queue(&(scheduler->jobsqueue));
    scheduler->DONE = false;
    pthread_cond_init(&(scheduler->work_cond),NULL);
    pthread_mutex_init(&(scheduler->work_mutex),NULL);
    //start worker threads
    for(int i=0;i<N_WORKERS;i++){
        //create thread
        pthread_create(&(scheduler->workers[i]),NULL,Worker,(void*)scheduler);
        //Workers are designed to work indefinetly ,so we detach them
       // pthread_detach(scheduler->workers[i]);
    }
}

//adds a job to the schedulers queue and notifies worker threads that work is available
void schedule_job(jobscheduler* scheduler,job j){
    //add job to queue
    pthread_mutex_lock(&(scheduler->jobsqueue.queue_lock));
    push(&(scheduler->jobsqueue),j);
    pthread_mutex_unlock(&(scheduler->jobsqueue.queue_lock));
    
    //signal all workers that job is available
    pthread_cond_broadcast(&(scheduler->work_cond));
}

//this function gets jobs from the schedulers queue and executes their function
//expects a jobscheduler pointer as arguement
void* Worker(void* sch){
    while(true){
        jobscheduler* scheduler = (jobscheduler*) sch;
        //lock work mutex
        pthread_mutex_lock(&(scheduler->work_mutex));

        //wait for work if none
        while(isEmpty(&(scheduler->jobsqueue)) && !scheduler->DONE){
            pthread_cond_wait(&(scheduler->work_cond),&(scheduler->work_mutex));
        }

        if(scheduler->DONE){
            pthread_mutex_unlock(&(scheduler->work_mutex));
            break;
        }

        //lock queue mutex
        pthread_mutex_lock(&(scheduler->jobsqueue.queue_lock));

        job todo = pop(&(scheduler->jobsqueue));
      //  fprintf(stderr,"worker %ld starting work\n",pthread_self());

        //unlock queue
        pthread_mutex_unlock(&(scheduler->jobsqueue.queue_lock));
        
        //unlock work mutex
        pthread_mutex_unlock(&(scheduler->work_mutex));

        //call job's function
        todo.work_funct(todo.parameters);
       // fprintf(stderr,"worker %ld finished work\n",pthread_self());

        //repeat
    }
    return NULL;
}

//==============================================================================