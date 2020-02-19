#include "processManagement_lab.h"

/**
 * The task function to simulate "work" for each worker process
 * Modify the function to be multiprocess-safe 
 * */
void task(long duration)
{
    // simulate computation for x number of seconds
    usleep(duration * TIME_MULTIPLIER);

    // protect the access of shared variable below
    sem_wait(sem_global_data);
    // update global variables to simulate statistics
    ShmPTR_global_data->sum_work += duration;
    ShmPTR_global_data->total_tasks++;
    if (duration % 2 == 1)
    {
        ShmPTR_global_data->odd++;
    }
    if (duration < ShmPTR_global_data->min)
    {
        ShmPTR_global_data->min = duration;
    }
    if (duration > ShmPTR_global_data->max)
    {
        ShmPTR_global_data->max = duration;
    }
    sem_post(sem_global_data);
    return;
}

/**
 * The function that is executed by each worker process to execute any available job given by the main process
 * */
void job_dispatch(int i)
{
    // a. Always check the corresponding shmPTR_jobs_buffer[i] for new jobs from the main process
    while (true)
    {
        // sleep(0.1);
        // b. Use semaphore so that you don't busy wait
        // int sem1;
        // sem_getvalue(sem_jobs_buffer[i],&sem1);
        // printf("waiting, semaphore value is %d \n",sem1);
        // printf("SEMAPHORE USED\n");
        sem_wait(sem_jobs_buffer[i]); // job will always be available once semaphore posted
        // printf("JOB: %c%d\n", shmPTR_jobs_buffer[i].task_type, shmPTR_jobs_buffer[i].task_duration);
        // c. If there's new job, execute the job accordingly: either by calling task(), usleep, exit(3) or kill(getpid(), SIGKILL)
        long duration = shmPTR_jobs_buffer[i].task_duration;
        if (shmPTR_jobs_buffer[i].task_type == 't')
        {   
            // printf("Task %d Called\n", duration);
            task(duration);
        }
        else if (shmPTR_jobs_buffer[i].task_type == 'w')
        {   
            // printf("Sleep %d Called\n", duration);
            usleep(duration * TIME_MULTIPLIER);
        }
        else if (shmPTR_jobs_buffer[i].task_type == 'i')
        {   
            // printf("Kill %d Called\n", duration);
            kill(getpid(), SIGKILL);
        }
        else if (shmPTR_jobs_buffer[i].task_type == 'z')
        {
            // printf("Exit %d Called\n", duration);
            exit(3);
        }
        shmPTR_jobs_buffer[i].task_status = 0;
        //  d. Loop back to check for new job
    }
}

/** 
 * Setup function to create shared mems and semaphores
 * **/
void setup()
{
    // a. Create shared memory for global_data struct (see processManagement_lab.h)
    ShmID_global_data = shmget(IPC_PRIVATE, sizeof(global_data), IPC_CREAT | 0666);
    //check for errors
    if (ShmID_global_data == -1)
    {
        printf("Global data shared memory creation failed\n");
        exit(EXIT_FAILURE);
    }
    //if no error, attach segment
    ShmPTR_global_data = (global_data *)shmat(ShmID_global_data, NULL, 0);
    // and check for error
    if ((int)ShmPTR_global_data == -1)
    {
        printf("Attachment of global data shared memory failed \n");
        exit(EXIT_FAILURE);
    }
    // b. When shared memory is successfully created, set the initial values of "max" and "min" of the global_data struct in the shared memory accordingly
    ShmPTR_global_data->max = -1;
    ShmPTR_global_data->min = INT_MAX;

    // c. Create semaphore of value 1 (available) which purpose is to protect this global_data struct in shared memory
    sem_global_data = sem_open("semglobaldata", O_CREAT | O_EXCL, 0644, 1);
    while (true)
    {
        if (sem_global_data == SEM_FAILED)
        {
            sem_unlink("semglobaldata");
            sem_global_data = sem_open("semglobaldata", O_CREAT | O_EXCL, 0644, 1);
        }
        else
        {
            break;
        }
    }

    // d. Create shared memory for number_of_processes job struct (see processManagement_lab.h)
    ShmID_jobs = shmget(IPC_PRIVATE, sizeof(job) * number_of_processes * 10, IPC_CREAT | 0666);
    //check for errors
    if (ShmID_jobs == -1)
    {
        printf("Jobs shared memory creation failed\n");
        exit(EXIT_FAILURE);
    }

    //if no error, attach segment
    shmPTR_jobs_buffer = (job *)shmat(ShmID_jobs, NULL, 0);
    // and check for error
    if ((int)shmPTR_jobs_buffer == -1)
    {
        printf("Attachment of jobs shared memory failed \n");
        exit(EXIT_FAILURE);
    }

    // TODO #1 e. When shared memory is successfully created, setup the content of the structs (see handout)
    for (int i = 0; i < number_of_processes; i++)
    {
        (shmPTR_jobs_buffer + i)->task_type = "t";
        (shmPTR_jobs_buffer + i)->task_duration = 0;
        (shmPTR_jobs_buffer + i)->task_status = 0;
    }

    // f. Create number_of_processes semaphores of value 0 each to protect each job struct in the shared memory. Store the returned pointer by sem_open in sem_jobs_buffer[i]
    for (int i = 0; i < number_of_processes; i++)
    {
        char semjobsi[10];
        sprintf(semjobsi, "semjobs%d", i);
        sem_jobs_buffer[i] = sem_open(semjobsi, O_CREAT | O_EXCL, 0644, 0);
        while (true)
        {
            if (sem_global_data == SEM_FAILED)
            {
                sem_unlink(semjobsi);
                sem_global_data = sem_open(semjobsi, O_CREAT | O_EXCL, 0644, 0);
            }
            else
            {
                break;
            }
        }
    }

    // g. Return to main
    return;
}

/**
 * Function to spawn all required children processes
 **/
void createchildren()
{
    // a. Create number_of_processes children processes
    int forkValue;
    for (int i = 0; i < number_of_processes; i++)
    {
        forkValue = fork();
        // b. Store the pid_t of children i at children_processes[i]
        children_processes[i] = forkValue;
        if (forkValue < 0)
        { //if fork fails
            perror("Failed to fork.\n");
            exit(1);
        }
        else if (forkValue == 0)
        { //if child process
            // c. For child process, invoke the method job_dispatch(i)
            job_dispatch(i);
            break; //break from loop and prevent spawning
        }
    }
    // e. After number_of_processes children are created, return to main
    return;
}

/**
 * The function where the main process loops and busy wait to dispatch job in available slots
 * */
void main_loop(char *fileName) {
    // load jobs and add them to the shared memory
    FILE *opened_file = fopen(fileName, "r");
    char action; //stores whether its a 'p' or 'w'
    long num;    //stores the argument of the job
    while (fscanf(opened_file, "%c %ld\n", &action, &num) == 2) { //while the file still has input
        // printf("%c %ld\n", action, num);
        // a. Busy wait and examine each shmPTR_jobs_buffer[i] for jobs that are done by checking that shmPTR_jobs_buffer[i].task_status == 0. You also need to ensure that the process i IS alive using waitpid(children_processes[i], NULL, WNOHANG). This WNOHANG option will not cause main process to block when the child is still alive. waitpid will return 0 if the child is still alive. 
        bool done = false;
        while(!done) { //busy wait loop
            for ( int i = 0 ; i < number_of_processes ; i++ ) { // go through all processes
                int alive = waitpid(children_processes[i], NULL, WNOHANG);
                if (alive==0) { //if child is alive
                    if (shmPTR_jobs_buffer[i].task_status==0) { //if job is done, produce another
                        shmPTR_jobs_buffer[i].task_type=action;
                        shmPTR_jobs_buffer[i].task_duration=num;
                        shmPTR_jobs_buffer[i].task_status=1;
                        // // b. If both conditions in (a) is satisfied update the contents of shmPTR_jobs_buffer[i], and increase the semaphore using sem_post(sem_jobs_buffer[i])
                        sem_post(sem_jobs_buffer[i]);
                        // printf("SEMAPHORE POSTED\n");
                        // // c. Break of busy wait loop, advance to the next task on file
                        done=true; // break busy wait
                        break; //break for loop
                    }
                } else { //if child is dead                    
                    shmPTR_jobs_buffer[i].task_status=0;
                    int forkValue = fork();
                    children_processes[i]=forkValue;
                    if (forkValue < 0 ){ //if fork fails
                        perror("Failed to fork.\n" );
                        exit(1);
                    } else if(forkValue == 0) { //if child process
                        job_dispatch(i);
                    } else{
                        //parent job dispatch
                        // printf("Process %d restarting...\n",i);
                        shmPTR_jobs_buffer[i].task_status=0;
                        shmPTR_jobs_buffer[i].task_duration = num;
                        shmPTR_jobs_buffer[i].task_status = 1; // new job available
                        shmPTR_jobs_buffer[i].task_type = action;
                        done = true;
                        sem_post(sem_jobs_buffer[i]); //post new job
                        break;
                    }
                }
            }
            // if (done) break;
        }
    }
    fclose(opened_file);
    // e. The outermost while loop will keep doing this until there's no more content in the input file.
    // printf("Main process is going to send termination signals\n");
    // TODO#4: Design a way to send termination jobs to ALL worker that are currently alive

    //check for all dead processes as file may not revive them at end (end of lines)
    for (int i = 0; i < number_of_processes; i++) {
        int count= 2000000;
        while(shmPTR_jobs_buffer[i].task_status == 1){
            count--;
            int alive = waitpid(children_processes[i], NULL, WNOHANG);
            if (alive!=0) { //if child is already dead, ignore
                break;
            }

            if (count==0) { //timeout on hang
                for (int i = 0; i < number_of_processes; i++) {
                    int sem1;
                    sem_getvalue(sem_jobs_buffer[i], &sem1);
                    printf("Task %d: semaphore %d \t",i, sem1);
                    printf("type - %c\t", shmPTR_jobs_buffer[i].task_type);
                    printf("duration - %d\t", shmPTR_jobs_buffer[i].task_duration);
                    printf("status - %d\n", shmPTR_jobs_buffer[i].task_status);
                }
                printf("**TIMEOUT ON %d**\n",i);
                cleanup();
                exit(1);
            }
        }
        // Once revived, terminate
        shmPTR_jobs_buffer[i].task_type = 'z'; //termination job
        shmPTR_jobs_buffer[i].task_duration = 1;
        shmPTR_jobs_buffer[i].task_status = 1; // termination signal
        // printf("SEMAPHORE POSTED\n");
        sem_post(sem_jobs_buffer[i]); // signal the child to start
    }

    //wait for all children processes to properly execute the 'z' termination jobs
    int waitpid_result;
    for (int i = 0; i < number_of_processes; i++) {
        waitpid_result = waitpid(children_processes[i], NULL, 0); // returns when child exits normally
        if (waitpid_result != -1) {
            // printf("Child %d with pid %d has exited successfully\n", i, waitpid_result);
        }
    }
    printf("Final results: sum -- %ld, odd -- %ld, min -- %ld, max -- %ld, total task -- %ld\n", ShmPTR_global_data->sum_work, ShmPTR_global_data->odd, ShmPTR_global_data->min, ShmPTR_global_data->max, ShmPTR_global_data->total_tasks);
    return;
}

void cleanup()
{
    // 1. Detach both shared memory (global_data and jobs)
    int detach_status = shmdt((void *)ShmPTR_global_data); //detach
    if (detach_status == -1)
        printf("Detach shared memory global_data ERROR\n");
    detach_status = shmdt((void *)shmPTR_jobs_buffer); //detach
    if (detach_status == -1)
        printf("Detach shared memory jobs ERROR\n");

    // 2. Delete both shared memory (global_data and jobs)
    int remove_status = shmctl(ShmID_global_data, IPC_RMID, NULL); //delete
    if (remove_status == -1)
        printf("Remove shared memory global_data ERROR\n");
    remove_status = shmctl(ShmID_jobs, IPC_RMID, NULL); //delete
    if (remove_status == -1)
        printf("Remove shared memory jobs ERROR\n");

    // 3. Unlink all semaphores in sem_jobs_buffer
    int sem_close_status = sem_unlink("semglobaldata");
    if (sem_close_status == 0)
    {
        // printf("sem_global_data closed successfully.\n");
    }
    else
    {
        printf("sem_global_data failed to close.\n");
    }

    for (int i = 0; i < number_of_processes; i++)
    {
        char *sem_name = malloc(sizeof(char) * 16);
        sprintf(sem_name, "semjobs%d", i);
        sem_close_status = sem_unlink(sem_name);
        if (sem_close_status == 0)
        {
            // printf("sem_jobs_buffer[%d] closed successfully.\n", i);
        }
        else
        {
            printf("sem_jobs_buffer[%d] failed to close.\n", i);
        }
        free(sem_name);
    }
}

// Real main
int main(int argc, char *argv[])
{
    struct timeval start, end;
    long secs_used, micros_used;

    //start timer
    gettimeofday(&start, NULL);

    //Check and parse command line options to be in the right format
    if (argc < 2)
    {
        printf("Usage: sum <infile> <numprocs>\n");
        exit(EXIT_FAILURE);
    }

    //Limit number_of_processes into 10.
    //If there's no third argument, set the default number_of_processes into 1.
    if (argc < 3)
    {
        number_of_processes = 1;
    }
    else
    {
        if (atoi(argv[2]) < MAX_PROCESS)
            number_of_processes = atoi(argv[2]);
        else
            number_of_processes = MAX_PROCESS;
    }
    setup();
    createchildren();
    main_loop(argv[1]);

    //parent cleanup
    cleanup();

    //stop timer
    gettimeofday(&end, NULL);
    double start_usec = (double)start.tv_sec * 1000000 + (double)start.tv_usec;
    double end_usec = (double)end.tv_sec * 1000000 + (double)end.tv_usec;
    printf("Your computation has used: %lf secs \n", (end_usec - start_usec) / (double)1000000);
    return (EXIT_SUCCESS);
}