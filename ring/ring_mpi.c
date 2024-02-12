/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2006      Cisco Systems, Inc.  All rights reserved.
 *
 * Simple ring test program in C.
 */

#include <stdio.h>
#include "mpi.h"

int main(int argc, char *argv[])
{
    int rank, size, next, prev, message, tag = 201;
    
    
    /* Start up MPI */
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    /* Calculate the rank of the next process in the ring.  Use the
       modulus operator so that the last process "wraps around" to
       rank zero. */
    next = (rank + 1) % size;
    prev = (rank + size - 1) % size;

    // printf("Current: %d     Next: %d    Prev: %d \n",rank, next, prev);

    /* If we are the "master" process (i.e., MPI_COMM_WORLD rank 0),
       put the number of times to go around the ring in the
       message. */

    if (0 == rank) {
        message = 10;
        printf("[Process %d] Sending message %d to Process %d (%d processes in ring)\n",rank,message,next,size);
        MPI_Send(&message, 1, MPI_INT, next, tag, MPI_COMM_WORLD);
    }

    /* 
        Pass the message around the ring.  The exit mechanism works as
        follows: the message (a positive integer) is passed around the
        ring.  Each time it passes rank 0, it is decremented.  When
        each processes receives a message containing a 0 value, it
        passes the message on to the next process and then quits.  By
        passing the 0 message first, every process gets the 0 message
        and can quit normally. 

        CHALLENGE: Edit code to replace decrement at each node. 
    */

    while (1) {
        printf("[Process %d] Waiting to receive message from Process %d \n",rank,prev);
        MPI_Recv(&message, 1, MPI_INT, prev, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("[Process %d] Message Received %d from Process  %d\n",rank,message,prev);
        --message;
        printf("[Process %d] Decrementing message to value: %d\n",rank,message);
        printf("[Process %d] Sending message to Process %d  \n",rank,next);
        MPI_Send(&message, 1, MPI_INT, next, tag, MPI_COMM_WORLD);
        if ( size > message  ) {
            printf("[Process %d] Exiting from Loop\n", rank);
            break;
        }
    }

    /* The last process does one extra send to process 0, which needs
    to be received before the program can exit */
    if ((10 % size + 1) == rank) {
        printf("[Process %d] Waiting to receive message from Process %d to end.\n",rank,prev);
        MPI_Recv(&message, 1, MPI_INT, prev, tag, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        printf("[Process %d] Message Received %d from Process  %d\n",rank,message,prev);
    }

    printf("[Process %d] Ended \n",rank);
    /* All done */
    MPI_Finalize();
    return 0;
}
