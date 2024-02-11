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
        printf("    Process 0 sending message %d to process %d, tag %d (%d processes in ring)\n",   message, next, tag, size);
        MPI_Send(&message, 1, MPI_INT, next, tag, MPI_COMM_WORLD);
        printf("    Process 0 sent to %d\n", next);
    }

    /* Pass the message around the ring.  The exit mechanism works as
       follows: the message (a positive integer) is passed around the
       ring.  Each time it passes rank 0, it is decremented.  When
       each processes receives a message containing a 0 value, it
       passes the message on to the next process and then quits.  By
       passing the 0 message first, every process gets the 0 message
       and can quit normally. 

       CHALLENGE: Edit code to replace decrement at each node. */

    while (1) {
        printf("    Waiting to receive message from %d in process %d \n",prev,rank);
        MPI_Recv(&message, 1, MPI_INT, prev, tag, MPI_COMM_WORLD,
                 MPI_STATUS_IGNORE);
        printf("    Process %d received message from %d\n",rank,prev);

        // if (0 == rank) {
            --message;
            printf("        Process %d decremented to value: %d\n",rank, message);
        // }



        if ( 0 == message ) {
            printf("    Process %d exiting\n", rank);
            break;
        }

        printf("    Process %d send message to %d  \n",rank,next);
        MPI_Send(&message, 1, MPI_INT, next, tag, MPI_COMM_WORLD);


    }

    /* The last process does one extra send to process 0, which needs
       to be received before the program can exit */

    // if (0 == rank) {
    //     MPI_Recv(&message, 1, MPI_INT, prev, tag, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
    // }

    /* All done */

    MPI_Finalize();
    return 0;
}
