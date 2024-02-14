#include <stdio.h>
#include "mpi.h"

int main(int argc, char *argv[])
{
    int rank, size, next, prev, message, tag = 201;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if ( size > 10 ){
        printf("[Terminating] Process must be only less than 11 processes in order to become ring \n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    printf("[Process %2d] Starting \n",rank);
    next = (rank + 1) % size;
    prev = (rank + size - 1) % size;
    if (0 == rank) {
        message = 10;
        printf("[Process %2d] Sending message %d to Process %d (%d processes in ring)\n",rank,message,next,size);
        MPI_Send(&message, 1, MPI_INT, next, tag, MPI_COMM_WORLD);
    }

    while (1) {
        printf("[Process %2d] Waiting to receive message from Process %d \n",rank,prev);
        MPI_Recv(&message, 1, MPI_INT, prev, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("[Process %2d] Message Received %d from Process  %d\n",rank,message,prev);
        --message;
        printf("[Process %2d] Decrementing message to value: %d\n",rank,message);
        printf("[Process %2d] Sending message to Process %d  \n",rank,next);
        MPI_Send(&message, 1, MPI_INT, next, tag, MPI_COMM_WORLD);
        
        // If the message value is less than the size it means it will not expect another message, thus ending the loop.
        if ( size > message  ) {
            printf("[Process %2d] Exiting   Loop\n", rank);
            break;
        }
    }

    /* receive the last message sent from the loop*/
    if ((10 % size + 1) == rank) {
        printf("[Process %2d] Waiting to receive message from Process %d to end.\n",rank,prev);
        MPI_Recv(&message, 1, MPI_INT, prev, tag, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        printf("[Process %2d] Message Received %d from Process  %d\n",rank,message,prev);
    }

    printf("[Process %2d] Ended \n",rank);
    /* All done */
    MPI_Finalize();
    return 0;
}
