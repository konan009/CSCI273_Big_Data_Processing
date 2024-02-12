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
    int rank, size, next, prev, sum, tag = 201;
    
    /* 
      Start up MPI 
   */
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    /* 
      Calculate the rank of the next process in the ring.  Use the
      modulus operator so that the last process "wraps around" to
      rank zero. 
   */
    next = (rank + 1) % size;
    prev = (rank + size - 1) % size;

    printf("[Process %.2d] Starting\n",rank);
    if (0 == rank) {
        int sum = rank;
        printf("[Process %.2d] Sending sum %.2d to process %.2d \n",rank,sum,next);
        MPI_Send(&sum, 1, MPI_INT, next, tag, MPI_COMM_WORLD);
    }

   printf("[Process %.2d] Waiting message from %.2d \n",rank,prev);
   MPI_Recv(&sum, 1, MPI_INT, prev, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
   printf("[Process %.2d] Message received \n",rank);
   sum += rank;
   printf("[Process %.2d] Message received %.2d\n",rank,sum);

   if (0 != rank) {
      printf("[Process %.2d] Sending sum %.2d to process %.2d \n",rank,sum,next);
      MPI_Send(&sum, 1, MPI_INT, next, tag, MPI_COMM_WORLD);
   }

   printf("[Process %.2d] Sum %.2d\n",rank,sum);
   printf("[Process %.2d] Ended \n",rank);
   /* All done */
   MPI_Finalize();
   return 0;
}
