#include <stdio.h>
#include "mpi.h"

int main(int argc, char *argv[])
{
    int rank, size, next, prev, tag = 201;
    
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
   float delta_x = 1.0 / (float)size;
   
   next = (rank + 1) % size;
   prev = (rank + size - 1) % size;
   printf("[Process %.2d] Starting \n",rank,prev);
   float pi = 0.0;
   if (0 == rank) {
      float x_mid = (rank + 0.5) * delta_x;
      float height = 4.0 / (1.0 + x_mid * x_mid);
      pi += height;
      printf("[Process %.2d] Sum : %.10f \n",rank,pi);
      MPI_Send(&pi, 1, MPI_INT, next, tag, MPI_COMM_WORLD);
   }else{
      printf("[Process %.2d] Waiting Message from Process %d \n",rank, prev);
      MPI_Recv(&pi, 1, MPI_INT, prev, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      float x_mid = (rank + 0.5) * delta_x;
      float height = 4.0 / (1.0 + x_mid * x_mid);
      pi += height;
      printf("[Process %.2d] Message received from Process %d %.10f \n",rank, prev, pi);
      printf("[Process %.2d] Sending pi to process %.2d \n",rank,next);
      MPI_Send(&pi, 1, MPI_INT, next, tag, MPI_COMM_WORLD);
   }

   if (0 == rank) {
      printf("[Process %.2d] Waiting Message from Process %d \n",rank, prev);
      MPI_Recv(&pi, 1, MPI_INT, prev, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      printf("[Process %.2d] Message received from Process %d \n",rank, prev);
      pi *= delta_x;
      printf("[Process %.2d] Answer Pi =  %.10f \n",rank,pi);
   }

   printf("[Process %.2d] Ended \n",rank);
   MPI_Finalize();
   return 0;
}
