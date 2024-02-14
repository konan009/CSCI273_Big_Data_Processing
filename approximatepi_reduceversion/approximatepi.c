#include <stdio.h>
#include <mpi.h>

int main(int argc, char *argv[]) {
   int rank, size;
   int ROOT_RANK = 0;

   MPI_Init(&argc, &argv);
   MPI_Comm_rank(MPI_COMM_WORLD, &rank);
   MPI_Comm_size(MPI_COMM_WORLD, &size);
   
   printf("[Process %.2d] Started \n", rank );
   float reduction_result = 0;
   float delta_x = 1.0 / (float)size;
   
   float sum = 0;
   float x_mid = (rank + 0.5) * delta_x;
   float height = 4.0 / (1.0 + x_mid * x_mid);
   sum += height;

   printf( "[Process %.2d] Sending %.4f to process 0 \n", rank, sum);
   MPI_Reduce(&sum, &reduction_result, 1, MPI_FLOAT, MPI_SUM, ROOT_RANK, MPI_COMM_WORLD);
   
   if ( rank == ROOT_RANK ){
      float pi = reduction_result * delta_x;
      printf("[Process %.2d] Pi is ≈ %.10f\n", rank, pi);
   }

   printf("[Process %.2d] Ended \n", rank );
   MPI_Finalize();
   return 0;
}
