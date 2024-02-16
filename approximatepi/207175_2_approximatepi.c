#include <stdio.h>
#include <mpi.h>

int main(argc,argv)
int argc;
char *argv[];
{
   int rank, interval, size;
   double delta_x,sum,x_mid,height,approx_pi,local_pi;
   int ROOT_RANK = 0;

   MPI_Init(&argc, &argv);
   MPI_Comm_rank(MPI_COMM_WORLD, &rank);
   MPI_Comm_size(MPI_COMM_WORLD, &size);
   
   if (rank == ROOT_RANK) {
      printf("Enter interval number:");
      fflush(stdout);  
      scanf("%d",&interval);
	}
	MPI_Bcast(&interval, 1, MPI_INT, 0, MPI_COMM_WORLD);

   delta_x = 1.0 / (double) interval;
   sum = 0.0;
   for (double i = rank + 1; i <= interval; i += size) {
      x_mid = ((double)i - 0.5) * delta_x;
      height = 4.0 / (1.0 + x_mid * x_mid);
      sum += height;
	}
   local_pi = delta_x * sum;
   printf("[Process %d] local_pi = %.16f \n", rank, local_pi);
   MPI_Reduce(&local_pi, &approx_pi, 1, MPI_DOUBLE, MPI_SUM, ROOT_RANK, MPI_COMM_WORLD);
   
   if ( rank == ROOT_RANK ){
      printf("[Process %d] Pi is ≈ %.16f \n", rank, approx_pi);
   }

   printf("[Process %d] Ended \n", rank );
   MPI_Finalize();
   return 0;
}
