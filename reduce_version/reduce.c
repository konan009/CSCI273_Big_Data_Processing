#include <stdio.h>
#include <mpi.h>

int main(int argc, char *argv[]) {
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int local_value = rank; 
    int global_sum = 0;

    printf("[Process %.2d] Started \n", rank );
    MPI_Allreduce(&local_value, &global_sum, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    if ( rank == 0 ){
      printf("[Process %.2d] Total Sum : %d\n", rank, global_sum);
    }
    printf("[Process %.2d] Ended \n", rank );
    MPI_Finalize();
    return 0;
}