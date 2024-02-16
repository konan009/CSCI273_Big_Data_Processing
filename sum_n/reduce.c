#include <stdio.h>
#include <mpi.h>

int main(int argc, char *argv[]) {
    int rank, size;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int reduction_result = 0;

    printf("[Process %2d] Starting \n",rank);

    MPI_Reduce(&rank, &reduction_result, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    if ( rank == 0 ){
      printf("[Process %2d] Total Sum : %d\n", rank, reduction_result);
    }

    printf("[Process %2d] Ended \n", rank );
    MPI_Finalize();
    return 0;
}
