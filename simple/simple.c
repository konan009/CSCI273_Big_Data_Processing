#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main(int argc, char** argv) {

    MPI_Init(&argc, &argv);
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    // We are assuming at least 2 processes for this task
    if (world_size < 4) {
        fprintf(stderr, "World size must be greater than 1 for %s\n", argv[0]);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    
    int number;

    printf(" World Rank %d \n",world_rank);

    if (world_rank == 0) {
        // If we are rank 0, set the number to -1 and send it to process 1
        number = -1;
        MPI_Send(
            /* data         = */ &number, 
            /* count        = */ 1, 
            /* datatype     = */ MPI_INT, 
            /* destination  = */ 1, 
            /* tag          = */ 0, 
            /* communicator = */ MPI_COMM_WORLD
        );
        
        MPI_Send(
            /* data         = */ &number, 
            /* count        = */ 1, 
            /* datatype     = */ MPI_INT, 
            /* destination  = */ 2, 
            /* tag          = */ 0, 
            /* communicator = */ MPI_COMM_WORLD
        );
    } else if (world_rank == 1) {
        MPI_Recv(
            /* data         = */ &number, 
            /* count        = */ 1, 
            /* datatype     = */ MPI_INT, 
            /* source       = */ 0, 
            /* tag          = */ 0, 
            /* communicator = */ MPI_COMM_WORLD, 
            /* status       = */ MPI_STATUS_IGNORE
        );
        printf("Process %d received number %d from process 0\n",world_rank, number);
    } else if (world_rank == 2) {
        MPI_Recv(
            /* data         = */ &number, 
            /* count        = */ 1, 
            /* datatype     = */ MPI_INT, 
            /* source       = */ 0, 
            /* tag          = */ 0, 
            /* communicator = */ MPI_COMM_WORLD, 
            /* status       = */ MPI_STATUS_IGNORE
        );
        printf("Process %d received number %d from process 0\n",world_rank, number);
    } 
    MPI_Finalize();
}