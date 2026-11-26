/* Two-host placement and integer reduction acceptance test. */
#include <mpi.h>
#include <stdio.h>
#include <string.h>

int main(int argc, char **argv) {
    int rank, size, length, sum;
    char name[MPI_MAX_PROCESSOR_NAME] = {0};
    char names[2][MPI_MAX_PROCESSOR_NAME] = {{0}};
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    if (size != 2) MPI_Abort(MPI_COMM_WORLD, 1);
    MPI_Get_processor_name(name, &length);
    MPI_Allgather(name, sizeof(name), MPI_CHAR, names, sizeof(name), MPI_CHAR, MPI_COMM_WORLD);
    int value = rank + 1;
    MPI_Allreduce(&value, &sum, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    int ok = sum == 3 && strcmp(names[0], names[1]) != 0;
    if (rank == 0) printf("distinct_hosts=%d ranks=%d sum=%d\n",
                          strcmp(names[0], names[1]) != 0, size, sum);
    MPI_Finalize();
    return ok ? 0 : 1;
}
