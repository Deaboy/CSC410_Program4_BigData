#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

int main(int argc, char** argv)
{
  // Variables
  int proc_count;
  int proc_rank;
  int proc_name_length;
  char proc_name[MPI_MAX_PROCESOR_NAME];
  
  // Initialize MPI stuff
  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &proc_count);
  MPI_Comm_rank(MPI_COMM_WORLD, &proc_rank);
  MPI_Get_processor_name(proc_name, &prog_name_length);

  printf("Hello from process %d of %d on %s!",
         proc_rank, proc_count, proc_name);

  MPI_Finalize();

  return 0;
}



