/**
 * Sorting Big Data
 *
 * Authors:
 *   Daniel Andrus, Erik Hattervig
 *
 * Description
 * This program will take lare amount of data spread across multiple
 * files and attempt to sort them in ascending order using multiple
 * processes. The program uses MPI to parallelize the work and to
 * handle communication.
 *
 * Compilation:
 *   mpicc -g -Wall -std=c99 -lm -o sort sort.c
 *
 * Usage:
 *   mpiexec [-n <processes>] [-hostfile <host_file>] ./sort
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <mpi.h>

typedef long long ll;
typedef long double ld;
typedef long long item;

#define MAX_POOL    (536870912 / sizeof(item))   // 512 MB
#define MAX_BUCKET  (67108864 / sizeof(item))    // 64MB
#define NUM_BUCKETS 8

int main(int argc, char** argv)
{
  // Variables
  int proc_count;
  int proc_rank;
  int proc_name_length;
  char proc_name[MPI_MAX_PROCESSOR_NAME];
  item* pool;         // Pool of items to be sorted
  item** buckets;     // Buckets
  int* bucket_sizes;  // Sizes of various buckets

  // Temp variables
  int i;
  int j;
  int k;

  // Initialize MPI stuff
  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &proc_count);
  MPI_Comm_rank(MPI_COMM_WORLD, &proc_rank);
  MPI_Get_processor_name(proc_name, &proc_name_length);
  
  // Verify arguments
  if (argc < 2)
  {
    if (proc_rank == 0)
      printf("Usage:\n  %s <file1> [file2] [file3]...\n", argv[0]);
    return 0;
  }
  
  // Verify file access
  for (i = 1; i < argc; ++i)
  {
    if (access(argv[i], R_OK | W_OK) == -1)
    {
      if (proc_rank == 0)
        printf("Error: unable to open file %s for reading/writing\n", argv[i]);
      return 0;
    }
  }
  
  // Allocate pool and buckets
  pool = malloc(sizeof(item) * MAX_POOL);
  buckets = malloc(sizeof(item*) * NUM_BUCKETS);
  bucket_sizes = malloc(sizeof(int) * NUM_BUCKETS);
  for (i = 0; i < NUM_BUCKETS; i++)
    buckets[i] = malloc(sizeof(item) * MAX_BUCKET);
  
  // Determine which files to open
  // Somehow find min/max
  // Begin bucket sort
  // when a bucket is full, send it to another process
  // when pool is empty, open next file and load up items
  // When all items have been sorted, start merging results

  // Free up resources
  free(pool);
  free(bucket_sizes);
  for (i = 0; i < NUM_BUCKETS; i++)
    free(buckets[i]);
  free(buckets);

  MPI_Finalize();

  return 0;
}



