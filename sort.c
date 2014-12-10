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
#include <stdlib.h>
#include <limits.h>
#include <unistd.h>
#include <stdio.h>
#include <mpi.h>

typedef long long ll;
typedef long double ld;
typedef long long item;

#define MAX_BUFFER  (536870912 / sizeof(item))   // 512 MB
#define MAX_BUCKET  (67108864 / sizeof(item))    // 64MB
#define NUM_BUCKETS 8

void quicksort(item* array, int begin, int end);

int main(int argc, char** argv)
{
  // Variables
  int proc_count;
  int proc_rank;
  int proc_name_length;
  char proc_name[MPI_MAX_PROCESSOR_NAME];
  item* buffer;         // buffer of items to be sorted
  item** buckets;     // Buckets
  int* bucket_sizes;  // Sizes of various buckets
  FILE* fp;

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
  
  // Allocate buffer and buckets
  buffer = malloc(sizeof(item) * MAX_BUFFER);
  buckets = malloc(sizeof(item*) * NUM_BUCKETS);
  bucket_sizes = malloc(sizeof(int) * NUM_BUCKETS);
  for (i = 0; i < NUM_BUCKETS; i++)
  {
    buckets[i] = malloc(sizeof(item) * MAX_BUCKET);
    bucket_sizes[i] = 0;
  }  
  
  // Go through all data and find min/max
  item min = LLONG_MAX, max = LLONG_MIN;
  
  if (proc_rank == 0)
  {
    // Open every file to read through and determine min/max
    for (i = 1; i < argc; ++i)
    {
      fopen(fp, "r");
      
      // Read through file
      while ((j = fread(buffer, sizeof(item), MAX_BUFFER, fp)) && j > 0)
      {
        for (k = 0; k < j; ++k)
        {
          if (buffer[k] < min) min = buffer[k];
          if (buffer[k] > max) max = buffer[k];
        }
      }
      
      fclose(fp);
    }
  }
  
  // Begin sorting data into buckets
  ld increment = (((ld) max) - ((ld) min)) / NUM_BUCKETS;
  ld offset = ((ld) -min) / NUM_BUCKETS;
  int bucket;
  
  if (proc_rank == 0)
  {
    // Open every file to read through and determine min/max
    for (i = 1; i < argc; ++i)
    {
      fopen(fp, "r");
      
      // Read through file
      while ((j = fread(buffer, sizeof(item), MAX_BUFFER, fp)) && j > 0)
      {
        for (k = 0; k < j; ++k)
        {
          bucket = (int) ((buffer[k] / increment) + offset);
          buckets[bucket][bucket_sizes[bucket]] = buffer[k];
          ++bucket_sizes[bucket];
          
          // If bucket is full, offload to other process for sorting
        }
      }
      
      fclose(fp);
    }
  }
  
  // when a bucket is full, send it to another process
  // when buffer is empty, open next file and load up items
  // When all items have been sorted, start merging results

  // Free up resources
  free(buffer);
  free(bucket_sizes);
  for (i = 0; i < NUM_BUCKETS; i++)
    free(buckets[i]);
  free(buckets);

  MPI_Finalize();

  return 0;
}

void swap(item& x, item& y)
{
  item temp = x;
  x = y;
  y = temp;
}

// Keep in mind that last valid index is end-1
void quicksort(item* array, int begin, int end)
{
  int i, j, p;   // (p)ivot

  // Terminating condition
  if (end - begin == 2 && array[begin] > array[begin + 1])
    swap(array[begin], array[begin + 1]);
  if (end - begin <= 2)
    return;

  // Choose pivot
  p = (end - begin) / 2;
  if ((array[p] < array[begin] && array[begin] < array[end-1])
      || (array[end-1] < array[begin] && array[begin] < array[p]))
    p = begin;
  else if ((array[p] < array[end-1] && array[end-1] < array[begin])
      || (array[begin] < array[end-1] && array[end-1] < array[p]))
    p = end-1;
  
  // Move pivot to front
  swap(array[begin], array[p]);
  
  // Initialize iterators
  i = 1;
  j = end-1;
  
  // Swap items that are out of order
  while (i < j)
  {
    while(array[i] <= array[begin] && i < j) ++i;
    while(array[j] > array[begin] && i < j) --j;
    if (i < j && array[i] > array[begin] && array[j] <= array[begin])
      swap(array[i], array[j]);
  }
  
  // Move pivot to center-most position
  swap(array[begin], array[i-1]);
  
  // Make recursive calls!
  quicksort(array, begin, i);
  quicksort(array, i, end);
  
  // Everything is sorted at this point.
}



