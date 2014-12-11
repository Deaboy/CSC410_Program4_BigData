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
#include <string.h>
#include <mpi.h>

typedef long long ll;
typedef long double ld;
typedef long long item;

#define MAX_BUFFER  (536870912 / sizeof(item))   // 512 MB
#define MAX_BUCKET  (67108864 / sizeof(item))    // 64 MB
#define NUM_BUCKETS 8
// #define DEBUG

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
  int num_buckets = NUM_BUCKETS;
  int tag;
  char* filename;
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
      printf("Usage:\n  %s <file1> [file2] [file3]... <output prefix>\n", argv[0]);
    MPI_Abort(MPI_COMM_WORLD, 1);
    return 0;
  }
  
  // Verify process count
  if (proc_count < 3)
  {
    if (proc_rank == 0)
      printf("Error: number of processes must be 3 or greater!\n");
    MPI_Abort(MPI_COMM_WORLD, 1);
    return 0;
  }
  num_buckets = proc_count - 1;
  
#ifdef DEBUG
  if (proc_rank == 0) printf("0. num_buckets = %d\n", num_buckets);
#endif
  
  // Verify file access
  if (proc_rank == 0)
  {
    for (i = 1; i < argc-1; ++i)
    {
      if (access(argv[i], R_OK | W_OK) == -1)
      {
        printf("Error: unable to open file %s for reading\n", argv[i]);
        MPI_Abort(MPI_COMM_WORLD, 1);
        return 0;
      }
    }
  }
  
  // Allocate buffer and buckets
  buffer = malloc(sizeof(item) * MAX_BUFFER);
  filename = malloc(strlen(argv[argc-1]) + 100);
  
  // Go through all data and find min/max
  item min = LLONG_MAX, max = LLONG_MIN;
  
  if (proc_rank == 0)
  {
    // Open every file to read through and determine min/max
    for (i = 1; i < argc-1; ++i)
    {
      fp = fopen(argv[i], "r");
      
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
  
#ifdef DEBUG
  if (proc_rank == 0)
    printf("1. made it here\n");
#endif
  
  // Begin sorting data into buckets
  int bucket;
  
  if (proc_rank == 0)
  {
    // Allocate space for buckets
    buckets = malloc(sizeof(item*) * num_buckets);
    bucket_sizes = malloc(sizeof(int) * num_buckets);
    for (i = 0; i < num_buckets; i++)
    {
      buckets[i] = malloc(sizeof(item) * MAX_BUCKET);
      bucket_sizes[i] = 0;
    }
#ifdef DEBUG
    printf("2. allocated space\n");
#endif
    
    // sort file contents into buckets
    for (i = 1; i < argc-1; ++i)
    {
      fp = fopen(argv[i], "r");
      
#ifdef DEBUG
      printf("3. opened file %s\n", argv[i]);
#endif
      
      // Read through file
      while ((j = fread(buffer, sizeof(item), MAX_BUFFER, fp)) && j > 0)
      {
#ifdef DEBUG
        printf("4. read %d items\n", j);
#endif

        for (k = 0; k < j; ++k)
        {
          bucket = (int) (((buffer[k] - min) / ((ld) max - (ld) min + 1)) * num_buckets);
#ifdef DEBUG
          printf("5. placing %lld into bucket %d\n", buffer[k], bucket);
#endif
          buckets[bucket][bucket_sizes[bucket]] = buffer[k];
          ++bucket_sizes[bucket];
          
          // If bucket is full, offload to other process for sorting
          if (bucket_sizes[bucket] >= MAX_BUCKET)
          {
            // Send "sort this content!" code
            tag = 0;
            MPI_Send(&tag, 1, MPI_INT, bucket+1, 0, MPI_COMM_WORLD);
            
            // Send bucket size
            MPI_Send(&bucket_sizes[bucket], 1, MPI_INT, bucket+1, 2,
              MPI_COMM_WORLD);
            
            // Send bucket contents
            MPI_Send(buckets[bucket], bucket_sizes[bucket], MPI_LONG_LONG,
              bucket+1, 1, MPI_COMM_WORLD);
            
            // Empty bucket
            bucket_sizes[bucket] = 0;
          }
        }
      }
      
      fclose(fp);
      
#ifdef DEBUG
      printf("6. closed file\n");
#endif
    }
    
    // Send all unempty buckets
    for (bucket = 0; bucket < num_buckets; bucket++)
    {
      if (bucket_sizes[bucket] > 0)
      {
        // Send "sort this content!" code
        tag = 0;
        MPI_Send(&tag, 1, MPI_INT, bucket+1, 0, MPI_COMM_WORLD);
        
        // Send bucket size
        MPI_Send(&bucket_sizes[bucket], 1, MPI_INT, bucket+1, 2,
          MPI_COMM_WORLD);
        
        // Send bucket contents
        MPI_Send(buckets[bucket], bucket_sizes[bucket], MPI_LONG_LONG,
          bucket+1, 1, MPI_COMM_WORLD);
        
        // Empty bucket
        bucket_sizes[bucket] = 0;
      }
    }
    
#ifdef DEBUG
    printf("6. finished bucketting\n");
#endif
    
    // Send "merge your bucket!" code to all processes
    tag = 1;
    for (i = 1; i < proc_count; ++i)
      MPI_Send(&tag, 1, MPI_INT, i, 0, MPI_COMM_WORLD);

    // Free up resources
    free(buffer);
    free(bucket_sizes);
    for (i = 0; i < num_buckets; i++)
      free(buckets[i]);
    free(buckets);
    
    // Listen for "all done!" message from processes
    for (i = 1; i < proc_count; ++i)
      MPI_Recv(&tag, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    printf("Sorting complete! Sorted data will be spread across files with the"
          " prefix %s\n", argv[argc-1]);
  }
  else
  {
    i = 0;
    
#ifdef DEBUG
    printf("2. %d waiting for tag\n", proc_rank);
#endif
    
    // All slave processes wait until they receive a message from master and
    // act accordingly
    MPI_Recv(&tag, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    while (tag == 0)
    {
    
#ifdef DEBUG
      printf("3. %d received bucket\n", proc_rank);
#endif

      // Receive size of bucket
      MPI_Recv(&j, 1, MPI_INT, 0, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      
      // Receive bucket and store into buffer
      MPI_Recv(buffer, j, MPI_LONG_LONG, 0, 1, MPI_COMM_WORLD,
        MPI_STATUS_IGNORE);
      
      // Run quicksort on buffer
      quicksort(buffer, 0, j);
      
      // Output results to file
      sprintf(filename, "%s_tmp_%d_%d", argv[argc-1], proc_rank, i+1);
      i++;
      fp = fopen(filename, "w");
      fwrite(buffer, (sizeof(item)), j, fp);
      fclose(fp);
      
      // Receive next command code
      MPI_Recv(&tag, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
    
    // Allocate space for merging
    buckets = malloc(sizeof(item*) * i);
    bucket_sizes = malloc(sizeof(int) * i * 2);
    for (j = 0; j < i; j++)
    {
      buckets[j] = malloc(sizeof(item) * MAX_BUCKET);
      bucket_sizes[j*2] = 0;
      bucket_sizes[j*2+1] = 0;
    }
    
    // Parts of bucket are individually sorted. Now to merge them
    
    // Open files for merging
    FILE** files = malloc(sizeof(FILE*) * i);
    for (j = 0; j < i; j++)
    {
      sprintf(filename, "%s_tmp_%d_%d", argv[argc-1], proc_rank, j+1);
      files[j] = fopen(filename, "r");
      bucket_sizes[j*2+1] = fread(buckets[j], sizeof(item), MAX_BUCKET, files[j]);
      bucket_sizes[j*2] = 0;
    }
    
    // Open file for writing
    sprintf(filename, "%s_%d.out", argv[argc-1], proc_rank);
    fp = fopen(filename, "w");
    
    // Stopping condition
    tag = 0;
    for (j = 0; j < i; j++)
    {
      if (bucket_sizes[j*2+1] > 0)
      {
        tag = 1;
        break;
      }
    }
    max = 0;
    
    // Begin merging
    while (tag)
    {
      min = LLONG_MAX;
      for (j = 0; j < i; j++)
      {
        if (bucket_sizes[j*2+1] > 0
          && bucket_sizes[j*2] < bucket_sizes[j*2+1]
          && buckets[j][bucket_sizes[j*2]] <= min)
        {
          min = buckets[j][bucket_sizes[j*2]];
          k = j;
        }
      }
      
      // Insert item into front of buffer
      buffer[max] = buckets[k][bucket_sizes[k*2]];
      max++;
      
      // remove item from front of chosen bucket
      bucket_sizes[k*2]++;
      
      // If buffer is full, dump into file
      if (max >= MAX_BUFFER)
      {
        fwrite(buffer, sizeof(item), max, fp);
        max = 0;
      }
      
      // If file's buffer is empty, load more numbers up!
      if (bucket_sizes[k*2+1] > 0
        && bucket_sizes[k*2] >= bucket_sizes[k*2+1])
      {
        bucket_sizes[k*2+1] = fread(buckets[k], sizeof(item), MAX_BUCKET, files[k]);
        bucket_sizes[k*2] = 0;
      }
      
      // Determine whether to continue or not
      tag = 0;
      for (j = 0; j < i; j++)
      {
        if (bucket_sizes[j*2+1] > 0)
        {
          tag = 1;
          break;
        }
      }
    }
    
    // If buffer has stuff, dump into file
    if (max > 0)
    {
      fwrite(buffer, sizeof(item), max, fp);
      max = 0;
    }
    
    // Close files
    for (j = 0; j < i; j++)
    {
      sprintf(filename, "%s_tmp_%d_%d", argv[argc-1], proc_rank, j+1);
      fclose(files[j]);
      remove(filename);
    }
    fclose(fp);
    
    // Free up resources
    free(files);
    free(buffer);
    free(bucket_sizes);
    for (j = 0; j < i; j++)
      free(buckets[j]);
    free(buckets);
    
    tag = 1;
    MPI_Send(&tag, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
  }

  MPI_Finalize();

  return 0;
}

void swap(item* x, item* y)
{
  item temp = *x;
  *x = *y;
  *y = temp;
}

// Keep in mind that last valid index is end-1
void quicksort(item* array, int begin, int end)
{
  int i, j, p;   // (p)ivot

  // Terminating condition
  if (end - begin == 2 && array[begin] > array[begin + 1])
    swap(&array[begin], &array[begin + 1]);
  if (end - begin <= 2)
    return;

  // Choose pivot
  p = begin + ((end - begin) / 2);
  if ((array[p] < array[begin] && array[begin] < array[end-1])
      || (array[end-1] < array[begin] && array[begin] < array[p]))
    p = begin;
  else if ((array[p] < array[end-1] && array[end-1] < array[begin])
      || (array[begin] < array[end-1] && array[end-1] < array[p]))
    p = end-1;
  
  // Move pivot to front
  swap(&array[begin], &array[p]);
  
  // Initialize iterators
  i = 1;
  j = end-1;
  
  // Swap items that are out of order
  while (i <= j)
  {
    while(array[i] <= array[begin] && i <= j) ++i;
    while(array[j] > array[begin] && i <= j) --j;
    if (i < j && array[i] > array[begin] && array[j] <= array[begin])
      swap(&array[i], &array[j]);
  }
  
  // Move pivot to center-most position
  swap(&array[begin], &array[i-1]);
  
  // Make recursive calls!
  quicksort(array, begin, i-1);
  quicksort(array, i, end);
  
  // Everything is sorted at this point.
}



