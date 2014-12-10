#include <stdio.h>
#include <stdlib.h>
#include <time.h>

typedef long long item;

#define BUFFER_SIZE  1048576

int main(int argc, char** argv)
{
  if (argc < 3)
  {
    printf("Usage:\n  %s <output file> <num items>\n", argv[0]);
    return 0;
  }
  
  FILE *fp;
  
  int num = atoi(argv[2]);
  fp = fopen(argv[1], "w");
  item* buffer = malloc(sizeof(item) * BUFFER_SIZE);
  
  srand((unsigned) time(NULL));
  item m1;
  
  m1 = ((unsigned) ~0) >> 1; // lower 31 bits
  
  while (num > 0)
  {
    int i;
    for (i = 0; i < num && i < BUFFER_SIZE; ++i)
    {
      // Generate 64 bit random number
      buffer[i] = ((rand() & m1) << 33) | ((rand() & m1) << 2) | (rand() % 3);
    }
    fwrite(buffer, sizeof(item), i, fp);
    num -= i;
  }
 
  free(buffer); 
  fclose(fp);
  return 0;
}
