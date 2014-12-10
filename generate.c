#include <stdio.h>
#include <stdlib.h>
#include <time.h>

typedef long long item;

#define BUFER_SIZE  1048576

int main(int argc, char** argv)
{
  FILE *fp;
  
  if (argc < 3)
  {
    printf("Usage:\n  %s <output file> <num items>", argv[0]);
    return 0;
  }
  
  int num = atoi(argv[2]);
  fp = fopen(argv[1], "w");
  item buffer[BUFFER_SIZE];
  
  srand((unsigned) time(NULL));
  item m1, m2;
  
  m1 = (~0) >>> 1; // lower 31 bits
  
  while (num > 0)
  {
    int i;
    for (i = 0; i < num && i < BUFFER_SIZE; ++i)
    {
      // Generate 64 bit random number
      buffer[i] = ((rand() & m1) << 33) | ((rand() & m1) << 2) | (rand() % 3);
    }
    fwrite(buffer, sizeof(item), BUFFER_SIZE, fp);
    num -= i;
  }
  
  fclose(fp);
  return 0;
}
