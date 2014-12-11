#include <stdio.h>
#include <stdlib.h>

typedef long long item;

#define BUFFER_SIZE  1048576

int main(int argc, char** argv)
{
  if (argc < 2)
  {
    printf("Usage:\n  %s <input file>\n", argv[0]);
    return 0;
  }
  
  FILE *fp;
  
  fp = fopen(argv[1], "r");
  
  int num;
  item* buffer = malloc(sizeof(item) * BUFFER_SIZE);
  
  while((num = fread(buffer, sizeof(item), BUFFER_SIZE, fp)) && num > 0)
  {
    int i;
    for (i = 0; i < num; ++i)
    {
      if (i % 2 == 0)
        printf("\n");
      printf(" %*lld", 39, buffer[i]);
    }
  }
  printf("\n");
  
  free(buffer);
  fclose(fp);
  return 0;
}
