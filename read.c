#include <stdio.h>

typedef long long item;

#define BUFER_SIZE  1048576

int main(int argc, char** argv)
{
  FILE *fp;
  
  if (argc < 2)
  {
    printf("Usage:\n  %s <input file>", argv[0]);
    return 0;
  }
  
  fp = fopen(argv[1], "r");
  
  int num;
  item buffer[BUFFER_SIZE];
  
  while(num = read(buffer, sizeof(item), BUFFER_SIZE, fp))
  {
    int i;
    for (i = 0; i < num; ++i)
      printf(" %*d", 19, buffer[i]);
      if (i % 4 == 3)
        printf("\n");
  }
  
  fclose(fp);
  return 0;
}
