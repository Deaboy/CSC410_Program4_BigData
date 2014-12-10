sort: sort.c
	mpicc -g -Wall -std=c99 -lm -o $@ $^

read: read.c
	mpicc -g -Wall -std=c99 -lm -o $@ $^
