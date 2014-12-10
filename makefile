sort: sort.c
	mpicc -g -Wall -std=c99 -lm -o $@ $^

read: read.c
	gcc -g -Wall -std=c99 -o $@ $^

generate: generate.c
	gcc -g -Wall -std=c99 -o $@ $^
