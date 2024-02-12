#!/bin/sh
clear
mpicc -o sum_n sum_n.c
mpirun -np 15 --hostfile ./myhost ./sum_n