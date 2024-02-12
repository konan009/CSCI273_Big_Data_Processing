#!/bin/sh
clear
mpicc -o reduce reduce.c
mpirun -np 15 --hostfile ./myhost ./reduce
rm ./reduce