#!/bin/sh
clear
mpicc -o simple simple.c
mpirun -np 4 --hostfile ./myhost ./simple
rm ./simple