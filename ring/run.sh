#!/bin/sh
clear
mpicc -o ring_c ring_mpi.c
mpirun -np 7 --hostfile ./myhost ./ring_c