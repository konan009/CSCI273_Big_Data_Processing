EXECS=execute clean
MPICC?=mpicc
MPIRUN?=mpirun
CODE_FILENAME?=ring_mpi

all: ${EXECS}

execute: 
	clear
	${MPICC} -o ${CODE_FILENAME} ${CODE_FILENAME}.c
	${MPIRUN} -np 6 --hostfile ./myhost ./${CODE_FILENAME}

clean:
	rm -f ./${CODE_FILENAME}