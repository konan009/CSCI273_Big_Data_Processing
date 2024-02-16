EXECS=compile execute clean
MPICC?=mpicc
MPIRUN?=mpirun
CODE_FILENAME?=sum_n
all: ${EXECS}

compile:
	clear
	${MPICC} -o ${CODE_FILENAME} ${CODE_FILENAME}.c

execute: 
	${MPIRUN} -np 10 --hostfile ./myhost ./${CODE_FILENAME}

clean:
	rm -f ./${CODE_FILENAME}