EXECS=execute clean
MPICC?=mpicc
MPIRUN?=mpirun
CODE_FILENAME?=approximatepi
all: ${EXECS}

execute: 
	clear
	${MPICC} -o ${CODE_FILENAME} ${CODE_FILENAME}.c
	${MPIRUN} -np 4 --hostfile ./myhost ./${CODE_FILENAME}

clean:
	rm -f ./${CODE_FILENAME}
