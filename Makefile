CC = gcc

CFLAGS = -g -std=c17 -D_POSIX_C_SOURCE=200809L \
		 -Wall -Werror -Wextra -pthread\
		 -Wcast-align -Wconversion -Wfloat-equal -Wformat=2 -Wnull-dereference -Wshadow -Wsign-conversion -Wswitch-enum -Wundef -Wunreachable-code -Wunused \
		 -fsanitize=undefined 
# -fsanitize=address removido pois causou problemas no sigma (solução dada no Piazza)

ifneq ($(shell uname -s),Darwin) 
	CFLAGS += -fmax-errors=5
endif

all: kvs

kvs: main.c constants.h operations.o parser.o kvs.o
	$(CC) $(CFLAGS) $(SLEEP) -o kvs main.c operations.o parser.o kvs.o

%.o: %.c %.h
	$(CC) $(CFLAGS) -c ${@:.o=.c}

run: kvs
	@./kvs

clean:
	rm -f *.o kvs

format:
	@which clang-format >/dev/null 2>&1 || echo "Please install clang-format to run this command"
	clang-format -i *.c *.h
