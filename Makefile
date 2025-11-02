CC = gcc
CFLAGS = -Wall -Wextra -pedantic -std=c99 -g
LDFLAGS = -pthread
PROGRAMS = hello lock sematest test_eventseq randprod

all: $(PROGRAMS)

hello: hello.c
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

lock: lock.c
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

sema.o: sema.c sema.h
	$(CC) $(CFLAGS) -c sema.c

sematest: sematest.c sema.o
	$(CC) $(CFLAGS) -o $@ sematest.c sema.o $(LDFLAGS)

test_eventseq: test_eventseq.c sequencer.c eventcnt.c ringbuf.c
	$(CC) $(CFLAGS) -o $@ test_eventseq.c sequencer.c eventcnt.c ringbuf.c $(LDFLAGS)

randprod: randprod.c sequencer.c eventcnt.c ringbuf.c
	$(CC) $(CFLAGS) -o $@ randprod.c sequencer.c eventcnt.c ringbuf.c $(LDFLAGS)

clean:
	rm -f $(PROGRAMS) *.o

.PHONY: all clean
