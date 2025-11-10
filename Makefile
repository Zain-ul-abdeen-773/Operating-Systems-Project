CC = gcc
CFLAGS = -Wall -Wextra -pedantic -std=c99 -g
CPPFLAGS = -I"Milestone 1" -I"Milestone 2" -I"Milestone 3" -I"Milestone 4" -I"Milestone 5"
LDFLAGS = -pthread

# Directories (escaped spaces for make prerequisites and shell commands)
M1 := Milestone\ 1
M2 := Milestone\ 2
M3 := Milestone\ 3
M4 := Milestone\ 4
M5 := Milestone\ 5

PROGRAMS = hello lock sematest test_eventseq randprod

all: $(PROGRAMS)

hello: $(M1)/hello.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -o $@ $(M1)/hello.c $(LDFLAGS)

lock: $(M2)/lock.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -o $@ $(M2)/lock.c $(LDFLAGS)

sematest: $(M3)/sematest.c $(M3)/sema.c $(M3)/sema.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -o $@ $(M3)/sematest.c $(M3)/sema.c $(LDFLAGS)

test_eventseq: $(M4)/test_eventseq.c $(M4)/sequencer.c $(M4)/eventcnt.c $(M4)/ringbuf.c $(M4)/sequencer.h $(M4)/eventcnt.h $(M4)/ringbuf.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -o $@ $(M4)/test_eventseq.c $(M4)/sequencer.c $(M4)/eventcnt.c $(M4)/ringbuf.c $(LDFLAGS)

randprod: $(M5)/randprod.c $(M4)/sequencer.c $(M4)/eventcnt.c $(M4)/ringbuf.c $(M4)/sequencer.h $(M4)/eventcnt.h $(M4)/ringbuf.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -o $@ $(M5)/randprod.c $(M4)/sequencer.c $(M4)/eventcnt.c $(M4)/ringbuf.c $(LDFLAGS)

clean:
	rm -f $(PROGRAMS) *.o

.PHONY: all clean
