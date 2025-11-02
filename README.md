# Threaded Programming Milestones

This project showcases five incremental milestones that explore pthread-based concurrency patterns in C.

## Build Instructions

```
make all
```

Individual targets:

- `make hello`
- `make lock`
- `make sematest`
- `make test_eventseq`
- `make randprod`
- `make clean`

All programs are built with `gcc -Wall -Wextra -pedantic -std=c99 -pthread -g`.

## Milestones

### 1. hello
Demonstrates basic thread creation and joining.

Sample run:
```
$ ./hello
Hello Parent
Hello Child
```

### 2. lock
Uses a mutex and condition variable to synchronize a parent and child thread exchanging a text line.

Sample run:
```
$ ./lock
Parent: Enter a line to share with the child thread:
This message blocks until the child sees it!
Child received: This message blocks until the child sees it!
Parent: Press Enter to exit.
```

### 3. sematest
Implements a counting semaphore and contrasts its behavior with a mutex.

Sample run:
```
$ ./sematest
=== Semaphore demo (capacity = 2) ===
[Semaphore] Worker 0 entering critical section
[Semaphore] Worker 1 entering critical section
[Semaphore] Worker 0 leaving critical section
[Semaphore] Worker 2 entering critical section
...
=== Mutex demo (capacity = 1) ===
[Mutex] Worker 0 entering critical section
...
```

### 4. test_eventseq
Combines the sequencer, event counter, and ring buffer implementations to highlight ordering, blocking waits, and bounded buffering.

Sample run:
```
$ ./test_eventseq
=== Sequencer Demo ===
Ticket 0 -> 1000
...
=== Event Counter Demo ===
Worker advanced counter to 1
...
=== Ring Buffer Demo ===
Producer queued value 1
Consumer processed value 1
...
```

### 5. randprod
Interactive producer/consumer that reads entropy from `/dev/random`, tags entries with sequencer tickets, coordinates readiness via event counters, and stores bytes in a ring buffer.

Sample session:
```
$ ./randprod -b 8 -f 2
[Prefill] ticket=0 value=177
[Prefill] ticket=1 value=92
Commands: print N | exit
> print 3
[Consumer] ticket=0 value=177 (total=1)
[Consumer] ticket=1 value=92 (total=2)
[Producer] ticket=2 value=64
[History] index=0 value=177
[History] index=1 value=92
[History] index=2 value=64
> exit
[Producer] stopping.
[Consumer] stopping.
```

## Notes

- The ring buffer exposes `rb_close` to wake blocked threads during shutdown without data races.
- `read_random_byte` fetches entropy from `/dev/random` and may block until sufficient randomness is available; comments in the code highlight this behavior.
- All pthread calls are checked for errors and print diagnostics when failures occur.
