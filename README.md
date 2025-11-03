# 🚀 **Threaded Programming Milestones**
This project explores the evolution of **pthread-based concurrency in C** — from baby threads to full-grown producer–consumer synchronization. Each milestone adds a new synchronization primitive or coordination concept, demonstrating real-world parallel control patterns.

## ⚙️ **Build & Run**
make all

🎯 Targets you can build individually:
```
make hello
make lock
make sematest
make test_eventseq
make randprod
make clean
```
🧠 All programs compile with:
gcc -Wall -Wextra -pedantic -std=c99 -pthread -g

## 🧩 **Milestones Overview**

### 🧵 **1. Hello**
The humble beginning — a parent and child thread proving they can coexist (barely). Shows basic thread creation and joining.
```
$ ./hello
Hello Parent
Hello Child
```
### 🔒 **2. Lock**
Parent and child share a message safely using a mutex and condition variable. No data races, no chaos — just polite thread communication.
```
$ ./lock
Parent: Enter a line to share with the child thread:
This message blocks until the child sees it!
Child received: This message blocks until the child sees it!
Parent: Press Enter to exit.
```
### 🧮 **3. Semaphore Test**
Implements a counting semaphore and compares it to a mutex. Watch threads enter and leave the critical section based on available permits.
```
$ ./sematest
=== Semaphore demo (capacity = 2) ===
[Semaphore] Worker 0 entering critical section
[Semaphore] Worker 1 entering critical section
[Semaphore] Worker 0 leaving critical section
[Semaphore] Worker 2 entering critical section
=== Mutex demo (capacity = 1) ===
[Mutex] Worker 0 entering critical section
```
### ⚡ **4. Event Counter & Sequencer Demo**
Combines a sequencer, event counter, and ring buffer to show atomic ticketing, blocking waits, and bounded buffering. Think of it as the control tower for thread coordination.
```
$ ./test_eventseq
=== Sequencer Demo ===
Ticket 0 -> 1000
=== Event Counter Demo ===
Worker advanced counter to 1
=== Ring Buffer Demo ===
Producer queued value 1
Consumer processed value 1
```
### 🎰 **5. Random Producer–Consumer**
The final boss. An interactive producer–consumer setup that:
- Pulls bytes from /dev/random
- Tags entries with sequencer tickets
- Coordinates progress with event counters
- Stores everything in a synchronized ring buffer
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
## 🧠 **Notes & Insights**
- 🌀 Ring buffer supports graceful shutdown — blocked threads wake cleanly without races.
- 🎲 /dev/random is used for entropy; may block if system randomness is low.
- 🧯 Every pthread call is checked and logged for safety.
- 💡 Designed to be modular, readable, and teachable — each milestone builds upon the previous one.

✨ “Concurrency is not about threads — it’s about coordination.”

