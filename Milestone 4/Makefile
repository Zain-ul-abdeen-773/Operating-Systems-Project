# =============================================================
#  CS-311 Project Milestone 4 - Makefile
#  Builds and runs the test_eventseq system (Steps 1–10)
# =============================================================

CC      := gcc
CFLAGS  := -Wall -Wextra -pedantic -std=c99 -g -pthread
SRC     := test_eventseq.c sequencer.c eventcnt.c ringbuf.c
OBJS    := $(SRC:.c=.o)
TARGET  := test_eventseq

# =============================================================
# Default build
# =============================================================

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) $(OBJS) -o $@

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# =============================================================
# Run targets
# =============================================================

# Demo mode (non-interactive showcase)
demo: $(TARGET)
	@echo "=== Running Demo Mode (-d) ==="
	./$(TARGET) -d

# Non-interactive test (buffer=5, fill=2, print 5 items)
run: $(TARGET)
	@echo "=== Running Non-Interactive Mode (-b 5 -f 2 -p 5) ==="
	./$(TARGET) -b 5 -f 2 -p 5

# Interactive mode (manual commands)
interactive: $(TARGET)
	@echo "=== Running Interactive Mode ==="
	@echo "Commands: print N | stats | exit"
	./$(TARGET)

# =============================================================
# Maintenance
# =============================================================

clean:
	rm -f $(OBJS) $(TARGET)

rebuild: clean all

# =============================================================
# Help
# =============================================================

help:
	@echo "Usage:"
	@echo "  make all          - Build program"
	@echo "  make demo         - Run demo mode"
	@echo "  make run          - Run non-interactive test"
	@echo "  make interactive  - Run interactive mode"
	@echo "  make clean        - Remove binaries"
	@echo "  make rebuild      - Clean and rebuild"

# =============================================================
