define \n


endef
#Test source files MUST be named <source_test_is_for>_tests.c or the compiling wont work
SOURCE_FILES = $(wildcard *.c)
OBJ_FILES = $(SOURCE_FILES:.c=.o)
EXECUTABLE_FILES = $(SOURCE_FILES:.c= )
TESTS = $(addsuffix _tests,$(EXECUTABLE_FILES:_tests= ))
GCC_FLAGS = -I../include -pthread
BUILD_DIR = ../build

.PHONY:	clean run_tests

run_tests: $(TESTS)
	$(foreach test,$(TESTS),@echo ==========$(test)==========$(\n)@./$(test)$(\n))
	@echo ====================================

run_tests_valgrind: $(TESTS)
	$(foreach test,$(TESTS),@echo ==========$(test)==========$(\n)@valgrind ./$(test)$(\n))
	@echo ====================================


%: %.c
	gcc $(GCC_FLAGS) $@.c $(BUILD_DIR)/$(@:_tests=.o) -o $@

filter_tests: filter_tests.c
	gcc $(GCC_FLAGS) filter_tests.c  $(BUILD_DIR)/jobscheduler.o $(BUILD_DIR)/filter.o $(BUILD_DIR)/utils.o $(BUILD_DIR)/intermediates.o -o filter_tests

partition_tests: partition_tests.c
	gcc $(GCC_FLAGS) $@.c $(BUILD_DIR)/$(@:_tests=.o) $(BUILD_DIR)/utils.o $(BUILD_DIR)/hash1.o  $(BUILD_DIR)/jobscheduler.o -o $@

join_tests: join_tests.c
	gcc $(GCC_FLAGS) $@.c $(BUILD_DIR)/intermediates.o $(BUILD_DIR)/$(@:_tests=.o) $(BUILD_DIR)/jobscheduler.o $(BUILD_DIR)/hash1.o $(BUILD_DIR)/partition.o $(BUILD_DIR)/utils.o $(BUILD_DIR)/hashtable.o $(BUILD_DIR)/relations.o -o $@

hashtable_tests: hashtable_tests.c
	gcc $(GCC_FLAGS) $@.c $(BUILD_DIR)/$(@:_tests=.o) $(BUILD_DIR)/utils.o -o $@

intermediates_tests: intermediates_tests.c
	gcc $(GCC_FLAGS) $@.c $(BUILD_DIR)/$(@:_tests=.o) $(BUILD_DIR)/relations.o $(BUILD_DIR)/utils.o -o $@

stats_tests: stats_tests.c
	gcc $(GCC_FLAGS) $@.c $(BUILD_DIR)/$(@:_tests=.o) $(BUILD_DIR)/relations.o $(BUILD_DIR)/parser.o -o $@ -lm

optimizer_tests: optimizer_tests.c
	gcc $(GCC_FLAGS) $@.c $(BUILD_DIR)/$(@:_tests=.o) $(BUILD_DIR)/relations.o $(BUILD_DIR)/stats.o $(BUILD_DIR)/hashtable.o $(BUILD_DIR)/sort.o $(BUILD_DIR)/utils.o $(BUILD_DIR)/parser.o -o $@ -lm

test_%: %_tests
	@./$^

clean:
	rm -rf *.o $(EXECUTABLE_FILES) 

