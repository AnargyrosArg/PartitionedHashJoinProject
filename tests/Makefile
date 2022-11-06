define \n


endef
#Test source files MUST be named <source_test_is_for>_test.c or the compiling wont work
SOURCE_FILES = $(wildcard *.c)
OBJ_FILES = $(SOURCE_FILES:.c=.o)
EXECUTABLE_FILES = $(SOURCE_FILES:.c= )
TESTS = $(addsuffix _tests,$(EXECUTABLE_FILES:_tests= ))
GCC_FLAGS = -I../include
BUILD_DIR = ../build

.PHONY:	clean run_tests

run_tests: $(TESTS)
	$(foreach test,$(TESTS),@echo ==========$(test)==========$(\n)@./$(test)$(\n))
	@echo ====================================

%: %.c
	gcc $(GCC_FLAGS) $@.c $(BUILD_DIR)/$(@:_tests=.o) -o $@

partition_tests: partition_tests.c
	gcc $(GCC_FLAGS) $@.c $(BUILD_DIR)/$(@:_tests=.o) $(BUILD_DIR)/utils.o $(BUILD_DIR)/hash1.o   -o $@

join_tests: join_tests.c
	gcc $(GCC_FLAGS) $@.c $(BUILD_DIR)/$(@:_tests=.o) $(BUILD_DIR)/hash1.o $(BUILD_DIR)/partition.o $(BUILD_DIR)/utils.o $(BUILD_DIR)/hashtable.o $(BUILD_DIR)/relations.o -o $@

hashtable_tests: hashtable_tests.c
	gcc $(GCC_FLAGS) $@.c $(BUILD_DIR)/$(@:_tests=.o) $(BUILD_DIR)/utils.o -o $@

test_%: %_tests
	@./$^

clean:
	rm -rf *.o $(EXECUTABLE_FILES) 

