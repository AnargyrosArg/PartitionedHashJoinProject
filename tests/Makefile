GCC_FLAGS = -I../ -I../include

#Test source files MUST be named <source_test_is_for>_test.c
#or the compiling wont work
#To add a test source file append it to list below
SOURCE_FILES = $(wildcard *.c)
OBJ_FILES = $(SOURCE_FILES:.c=.o)
EXECUTABLE_FILES = $(SOURCE_FILES:.c= )
TESTS = $(addprefix test_,$(EXECUTABLE_FILES:_tests= ))
BUILD_DIR = ../build

.PHONY:	clean run_tests

run_tests: $(TESTS)

%: %.c
	gcc $(GCC_FLAGS) $@.c $(BUILD_DIR)/$(@:_tests=.o) -o $@  

partition_tests: partition_tests.c
	gcc $(GCC_FLAGS) $@.c $(BUILD_DIR)/$(@:_tests=.o) $(BUILD_DIR)/utils.o $(BUILD_DIR)/hash1.o   -o $@  

test_%: %_tests
	@./$<

clean:
	rm -rf *.o $(EXECUTABLE_FILES) 

