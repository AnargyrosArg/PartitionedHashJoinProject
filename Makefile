BUILD_DIR = ./build
TEST_DIR = ./tests
SRC_DIR = ./src
INCL_DIR = ./include 
GCC_FLAGS = -I$(INCL_DIR) -Wall -O2
SOURCE_FILES = main.c hash1.c partition.c utils.c hashtable.c join.c relations.c parser.c intermediates.c filter.c execqueries.c optimizer.c
HARNESS_SRC = harness.cpp
OBJ_FILES = $(addprefix $(BUILD_DIR)/,$(SOURCE_FILES:.c=.o))


.PHONY: clean clean_tests


#Default rule, makes executable
out: $(BUILD_DIR) $(OBJ_FILES) $(INCL_DIR)/* harness
	gcc $(GCC_FLAGS) $(BUILD_DIR)/*.o -o out

harness: $(SRC_DIR)/$(HARNESS_SRC)
	g++ $(SRC_DIR)/$(HARNESS_SRC) -o $@

#Compiles each source file into its object file individually 
$(BUILD_DIR)/%.o : $(SRC_DIR)/%.c
	gcc $(GCC_FLAGS) -c $< -o $@

#Rule to run all tests , there is a separate Makefile in the tests directory that we simply run 
test: $(OBJ_FILES)  $(TEST_DIR)
	cd $(TEST_DIR) && make

test_valgrind: $(OBJ_FILES)  $(TEST_DIR)
	cd $(TEST_DIR) && make run_tests_valgrind

#Rule to run individual test , eg: make test_hash1
test_%: $(BUILD_DIR)/%.o $(TEST_DIR)
	cd $(TEST_DIR) && make $@

#Rule to make sure build directory exists
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

clean: clean_tests
	rm -rf $(BUILD_DIR)/* ./out ./harness ./gmon.out ./profile.txt ./profile_exec

clean_tests:
	@cd $(TEST_DIR) && make clean


profile: $(BUILD_DIR) $(OBJ_FILES)
	gcc $(GCC_FLAGS) -g -pg $(BUILD_DIR)/*.o -o profile_exec
	./profile_exec < test_input.txt
	gprof -l ./profile_exec gmon.out > profile.txt
	rm -rf ./gmon.out ./profile_exec
