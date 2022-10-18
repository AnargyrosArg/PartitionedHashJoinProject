BUILD_DIR = ./build
GCC_FLAGS = -I ./include/ -Wall
OBJ_FILES = main.o hash1.o partition.o

main: $(OBJ_FILES)
	gcc $(GCC_FLAGS) $(BUILD_DIR)/*.o -o out
 
hash1.o: hash1.c
	gcc $(GCC_FLAGS) -c hash1.c -o $(BUILD_DIR)/hash1.o

partition.o: partition.c
	gcc $(GCC_FLAGS) -c partition.c -o $(BUILD_DIR)/partition.o

main.o: main.c
	gcc $(GCC_FLAGS) -c main.c -o $(BUILD_DIR)/main.o


clean:
	rm -rf $(BUILD_DIR)/* ./out