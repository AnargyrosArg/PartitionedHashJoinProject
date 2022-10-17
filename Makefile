BUILD_DIR = ./build
GCC_FLAGS = -I ./headers/

main: main.o hash1.o
	gcc $(GCC_FLAGS) $(BUILD_DIR)/*.o -o out
 
main.o: main.c
	gcc $(GCC_FLAGS) -c main.c -o $(BUILD_DIR)/main.o

hash1.o: hash1.c
	gcc $(GCC_FLAGS) -c hash1.c -o $(BUILD_DIR)/hash1.o

clean:
	rm -rf $(BUILD_DIR)/* ./out