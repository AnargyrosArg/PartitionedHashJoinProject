BUILD_DIR = ./build
GCC_FLAGS = -I ./include/ -Wall
SOURCE_FILES = main.c hash1.c partition.c utils.c hashtable.c
OBJ_FILES = $(addprefix $(BUILD_DIR)/,$(SOURCE_FILES:.c=.o))

out: $(BUILD_DIR) $(OBJ_FILES)
	gcc $(GCC_FLAGS) $(BUILD_DIR)/*.o -o out

$(BUILD_DIR)/%.o : %.c
	gcc $(GCC_FLAGS) -c $< -o $@

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

clean:
	rm -rf $(BUILD_DIR)/* ./out