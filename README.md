[![C/C++ CI](https://github.com/AnargyrosArg/PartitionedHashJoinProject/actions/workflows/c-cpp.yml/badge.svg?branch=anarg)](https://github.com/AnargyrosArg/PartitionedHashJoinProject/actions/workflows/c-cpp.yml)

# PartitionedHashJoinProject
Υλοποίηση ενός Partitioned hash join αλγόριθμου για το μάθημα "Ανάπτυξη Λογισμικού για Πληροφοριακά Συστήματα" - 2022-2023
- Ιωάννης Μαυροειδής - sdi1900108
- Ιωάννης Αποστολάτος - sdi1900012
- Ανάργυρος Αργυρός - sdi1900014
------
## Report:
https://docs.google.com/document/d/1x_Lit7WWUSuO1zJxo-dPVdW0XdW-UuxbamUebPIFWMY/edit?usp=sharing
------
## Compiling:
To compile project:

    make
------
## Running:
The executable is named ./out and it expects 2 newline separated value files as parameters

    ./out <filename1> <filename2>
eg: ./out relation_a.txt relation_b.txt

------
## Testing:
To run all tests:

    make test

To run a specific test:

    make test_(name)
  
e.g.: make test_hash1

To run valgrind for all tests:

    make test_valgrind

------
