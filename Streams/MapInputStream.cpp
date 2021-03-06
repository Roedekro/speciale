//
// Created by martin on 10/10/17.
//

#include "MapInputStream.h"
#include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>

MapInputStream::MapInputStream(int portionSize, int n) {
    this->n = n;
    index = 0;
    filedesc = -1;
    size = 0;
    /*int temp = portionSize / getpagesize();
    if(temp == 0) {
        this->portionSize = getpagesize();
    }
    else {
        this->portionSize = temp * getpagesize();
    }*/
    this->portionSize = portionSize;
    portionIndex = 0;
}

MapInputStream::~MapInputStream() {
    // TODO Auto-generated destructor stub
}

void MapInputStream::open(const char* s) {
    file = fopen(s, "r+");
    if(file == NULL) {
        perror("File is NULL");
    }
    //filedesc = fileno(file);
    filedesc = ::open(s, O_RDWR);
    fseek(file, 0, SEEK_END);
    size = ftell(file);
    map = (int *) mmap(0, portionSize*sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, filedesc, 0);
    if (map == MAP_FAILED) {
        perror("Error mmapping the file");
        exit(EXIT_FAILURE);
    }
}

int MapInputStream::readNext() {
    if (index == portionSize / sizeof(int)) {
        munmap(map, portionSize);
        portionIndex++;
        if(n >= portionSize*(portionIndex/sizeof(int))) {
            map = (int *) mmap(0, portionSize, PROT_READ | PROT_WRITE, MAP_SHARED, filedesc, portionIndex*portionSize);
        }
        else {
            long original = portionIndex*portionSize;
            portionSize = (n % (portionSize/sizeof(int)))*sizeof(int);
            map = (int *) mmap(0, portionSize, PROT_READ | PROT_WRITE, MAP_SHARED, filedesc, original);
        }
        index = 0;
        iocounter++;
    }

    int elm = map[index];
    index++;
    return elm;
}

bool MapInputStream::endOfStream() {
    return (index >= size / sizeof(int));
}

void MapInputStream::close() {
    fclose(file);
    munmap(map, portionSize);
}