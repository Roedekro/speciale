//
// Created by martin on 10/10/17.
// Based on work from IOAlgorithm course.
//

#include "BufferedInputStream.h"
#include <fcntl.h>
#include <sys/types.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <iostream>


BufferedInputStream::BufferedInputStream(int bufferSize) {
    index = 2000000000;
    buffer = new int[bufferSize/sizeof(int)];
    size = bufferSize/sizeof(int);
}

BufferedInputStream::~BufferedInputStream() {
    // TODO Auto-generated destructor stub
}

void BufferedInputStream::open(const char* s) {
    filedesc = ::open(s, O_RDONLY);
    if(filedesc == -1) {
        std::cout << s << "\n";
        perror("Error opening the file in BufferedInputStream ");
        exit(EXIT_FAILURE);
    }
}

int BufferedInputStream::readNext() {

    if(index >= size) {
        int bytesRead = ::read(filedesc, buffer, size*sizeof(int));
        endoffileIndex = bytesRead / sizeof(int);
        index = 0;
        iocounter++;
        read = true;
    }
    int elm = buffer[index];
    index++;
    return elm;
}

bool BufferedInputStream::endOfStream() {
    if(!read) {
        return false;
    }
    if (index == size) { // Se næste blok
        bool b;
        int val;
        int bytesRead = ::read(filedesc, &val, sizeof(int));
        if (bytesRead == 0) b = true;
        lseek(filedesc, -bytesRead, SEEK_CUR);
        return b;
    }
    else { // Nuværende blok
        if(endoffileIndex == size)     {
            return false;
        }
        else if(index < endoffileIndex) { // Er allerede +1
            return false;
        }
        else return true;
    }
}

void BufferedInputStream::close() {
    ::close(filedesc);
}