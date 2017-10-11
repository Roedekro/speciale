//
// Created by martin on 10/10/17.
// Based on work from IOAlgorithm course.
//

#include "BufferedOutputStream.h"
#include <fcntl.h>
#include <sys/types.h>
#include <stdio.h>
#include <unistd.h>
#include <initializer_list>
#include <iostream>
#include <sys/stat.h>

BufferedOutputStream::BufferedOutputStream(int bufferSize) {
    index = 0;
    buffer = new int[bufferSize/sizeof(int)];
    size = bufferSize/sizeof(int);
}

BufferedOutputStream::~BufferedOutputStream() {
    // TODO Auto-generated destructor stub
}

void BufferedOutputStream::create(const char* s) {
    filedesc = ::open(s, O_CREAT|O_RDWR);
    name = s;
}

void BufferedOutputStream::write(int* number) {
    if (index >= size) {
        int tmp = ::write(filedesc, buffer, sizeof(int)*size);
        if(tmp == -1) {
            std::cout << "Error writing to file in BufferedOutputStream " << name << "\n";
        }
        index = 0;
        iocounter++;
    }
    buffer[index] = *number;
    index++;
}

void BufferedOutputStream::close() {
    if (index > 0) {
        ::write(filedesc, buffer, index * sizeof(int));
        iocounter++;
    }

    int tmp = ::close(filedesc);
    if(tmp == -1) std::cout << "Error closing file in BufferedOutputStream " << name << "\n";
    delete(buffer);
    chmod(name, 0666);
    index = 0;
}