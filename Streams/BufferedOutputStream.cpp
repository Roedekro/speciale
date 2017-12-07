//
// Created by martin on 10/10/17.
//

#include "BufferedOutputStream.h"
#include <fcntl.h>
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
    filedesc = ::open(s, O_CREAT|O_RDWR, 0777);
    name = s;
}

// Will append rather than overwrite.
void BufferedOutputStream::open(const char* s) {
    filedesc = ::open(s, O_CREAT|O_RDWR|O_APPEND, 0777);
    //filedesc = ::open(s, O_RDWR|O_APPEND);
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
    delete [] buffer;
    chmod(name, 0777); // To be sure we have access later
    index = 0;
}