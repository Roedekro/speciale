//
// Created by martin on 10/10/17.
// Naively uses up to one I/O pr. read.
// Might get buffered by OS, might not.
//

#include "NaiveInputStream.h"
#include <fcntl.h>
#include <sys/types.h>
#include <stdio.h>
#include <unistd.h>

NaiveInputStream::NaiveInputStream() {
    filedesc = -1;
}

NaiveInputStream::~NaiveInputStream() {
    // TODO Auto-generated destructor stub
}

void NaiveInputStream::open(char* s) {
    filedesc = ::open(s, O_RDONLY);
}

int NaiveInputStream::readNext() {
    int ret;
    int bytesRead = ::read(filedesc, &ret , sizeof(int));
    iocounter++;
    return bytesRead > 0 ? ret : bytesRead;
}

bool NaiveInputStream::endOfStream() {
    bool b = false;
    int val;
    int bytesRead = ::read(filedesc, &val, sizeof(int));
    if (bytesRead == 0) b = true;
    lseek(filedesc, -bytesRead, SEEK_CUR);
    return b;
}

void NaiveInputStream::close() {
    ::close(filedesc);
}