//
// Created by martin on 10/10/17.
// Naively uses up to one I/O pr. write.
//

#include "NaiveOutputStream.h"
#include <fcntl.h>
#include <sys/types.h>
#include <stdio.h>
#include <unistd.h>

NaiveOutputStream::NaiveOutputStream() {
    filedesc = -1;
}

NaiveOutputStream::~NaiveOutputStream() {
    // TODO Auto-generated destructor stub
}
void NaiveOutputStream::create(const char* s) {
    filedesc = ::open(s, O_CREAT|O_RDWR);
}

void NaiveOutputStream::write(int* number) {
    ::write(filedesc, number, sizeof(int));
    iocounter++;
}

void NaiveOutputStream::close() {
    ::close(filedesc);
}