//
// Created by martin on 10/10/17.
// Uses the OS buffered stream fread.
//

#include "SystemInputStream.h"

SystemInputStream::SystemInputStream() {

}

SystemInputStream::~SystemInputStream() {
    // TODO Auto-generated destructor stub
}

void SystemInputStream::open(const char* s) {
    file = fopen(s, "rb");
}

int SystemInputStream::readNext() {
    int ret;
    int bytesRead = fread(&ret , sizeof(int), 1, file);
    iocounter++;
    return bytesRead > 0 ? ret : bytesRead;
}

bool SystemInputStream::endOfStream() {
    bool b = false;
    int val;
    int bytesRead = fread(&val , sizeof(int), 1, file) * sizeof(int);
    if (bytesRead == 0) b = true;
    fseek(file, -bytesRead, SEEK_CUR);
    return b;
}

void SystemInputStream::close() {
    fclose(file);
}