//
// Created by martin on 10/10/17.
//

#include "SystemOutputStream.h"

SystemOutputStream::SystemOutputStream() {

}

SystemOutputStream::~SystemOutputStream() {
    // TODO Auto-generated destructor stub
}

void SystemOutputStream::create(const char* s) {
    file = fopen(s, "wb");
    if(file == NULL) {
        perror("File SystemOutputStream");
        //exit(EXIT_FAILURE);
    }
}

void SystemOutputStream::write(int* number) {
    fwrite(number, sizeof(int), 1, file);
    iocounter++;
}

void SystemOutputStream::close() {
    fclose(file);
    iocounter++;
}