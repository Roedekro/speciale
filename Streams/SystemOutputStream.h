//
// Created by martin on 10/10/17.
//

#ifndef SPECIALE_SYSTEMOUTPUTSTREAM_H
#define SPECIALE_SYSTEMOUTPUTSTREAM_H

#include "OutputStream.h"
#include <stdio.h>

class SystemOutputStream: public OutputStream {
public:
    FILE* file;
    SystemOutputStream();
    virtual ~SystemOutputStream();
    void create(char* s);
    void write(int* number);
    void close();
};
};


#endif //SPECIALE_SYSTEMOUTPUTSTREAM_H
