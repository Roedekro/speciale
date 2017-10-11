//
// Created by martin on 10/10/17.
//

#ifndef SPECIALE_SYSTEMINPUTSTREAM_H
#define SPECIALE_SYSTEMINPUTSTREAM_H

#include "InputStream.h"
#include <stdio.h>

class SystemInputStream: public InputStream {
public:
    long iocounter = 0;
    int filedesc;
    int size;
    int * buffer;
    int index;
    int endoffileIndex;
    FILE* file;
    SystemInputStream();
    virtual ~SystemInputStream();
    void open(const char* s);
    int readNext();
    bool endOfStream();
    void close();

};


#endif //SPECIALE_SYSTEMINPUTSTREAM_H
