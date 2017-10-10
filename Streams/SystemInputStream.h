//
// Created by martin on 10/10/17.
//

#ifndef SPECIALE_SYSTEMINPUTSTREAM_H
#define SPECIALE_SYSTEMINPUTSTREAM_H

#include "InputStream.h"
#include <stdio.h>

class SystemInputStream: public InputStream {
public:
    FILE* file;
    SystemInputStream();
    virtual ~SystemInputStream();
    void open(char* s);
    int readNext();
    bool endOfStream();
    void close();
};


#endif //SPECIALE_SYSTEMINPUTSTREAM_H
