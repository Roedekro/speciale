//
// Created by martin on 10/10/17.
//

#ifndef SPECIALE_NAIVEOUTPUTSTREAM_H
#define SPECIALE_NAIVEOUTPUTSTREAM_H

#include "OutputStream.h"

class NaiveOutputStream: public OutputStream {
public:
    NaiveOutputStream();
    virtual ~NaiveOutputStream();
    void create(const char* s);
    void write(int* number);
    void close();

};


#endif //SPECIALE_NAIVEOUTPUTSTREAM_H
