//
// Created by martin on 10/10/17.
//

#ifndef SPECIALE_BUFFEREDOUTPUTSTREAM_H
#define SPECIALE_BUFFEREDOUTPUTSTREAM_H

#include "OutputStream.h"

class BufferedOutputStream: public OutputStream {
public:
    BufferedOutputStream(int bufferSize);
    virtual ~BufferedOutputStream();
    void create(const char* s);
    void write(int* number);
    void close();
};


#endif //SPECIALE_BUFFEREDOUTPUTSTREAM_H
