//
// Created by martin on 10/10/17.
// Buffered implementation of InputStream interface
//

#ifndef SPECIALE_BUFFEREDINPUTSTREAM_H
#define SPECIALE_BUFFEREDINPUTSTREAM_H

#include "InputStream.h"

class BufferedInputStream: public InputStream {
public:
    long iocounter = 0;
    int filedesc;
    int size;
    int * buffer;
    int index;
    int endoffileIndex;
    bool read = false;
    BufferedInputStream(int bufferSize);
    virtual ~BufferedInputStream();
    void open(const char* s);
    int readNext();
    bool endOfStream();
    void close();
};


#endif //SPECIALE_BUFFEREDINPUTSTREAM_H
