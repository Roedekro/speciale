//
// Created by martin on 10/10/17.
//

#ifndef SPECIALE_OUTPUTSTREAM_H
#define SPECIALE_OUTPUTSTREAM_H


class OutputStream {
public:
    long iocounter = 0;
    int index;
    int * buffer;
    int size;
    int filedesc;
    const char * name;
    virtual void create(const char* s) = 0;
    virtual void write(int* number) = 0;
    virtual void close() = 0;
};


#endif //SPECIALE_OUTPUTSTREAM_H
