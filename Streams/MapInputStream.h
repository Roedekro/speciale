//
// Created by martin on 10/10/17.
//

#ifndef SPECIALE_MAPINPUTSTREAM_H
#define SPECIALE_MAPINPUTSTREAM_H

#include "InputStream.h"
#include <stdio.h>

class MapInputStream: public InputStream {
public:
    long iocounter = 0;
    int filedesc;
    int size;
    int * buffer;
    int index;
    int endoffileIndex;
    FILE* file;
    int * map;
    int portionSize;
    int portionIndex;
    int n;
    MapInputStream(int portionSize, int n);
    virtual ~MapInputStream();
    void open(const char* s);
    int readNext();
    bool endOfStream();
    void close();
};


#endif //SPECIALE_MAPINPUTSTREAM_H
