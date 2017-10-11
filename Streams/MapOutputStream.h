//
// Created by martin on 10/10/17.
//

#ifndef SPECIALE_MAPOUTPUTSTREAM_H
#define SPECIALE_MAPOUTPUTSTREAM_H

#include "OutputStream.h"

class MapOutputStream: public OutputStream {
public:
    int portionSize;
    int portionIndex;
    int *map;
    int n;
    MapOutputStream(int portionSize, int n);
    virtual ~MapOutputStream();
    void create(const char* s);
    void write(int* number);
    void close();
};


#endif //SPECIALE_MAPOUTPUTSTREAM_H
