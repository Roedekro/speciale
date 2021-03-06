//
// Created by martin on 10/10/17.
//

#ifndef SPECIALE_NAIVEINPUTSTREAM_H
#define SPECIALE_NAIVEINPUTSTREAM_H

#include "InputStream.h"

class NaiveInputStream: public InputStream {
public:
    int filedesc;
    NaiveInputStream();
    virtual ~NaiveInputStream();
    void open(const char* s);
    int readNext();
    bool endOfStream();
    void close();

};


#endif //SPECIALE_NAIVEINPUTSTREAM_H
