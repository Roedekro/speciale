//
// Created by martin on 10/10/17.
// Interface for InputStreams.
//

#ifndef SPECIALE_INPUTSTREAM_H
#define SPECIALE_INPUTSTREAM_H


class InputStream {
public:
    long iocounter = 0;
    int filedesc;
    int size;
    int * buffer;
    int index;
    int endoffileIndex;
    virtual void open(const char* s);
    virtual int readNext();
    virtual bool endOfStream();
    virtual void close();

};


#endif //SPECIALE_INPUTSTREAM_H
