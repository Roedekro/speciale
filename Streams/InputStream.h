//
// Created by martin on 10/10/17.
// Interface for InputStreams.
//

#ifndef SPECIALE_INPUTSTREAM_H
#define SPECIALE_INPUTSTREAM_H


class InputStream {
public:
    virtual void open(const char* s) = 0;
    virtual int readNext() = 0;
    virtual bool endOfStream() = 0;
    virtual void close() = 0;

};


#endif //SPECIALE_INPUTSTREAM_H
