//
// Created by martin on 12/18/17.
//

#ifndef SPECIALE_HEAP_H
#define SPECIALE_HEAP_H


#include "KeyValue.h"

class Heap {
public:
    Heap();
    virtual ~Heap();
    KeyValue* SWOPHeap(KeyValue** a, int n, KeyValue* in);
    void inheap(KeyValue** a, int n, KeyValue* in);
    KeyValue* outheap(KeyValue** a, int n);
    void setheap(KeyValue** a, int n);
};


#endif //SPECIALE_HEAP_H
