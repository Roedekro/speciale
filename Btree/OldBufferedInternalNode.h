//
// Created by martin on 10/30/17.
//

#ifndef SPECIALE_BUFFEREDINTERNALNODE_H
#define SPECIALE_BUFFEREDINTERNALNODE_H


#include "keyValueTime.h"

class OldBufferedInternalNode {
public:
    int id;
    bool externalChildren;
    int nodeSize; // # of keys
    int bufferSize;
    int height;
    int* keys;
    int* values;
    OldBufferedInternalNode** children;
    KeyValueTime** buffer;
    OldBufferedInternalNode(int id, int height, int size, bool externalChildren, int bufferSize);
    ~OldBufferedInternalNode();
};


#endif //SPECIALE_BUFFEREDINTERNALNODE_H
