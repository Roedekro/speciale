//
// Created by martin on 10/30/17.
//

#ifndef SPECIALE_BUFFEREDINTERNALNODE_H
#define SPECIALE_BUFFEREDINTERNALNODE_H


#include "keyValueTime.h"

class BufferedInternalNode {
public:
    int id;
    bool externalChildren;
    int nodeSize; // # of keys
    int height;
    int* keys;
    int* values;
    BufferedInternalNode** children;
    KeyValueTime** buffer;
    BufferedInternalNode(int id, int height, int size, bool externalChildren);
    ~BufferedInternalNode();
};


#endif //SPECIALE_BUFFEREDINTERNALNODE_H
