//
// Created by martin on 10/31/17.
//

#ifndef SPECIALE_BUFFEREDINTERNALNODE_H
#define SPECIALE_BUFFEREDINTERNALNODE_H

#include <vector>
#include "keyValueTime.h"

class BufferedInternalNode {
public:
    int id;
    int height;
    int nodeSize;
    bool externalChildren;
    //int bufferSize; // Use vectors .size() method instead.
    /*int* keys;
    int* values;
    BufferedInternalNode** children;*/
    std::vector<int>* keys;
    std::vector<int>* values;
    std::vector<BufferedInternalNode*>* children;
    std::vector<int>* leafInfo;
    //std::vector<int>* bufferSizes; // Of children
    std::vector<KeyValueTime>* buffer;
    BufferedInternalNode(int id, int height, int size, bool externalChildren, int bufferSize);
    ~BufferedInternalNode();

};


#endif //SPECIALE_BUFFEREDINTERNALNODE_H
