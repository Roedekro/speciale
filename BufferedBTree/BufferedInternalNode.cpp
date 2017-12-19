//
// Created by martin on 10/31/17.
//

#include "BufferedInternalNode.h"

BufferedInternalNode::BufferedInternalNode(int id, int height, int size, bool externalChildren, int bufferSize) {
    this->id = id;
    this->height = height;
    this-> nodeSize = 0;
    this->externalChildren = externalChildren;

    this->keys = new std::vector<int>();
    keys->reserve(4*size-1);
    if(externalChildren) {
        this->values = new std::vector<int>();
        values->reserve(4*size);
    }
    else {
        this->children = new std::vector<BufferedInternalNode*>();
        children->reserve(4*size);
    }
    this->buffer = new std::vector<KeyValueTime>();
    buffer->reserve(bufferSize);
}

BufferedInternalNode::~BufferedInternalNode() {
    delete(keys);
    if(externalChildren){
        delete(values);
    }
    else {
        delete(children);
    }
    delete(buffer);
}