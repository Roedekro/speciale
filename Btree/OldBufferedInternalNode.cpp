//
// Created by martin on 10/30/17.
//

#include "OldBufferedInternalNode.h"

OldBufferedInternalNode::OldBufferedInternalNode(int id, int height, int size, bool externalChildren, int bufferSize) {
    this->id = id;
    this->externalChildren = false; // False for most cases, otherwise change
    this->height = height;
    this-> nodeSize = 0;
    this->bufferSize = bufferSize;
    this->keys = new int[4*size-1];
    if(externalChildren) {
        this->values = new int[4*size];
    }
    else {
        this->children = new OldBufferedInternalNode*[4*size];
    }
    this->buffer = new KeyValueTime*[bufferSize];
}

OldBufferedInternalNode::~OldBufferedInternalNode() {

}