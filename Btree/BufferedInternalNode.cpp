//
// Created by martin on 10/30/17.
//

#include "BufferedInternalNode.h"

BufferedInternalNode::BufferedInternalNode(int id, int height, int size, bool externalChildren) {
    this->id = id;
    this->externalChildren = false; // False for most cases, otherwise change
    this->height = height;
    this-> nodeSize = 0;
    this->keys = new int[4*size-1];
    if(externalChildren) {
        this->values = new int[4*size];
    }
    else {
        this->children = new BufferedInternalNode*[4*size];
    }

}

BufferedInternalNode::~BufferedInternalNode() {

}