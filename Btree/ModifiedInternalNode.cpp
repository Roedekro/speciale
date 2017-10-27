//
// Created by martin on 10/26/17.
//

#include "ModifiedInternalNode.h"

ModifiedInternalNode::ModifiedInternalNode(int id, int height, int size, bool externalChildren) {
    this->id = id;
    this->externalChildren = false; // False for most cases, otherwise change
    this->height = height;
    this-> nodeSize = 0;
    this->keys = new int[2*size-1];
    if(externalChildren) {
        this->values = new int[2*size];
    }
    else {
        this->children = new ModifiedInternalNode*[2*size];
    }

}

ModifiedInternalNode::~ModifiedInternalNode() {
    // Removed because writeNode can delete these.
    //delete[] keys;
    //delete[] values;
}

